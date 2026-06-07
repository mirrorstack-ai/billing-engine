package webhook

import (
	"context"
	"errors"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// NewStore returns a Store backed by the given pgxpool. Used by
// cmd/account-webhook/main.go to wire the webhook Router.
func NewStore(pool *pgxpool.Pool) Store {
	if pool == nil {
		panic("webhook.NewStore: pool must not be nil")
	}
	return &pgxStore{pool: pool}
}

type pgxStore struct {
	pool *pgxpool.Pool
}

// MarkEventProcessed inserts the event_id into webhook_events_processed
// with ON CONFLICT DO NOTHING. Returns (firstTime, error):
//   - firstTime=true  → row was inserted; caller should run side effects.
//   - firstTime=false → row already existed; caller should short-circuit.
//
// The check is done via pgx.CommandTag.RowsAffected rather than a
// SELECT-then-INSERT race: one round-trip, race-free by virtue of
// Postgres's atomic INSERT semantics.
func (s *pgxStore) MarkEventProcessed(ctx context.Context, eventID, eventType string) (bool, error) {
	const q = `
		INSERT INTO ms_billing.webhook_events_processed (event_id, event_type)
		VALUES ($1, $2)
		ON CONFLICT (event_id) DO NOTHING
	`
	tag, err := s.pool.Exec(ctx, q, eventID, eventType)
	if err != nil {
		return false, err
	}
	return tag.RowsAffected() == 1, nil
}

// TouchAccountByStripeCustomer updates accounts.updated_at for the
// row matching stripeCustomerID. Returns (found, error). The trigger
// installed by migration 001 maintains updated_at, but the explicit
// SET is more discoverable than relying on a no-op UPDATE to fire it.
func (s *pgxStore) TouchAccountByStripeCustomer(ctx context.Context, stripeCustomerID string) (bool, error) {
	const q = `UPDATE ms_billing.accounts SET updated_at = now() WHERE stripe_customer_id = $1`
	tag, err := s.pool.Exec(ctx, q, stripeCustomerID)
	if err != nil {
		return false, err
	}
	return tag.RowsAffected() > 0, nil
}

// SetDefaultPaymentMethod marks one PM as default and unmarks all
// others for the same account in a single UPDATE: the new is_default
// value per row is computed by column-comparison against the new
// default's Stripe PM ID. Empty defaultStripePMID clears the flag
// everywhere (no PM is default).
//
// account_id is resolved inline via the stripe_customer_id lookup;
// no separate fetch.
func (s *pgxStore) SetDefaultPaymentMethod(ctx context.Context, stripeCustomerID, defaultStripePMID string) error {
	const q = `
		UPDATE ms_billing.payment_methods_mirror
		SET is_default = (stripe_payment_method_id = $2)
		WHERE account_id = (
			SELECT id FROM ms_billing.accounts WHERE stripe_customer_id = $1
		)
		AND deleted_at IS NULL
	`
	_, err := s.pool.Exec(ctx, q, stripeCustomerID, defaultStripePMID)
	return err
}

// InsertPaymentMethod inserts a row into payment_methods_mirror after
// resolving account_id from stripeCustomerID. Returns (found, error):
//   - found=false signals Stripe→DB drift (no accounts row for this
//     customer); webhook handler converts to drift_warning response.
//   - found=true on either successful insert OR ON CONFLICT no-op
//     (idempotent retry; mirror already in the right state).
func (s *pgxStore) InsertPaymentMethod(ctx context.Context, stripeCustomerID string, pm InsertPaymentMethodParams) (bool, error) {
	const q = `
		WITH acct AS (
			SELECT id FROM ms_billing.accounts WHERE stripe_customer_id = $1
		)
		INSERT INTO ms_billing.payment_methods_mirror
			(account_id, stripe_payment_method_id, brand, last4, exp_month, exp_year, is_default, fingerprint)
		SELECT acct.id, $2, $3, $4, $5, $6,
			-- First active card for the account becomes the default, so the
			-- user always has a usable default without an explicit choice.
			NOT EXISTS (
				SELECT 1 FROM ms_billing.payment_methods_mirror p
				WHERE p.account_id = acct.id AND p.deleted_at IS NULL
			),
			NULLIF($7, '')
		FROM acct
		-- Dedupe by card identity: re-binding the same card mints a fresh
		-- Stripe PM id, so the (stripe_payment_method_id) unique constraint
		-- doesn't catch it. Skip when an active row already has the same
		-- brand/last4/exp on this account so the user doesn't end up with
		-- duplicate rows. (Best-effort — two physical cards with identical
		-- brand/last4/exp would collide; fingerprint-based dedupe is a
		-- follow-up.)
		WHERE NOT EXISTS (
			SELECT 1 FROM ms_billing.payment_methods_mirror p2
			WHERE p2.account_id = acct.id
			  AND p2.deleted_at IS NULL
			  AND p2.brand = $3
			  AND p2.last4 = $4
			  AND p2.exp_month = $5
			  AND p2.exp_year = $6
		)
		ON CONFLICT (stripe_payment_method_id) DO NOTHING
	`
	tag, err := s.pool.Exec(ctx, q,
		stripeCustomerID,
		pm.StripePaymentMethodID,
		pm.Brand,
		pm.Last4,
		pm.ExpMonth,
		pm.ExpYear,
		pm.Fingerprint,
	)
	if err != nil {
		return false, err
	}
	if tag.RowsAffected() == 1 {
		return true, nil
	}
	// 0 rows: either drift (no accounts row) or ON CONFLICT (PM already
	// mirrored). Disambiguate; the drift case needs a different status.
	var exists bool
	const checkQ = `SELECT EXISTS (SELECT 1 FROM ms_billing.accounts WHERE stripe_customer_id = $1)`
	if err := s.pool.QueryRow(ctx, checkQ, stripeCustomerID).Scan(&exists); err != nil {
		return false, err
	}
	return exists, nil
}

// SoftDeletePaymentMethod sets deleted_at=now() on the row matching
// stripePaymentMethodID. Returns (found, error). found=false is an
// idempotent no-op (the PM was never mirrored, or was already soft-
// deleted in a prior call).
func (s *pgxStore) SoftDeletePaymentMethod(ctx context.Context, stripePaymentMethodID string) (bool, error) {
	const q = `
		UPDATE ms_billing.payment_methods_mirror
		SET deleted_at = now()
		WHERE stripe_payment_method_id = $1 AND deleted_at IS NULL
	`
	tag, err := s.pool.Exec(ctx, q, stripePaymentMethodID)
	if err != nil {
		return false, err
	}
	return tag.RowsAffected() > 0, nil
}

// SetAddCardRequestStripePM stamps the resolved Stripe payment_method
// id onto a still-pending add_card_requests row. Matched by the row's
// setup_intent_id; the partial index acr_setup_intent_pending_idx
// (migration 004) covers this query. No-op when the row is no longer
// pending — the other resolution handler has already finalized it.
func (s *pgxStore) SetAddCardRequestStripePM(ctx context.Context, setupIntentID, stripePaymentMethodID string) error {
	const q = `
		UPDATE ms_billing.add_card_requests
		SET stripe_pm_id = $2
		WHERE setup_intent_id = $1 AND status = 'pending'
	`
	_, err := s.pool.Exec(ctx, q, setupIntentID, stripePaymentMethodID)
	return err
}

// ResolvePendingAddCardRequest is the terminal step of the add-card
// flow. Runs in a single transaction:
//
//  1. Look up the just-mirrored row by stripe_payment_method_id. Returns
//     no-op when the row hasn't been inserted yet (event ordering: the
//     other webhook handler will resolve once both rows exist).
//  2. Probe for ANOTHER active mirror row on the same account with the
//     same Stripe card fingerprint. Hit → 'duplicate'; miss → 'completed'.
//     Stripe issues a fresh pm_* ID per setup_intent confirm even when
//     the customer enters the same card, so stripe_payment_method_id
//     equality (the previous predicate) never fired in practice;
//     fingerprint equality is the canonical "same card" check.
//  3. On duplicate, soft-delete the just-mirrored row so the UI doesn't
//     show two rows that share brand+last4. The duplicate PM stays
//     attached on Stripe's side until the future reconciliation job
//     detaches it — orphan PMs are harmless (no auto-charge).
//  4. Resolve the request row in one UPDATE, pointing payment_method_id
//     at the surviving row (the pre-existing card on duplicate, the
//     just-mirrored row on completed).
//
// Idempotent: the WHERE status = 'pending' filter on the final UPDATE
// is a no-op for rows another event already finalized.
func (s *pgxStore) ResolvePendingAddCardRequest(ctx context.Context, stripePaymentMethodID string) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	// Step 1: lookup the just-mirrored row. ErrNoRows means the other
	// webhook event (handlePaymentMethodAttached) hasn't run yet — bail
	// out cleanly; the partner handler will re-enter this function once
	// the mirror row lands.
	var (
		newID       uuid.UUID
		accountID   uuid.UUID
		fingerprint *string
	)
	const lookupQ = `
		SELECT id, account_id, fingerprint
		FROM ms_billing.payment_methods_mirror
		WHERE stripe_payment_method_id = $1
	`
	if err := tx.QueryRow(ctx, lookupQ, stripePaymentMethodID).
		Scan(&newID, &accountID, &fingerprint); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil
		}
		return err
	}

	// Step 2: dedupe probe. Skip when fingerprint is unavailable
	// (legacy rows pre-migration 005, or non-card Stripe PMs). The
	// partial index pmm_account_fingerprint_active_idx already filters
	// out NULL fingerprints, but the explicit Go guard keeps the empty
	// case from emitting a needless query.
	var duplicatePMID uuid.UUID
	hasDuplicate := false
	if fingerprint != nil && *fingerprint != "" {
		const dupQ = `
			SELECT id
			FROM ms_billing.payment_methods_mirror
			WHERE account_id = $1
			  AND fingerprint = $2
			  AND id <> $3
			  AND deleted_at IS NULL
			LIMIT 1
		`
		err := tx.QueryRow(ctx, dupQ, accountID, *fingerprint, newID).Scan(&duplicatePMID)
		if err != nil && !errors.Is(err, pgx.ErrNoRows) {
			return err
		}
		hasDuplicate = err == nil
	}

	// Step 3: when a pre-existing card already covers this fingerprint,
	// soft-delete the freshly-mirrored row so the UI returns to a single
	// canonical card per (account, fingerprint).
	resolvedPMID := newID
	resolvedStatus := "completed"
	if hasDuplicate {
		resolvedPMID = duplicatePMID
		resolvedStatus = "duplicate"
		const softDeleteQ = `
			UPDATE ms_billing.payment_methods_mirror
			SET deleted_at = now()
			WHERE id = $1 AND deleted_at IS NULL
		`
		if _, err := tx.Exec(ctx, softDeleteQ, newID); err != nil {
			return err
		}
	}

	// Step 4: terminal resolve. WHERE status='pending' makes this safe to
	// re-run after the partner handler already finalized the row.
	const resolveQ = `
		UPDATE ms_billing.add_card_requests
		SET status = $2::ms_billing.add_card_request_status,
		    payment_method_id = $3,
		    resolved_at = now()
		WHERE stripe_pm_id = $1 AND status = 'pending'
	`
	if _, err := tx.Exec(ctx, resolveQ, stripePaymentMethodID, resolvedStatus, resolvedPMID); err != nil {
		return err
	}

	return tx.Commit(ctx)
}

// Compile-time interface check: pgxStore must satisfy Store.
var _ Store = (*pgxStore)(nil)
