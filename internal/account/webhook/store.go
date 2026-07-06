package webhook

import (
	"context"
	"errors"
	"strconv"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/mirrorstack-ai/billing-engine/internal/account/db"
)

// NewStore returns a Store backed by the given pgxpool. Used by
// cmd/account-webhook/main.go to wire the webhook Router.
func NewStore(pool *pgxpool.Pool) Store {
	if pool == nil {
		panic("webhook.NewStore: pool must not be nil")
	}
	return &pgxStore{pool: pool, q: db.New(pool)}
}

type pgxStore struct {
	pool *pgxpool.Pool
	q    *db.Queries
}

// MarkEventProcessed inserts the event_id into webhook_events_processed
// with ON CONFLICT DO NOTHING. Returns (firstTime, error):
//   - firstTime=true  → row was inserted; caller should run side effects.
//   - firstTime=false → row already existed; caller should short-circuit.
//
// The check is done via RowsAffected rather than a SELECT-then-INSERT
// race: one round-trip, race-free by virtue of Postgres's atomic
// INSERT semantics.
func (s *pgxStore) MarkEventProcessed(ctx context.Context, eventID, eventType string) (bool, error) {
	rows, err := s.q.MarkEventProcessed(ctx, db.MarkEventProcessedParams{
		EventID:   eventID,
		EventType: eventType,
	})
	if err != nil {
		return false, err
	}
	return rows == 1, nil
}

// UnmarkEventProcessed deletes the idempotency row so a 5xx-failed event is
// re-run on Stripe's redelivery (see the query/interface docs).
func (s *pgxStore) UnmarkEventProcessed(ctx context.Context, eventID string) error {
	return s.q.UnmarkEventProcessed(ctx, eventID)
}

// TouchAccountByStripeCustomer updates accounts.updated_at for the
// row matching stripeCustomerID. Returns (found, error). The trigger
// installed by migration 001 maintains updated_at, but the explicit
// SET is more discoverable than relying on a no-op UPDATE to fire it.
func (s *pgxStore) TouchAccountByStripeCustomer(ctx context.Context, stripeCustomerID string) (bool, error) {
	rows, err := s.q.TouchAccountByStripeCustomer(ctx, text(stripeCustomerID))
	if err != nil {
		return false, err
	}
	return rows > 0, nil
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
	return s.q.SetDefaultPaymentMethodByCustomer(ctx, db.SetDefaultPaymentMethodByCustomerParams{
		StripeCustomerID:      text(stripeCustomerID),
		StripePaymentMethodID: defaultStripePMID,
	})
}

// InsertPaymentMethod inserts a row into payment_methods_mirror after
// resolving account_id from stripeCustomerID. Returns (found, error):
//   - found=false signals Stripe→DB drift (no accounts row for this
//     customer); webhook handler converts to drift_warning response.
//   - found=true on either successful insert OR a no-op (ON CONFLICT,
//     or the insert-time dedupe skip) when the account does exist.
//
// First active card on the account becomes the default; subsequent cards
// insert is_default=false. The INSERT…SELECT also skips when an active
// row already shares brand/last4/exp (best-effort insert-time dedupe);
// fingerprint-equality dedupe is the canonical check, applied later in
// ResolvePendingAddCardRequest.
func (s *pgxStore) InsertPaymentMethod(ctx context.Context, stripeCustomerID string, pm InsertPaymentMethodParams) (bool, error) {
	// Column7 is the NULLIF($7,'') fingerprint argument: the empty string
	// becomes SQL NULL so legacy/non-card PMs don't pollute the dedupe
	// index. sqlc types it as interface{}; pass the raw string.
	rows, err := s.q.InsertPaymentMethod(ctx, db.InsertPaymentMethodParams{
		StripeCustomerID:      text(stripeCustomerID),
		StripePaymentMethodID: pm.StripePaymentMethodID,
		Brand:                 pm.Brand,
		Last4:                 pm.Last4,
		ExpMonth:              int32(pm.ExpMonth),
		ExpYear:               int32(pm.ExpYear),
		Column7:               pm.Fingerprint,
	})
	if err != nil {
		return false, err
	}
	if rows == 1 {
		return true, nil
	}
	// 0 rows: either drift (no accounts row) or a no-op (ON CONFLICT /
	// dedupe skip). Disambiguate; the drift case needs a different status.
	exists, err := s.q.AccountExistsByStripeCustomer(ctx, text(stripeCustomerID))
	if err != nil {
		return false, err
	}
	return exists, nil
}

// StampAccountActivated freezes the account's billing-period anchor (migration
// 025) — the first-card-bind instant — keyed by stripe_customer_id. The query's
// `WHERE activated_at IS NULL` makes it first-bind-wins + idempotent (a re-add or
// a webhook retry updates 0 rows). Returns (firstBind, error): firstBind=true
// only when THIS call set the anchor; 0 rows (already activated, or no accounts
// row for the customer) is a benign no-op the caller logs, never an error.
func (s *pgxStore) StampAccountActivated(ctx context.Context, stripeCustomerID string) (bool, error) {
	rows, err := s.q.StampAccountActivated(ctx, text(stripeCustomerID))
	if err != nil {
		return false, err
	}
	return rows > 0, nil
}

// SoftDeletePaymentMethod sets deleted_at=now() on the row matching
// stripePaymentMethodID. Returns (found, error). found=false is an
// idempotent no-op (the PM was never mirrored, or was already soft-
// deleted in a prior call).
func (s *pgxStore) SoftDeletePaymentMethod(ctx context.Context, stripePaymentMethodID string) (bool, error) {
	rows, err := s.q.SoftDeletePaymentMethod(ctx, stripePaymentMethodID)
	if err != nil {
		return false, err
	}
	return rows > 0, nil
}

// SetAddCardRequestStripePM stamps the resolved Stripe payment_method
// id onto a still-pending add_card_requests row. Matched by the row's
// setup_intent_id; the partial index acr_setup_intent_pending_idx
// (migration 004) covers this query. No-op when the row is no longer
// pending — the other resolution handler has already finalized it.
func (s *pgxStore) SetAddCardRequestStripePM(ctx context.Context, setupIntentID, stripePaymentMethodID string) error {
	return s.q.SetAddCardRequestStripePM(ctx, db.SetAddCardRequestStripePMParams{
		SetupIntentID: text(setupIntentID),
		StripePmID:    text(stripePaymentMethodID),
	})
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
	qtx := s.q.WithTx(tx)

	// Step 1: lookup the just-mirrored row. ErrNoRows means the other
	// webhook event (handlePaymentMethodAttached) hasn't run yet — bail
	// out cleanly; the partner handler will re-enter this function once
	// the mirror row lands.
	mirror, err := qtx.MirrorRowByStripePM(ctx, stripePaymentMethodID)
	if err != nil {
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
	var duplicatePMID string
	hasDuplicate := false
	if mirror.Fingerprint.Valid && mirror.Fingerprint.String != "" {
		duplicatePMID, err = qtx.DuplicateFingerprintPM(ctx, db.DuplicateFingerprintPMParams{
			AccountID:   mirror.AccountID,
			Fingerprint: mirror.Fingerprint,
			ID:          mirror.ID,
		})
		if err != nil && !errors.Is(err, pgx.ErrNoRows) {
			return err
		}
		hasDuplicate = err == nil
	}

	// Step 3: when a pre-existing card already covers this fingerprint,
	// soft-delete the freshly-mirrored row so the UI returns to a single
	// canonical card per (account, fingerprint).
	resolvedPMID := mirror.ID
	resolvedStatus := db.MsBillingAddCardRequestStatusCompleted
	if hasDuplicate {
		resolvedPMID = duplicatePMID
		resolvedStatus = db.MsBillingAddCardRequestStatusDuplicate
		if err := qtx.SoftDeleteMirrorByID(ctx, mirror.ID); err != nil {
			return err
		}
	}

	resolvedPMUUID, err := uuidText(resolvedPMID)
	if err != nil {
		return err
	}

	// Step 4: terminal resolve. WHERE status='pending' makes this safe to
	// re-run after the partner handler already finalized the row.
	if err := qtx.ResolveAddCardRequest(ctx, db.ResolveAddCardRequestParams{
		StripePmID:      text(stripePaymentMethodID),
		Column2:         resolvedStatus,
		PaymentMethodID: resolvedPMUUID,
	}); err != nil {
		return err
	}

	return tx.Commit(ctx)
}

// ApplyInvoiceStatus reconciles a Stripe invoice.* event onto the mirror row
// keyed by stripe_invoice_id. Returns (found, error):
//   - found=true  → the row existed and the monotonic-status guard let the
//     update through (status + amounts written).
//   - found=false → no-op: either no mirror row exists (drift — invoice created
//     out-of-band, or the charge spine hasn't mirrored it yet) OR the guard
//     rejected a stale/out-of-order event (a late finalized after paid, etc.).
//     The webhook handler maps found=false to a drift_warning, never an error,
//     because Stripe delivers events at-least-once and out of order.
//
// Amounts are whole cents (Stripe minor units) encoded as the NUMERIC the
// invoices money columns expect — never float.
//
// The presentment fields (number / hosted_invoice_url / invoice_pdf,
// migration 026) pass through as plain strings: the query's
// COALESCE(NULLIF(new, ”), old) turns "" into "keep the stored value", so
// the Go layer never needs a nullable type for them.
func (s *pgxStore) ApplyInvoiceStatus(ctx context.Context, params ApplyInvoiceStatusParams) (bool, error) {
	paid, err := centsNumeric(params.AmountPaidCents)
	if err != nil {
		return false, err
	}
	due, err := centsNumeric(params.AmountDueCents)
	if err != nil {
		return false, err
	}
	rows, err := s.q.ApplyInvoiceStatus(ctx, db.ApplyInvoiceStatusParams{
		StripeInvoiceID:  params.StripeInvoiceID,
		Status:           params.Status,
		AmountPaid:       paid,
		AmountDue:        due,
		Number:           params.Number,
		HostedInvoiceUrl: params.HostedInvoiceURL,
		InvoicePdf:       params.InvoicePDF,
	})
	if err != nil {
		return false, err
	}
	return rows > 0, nil
}

// RelaxCollectionOnPaidInvoice re-trusts a prepaid account back to arrears when
// an invoice is paid and no delinquency remains (see the query doc). Returns
// (relaxed, error): relaxed=false (0 rows) is a no-op — not prepaid, still
// delinquent, or no mirror row. It never charges.
func (s *pgxStore) RelaxCollectionOnPaidInvoice(ctx context.Context, stripeInvoiceID string) (bool, error) {
	rows, err := s.q.RelaxCollectionOnPaidInvoice(ctx, stripeInvoiceID)
	if err != nil {
		return false, err
	}
	return rows > 0, nil
}

// MarkInvoiceFailed latches the sticky ever_failed flag on an invoice that
// failed a payment (invoice.payment_failed / marked_uncollectible). It is a
// single set-only UPDATE — NO account counter, NO transaction: the failed-charge
// streak is DERIVED at read time by ServiceBlockSignals from ever_failed +
// status + created_at, so there is nothing to increment atomically and nothing
// a reordered/duplicate delivery can double-count. The rows-affected count is
// irrelevant (a repeat failure event for the same invoice is a harmless 0-row
// no-op), so the method returns only an error. A no-op when the mirror row has
// not landed yet (keyed on the invoice) — the currently-'uncollectible' case is
// counted by status regardless, and an 'open'-after-failure invoice is remarked
// by the next payment_failed once the row exists.
func (s *pgxStore) MarkInvoiceFailed(ctx context.Context, stripeInvoiceID string) error {
	_, err := s.q.MarkInvoiceFailed(ctx, stripeInvoiceID)
	return err
}

// FlagPaymentMethodFraud latches fraud_blocked on the disputed/warned card
// (card-scoped, account-bounded; see the query doc). Returns (found, error):
// found=false (0 rows) is a drift no-op the handler ACKs 200.
func (s *pgxStore) FlagPaymentMethodFraud(ctx context.Context, stripeCustomerID, fingerprint, stripePaymentMethodID, reason string) (bool, error) {
	rows, err := s.q.FlagPaymentMethodFraud(ctx, db.FlagPaymentMethodFraudParams{
		FraudReason:           text(reason),
		StripeCustomerID:      text(stripeCustomerID),
		Fingerprint:           fingerprint,
		StripePaymentMethodID: stripePaymentMethodID,
	})
	if err != nil {
		return false, err
	}
	return rows > 0, nil
}

// text wraps a non-null Go string in the pgtype.Text the generated
// queries expect for nullable TEXT columns.
func text(s string) pgtype.Text {
	return pgtype.Text{String: s, Valid: true}
}

// centsNumeric encodes a whole-cent int64 as the pgtype.Numeric the invoices
// NUMERIC money columns expect. Cents are whole integers, so the numeric is
// exact (no scale). Mirrors cycle.centsNumeric — kept local rather than
// widening the cycle package's API for this single webhook consumer (the same
// 2-line strconv.FormatInt → pgtype.Numeric pattern).
func centsNumeric(cents int64) (pgtype.Numeric, error) {
	var n pgtype.Numeric
	if err := n.Scan(strconv.FormatInt(cents, 10)); err != nil {
		return pgtype.Numeric{}, err
	}
	return n, nil
}

// uuidText parses a UUID string into the pgtype.UUID the generated
// resolve query expects for the nullable payment_method_id column.
func uuidText(s string) (pgtype.UUID, error) {
	var u pgtype.UUID
	if err := u.Scan(s); err != nil {
		return pgtype.UUID{}, err
	}
	return u, nil
}

// Compile-time interface check: pgxStore must satisfy Store.
var _ Store = (*pgxStore)(nil)
