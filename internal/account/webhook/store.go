package webhook

import (
	"context"

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
			(account_id, stripe_payment_method_id, brand, last4, exp_month, exp_year, is_default)
		SELECT acct.id, $2, $3, $4, $5, $6, false
		FROM acct
		ON CONFLICT (stripe_payment_method_id) DO NOTHING
	`
	tag, err := s.pool.Exec(ctx, q,
		stripeCustomerID,
		pm.StripePaymentMethodID,
		pm.Brand,
		pm.Last4,
		pm.ExpMonth,
		pm.ExpYear,
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

// Compile-time interface check: pgxStore must satisfy Store.
var _ Store = (*pgxStore)(nil)
