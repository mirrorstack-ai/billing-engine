// Package db hosts hand-written pgx query helpers for the
// billing-engine account surface. Webhook handlers call into here so
// the SQL stays out of the dispatch logic.
package db

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// ErrAccountNotFound is returned when no billing_accounts row exists
// for a given Stripe customer id. Webhook handlers surface this as a
// non-retryable error and return 200 — Stripe retries won't help if we
// don't yet have an account row, and #5/#6 are responsible for creating
// the row before subscriptions land.
var ErrAccountNotFound = errors.New("billing account not found for stripe customer")

const lookupBillingAccountIDSQL = `
SELECT id FROM ms_billing_account.billing_accounts WHERE stripe_customer_id = $1
`

// LookupBillingAccountID resolves a Stripe customer id to the local
// billing_accounts UUID. ErrAccountNotFound is returned when there is
// no matching row.
func LookupBillingAccountID(ctx context.Context, q pgx.Tx, stripeCustomerID string) (uuid.UUID, error) {
	var id uuid.UUID
	err := q.QueryRow(ctx, lookupBillingAccountIDSQL, stripeCustomerID).Scan(&id)
	if errors.Is(err, pgx.ErrNoRows) {
		return uuid.Nil, ErrAccountNotFound
	}
	if err != nil {
		return uuid.Nil, fmt.Errorf("lookup billing account: %w", err)
	}
	return id, nil
}

const upsertSubscriptionSQL = `
INSERT INTO ms_billing_account.billing_subscriptions (
    billing_account_id, stripe_subscription_id, status,
    current_period_start, current_period_end, cancel_at_period_end,
    created_at, updated_at
) VALUES ($1, $2, $3, $4, $5, $6, now(), now())
ON CONFLICT (stripe_subscription_id) DO UPDATE
SET status               = EXCLUDED.status,
    current_period_start = EXCLUDED.current_period_start,
    current_period_end   = EXCLUDED.current_period_end,
    cancel_at_period_end = EXCLUDED.cancel_at_period_end,
    updated_at           = now()
RETURNING id
`

// SubscriptionUpsert is the payload for UpsertSubscription. Keeping the
// field set narrow forces handlers to map Stripe types into our
// vocabulary rather than passing Stripe structs across package
// boundaries.
type SubscriptionUpsert struct {
	BillingAccountID     uuid.UUID
	StripeSubscriptionID string
	Status               string
	CurrentPeriodStart   time.Time
	CurrentPeriodEnd     time.Time
	CancelAtPeriodEnd    bool
}

// UpsertSubscription inserts-or-updates a billing_subscriptions row and
// returns the local UUID, which child item rows reference.
func UpsertSubscription(ctx context.Context, tx pgx.Tx, in SubscriptionUpsert) (uuid.UUID, error) {
	var id uuid.UUID
	err := tx.QueryRow(ctx, upsertSubscriptionSQL,
		in.BillingAccountID,
		in.StripeSubscriptionID,
		in.Status,
		in.CurrentPeriodStart,
		in.CurrentPeriodEnd,
		in.CancelAtPeriodEnd,
	).Scan(&id)
	if err != nil {
		return uuid.Nil, fmt.Errorf("upsert subscription: %w", err)
	}
	return id, nil
}

// UpdateSubscriptionStatusSQL is exported so the past_due update in the
// invoice.payment_failed handler doesn't need a separate helper — it's
// a one-liner that benefits from sharing the same SQL string.
const UpdateSubscriptionStatusSQL = `
UPDATE ms_billing_account.billing_subscriptions
SET status = $2, updated_at = now()
WHERE stripe_subscription_id = $1
`
