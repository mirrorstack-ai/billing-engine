package db

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// upsertSubscriptionItemSQL persists either a 'seat' (quantity NOT NULL,
// metric NULL) or a 'meter' (metric NOT NULL, quantity NULL) row.
//
// We pass quantity through pgx's NULLability rules: a Go pointer that is
// nil becomes SQL NULL, and an int64 value becomes a non-null integer.
// This is required by the kind/shape CHECK constraint on the table.
const upsertSubscriptionItemSQL = `
INSERT INTO ms_billing_account.billing_subscription_items (
    subscription_id, kind, stripe_subscription_item_id,
    module_id, metric, quantity, created_at, updated_at
) VALUES ($1, $2, $3, $4, $5, $6, now(), now())
ON CONFLICT (stripe_subscription_item_id) DO UPDATE
SET kind       = EXCLUDED.kind,
    module_id  = EXCLUDED.module_id,
    metric     = EXCLUDED.metric,
    quantity   = EXCLUDED.quantity,
    updated_at = now()
`

// SubscriptionItemUpsert mirrors the table shape, with Quantity as a
// pointer so 'meter' rows can pass nil and respect the CHECK constraint.
type SubscriptionItemUpsert struct {
	SubscriptionID            uuid.UUID
	Kind                      string
	StripeSubscriptionItemID  string
	ModuleID                  *string
	Metric                    *string
	Quantity                  *int64
}

// UpsertSubscriptionItem is the public helper webhook handlers call.
func UpsertSubscriptionItem(ctx context.Context, tx pgx.Tx, in SubscriptionItemUpsert) error {
	if _, err := tx.Exec(ctx, upsertSubscriptionItemSQL,
		in.SubscriptionID,
		in.Kind,
		in.StripeSubscriptionItemID,
		in.ModuleID,
		in.Metric,
		in.Quantity,
	); err != nil {
		return fmt.Errorf("upsert subscription item: %w", err)
	}
	return nil
}
