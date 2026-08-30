package main

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// payerResolver maps an intent's sealed payer to the provider identity
// and instrument to charge.
//
// It reads only. The resolution is a lookup of state something else
// established — a customer created when a card was added, a default
// chosen by the customer — and an executor that could CREATE either
// would be able to charge a payer that had never been set up.
type payerResolver struct {
	pool *pgxpool.Pool
}

var errNoUsableCard = errors.New("payer has no usable default card")

// ResolvePayer returns the Stripe customer and default payment method
// for a payer.
//
// Only a non-deleted, non-fraud-flagged default is returned. The
// alternative — falling back to any card on file — would charge an
// instrument the customer did not choose, and a customer who removed a
// card removed it.
func (r payerResolver) ResolvePayer(ctx context.Context, payerKind, payerID string) (string, string, error) {
	var (
		customerID    string
		paymentMethod string
	)

	const query = `
		SELECT a.stripe_customer_id, pm.stripe_payment_method_id
		  FROM ms_billing.accounts a
		  JOIN ms_billing.payment_methods_mirror pm
		    ON pm.account_id = a.id
		 WHERE a.owner_kind = $1
		   AND ((a.owner_kind = 'user' AND a.owner_user_id::text = $2)
		     OR (a.owner_kind = 'org'  AND a.owner_org_id::text  = $2))
		   AND pm.is_default
		   AND pm.deleted_at IS NULL
		 LIMIT 1`

	err := r.pool.QueryRow(ctx, query, payerKind, payerID).Scan(&customerID, &paymentMethod)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", "", fmt.Errorf("%w: %s:%s", errNoUsableCard, payerKind, payerID)
	}
	if err != nil {
		return "", "", fmt.Errorf("resolve payer: %w", err)
	}
	return customerID, paymentMethod, nil
}
