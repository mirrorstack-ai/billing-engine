// Package db hosts hand-written pgx queries for the account-side
// billing-engine surface. Queries are not generated; service layers
// narrow to small reader/writer interfaces so unit tests can avoid
// standing up Postgres.
package db

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// ErrBillingAccountNotFound is returned by GetBillingAccountByID when
// no row matches the supplied id. It maps to a 404 at the HTTP layer.
var ErrBillingAccountNotFound = errors.New("db: billing account not found")

// BillingAccount is the minimal projection used by the billing-portal
// flow. Other fields on the table are intentionally omitted — consumers
// that need them can extend or add a sibling reader.
type BillingAccount struct {
	ID               uuid.UUID
	StripeCustomerID string
}

// PgxQuerier is the subset of pgx the lookup needs. Defined here so
// tests can inject a fake without standing up Postgres. Matches the
// EventDB interface pattern used in internal/account/webhook.
type PgxQuerier interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
}

// BillingAccountReader exposes only the read paths required by the
// billing-portal service. Splitting reads from writes keeps each
// caller's mock surface tiny.
type BillingAccountReader interface {
	GetBillingAccountByID(ctx context.Context, id uuid.UUID) (BillingAccount, error)
}

// BillingAccountLookupQueries implements BillingAccountReader against
// a real pgx pool/conn. The struct exists (rather than free functions)
// so future related read queries can hang off the same receiver.
type BillingAccountLookupQueries struct {
	q PgxQuerier
}

// NewBillingAccountLookupQueries wires the reader to a pgx querier.
func NewBillingAccountLookupQueries(q PgxQuerier) *BillingAccountLookupQueries {
	return &BillingAccountLookupQueries{q: q}
}

const getBillingAccountByIDSQL = `
SELECT id, stripe_customer_id
FROM ms_billing_account.billing_accounts
WHERE id = $1
`

// GetBillingAccountByID returns the billing account row keyed by our
// internal UUID. Returns ErrBillingAccountNotFound if no row matches.
func (q *BillingAccountLookupQueries) GetBillingAccountByID(ctx context.Context, id uuid.UUID) (BillingAccount, error) {
	var ba BillingAccount
	err := q.q.QueryRow(ctx, getBillingAccountByIDSQL, id).Scan(&ba.ID, &ba.StripeCustomerID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return BillingAccount{}, ErrBillingAccountNotFound
		}
		return BillingAccount{}, fmt.Errorf("get billing account by id: %w", err)
	}
	return ba, nil
}
