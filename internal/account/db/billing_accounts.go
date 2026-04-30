// Package db hosts hand-written pgx queries for the account-api Lambda.
//
// We deliberately use plain SQL with pgx instead of sqlc so the query
// shapes can evolve alongside the Stripe-mirror tables in
// migrations/account/ without a code-gen step. Match this pattern when
// adding new account-side queries.
package db

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// ErrBillingAccountNotFound is returned when no row matches an owner lookup.
var ErrBillingAccountNotFound = errors.New("billing_account not found")

// PgxDB is the subset of pgx the queries below need. Defined as an
// interface so unit tests can supply an in-memory fake without standing
// up Postgres. Both *pgx.Conn and *pgxpool.Pool satisfy it.
type PgxDB interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
}

// BillingAccount is the row shape for ms_billing_account.billing_accounts.
type BillingAccount struct {
	ID               uuid.UUID
	OwnerType        string
	OwnerID          uuid.UUID
	StripeCustomerID string
	Currency         string
}

// BillingAccounts wraps a PgxDB and exposes typed accessors.
type BillingAccounts struct {
	db PgxDB
}

// NewBillingAccounts constructs a BillingAccounts query helper.
func NewBillingAccounts(db PgxDB) *BillingAccounts {
	return &BillingAccounts{db: db}
}

const getBillingAccountByOwnerSQL = `
SELECT id, owner_type, owner_id, stripe_customer_id, currency
FROM ms_billing_account.billing_accounts
WHERE owner_type = $1 AND owner_id = $2
`

// GetByOwner looks up a billing_accounts row by (owner_type, owner_id).
// Returns ErrBillingAccountNotFound when no row matches.
func (b *BillingAccounts) GetByOwner(ctx context.Context, ownerType string, ownerID uuid.UUID) (*BillingAccount, error) {
	row := b.db.QueryRow(ctx, getBillingAccountByOwnerSQL, ownerType, ownerID)
	var ba BillingAccount
	if err := row.Scan(&ba.ID, &ba.OwnerType, &ba.OwnerID, &ba.StripeCustomerID, &ba.Currency); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrBillingAccountNotFound
		}
		return nil, fmt.Errorf("get billing_account by owner: %w", err)
	}
	return &ba, nil
}

const insertBillingAccountSQL = `
INSERT INTO ms_billing_account.billing_accounts
    (owner_type, owner_id, stripe_customer_id, currency)
VALUES ($1, $2, $3, $4)
RETURNING id
`

// Insert creates a new billing_accounts row and returns the generated id.
func (b *BillingAccounts) Insert(ctx context.Context, ownerType string, ownerID uuid.UUID, stripeCustomerID, currency string) (uuid.UUID, error) {
	row := b.db.QueryRow(ctx, insertBillingAccountSQL, ownerType, ownerID, stripeCustomerID, currency)
	var id uuid.UUID
	if err := row.Scan(&id); err != nil {
		return uuid.Nil, fmt.Errorf("insert billing_account: %w", err)
	}
	return id, nil
}
