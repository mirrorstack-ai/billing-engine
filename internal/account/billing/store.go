package billing

import (
	"context"
	"errors"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Store is the persistence interface Service depends on. The shape
// is narrow on purpose — every method maps to a specific RPC need —
// so tests can satisfy it with a small fake (see service_test.go).
type Store interface {
	// EnsureAccount returns the account for the user, inserting one
	// if missing. Caller-side serialization via a per-user advisory
	// lock prevents the SELECT-then-INSERT race; the caller does NOT
	// need its own lock. Returns (account_id, stripe_customer_id);
	// stripe_customer_id is empty when not yet set.
	EnsureAccount(ctx context.Context, userID uuid.UUID) (accountID uuid.UUID, stripeCustomerID string, err error)

	// SetStripeCustomer associates a Stripe Customer ID with an
	// existing account. Idempotent: called only when the account
	// row has stripe_customer_id IS NULL.
	SetStripeCustomer(ctx context.Context, accountID uuid.UUID, stripeCustomerID string) error

	// AccountByUser returns the account ID for the user, or (Nil, false)
	// if no row exists. Read-only; used by Ensure / GetPaymentMethods
	// where missing-account is a normal-path "missing": billing_account
	// outcome rather than an error.
	AccountByUser(ctx context.Context, userID uuid.UUID) (uuid.UUID, bool, error)

	// HasUsablePaymentMethod returns true iff the account has at least
	// one row in payment_methods_mirror that's both not soft-deleted and
	// not expired. The hot-path predicate for Ensure.
	HasUsablePaymentMethod(ctx context.Context, accountID uuid.UUID) (bool, error)

	// ListPaymentMethods returns the active (not soft-deleted) payment
	// methods for an account, newest-first. Empty slice (not nil) when
	// none exist.
	ListPaymentMethods(ctx context.Context, accountID uuid.UUID) ([]PaymentMethod, error)
}

// NewStore returns a Store backed by the given pgxpool.
func NewStore(pool *pgxpool.Pool) Store {
	return &pgxStore{pool: pool}
}

type pgxStore struct {
	pool *pgxpool.Pool
}

// advisoryLockNamespaceBillingAccountUser is the first argument to
// pg_advisory_xact_lock(int, int) for EnsureAccount's per-user lock.
// Using the 2-arg form (namespace, key) means the per-user key occupies
// a full 32-bit space without colliding with unrelated advisory locks
// across the codebase — hashtext alone collides at ~65K users (birthday
// paradox on int32) and would silently serialize unrelated users.
const advisoryLockNamespaceBillingAccountUser = 0x6c627461 // "lbta" — billing_account, easy to grep

func (s *pgxStore) EnsureAccount(ctx context.Context, userID uuid.UUID) (uuid.UUID, string, error) {
	var id uuid.UUID
	var stripeCustomerID string
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		// pg_advisory_xact_lock(namespace, key) serializes concurrent
		// EnsureAccount calls per user. Held for the transaction duration.
		const lockQ = `SELECT pg_advisory_xact_lock($1::int, hashtext($2::text))`
		if _, err := tx.Exec(ctx, lockQ, advisoryLockNamespaceBillingAccountUser, userID); err != nil {
			return err
		}

		const selectQ = `
			SELECT id, COALESCE(stripe_customer_id, '')
			FROM ms_billing.accounts
			WHERE owner_kind = 'user' AND owner_user_id = $1
		`
		err := tx.QueryRow(ctx, selectQ, userID).Scan(&id, &stripeCustomerID)
		if err == nil {
			return nil
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			return err
		}

		const insertQ = `
			INSERT INTO ms_billing.accounts (owner_kind, owner_user_id)
			VALUES ('user', $1)
			RETURNING id, COALESCE(stripe_customer_id, '')
		`
		return tx.QueryRow(ctx, insertQ, userID).Scan(&id, &stripeCustomerID)
	})
	if err != nil {
		return uuid.Nil, "", err
	}
	return id, stripeCustomerID, nil
}

// ErrAccountNotFound is returned when SetStripeCustomer can't find the
// account row to update. Service layer maps this to billing.Internal —
// it means the EnsureAccount→SetStripeCustomer happy-path broke (the
// row was just inserted/selected in the same RPC), so the orphan
// reconciliation runbook should be checked.
var ErrAccountNotFound = errors.New("billing account row not found for update")

func (s *pgxStore) SetStripeCustomer(ctx context.Context, accountID uuid.UUID, stripeCustomerID string) error {
	const q = `UPDATE ms_billing.accounts SET stripe_customer_id = $2 WHERE id = $1`
	tag, err := s.pool.Exec(ctx, q, accountID, stripeCustomerID)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		// Zero rows: the account_id we just ensured doesn't match any
		// row. Almost certainly a code bug; surface it instead of
		// silently proceeding to a CreateSetupIntent that points at a
		// Stripe Customer with no DB anchor.
		return ErrAccountNotFound
	}
	return nil
}

func (s *pgxStore) AccountByUser(ctx context.Context, userID uuid.UUID) (uuid.UUID, bool, error) {
	const q = `SELECT id FROM ms_billing.accounts WHERE owner_kind = 'user' AND owner_user_id = $1`
	var id uuid.UUID
	err := s.pool.QueryRow(ctx, q, userID).Scan(&id)
	if errors.Is(err, pgx.ErrNoRows) {
		return uuid.Nil, false, nil
	}
	if err != nil {
		return uuid.Nil, false, err
	}
	return id, true, nil
}

func (s *pgxStore) HasUsablePaymentMethod(ctx context.Context, accountID uuid.UUID) (bool, error) {
	// "Usable" = not soft-deleted AND not expired (exp_year, exp_month >=
	// current year-month). Tuple comparison via row constructor; uses
	// the partial index pmm_account_active_idx for the deleted_at filter.
	const q = `
		SELECT EXISTS (
			SELECT 1
			FROM ms_billing.payment_methods_mirror
			WHERE account_id = $1
			  AND deleted_at IS NULL
			  AND (exp_year, exp_month) >= (
			      EXTRACT(YEAR  FROM current_date)::INT,
			      EXTRACT(MONTH FROM current_date)::INT
			  )
		)
	`
	var has bool
	err := s.pool.QueryRow(ctx, q, accountID).Scan(&has)
	return has, err
}

func (s *pgxStore) ListPaymentMethods(ctx context.Context, accountID uuid.UUID) ([]PaymentMethod, error) {
	const q = `
		SELECT id, stripe_payment_method_id, brand, last4, exp_month, exp_year, is_default
		FROM ms_billing.payment_methods_mirror
		WHERE account_id = $1 AND deleted_at IS NULL
		ORDER BY attached_at DESC
	`
	rows, err := s.pool.Query(ctx, q, accountID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := []PaymentMethod{}
	for rows.Next() {
		var pm PaymentMethod
		if err := rows.Scan(&pm.ID, &pm.StripePaymentMethodID, &pm.Brand, &pm.Last4, &pm.ExpMonth, &pm.ExpYear, &pm.IsDefault); err != nil {
			return nil, err
		}
		out = append(out, pm)
	}
	return out, rows.Err()
}
