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

	// PaymentMethodTarget resolves an active payment method owned by the
	// user, returning its Stripe PM id, the account's Stripe customer
	// id, and whether the row is currently the default. found=false when
	// no active row matches (wrong owner, unknown id, or already soft-
	// deleted) — the ownership check for detach/set-default. isDefault
	// lets DetachPaymentMethod refuse to remove the default card.
	PaymentMethodTarget(ctx context.Context, userID, paymentMethodID uuid.UUID) (stripePMID, stripeCustomerID string, isDefault, found bool, err error)

	// InsertAddCardRequest creates a pending row in
	// ms_billing.add_card_requests for accountID and returns its id.
	// The setup_intent_id is patched in via
	// SetAddCardRequestSetupIntent once Stripe has returned the session.
	InsertAddCardRequest(ctx context.Context, accountID uuid.UUID) (uuid.UUID, error)

	// SetAddCardRequestSetupIntent stamps the Stripe setup_intent_id
	// onto a pending request row. Idempotent: if the row is no longer
	// pending (already resolved by webhook), this is a no-op.
	SetAddCardRequestSetupIntent(ctx context.Context, requestID uuid.UUID, setupIntentID string) error

	// GetAddCardRequest returns the status of an add-card request row,
	// scoped to accountID so a user can only poll requests they own.
	// When status is "completed" or "duplicate", the associated
	// PaymentMethod is populated via JOIN. Returns (nil, nil) when no
	// row matches both id + account_id (treated as 404 by the service).
	GetAddCardRequest(ctx context.Context, requestID, accountID uuid.UUID) (*AddCardRequestStatus, error)
}

// AddCardRequestStatus is the projection of an add_card_requests row
// returned to the service layer. PaymentMethod is non-nil only when
// the request has resolved to a card (completed or duplicate).
type AddCardRequestStatus struct {
	Status        AddCardStatus
	PaymentMethod *PaymentMethod
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

func (s *pgxStore) InsertAddCardRequest(ctx context.Context, accountID uuid.UUID) (uuid.UUID, error) {
	const q = `
		INSERT INTO ms_billing.add_card_requests (account_id)
		VALUES ($1)
		RETURNING id
	`
	var id uuid.UUID
	if err := s.pool.QueryRow(ctx, q, accountID).Scan(&id); err != nil {
		return uuid.Nil, err
	}
	return id, nil
}

func (s *pgxStore) SetAddCardRequestSetupIntent(ctx context.Context, requestID uuid.UUID, setupIntentID string) error {
	// `AND status = 'pending'` is the idempotency guard: if the webhook
	// has already resolved the row, we don't reopen its setup_intent_id.
	const q = `
		UPDATE ms_billing.add_card_requests
		SET setup_intent_id = $2
		WHERE id = $1 AND status = 'pending'
	`
	_, err := s.pool.Exec(ctx, q, requestID, setupIntentID)
	return err
}

func (s *pgxStore) GetAddCardRequest(ctx context.Context, requestID, accountID uuid.UUID) (*AddCardRequestStatus, error) {
	// LEFT JOIN: payment_method_id is NULL until the webhook resolves
	// the row, so a single query handles every status. pm.id is the
	// nullable sentinel — when NULL, the row hasn't resolved yet and
	// the COALESCE'd defaults are ignored at the Go level. We keep
	// COALESCEs on the other columns so Scan into non-nullable types
	// still succeeds.
	const q = `
		SELECT
			r.status,
			pm.id,
			COALESCE(pm.stripe_payment_method_id, ''),
			COALESCE(pm.brand, ''),
			COALESCE(pm.last4, ''),
			COALESCE(pm.exp_month, 0),
			COALESCE(pm.exp_year, 0),
			COALESCE(pm.is_default, false)
		FROM ms_billing.add_card_requests r
		LEFT JOIN ms_billing.payment_methods_mirror pm
			ON pm.id = r.payment_method_id
		WHERE r.id = $1 AND r.account_id = $2
	`
	var (
		status AddCardStatus
		pm     PaymentMethod
		pmID   *uuid.UUID
	)
	err := s.pool.QueryRow(ctx, q, requestID, accountID).Scan(
		&status,
		&pmID, &pm.StripePaymentMethodID, &pm.Brand, &pm.Last4,
		&pm.ExpMonth, &pm.ExpYear, &pm.IsDefault,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	out := &AddCardRequestStatus{Status: status}
	if pmID != nil {
		pm.ID = *pmID
		out.PaymentMethod = &pm
	}
	return out, nil
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

func (s *pgxStore) PaymentMethodTarget(ctx context.Context, userID, paymentMethodID uuid.UUID) (string, string, bool, bool, error) {
	const q = `
		SELECT pmm.stripe_payment_method_id, COALESCE(a.stripe_customer_id, ''), pmm.is_default
		FROM ms_billing.payment_methods_mirror pmm
		JOIN ms_billing.accounts a ON a.id = pmm.account_id
		WHERE a.owner_kind = 'user' AND a.owner_user_id = $1
		  AND pmm.id = $2 AND pmm.deleted_at IS NULL
	`
	var (
		stripePMID, stripeCustomerID string
		isDefault                    bool
	)
	err := s.pool.QueryRow(ctx, q, userID, paymentMethodID).
		Scan(&stripePMID, &stripeCustomerID, &isDefault)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", "", false, false, nil
	}
	if err != nil {
		return "", "", false, false, err
	}
	return stripePMID, stripeCustomerID, isDefault, true, nil
}
