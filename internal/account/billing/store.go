package billing

import (
	"context"
	"errors"
	"log/slog"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/mirrorstack-ai/billing-engine/internal/account/db"
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

	// HasUnpaidInvoice returns true iff the account has at least one invoice
	// in an unpaid, collection-relevant state (open or uncollectible). The
	// delinquency predicate for Ensure — derived from the invoices mirror
	// (reconciled by the invoice.* webhooks), not a stored flag. 'draft' is
	// excluded (not finalized); 'paid'/'void' are clean.
	HasUnpaidInvoice(ctx context.Context, accountID uuid.UUID) (bool, error)

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

	// EnsureOrgAccount is EnsureAccount's org twin (migration 041): the same
	// advisory-locked get-or-create on the org namespace ('lbto'). Created
	// regardless of designation state so a funding='org' card can bind before
	// the designation completes.
	EnsureOrgAccount(ctx context.Context, orgID uuid.UUID) (accountID uuid.UUID, stripeCustomerID string, err error)

	// AccountByOrg resolves the org's account row by EXISTENCE (not
	// funded-gated) — cards are manageable while a funding='org' designation
	// awaits its first bind.
	AccountByOrg(ctx context.Context, orgID uuid.UUID) (uuid.UUID, bool, error)

	// ResolveOrgFundedAccount resolves the org's account through the
	// designation + activation gate — Ensure's org resolution ("the pointer
	// never flips to an unfunded account", design D1). Named identically in
	// every store that wraps the one generated query, so THE org account
	// resolution greps as one thing.
	ResolveOrgFundedAccount(ctx context.Context, orgID uuid.UUID) (uuid.UUID, bool, error)

	// ChargeFundingAccount maps an account to the account whose Stripe
	// customer / default PM pays its invoices — itself, unless it is an org
	// account with a sponsor designation. Ensure's payment_method capability
	// checks the FUNDING account: a sponsor-funded org account owns no PM
	// rows itself.
	ChargeFundingAccount(ctx context.Context, accountID uuid.UUID) (uuid.UUID, error)

	// PaymentMethodTargetForOrg is PaymentMethodTarget's org twin: the PM must
	// belong to the ORG account (detach / set-default ownership gate).
	PaymentMethodTargetForOrg(ctx context.Context, orgID, paymentMethodID uuid.UUID) (stripePMID, stripeCustomerID string, isDefault, found bool, err error)
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
	return &pgxStore{pool: pool, q: db.New(pool)}
}

type pgxStore struct {
	pool *pgxpool.Pool
	q    *db.Queries
}

// AdvisoryLockNamespaceBillingAccountUser is the first argument to
// pg_advisory_xact_lock(int, int) for the per-user get-or-create account
// lock. Using the 2-arg form (namespace, key) means the per-user key
// occupies a full 32-bit space without colliding with unrelated advisory
// locks across the codebase — hashtext alone collides at ~65K users
// (birthday paradox on int32) and would silently serialize unrelated users.
// Explicit int32: pg_advisory_xact_lock(int, int) takes 32-bit args and
// the generated param field is int32; typing the constant here keeps the
// value in range and avoids an implicit untyped-const conversion at the
// call site.
//
// Exported because EVERY writer that get-or-creates an accounts row (this
// package's EnsureAccount, cycle's RegisterApp path) MUST serialize on this
// SAME (namespace, key) pair — the accounts table has no owner UNIQUE
// constraint; this lock IS the uniqueness guard.
const AdvisoryLockNamespaceBillingAccountUser int32 = 0x6c627461 // "lbta" — billing_account, easy to grep

// AdvisoryLockNamespaceBillingAccountOrg is the org twin of the user
// namespace above: every writer that get-or-creates an ORG-owned accounts row
// (migration 041 — cycle's EnsureOrgAccount, this package's org add-card leg)
// serializes on this (namespace, hashtext(org_id)) pair for the same reason —
// the advisory lock IS the uniqueness guard.
const AdvisoryLockNamespaceBillingAccountOrg int32 = 0x6c62746f // "lbto"

func (s *pgxStore) EnsureAccount(ctx context.Context, userID uuid.UUID) (uuid.UUID, string, error) {
	var id string
	var stripeCustomerID string
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		qtx := s.q.WithTx(tx)

		// pg_advisory_xact_lock(namespace, key) serializes concurrent
		// EnsureAccount calls per user. Held for the transaction duration.
		if err := qtx.AcquireBillingAccountUserLock(ctx, db.AcquireBillingAccountUserLockParams{
			Column1: AdvisoryLockNamespaceBillingAccountUser,
			Column2: userID.String(),
		}); err != nil {
			return err
		}

		existing, err := qtx.SelectAccountByUser(ctx, nullableUUID(userID))
		if err == nil {
			id, stripeCustomerID = existing.ID, existing.StripeCustomerID
			return nil
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			return err
		}

		inserted, err := qtx.InsertUserAccount(ctx, nullableUUID(userID))
		if err != nil {
			return err
		}
		id, stripeCustomerID = inserted.ID, inserted.StripeCustomerID
		return nil
	})
	if err != nil {
		return uuid.Nil, "", err
	}
	parsed, err := uuid.Parse(id)
	if err != nil {
		return uuid.Nil, "", err
	}
	return parsed, stripeCustomerID, nil
}

// ErrAccountNotFound is returned when SetStripeCustomer can't find the
// account row to update. Service layer maps this to billing.Internal —
// it means the EnsureAccount→SetStripeCustomer happy-path broke (the
// row was just inserted/selected in the same RPC), so the orphan
// reconciliation runbook should be checked.
var ErrAccountNotFound = errors.New("billing account row not found for update")

func (s *pgxStore) SetStripeCustomer(ctx context.Context, accountID uuid.UUID, stripeCustomerID string) error {
	rows, err := s.q.SetStripeCustomer(ctx, db.SetStripeCustomerParams{
		ID:               accountID.String(),
		StripeCustomerID: pgtype.Text{String: stripeCustomerID, Valid: true},
	})
	if err != nil {
		return err
	}
	if rows == 0 {
		// Zero rows: the account_id we just ensured doesn't match any
		// row. Almost certainly a code bug; surface it instead of
		// silently proceeding to a CreateSetupIntent that points at a
		// Stripe Customer with no DB anchor.
		return ErrAccountNotFound
	}
	return nil
}

func (s *pgxStore) AccountByUser(ctx context.Context, userID uuid.UUID) (uuid.UUID, bool, error) {
	id, err := s.q.AccountIDByUser(ctx, nullableUUID(userID))
	if errors.Is(err, pgx.ErrNoRows) {
		return uuid.Nil, false, nil
	}
	if err != nil {
		return uuid.Nil, false, err
	}
	parsed, err := uuid.Parse(id)
	if err != nil {
		return uuid.Nil, false, err
	}
	return parsed, true, nil
}

func (s *pgxStore) HasUsablePaymentMethod(ctx context.Context, accountID uuid.UUID) (bool, error) {
	return s.q.HasUsablePaymentMethod(ctx, accountID.String())
}

// HasUnpaidInvoice mirrors HasUsablePaymentMethod's NOT NULL uuid → string
// override: the generated query takes the account id as a plain string.
func (s *pgxStore) HasUnpaidInvoice(ctx context.Context, accountID uuid.UUID) (bool, error) {
	return s.q.AccountHasUnpaidInvoice(ctx, accountID.String())
}

func (s *pgxStore) ListPaymentMethods(ctx context.Context, accountID uuid.UUID) ([]PaymentMethod, error) {
	rows, err := s.q.ListPaymentMethods(ctx, accountID.String())
	if err != nil {
		return nil, err
	}
	out := make([]PaymentMethod, 0, len(rows))
	for _, r := range rows {
		id, err := uuid.Parse(r.ID)
		if err != nil {
			return nil, err
		}
		out = append(out, PaymentMethod{
			ID:                    id,
			StripePaymentMethodID: r.StripePaymentMethodID,
			Brand:                 r.Brand,
			Last4:                 r.Last4,
			ExpMonth:              int(r.ExpMonth),
			ExpYear:               int(r.ExpYear),
			IsDefault:             r.IsDefault,
		})
	}
	return out, nil
}

func (s *pgxStore) PaymentMethodTarget(ctx context.Context, userID, paymentMethodID uuid.UUID) (string, string, bool, bool, error) {
	row, err := s.q.PaymentMethodTarget(ctx, db.PaymentMethodTargetParams{
		OwnerUserID: nullableUUID(userID),
		ID:          paymentMethodID.String(),
	})
	if errors.Is(err, pgx.ErrNoRows) {
		return "", "", false, false, nil
	}
	if err != nil {
		return "", "", false, false, err
	}
	return row.StripePaymentMethodID, row.StripeCustomerID, row.IsDefault, true, nil
}

func (s *pgxStore) InsertAddCardRequest(ctx context.Context, accountID uuid.UUID) (uuid.UUID, error) {
	id, err := s.q.InsertAddCardRequest(ctx, accountID.String())
	if err != nil {
		return uuid.Nil, err
	}
	parsed, err := uuid.Parse(id)
	if err != nil {
		return uuid.Nil, err
	}
	return parsed, nil
}

func (s *pgxStore) SetAddCardRequestSetupIntent(ctx context.Context, requestID uuid.UUID, setupIntentID string) error {
	// `AND status = 'pending'` is the idempotency guard: if the webhook
	// has already resolved the row, we don't reopen its setup_intent_id.
	rows, err := s.q.SetAddCardRequestSetupIntent(ctx, db.SetAddCardRequestSetupIntentParams{
		ID:            requestID.String(),
		SetupIntentID: pgtype.Text{String: setupIntentID, Valid: true},
	})
	if err != nil {
		return err
	}
	if rows == 0 {
		// No-op stamp: the row was already resolved by the webhook before
		// Stripe returned the session. Expected and harmless — debug-log it
		// so the no-op is observable, matching SetStripeCustomer's
		// RowsAffected branch. Not an error.
		slog.DebugContext(ctx, "SetAddCardRequestSetupIntent no-op: request not pending",
			"request_id", requestID.String())
	}
	return nil
}

func (s *pgxStore) GetAddCardRequest(ctx context.Context, requestID, accountID uuid.UUID) (*AddCardRequestStatus, error) {
	// LEFT JOIN: payment_method_id is NULL until the webhook resolves
	// the row, so a single query handles every status. payment_method_id
	// is the nullable sentinel — when invalid, the row hasn't resolved
	// yet and the COALESCE'd defaults are ignored at the Go level.
	row, err := s.q.GetAddCardRequest(ctx, db.GetAddCardRequestParams{
		ID:        requestID.String(),
		AccountID: accountID.String(),
	})
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	out := &AddCardRequestStatus{Status: AddCardStatus(row.Status)}
	if row.PaymentMethodID.Valid {
		pmID, err := uuid.FromBytes(row.PaymentMethodID.Bytes[:])
		if err != nil {
			return nil, err
		}
		out.PaymentMethod = &PaymentMethod{
			ID:                    pmID,
			StripePaymentMethodID: row.StripePaymentMethodID,
			Brand:                 row.Brand,
			Last4:                 row.Last4,
			ExpMonth:              int(row.ExpMonth),
			ExpYear:               int(row.ExpYear),
			IsDefault:             row.IsDefault,
		}
	}
	return out, nil
}

// EnsureOrgAccount mirrors EnsureAccount on the org leg — same
// transaction + advisory-lock shape, org namespace, org queries.
func (s *pgxStore) EnsureOrgAccount(ctx context.Context, orgID uuid.UUID) (uuid.UUID, string, error) {
	var id string
	var stripeCustomerID string
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		qtx := s.q.WithTx(tx)
		if err := qtx.AcquireBillingAccountUserLock(ctx, db.AcquireBillingAccountUserLockParams{
			Column1: AdvisoryLockNamespaceBillingAccountOrg,
			Column2: orgID.String(),
		}); err != nil {
			return err
		}
		existing, err := qtx.SelectAccountByOrg(ctx, nullableUUID(orgID))
		if err == nil {
			id, stripeCustomerID = existing.ID, existing.StripeCustomerID
			return nil
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			return err
		}
		inserted, err := qtx.InsertOrgAccount(ctx, nullableUUID(orgID))
		if err != nil {
			return err
		}
		id, stripeCustomerID = inserted.ID, inserted.StripeCustomerID
		return nil
	})
	if err != nil {
		return uuid.Nil, "", err
	}
	parsed, err := uuid.Parse(id)
	if err != nil {
		return uuid.Nil, "", err
	}
	return parsed, stripeCustomerID, nil
}

func (s *pgxStore) AccountByOrg(ctx context.Context, orgID uuid.UUID) (uuid.UUID, bool, error) {
	row, err := s.q.SelectAccountByOrg(ctx, nullableUUID(orgID))
	return uuidRowFound(row.ID, err)
}

func (s *pgxStore) ResolveOrgFundedAccount(ctx context.Context, orgID uuid.UUID) (uuid.UUID, bool, error) {
	// ErrNoRows = no designation / not activated — unbilled, a normal outcome.
	id, err := s.q.ResolveOrgFundedAccount(ctx, orgID.String())
	return uuidRowFound(id, err)
}

func (s *pgxStore) ChargeFundingAccount(ctx context.Context, accountID uuid.UUID) (uuid.UUID, error) {
	id, err := s.q.ChargeFundingAccount(ctx, accountID.String())
	if err != nil {
		return uuid.Nil, err // incl. ErrNoRows: the account was just resolved — a missing row is a code bug
	}
	return uuid.Parse(id)
}

func (s *pgxStore) PaymentMethodTargetForOrg(ctx context.Context, orgID, paymentMethodID uuid.UUID) (string, string, bool, bool, error) {
	row, err := s.q.PaymentMethodTargetForOrg(ctx, db.PaymentMethodTargetForOrgParams{
		OwnerOrgID: nullableUUID(orgID),
		ID:         paymentMethodID.String(),
	})
	if errors.Is(err, pgx.ErrNoRows) {
		return "", "", false, false, nil
	}
	if err != nil {
		return "", "", false, false, err
	}
	return row.StripePaymentMethodID, row.StripeCustomerID, row.IsDefault, true, nil
}

// uuidRowFound decodes the (uuid-as-string, error) shape every single-row
// account-resolution query yields: ErrNoRows → (Nil, false, nil) — a normal
// lazy/missing outcome, not an error — else the parsed id. One home for the
// ceremony so the resolution wrappers stay one line each.
func uuidRowFound(id string, err error) (uuid.UUID, bool, error) {
	if errors.Is(err, pgx.ErrNoRows) {
		return uuid.Nil, false, nil
	}
	if err != nil {
		return uuid.Nil, false, err
	}
	parsed, err := uuid.Parse(id)
	if err != nil {
		return uuid.Nil, false, err
	}
	return parsed, true, nil
}

// nullableUUID converts a google/uuid.UUID into the pgtype.UUID the
// generated queries expect for the nullable owner_user_id column. A
// non-Nil UUID is always marked Valid; callers never pass Nil here
// (the service layer rejects Nil user ids before reaching the store).
func nullableUUID(id uuid.UUID) pgtype.UUID {
	return pgtype.UUID{Bytes: id, Valid: true}
}
