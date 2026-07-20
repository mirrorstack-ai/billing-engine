package billing

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/mirrorstack-ai/billing-engine/internal/account/db"
	billingstripe "github.com/mirrorstack-ai/billing-engine/internal/shared/stripe"
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

	// ServiceBlockSignals reads, in one round-trip, the three inputs the
	// service-block eligibility gate reasons over for an account: the usable
	// non-fraud card count, the consecutive failed-charge streak, and the
	// earliest real charge's status ("" when the account has none yet). See
	// db.ServiceBlockSignals.
	ServiceBlockSignals(ctx context.Context, accountID uuid.UUID) (ServiceSignals, error)

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

	// UnpaidInvoiceCount counts the account's unpaid (open/uncollectible,
	// amount_due > 0) mirror invoices — GetServiceStatus's gate-4 signal
	// (blocked at >= eligibility.MaxUnpaidInvoices).
	UnpaidInvoiceCount(ctx context.Context, accountID uuid.UUID) (int, error)

	// ListUnpaidInvoices returns the account's unpaid mirror invoices
	// oldest-first (the same predicate as UnpaidInvoiceCount). Empty slice
	// (not nil) when none.
	ListUnpaidInvoices(ctx context.Context, accountID uuid.UUID) ([]UnpaidInvoiceRow, error)

	// InvoiceForPayment resolves a mirror invoice by (id, account) — the
	// PayInvoice ownership gate. found=false when no row matches both (wrong
	// owner or unknown id), which the service maps to NOT_FOUND.
	InvoiceForPayment(ctx context.Context, invoiceID, accountID uuid.UUID) (InvoicePayTarget, bool, error)

	// SyncInvoiceMirror applies a Stripe invoice snapshot onto the mirror row
	// through the SAME monotonic guard as the webhook's ApplyInvoiceStatus
	// (db.ApplyInvoiceStatus): forward transitions only, identical re-apply
	// allowed. Presentment fields pass "" (COALESCE keeps stored values);
	// ever_failed is never touched. applied=false = no mirror row or the
	// guard rejected the transition — a safe no-op for the caller.
	SyncInvoiceMirror(ctx context.Context, inv billingstripe.Invoice) (applied bool, err error)

	// HasUsableDefaultPM is the charge legs' no-PM gate (cycle.sql), reused by
	// PayInvoice: Stripe pays an invoice with the Customer's default PM, so a
	// usable mirror card must exist on the FUNDING account before we ask
	// Stripe to collect.
	HasUsableDefaultPM(ctx context.Context, accountID uuid.UUID) (bool, error)

	// AccountStripeCustomer returns the account's Stripe Customer id ("" when
	// none yet) — the charge legs' customer resolution (cycle.sql), reused by
	// PayInvoice's gate/charge coherence check: the pay-time funding account's
	// customer must still be the invoice's Stripe customer before Stripe is
	// asked to collect (the invoice's payer was frozen at creation).
	AccountStripeCustomer(ctx context.Context, accountID uuid.UUID) (string, error)

	// CreditStanding returns the account's authoritative posted wallet balance,
	// billing policy, credit limit, and optional auto-top-up configuration.
	CreditStanding(ctx context.Context, accountID uuid.UUID) (CreditStandingRow, error)

	// ListCreditLedger returns a newest-first keyset page. limit is already
	// service-clamped and normally includes one look-ahead row.
	ListCreditLedger(ctx context.Context, accountID uuid.UUID, limit int32, cursor *CreditLedgerCursor) ([]CreditLedgerEntry, error)

	// CreditLedgerByIdempotencyKey resolves a global client/server idempotency
	// key across purchases and grants. found=false is the normal fresh-key case.
	CreditLedgerByIdempotencyKey(ctx context.Context, key string) (CreditLedgerRecord, bool, error)

	// CreatePendingCreditPurchase serializes on the account, computes the
	// prospective balance snapshot, and appends one pending purchase row.
	CreatePendingCreditPurchase(ctx context.Context, accountID uuid.UUID, amountMicros int64, idempotencyKey string) (CreditPurchaseRow, error)

	// CreditPurchase returns an owned purchase row. The account scope prevents
	// purchase-id enumeration across owners.
	CreditPurchase(ctx context.Context, purchaseID, accountID uuid.UUID) (CreditPurchaseRow, bool, error)

	// AttachCreditPurchaseInvoice durably binds Stripe's invoice before it is
	// finalized. Reapplying the same invoice is idempotent.
	AttachCreditPurchaseInvoice(ctx context.Context, purchaseID, accountID uuid.UUID, stripeInvoiceID, receiptURL string) error

	// FinalizeCreditPurchase applies a terminal pending→settled|failed
	// transition and enriches the receipt URL. Terminal replays are no-ops.
	FinalizeCreditPurchase(ctx context.Context, purchaseID, accountID uuid.UUID, status, receiptURL string) (CreditPurchaseRow, error)

	// UpsertCreditAutoTopUp stores the already validated account configuration.
	UpsertCreditAutoTopUp(ctx context.Context, accountID uuid.UUID, cfg AutoTopUpConfig) (AutoTopUpConfig, error)

	// SetCreditBillingMode updates the wallet mode and its resolved non-negative
	// limit in one statement.
	SetCreditBillingMode(ctx context.Context, accountID uuid.UUID, mode BillingMode, creditLimitMicros int64) error

	// DistributorCustomerAccount validates the distributor→customer relation
	// represented by org_billing_designations and returns the customer account.
	DistributorCustomerAccount(ctx context.Context, distributorOrgID, customerOrgID uuid.UUID) (uuid.UUID, bool, error)

	// ListDistributorCustomerStates returns wallet snapshots for every related
	// customer org, sorted deterministically by org id.
	ListDistributorCustomerStates(ctx context.Context, distributorOrgID uuid.UUID) ([]DistributorCustomerState, error)

	// InsertCreditGrant serializes on the customer account and appends a settled
	// grant with its post-entry balance snapshot.
	InsertCreditGrant(ctx context.Context, accountID uuid.UUID, amountMicros int64, actor, idempotencyKey string, expiresAt *time.Time) (CreditLedgerRecord, error)
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

func (s *pgxStore) ServiceBlockSignals(ctx context.Context, accountID uuid.UUID) (ServiceSignals, error) {
	row, err := s.q.ServiceBlockSignals(ctx, accountID.String())
	if err != nil {
		return ServiceSignals{}, err
	}
	return ServiceSignals{
		UsableCardCount:    int(row.UsableCardCount),
		FailedChargeStreak: int(row.FailedChargeStreak),
		FirstChargeStatus:  row.FirstChargeStatus,
	}, nil
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

func (s *pgxStore) UnpaidInvoiceCount(ctx context.Context, accountID uuid.UUID) (int, error) {
	n, err := s.q.CountUnpaidInvoicesForAccount(ctx, accountID.String())
	return int(n), err
}

func (s *pgxStore) ListUnpaidInvoices(ctx context.Context, accountID uuid.UUID) ([]UnpaidInvoiceRow, error) {
	rows, err := s.q.ListUnpaidInvoicesForAccount(ctx, accountID.String())
	if err != nil {
		return nil, err
	}
	out := make([]UnpaidInvoiceRow, 0, len(rows))
	for _, r := range rows {
		id, err := uuid.Parse(r.ID)
		if err != nil {
			return nil, err
		}
		due, err := centsNumericToMicros(r.AmountDue)
		if err != nil {
			return nil, fmt.Errorf("decode amount_due for invoice %s: %w", r.ID, err)
		}
		out = append(out, UnpaidInvoiceRow{
			ID:              id,
			Number:          r.Number,
			AmountDueMicros: due,
			CreatedAt:       r.CreatedAt,
		})
	}
	return out, nil
}

func (s *pgxStore) InvoiceForPayment(ctx context.Context, invoiceID, accountID uuid.UUID) (InvoicePayTarget, bool, error) {
	row, err := s.q.InvoiceForPayment(ctx, db.InvoiceForPaymentParams{
		ID:        invoiceID.String(),
		AccountID: accountID.String(),
	})
	if errors.Is(err, pgx.ErrNoRows) {
		return InvoicePayTarget{}, false, nil
	}
	if err != nil {
		return InvoicePayTarget{}, false, err
	}
	return InvoicePayTarget{StripeInvoiceID: row.StripeInvoiceID, Status: row.Status}, true, nil
}

func (s *pgxStore) SyncInvoiceMirror(ctx context.Context, inv billingstripe.Invoice) (bool, error) {
	paid, err := centsNumeric(inv.AmountPaid)
	if err != nil {
		return false, err
	}
	due, err := centsNumeric(inv.AmountDue)
	if err != nil {
		return false, err
	}
	rows, err := s.q.ApplyInvoiceStatus(ctx, db.ApplyInvoiceStatusParams{
		StripeInvoiceID:  inv.ID,
		Status:           inv.Status,
		AmountPaid:       paid,
		AmountDue:        due,
		Number:           "",
		HostedInvoiceUrl: "",
		InvoicePdf:       "",
	})
	if err != nil {
		return false, err
	}
	return rows > 0, nil
}

func (s *pgxStore) HasUsableDefaultPM(ctx context.Context, accountID uuid.UUID) (bool, error) {
	return s.q.HasUsableDefaultPM(ctx, accountID.String())
}

func (s *pgxStore) AccountStripeCustomer(ctx context.Context, accountID uuid.UUID) (string, error) {
	return s.q.AccountStripeCustomer(ctx, accountID.String())
}

func (s *pgxStore) CreditStanding(ctx context.Context, accountID uuid.UUID) (CreditStandingRow, error) {
	row, err := s.q.GetCreditStandingSnapshot(ctx, accountID.String())
	if err != nil {
		return CreditStandingRow{}, err
	}
	mode, err := parseCreditBillingMode(row.BillingMode)
	if err != nil {
		return CreditStandingRow{}, err
	}
	out := CreditStandingRow{
		BillingMode:       mode,
		BalanceMicros:     row.BalanceMicros,
		CreditLimitMicros: row.CreditLimitMicros,
	}
	if row.AutoTopupConfigured {
		out.AutoTopUp = &AutoTopUpConfig{
			Enabled:         row.AutoTopupEnabled,
			ThresholdMicros: row.AutoTopupThresholdMicros,
			AmountMicros:    row.AutoTopupAmountMicros,
			PaymentMethodID: row.AutoTopupPaymentMethodID,
		}
	}
	return out, nil
}

func (s *pgxStore) ListCreditLedger(ctx context.Context, accountID uuid.UUID, limit int32, cursor *CreditLedgerCursor) ([]CreditLedgerEntry, error) {
	params := db.ListCreditLedgerPageParams{
		AccountID: accountID.String(),
		PageLimit: limit,
	}
	if cursor != nil {
		params.CursorCreatedAt = pgtype.Timestamptz{Time: cursor.CreatedAt, Valid: true}
		params.CursorID = nullableUUID(cursor.ID)
	}
	rows, err := s.q.ListCreditLedgerPage(ctx, params)
	if err != nil {
		return nil, err
	}
	out := make([]CreditLedgerEntry, 0, len(rows))
	for _, row := range rows {
		if _, err := uuid.Parse(row.ID); err != nil {
			return nil, fmt.Errorf("parse credit ledger id %q: %w", row.ID, err)
		}
		out = append(out, CreditLedgerEntry{
			ID:                 row.ID,
			AmountMicros:       row.AmountMicros,
			Type:               row.Type,
			Status:             row.Status,
			BalanceAfterMicros: row.BalanceAfterMicros,
			Actor:              row.Actor,
			ReceiptURL:         nullableTextValue(row.ReceiptUrl),
			ExpiresAt:          nullableTimeValue(row.ExpiresAt),
			CreatedAt:          row.CreatedAt,
		})
	}
	return out, nil
}

func (s *pgxStore) CreditLedgerByIdempotencyKey(ctx context.Context, key string) (CreditLedgerRecord, bool, error) {
	row, err := s.q.GetCreditLedgerEntryByIdempotencyKey(ctx, key)
	if errors.Is(err, pgx.ErrNoRows) {
		return CreditLedgerRecord{}, false, nil
	}
	if err != nil {
		return CreditLedgerRecord{}, false, err
	}
	record, err := creditLedgerRecordFromGenerated(
		row.ID,
		row.AccountID,
		row.AmountMicros,
		row.Type,
		row.Status,
		row.BalanceAfterMicros,
		row.Actor,
		row.IdempotencyKey,
		row.StripeInvoiceID,
		row.ReceiptUrl,
		row.ExpiresAt,
		row.CreatedAt,
	)
	if err != nil {
		return CreditLedgerRecord{}, false, err
	}
	return record, true, nil
}

func (s *pgxStore) CreatePendingCreditPurchase(ctx context.Context, accountID uuid.UUID, amountMicros int64, idempotencyKey string) (CreditPurchaseRow, error) {
	var row db.InsertPendingCreditPurchaseRow
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		qtx := s.q.WithTx(tx)
		balance, err := qtx.LockCreditAccountBalance(ctx, accountID.String())
		if err != nil {
			return err
		}
		balanceAfter, err := checkedAddMicros(balance.BalanceMicros, amountMicros)
		if err != nil {
			return err
		}
		row, err = qtx.InsertPendingCreditPurchase(ctx, db.InsertPendingCreditPurchaseParams{
			AccountID:          accountID.String(),
			AmountMicros:       amountMicros,
			BalanceAfterMicros: balanceAfter,
			IdempotencyKey:     idempotencyKey,
		})
		return err
	})
	if err != nil {
		return CreditPurchaseRow{}, err
	}
	return creditPurchaseRowFromGenerated(
		row.ID,
		row.AccountID,
		row.AmountMicros,
		row.Type,
		row.Status,
		row.BalanceAfterMicros,
		row.Actor,
		row.IdempotencyKey,
		row.StripeInvoiceID,
		row.ReceiptUrl,
		row.CreatedAt,
	)
}

func (s *pgxStore) CreditPurchase(ctx context.Context, purchaseID, accountID uuid.UUID) (CreditPurchaseRow, bool, error) {
	row, err := s.q.GetCreditPurchaseByID(ctx, db.GetCreditPurchaseByIDParams{
		PurchaseID: purchaseID.String(),
		AccountID:  accountID.String(),
	})
	if errors.Is(err, pgx.ErrNoRows) {
		return CreditPurchaseRow{}, false, nil
	}
	if err != nil {
		return CreditPurchaseRow{}, false, err
	}
	out, err := creditPurchaseRowFromGenerated(
		row.ID,
		row.AccountID,
		row.AmountMicros,
		row.Type,
		row.Status,
		row.BalanceAfterMicros,
		row.Actor,
		row.IdempotencyKey,
		row.StripeInvoiceID,
		row.ReceiptUrl,
		row.CreatedAt,
	)
	if err != nil {
		return CreditPurchaseRow{}, false, err
	}
	return out, true, nil
}

func (s *pgxStore) AttachCreditPurchaseInvoice(ctx context.Context, purchaseID, accountID uuid.UUID, stripeInvoiceID, receiptURL string) error {
	_, err := s.q.AttachCreditPurchaseInvoice(ctx, db.AttachCreditPurchaseInvoiceParams{
		StripeInvoiceID: stripeInvoiceID,
		ReceiptUrl:      receiptURL,
		PurchaseID:      purchaseID.String(),
		AccountID:       accountID.String(),
	})
	return err
}

func (s *pgxStore) FinalizeCreditPurchase(ctx context.Context, purchaseID, accountID uuid.UUID, status, receiptURL string) (CreditPurchaseRow, error) {
	var out CreditPurchaseRow
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		qtx := s.q.WithTx(tx)
		balance, err := qtx.LockCreditAccountBalance(ctx, accountID.String())
		if err != nil {
			return err
		}
		purchase, err := qtx.GetCreditPurchaseByID(ctx, db.GetCreditPurchaseByIDParams{
			PurchaseID: purchaseID.String(),
			AccountID:  accountID.String(),
		})
		if err != nil {
			return err
		}
		if purchase.Status != "pending" {
			out, err = creditPurchaseRowFromGenerated(
				purchase.ID,
				purchase.AccountID,
				purchase.AmountMicros,
				purchase.Type,
				purchase.Status,
				purchase.BalanceAfterMicros,
				purchase.Actor,
				purchase.IdempotencyKey,
				purchase.StripeInvoiceID,
				purchase.ReceiptUrl,
				purchase.CreatedAt,
			)
			return err
		}

		balanceAfter := balance.BalanceMicros
		if status == "settled" {
			balanceAfter, err = checkedAddMicros(balanceAfter, purchase.AmountMicros)
			if err != nil {
				return err
			}
		} else if status != "failed" {
			return fmt.Errorf("unsupported credit purchase terminal status %q", status)
		}

		finalized, err := qtx.FinalizeCreditPurchase(ctx, db.FinalizeCreditPurchaseParams{
			Status:             status,
			BalanceAfterMicros: balanceAfter,
			ReceiptUrl:         receiptURL,
			PurchaseID:         purchaseID.String(),
			AccountID:          accountID.String(),
		})
		if err != nil {
			return err
		}
		out, err = creditPurchaseRowFromGenerated(
			finalized.ID,
			finalized.AccountID,
			finalized.AmountMicros,
			finalized.Type,
			finalized.Status,
			finalized.BalanceAfterMicros,
			finalized.Actor,
			finalized.IdempotencyKey,
			finalized.StripeInvoiceID,
			finalized.ReceiptUrl,
			finalized.CreatedAt,
		)
		return err
	})
	return out, err
}

func (s *pgxStore) UpsertCreditAutoTopUp(ctx context.Context, accountID uuid.UUID, cfg AutoTopUpConfig) (AutoTopUpConfig, error) {
	row, err := s.q.UpsertCreditAutoTopUp(ctx, db.UpsertCreditAutoTopUpParams{
		AccountID:       accountID.String(),
		Enabled:         cfg.Enabled,
		ThresholdMicros: cfg.ThresholdMicros,
		AmountMicros:    cfg.AmountMicros,
		PaymentMethodID: cfg.PaymentMethodID,
	})
	if err != nil {
		return AutoTopUpConfig{}, err
	}
	return AutoTopUpConfig{
		Enabled:         row.Enabled,
		ThresholdMicros: row.ThresholdMicros,
		AmountMicros:    row.AmountMicros,
		PaymentMethodID: row.PaymentMethodID,
	}, nil
}

func (s *pgxStore) SetCreditBillingMode(ctx context.Context, accountID uuid.UUID, mode BillingMode, creditLimitMicros int64) error {
	_, err := s.q.SetCreditAccountBillingMode(ctx, db.SetCreditAccountBillingModeParams{
		BillingMode:       string(mode),
		CreditLimitMicros: creditLimitMicros,
		AccountID:         accountID.String(),
	})
	return err
}

func (s *pgxStore) DistributorCustomerAccount(ctx context.Context, distributorOrgID, customerOrgID uuid.UUID) (uuid.UUID, bool, error) {
	id, err := s.q.GetDistributorCustomerAccount(ctx, db.GetDistributorCustomerAccountParams{
		DistributorOrgID: distributorOrgID.String(),
		CustomerOrgID:    customerOrgID.String(),
	})
	return uuidRowFound(id, err)
}

func (s *pgxStore) ListDistributorCustomerStates(ctx context.Context, distributorOrgID uuid.UUID) ([]DistributorCustomerState, error) {
	rows, err := s.q.ListDistributorCustomerSnapshots(ctx, distributorOrgID.String())
	if err != nil {
		return nil, err
	}
	out := make([]DistributorCustomerState, 0, len(rows))
	for _, row := range rows {
		customerOrgID, err := uuid.Parse(row.CustomerOrgID)
		if err != nil {
			return nil, fmt.Errorf("parse distributor customer org id %q: %w", row.CustomerOrgID, err)
		}
		mode, err := parseCreditBillingMode(row.BillingMode)
		if err != nil {
			return nil, err
		}
		out = append(out, DistributorCustomerState{
			CustomerOrgID:            customerOrgID,
			BillingMode:              mode,
			CreditLimitMicros:        row.CreditLimitMicros,
			AutoTopUpEnabled:         row.AutoTopupEnabled,
			AutoTopUpThresholdMicros: row.AutoTopupThresholdMicros,
			BalanceMicros:            row.BalanceMicros,
		})
	}
	return out, nil
}

func (s *pgxStore) InsertCreditGrant(ctx context.Context, accountID uuid.UUID, amountMicros int64, actor, idempotencyKey string, expiresAt *time.Time) (CreditLedgerRecord, error) {
	var row db.InsertSettledCreditGrantRow
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		qtx := s.q.WithTx(tx)
		balance, err := qtx.LockCreditAccountBalance(ctx, accountID.String())
		if err != nil {
			return err
		}
		balanceAfter, err := checkedAddMicros(balance.BalanceMicros, amountMicros)
		if err != nil {
			return err
		}
		row, err = qtx.InsertSettledCreditGrant(ctx, db.InsertSettledCreditGrantParams{
			AccountID:          accountID.String(),
			AmountMicros:       amountMicros,
			BalanceAfterMicros: balanceAfter,
			Actor:              actor,
			IdempotencyKey:     idempotencyKey,
			ExpiresAt:          nullableTimestamptz(expiresAt),
		})
		return err
	})
	if err != nil {
		return CreditLedgerRecord{}, err
	}
	id, err := uuid.Parse(row.ID)
	if err != nil {
		return CreditLedgerRecord{}, err
	}
	return CreditLedgerRecord{
		ID:                 id,
		AccountID:          accountID,
		AmountMicros:       amountMicros,
		Type:               "grant",
		Status:             "settled",
		BalanceAfterMicros: row.BalanceAfterMicros,
		Actor:              actor,
		IdempotencyKey:     idempotencyKey,
		ExpiresAt:          expiresAt,
	}, nil
}

func parseCreditBillingMode(raw string) (BillingMode, error) {
	mode := BillingMode(raw)
	if mode != BillingModeStandard && mode != BillingModeCredits {
		return "", fmt.Errorf("unknown credit billing mode %q", raw)
	}
	return mode, nil
}

func creditPurchaseRowFromGenerated(idRaw, accountIDRaw string, amountMicros int64, typ, status string, balanceAfterMicros int64, actor, idempotencyKey, stripeInvoiceID, receiptURL string, createdAt time.Time) (CreditPurchaseRow, error) {
	id, err := uuid.Parse(idRaw)
	if err != nil {
		return CreditPurchaseRow{}, err
	}
	accountID, err := uuid.Parse(accountIDRaw)
	if err != nil {
		return CreditPurchaseRow{}, err
	}
	return CreditPurchaseRow{
		ID:                 id,
		AccountID:          accountID,
		AmountMicros:       amountMicros,
		Type:               typ,
		Status:             status,
		BalanceAfterMicros: balanceAfterMicros,
		Actor:              actor,
		IdempotencyKey:     idempotencyKey,
		StripeInvoiceID:    stripeInvoiceID,
		ReceiptURL:         receiptURL,
		CreatedAt:          createdAt,
	}, nil
}

func creditLedgerRecordFromGenerated(idRaw, accountIDRaw string, amountMicros int64, typ, status string, balanceAfterMicros int64, actor, idempotencyKey, stripeInvoiceID, receiptURL string, expiresAt pgtype.Timestamptz, createdAt time.Time) (CreditLedgerRecord, error) {
	id, err := uuid.Parse(idRaw)
	if err != nil {
		return CreditLedgerRecord{}, err
	}
	accountID, err := uuid.Parse(accountIDRaw)
	if err != nil {
		return CreditLedgerRecord{}, err
	}
	return CreditLedgerRecord{
		ID:                 id,
		AccountID:          accountID,
		AmountMicros:       amountMicros,
		Type:               typ,
		Status:             status,
		BalanceAfterMicros: balanceAfterMicros,
		Actor:              actor,
		IdempotencyKey:     idempotencyKey,
		StripeInvoiceID:    stripeInvoiceID,
		ReceiptURL:         receiptURL,
		ExpiresAt:          nullableTimeValue(expiresAt),
		CreatedAt:          createdAt,
	}, nil
}

func checkedAddMicros(a, b int64) (int64, error) {
	if (b > 0 && a > math.MaxInt64-b) || (b < 0 && a < math.MinInt64-b) {
		return 0, fmt.Errorf("credit balance overflows int64 micros")
	}
	return a + b, nil
}

func nullableTextValue(value pgtype.Text) string {
	if !value.Valid {
		return ""
	}
	return value.String
}

func nullableTimeValue(value pgtype.Timestamptz) *time.Time {
	if !value.Valid {
		return nil
	}
	t := value.Time
	return &t
}

func nullableTimestamptz(value *time.Time) pgtype.Timestamptz {
	if value == nil {
		return pgtype.Timestamptz{}
	}
	return pgtype.Timestamptz{Time: *value, Valid: true}
}

// centsNumericToMicros converts the invoices mirror's NUMERIC whole Stripe
// cents to int64 micro-dollars (×10_000) — the same store-boundary conversion
// usage's invoice reads perform (usage can't be imported here: it depends on
// this package). Mirror amounts are whole cents by construction, so the
// strict integer decode (Int64Value errors on any fractional value) is
// honest, never a silent rounding.
func centsNumericToMicros(n pgtype.Numeric) (int64, error) {
	v, err := n.Int64Value()
	if err != nil {
		return 0, err
	}
	cents := v.Int64
	const microsPerCent = 10_000
	if cents > math.MaxInt64/microsPerCent || cents < math.MinInt64/microsPerCent {
		return 0, fmt.Errorf("cents amount %d overflows int64 micros", cents)
	}
	return cents * microsPerCent, nil
}

// centsNumeric encodes whole Stripe minor units as an exact NUMERIC with no
// scale. It mirrors webhook.centsNumeric so both callers feed
// db.ApplyInvoiceStatus the same representation without coupling packages.
func centsNumeric(cents int64) (pgtype.Numeric, error) {
	var n pgtype.Numeric
	if err := n.Scan(strconv.FormatInt(cents, 10)); err != nil {
		return pgtype.Numeric{}, err
	}
	return n, nil
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
