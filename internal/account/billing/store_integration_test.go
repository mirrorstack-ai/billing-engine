//go:build integration

package billing_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/mirrorstack-ai/billing-engine/internal/account/billing"
	"github.com/mirrorstack-ai/billing-engine/internal/account/webhook"
	billingstripe "github.com/mirrorstack-ai/billing-engine/internal/shared/stripe"
	"github.com/mirrorstack-ai/billing-engine/internal/shared/testutil"
)

func TestPgxStore_EnsureAccount_Creates(t *testing.T) {
	pool := testutil.NewTestDB(t)
	store := billing.NewStore(pool)
	userID := uuid.New()

	id1, sid1, err := store.EnsureAccount(context.Background(), userID)
	require.NoError(t, err)
	require.NotEqual(t, uuid.Nil, id1)
	require.Equal(t, "", sid1, "stripe_customer_id should be empty on first create")
}

func TestPgxStore_EnsureAccount_Idempotent(t *testing.T) {
	pool := testutil.NewTestDB(t)
	store := billing.NewStore(pool)
	userID := uuid.New()

	id1, _, err := store.EnsureAccount(context.Background(), userID)
	require.NoError(t, err)
	id2, _, err := store.EnsureAccount(context.Background(), userID)
	require.NoError(t, err)

	require.Equal(t, id1, id2, "second call should return the same account_id")
}

func TestPgxStore_EnsureAccount_Concurrent_NoDuplicates(t *testing.T) {
	// Per-user advisory lock serializes concurrent inserts. After N
	// concurrent EnsureAccount calls for the same user_id, exactly
	// one row must exist.
	pool := testutil.NewTestDB(t)
	store := billing.NewStore(pool)
	userID := uuid.New()

	const goroutines = 8
	ids := make([]uuid.UUID, goroutines)
	var wg sync.WaitGroup
	for i := range goroutines {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			id, _, err := store.EnsureAccount(context.Background(), userID)
			if err != nil {
				t.Errorf("goroutine %d: %v", i, err)
				return
			}
			ids[i] = id
		}(i)
	}
	wg.Wait()

	for i := 1; i < goroutines; i++ {
		require.Equal(t, ids[0], ids[i], "all concurrent calls must return the same account_id")
	}

	// Authoritative check: exactly one row in the DB.
	var count int
	err := pool.QueryRow(context.Background(),
		`SELECT count(*) FROM ms_billing.accounts WHERE owner_user_id = $1`, userID).Scan(&count)
	require.NoError(t, err)
	require.Equal(t, 1, count)
}

func TestPgxStore_SetStripeCustomer_Found(t *testing.T) {
	pool := testutil.NewTestDB(t)
	store := billing.NewStore(pool)
	userID := uuid.New()
	accountID, _, err := store.EnsureAccount(context.Background(), userID)
	require.NoError(t, err)

	err = store.SetStripeCustomer(context.Background(), accountID, "cus_test_x")
	require.NoError(t, err)

	// Verify via a fresh EnsureAccount lookup.
	_, sid, err := store.EnsureAccount(context.Background(), userID)
	require.NoError(t, err)
	require.Equal(t, "cus_test_x", sid)
}

func TestPgxStore_SetStripeCustomer_NoRow_ReturnsErrAccountNotFound(t *testing.T) {
	pool := testutil.NewTestDB(t)
	store := billing.NewStore(pool)

	err := store.SetStripeCustomer(context.Background(), uuid.New(), "cus_orphan")

	require.Error(t, err)
	require.True(t, errors.Is(err, billing.ErrAccountNotFound), "expected ErrAccountNotFound, got %v", err)
}

func TestPgxStore_AccountByUser_FoundAndNotFound(t *testing.T) {
	pool := testutil.NewTestDB(t)
	store := billing.NewStore(pool)
	userID := uuid.New()

	_, found, err := store.AccountByUser(context.Background(), userID)
	require.NoError(t, err)
	require.False(t, found, "should be not-found before EnsureAccount")

	accountID, _, err := store.EnsureAccount(context.Background(), userID)
	require.NoError(t, err)

	got, found, err := store.AccountByUser(context.Background(), userID)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, accountID, got)
}

func TestPgxStore_HasUsablePaymentMethod_None(t *testing.T) {
	pool := testutil.NewTestDB(t)
	store := billing.NewStore(pool)
	accountID := seedAccount(t, pool, "cus_test_a")

	has, err := store.HasUsablePaymentMethod(context.Background(), accountID)
	require.NoError(t, err)
	require.False(t, has)
}

func TestPgxStore_HasUsablePaymentMethod_NotExpired(t *testing.T) {
	pool := testutil.NewTestDB(t)
	store := billing.NewStore(pool)
	accountID := seedAccount(t, pool, "cus_test_b")
	seedPaymentMethod(t, pool, accountID, "pm_b", 12, 2099, false, false)

	has, err := store.HasUsablePaymentMethod(context.Background(), accountID)
	require.NoError(t, err)
	require.True(t, has)
}

func TestPgxStore_HasUsablePaymentMethod_Expired(t *testing.T) {
	pool := testutil.NewTestDB(t)
	store := billing.NewStore(pool)
	accountID := seedAccount(t, pool, "cus_test_c")
	seedPaymentMethod(t, pool, accountID, "pm_c_expired", 1, 1970, false, false)

	has, err := store.HasUsablePaymentMethod(context.Background(), accountID)
	require.NoError(t, err)
	require.False(t, has, "1970-01 should be expired")
}

func TestPgxStore_HasUsablePaymentMethod_SoftDeleted_NotUsable(t *testing.T) {
	pool := testutil.NewTestDB(t)
	store := billing.NewStore(pool)
	accountID := seedAccount(t, pool, "cus_test_d")
	seedPaymentMethod(t, pool, accountID, "pm_d_deleted", 12, 2099, false, true)

	has, err := store.HasUsablePaymentMethod(context.Background(), accountID)
	require.NoError(t, err)
	require.False(t, has, "soft-deleted PM should not count as usable")
}

func TestPgxStore_ListPaymentMethods_FiltersSoftDeleted(t *testing.T) {
	pool := testutil.NewTestDB(t)
	store := billing.NewStore(pool)
	accountID := seedAccount(t, pool, "cus_test_e")

	seedPaymentMethod(t, pool, accountID, "pm_e_active1", 12, 2099, false, false)
	seedPaymentMethod(t, pool, accountID, "pm_e_active2", 6, 2099, true, false)
	seedPaymentMethod(t, pool, accountID, "pm_e_deleted", 12, 2099, false, true)

	methods, err := store.ListPaymentMethods(context.Background(), accountID)
	require.NoError(t, err)
	require.Len(t, methods, 2, "soft-deleted PM should be filtered out")

	pmIDs := make(map[string]bool)
	for _, m := range methods {
		pmIDs[m.StripePaymentMethodID] = true
	}
	require.True(t, pmIDs["pm_e_active1"])
	require.True(t, pmIDs["pm_e_active2"])
	require.False(t, pmIDs["pm_e_deleted"])
}

// --- helpers --------------------------------------------------------------
//
// seedAccount + seedPaymentMethod insert directly via raw SQL, bypassing
// the pgxStore API. This lets tests construct scenarios the store API
// can't reach (soft-deleted rows, specific expiry combinations).

func seedAccount(t *testing.T, pool *pgxpool.Pool, stripeCustomerID string) uuid.UUID {
	t.Helper()
	accountID := uuid.New()
	_, err := pool.Exec(context.Background(),
		`INSERT INTO ms_billing.accounts (id, owner_kind, owner_user_id, stripe_customer_id)
		 VALUES ($1, 'user', $2, $3)`,
		accountID, uuid.New(), stripeCustomerID,
	)
	require.NoError(t, err)
	return accountID
}

func seedPaymentMethod(t *testing.T, pool *pgxpool.Pool, accountID uuid.UUID, pmID string, expMonth, expYear int, isDefault, deleted bool) {
	t.Helper()
	if deleted {
		_, err := pool.Exec(context.Background(),
			`INSERT INTO ms_billing.payment_methods_mirror
			   (account_id, stripe_payment_method_id, brand, last4, exp_month, exp_year, is_default, deleted_at)
			 VALUES ($1, $2, 'visa', '4242', $3, $4, $5, now())`,
			accountID, pmID, expMonth, expYear, isDefault,
		)
		require.NoError(t, err)
		return
	}
	_, err := pool.Exec(context.Background(),
		`INSERT INTO ms_billing.payment_methods_mirror
		   (account_id, stripe_payment_method_id, brand, last4, exp_month, exp_year, is_default)
		 VALUES ($1, $2, 'visa', '4242', $3, $4, $5)`,
		accountID, pmID, expMonth, expYear, isDefault,
	)
	require.NoError(t, err)
}

// seedMirrorInvoice inserts one ms_billing.invoices row (whole Stripe cents,
// explicit created_at so ordering assertions are deterministic). number stays
// NULL — the pre-enrichment state — unless provided.
func seedMirrorInvoice(t *testing.T, pool *pgxpool.Pool, acct uuid.UUID, stripeID, status string, dueCents int64, createdAt time.Time) uuid.UUID {
	t.Helper()
	id := uuid.New()
	_, err := pool.Exec(context.Background(),
		`INSERT INTO ms_billing.invoices
		   (id, account_id, stripe_invoice_id, status, amount_due, amount_paid, currency, created_at)
		 VALUES ($1, $2, $3, $4, $5, 0, 'usd', $6)`,
		id, acct, stripeID, status, dueCents, createdAt,
	)
	require.NoError(t, err)
	return id
}

// TestPgxStore_UnpaidInvoices_PredicateAndDecode pins the UNPAID predicate
// (funding-gates design) against the real SQL: only open/uncollectible rows
// with amount_due > 0 count — paid, void, draft, and zero-amount rows are all
// excluded — and the list decodes whole cents into ×10_000 micros,
// oldest-first, NULL number as "".
func TestPgxStore_UnpaidInvoices_PredicateAndDecode(t *testing.T) {
	pool := testutil.NewTestDB(t)
	store := billing.NewStore(pool)
	ctx := context.Background()

	acct := seedAccount(t, pool, "cus_unpaid_1")
	base := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)

	uncol := seedMirrorInvoice(t, pool, acct, "in_uncol", "uncollectible", 500, base) // unpaid (oldest)
	open := seedMirrorInvoice(t, pool, acct, "in_open", "open", 2000, base.AddDate(0, 1, 0))
	seedMirrorInvoice(t, pool, acct, "in_paid", "paid", 2000, base)   // clean
	seedMirrorInvoice(t, pool, acct, "in_void", "void", 2000, base)   // debt forgiven
	seedMirrorInvoice(t, pool, acct, "in_draft", "draft", 2000, base) // never finalized
	seedMirrorInvoice(t, pool, acct, "in_zero", "open", 0, base)      // no money owed

	count, err := store.UnpaidInvoiceCount(ctx, acct)
	require.NoError(t, err)
	require.Equal(t, 2, count)

	rows, err := store.ListUnpaidInvoices(ctx, acct)
	require.NoError(t, err)
	require.Len(t, rows, 2)
	require.Equal(t, uncol, rows[0].ID, "oldest first")
	require.EqualValues(t, 5_000_000, rows[0].AmountDueMicros, "500 cents → 5e6 micros")
	require.Equal(t, "", rows[0].Number, "NULL number decodes to empty")
	require.Equal(t, open, rows[1].ID)
	require.EqualValues(t, 20_000_000, rows[1].AmountDueMicros)
}

// TestPgxStore_InvoiceForPayment_OwnershipScope pins the PayInvoice ownership
// gate: the row resolves only under its own account; a foreign account (or an
// unknown id) is found=false — indistinguishable on purpose.
func TestPgxStore_InvoiceForPayment_OwnershipScope(t *testing.T) {
	pool := testutil.NewTestDB(t)
	store := billing.NewStore(pool)
	ctx := context.Background()

	owner := seedAccount(t, pool, "cus_pay_owner")
	stranger := seedAccount(t, pool, "cus_pay_stranger")
	invID := seedMirrorInvoice(t, pool, owner, "in_owned", "open", 2000, time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC))

	target, found, err := store.InvoiceForPayment(ctx, invID, owner)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, "in_owned", target.StripeInvoiceID)
	require.Equal(t, "open", target.Status)

	_, found, err = store.InvoiceForPayment(ctx, invID, stranger)
	require.NoError(t, err)
	require.False(t, found, "another account's invoice must not resolve")

	_, found, err = store.InvoiceForPayment(ctx, uuid.New(), owner)
	require.NoError(t, err)
	require.False(t, found)
}

func TestPgxStore_SyncInvoiceMirror_Integration(t *testing.T) {
	pool := testutil.NewTestDB(t)
	store := billing.NewStore(pool)
	ctx := context.Background()

	accountID := seedAccount(t, pool, "cus_pay_sync")
	stripeInvoiceID := "in_pay_sync"
	_, err := pool.Exec(ctx,
		`INSERT INTO ms_billing.invoices
		   (id, account_id, stripe_invoice_id, status, amount_due, amount_paid, currency, ever_failed)
		 VALUES ($1, $2, $3, 'open', 200, 0, 'usd', true)`,
		uuid.New(), accountID, stripeInvoiceID,
	)
	require.NoError(t, err)

	type invoiceState struct {
		status                string
		amountPaid, amountDue int64
		everFailed            bool
	}
	readState := func() invoiceState {
		t.Helper()
		var state invoiceState
		err := pool.QueryRow(ctx,
			`SELECT status, amount_paid, amount_due, ever_failed
			 FROM ms_billing.invoices
			 WHERE stripe_invoice_id = $1`,
			stripeInvoiceID,
		).Scan(&state.status, &state.amountPaid, &state.amountDue, &state.everFailed)
		require.NoError(t, err)
		return state
	}

	applied, err := store.SyncInvoiceMirror(ctx, billingstripe.Invoice{
		ID:         stripeInvoiceID,
		Status:     "paid",
		AmountPaid: 200,
		AmountDue:  200,
	})
	require.NoError(t, err)
	require.True(t, applied)

	paidState := invoiceState{
		status:     "paid",
		amountPaid: 200,
		amountDue:  200,
		everFailed: true,
	}
	require.Equal(t, paidState, readState(), "sync settle must retain the webhook-authored failure latch")

	found, err := webhook.NewStore(pool).ApplyInvoiceStatus(ctx, webhook.ApplyInvoiceStatusParams{
		StripeInvoiceID: stripeInvoiceID,
		Status:          "paid",
		AmountPaidCents: 200,
		AmountDueCents:  200,
	})
	require.NoError(t, err)
	require.True(t, found, "identical webhook replay must pass the guard so policy effects can still run")
	require.Equal(t, paidState, readState(), "webhook replay must leave the settled mirror unchanged")

	applied, err = store.SyncInvoiceMirror(ctx, billingstripe.Invoice{
		ID:     stripeInvoiceID,
		Status: "open",
	})
	require.NoError(t, err)
	require.False(t, applied, "an open snapshot must not regress a paid mirror")
	require.Equal(t, paidState, readState(), "rejected regression must not alter the settled mirror")
}
