//go:build integration

package billing_test

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/mirrorstack-ai/billing-engine/internal/account/billing"
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
