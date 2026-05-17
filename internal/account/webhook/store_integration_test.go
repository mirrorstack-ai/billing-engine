//go:build integration

package webhook_test

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/mirrorstack-ai/billing-engine/internal/account/webhook"
	"github.com/mirrorstack-ai/billing-engine/internal/shared/testutil"
)

func TestPgxStore_MarkEventProcessed_FirstTimeAndDuplicate(t *testing.T) {
	pool := testutil.NewTestDB(t)
	store := webhook.NewStore(pool)

	firstTime, err := store.MarkEventProcessed(context.Background(), "evt_test_1", "customer.created")
	require.NoError(t, err)
	require.True(t, firstTime)

	firstTime2, err := store.MarkEventProcessed(context.Background(), "evt_test_1", "customer.created")
	require.NoError(t, err)
	require.False(t, firstTime2, "second insert of same event_id should return firstTime=false")
}

func TestPgxStore_TouchAccountByStripeCustomer_FoundAndNotFound(t *testing.T) {
	pool := testutil.NewTestDB(t)
	store := webhook.NewStore(pool)
	_ = seedAccount(t, pool, "cus_touch_a")

	found, err := store.TouchAccountByStripeCustomer(context.Background(), "cus_touch_a")
	require.NoError(t, err)
	require.True(t, found)

	found, err = store.TouchAccountByStripeCustomer(context.Background(), "cus_nonexistent")
	require.NoError(t, err)
	require.False(t, found, "non-existent stripe_customer_id should return found=false")
}

func TestPgxStore_InsertPaymentMethod_Success(t *testing.T) {
	pool := testutil.NewTestDB(t)
	store := webhook.NewStore(pool)
	_ = seedAccount(t, pool, "cus_pm_x")

	found, err := store.InsertPaymentMethod(context.Background(), "cus_pm_x", webhook.InsertPaymentMethodParams{
		StripePaymentMethodID: "pm_new_1",
		Brand:                 "visa",
		Last4:                 "4242",
		ExpMonth:              12,
		ExpYear:               2099,
	})
	require.NoError(t, err)
	require.True(t, found)

	var count int
	err = pool.QueryRow(context.Background(),
		`SELECT count(*) FROM ms_billing.payment_methods_mirror WHERE stripe_payment_method_id = $1`,
		"pm_new_1").Scan(&count)
	require.NoError(t, err)
	require.Equal(t, 1, count)
}

func TestPgxStore_InsertPaymentMethod_Drift_NoAccountsRow(t *testing.T) {
	// The CTE inserts via `SELECT acct.id ... FROM acct` — if the acct
	// CTE produces zero rows (no matching stripe_customer_id), nothing
	// is inserted and RowsAffected=0. The follow-up existence check
	// confirms drift; store returns found=false.
	pool := testutil.NewTestDB(t)
	store := webhook.NewStore(pool)

	found, err := store.InsertPaymentMethod(context.Background(), "cus_orphan", webhook.InsertPaymentMethodParams{
		StripePaymentMethodID: "pm_orphan",
		Brand:                 "visa",
		Last4:                 "4242",
		ExpMonth:              12,
		ExpYear:               2099,
	})
	require.NoError(t, err)
	require.False(t, found, "expected drift signal (no accounts row)")

	var count int
	err = pool.QueryRow(context.Background(),
		`SELECT count(*) FROM ms_billing.payment_methods_mirror WHERE stripe_payment_method_id = $1`,
		"pm_orphan").Scan(&count)
	require.NoError(t, err)
	require.Equal(t, 0, count, "no mirror row should have been inserted")
}

func TestPgxStore_InsertPaymentMethod_Idempotent_AccountExistsPMExists(t *testing.T) {
	// PM already in mirror → ON CONFLICT DO NOTHING → RowsAffected=0
	// BUT the follow-up existence check confirms the accounts row IS
	// present, so found=true (idempotent retry; mirror unchanged).
	pool := testutil.NewTestDB(t)
	store := webhook.NewStore(pool)
	_ = seedAccount(t, pool, "cus_idem")

	found, err := store.InsertPaymentMethod(context.Background(), "cus_idem", webhook.InsertPaymentMethodParams{
		StripePaymentMethodID: "pm_idem_1",
		Brand:                 "visa",
		Last4:                 "4242",
		ExpMonth:              12,
		ExpYear:               2099,
	})
	require.NoError(t, err)
	require.True(t, found)

	// Duplicate insert (simulated Stripe webhook retry).
	found, err = store.InsertPaymentMethod(context.Background(), "cus_idem", webhook.InsertPaymentMethodParams{
		StripePaymentMethodID: "pm_idem_1",
		Brand:                 "visa",
		Last4:                 "4242",
		ExpMonth:              12,
		ExpYear:               2099,
	})
	require.NoError(t, err)
	require.True(t, found, "duplicate insert should return found=true (idempotent)")

	var count int
	err = pool.QueryRow(context.Background(),
		`SELECT count(*) FROM ms_billing.payment_methods_mirror WHERE stripe_payment_method_id = $1`,
		"pm_idem_1").Scan(&count)
	require.NoError(t, err)
	require.Equal(t, 1, count)
}

func TestPgxStore_SoftDeletePaymentMethod_FoundAndIdempotent(t *testing.T) {
	pool := testutil.NewTestDB(t)
	store := webhook.NewStore(pool)
	accountID := seedAccount(t, pool, "cus_del")
	seedActivePM(t, pool, accountID, "pm_del_1", 12, 2099)

	found, err := store.SoftDeletePaymentMethod(context.Background(), "pm_del_1")
	require.NoError(t, err)
	require.True(t, found)

	// Second call: row is already soft-deleted → found=false (idempotent).
	found, err = store.SoftDeletePaymentMethod(context.Background(), "pm_del_1")
	require.NoError(t, err)
	require.False(t, found, "soft-delete on already-deleted row is a no-op")

	var hasDeleted bool
	err = pool.QueryRow(context.Background(),
		`SELECT deleted_at IS NOT NULL FROM ms_billing.payment_methods_mirror WHERE stripe_payment_method_id = $1`,
		"pm_del_1").Scan(&hasDeleted)
	require.NoError(t, err)
	require.True(t, hasDeleted)
}

func TestPgxStore_SetDefaultPaymentMethod_FlipsOneAndUnflipsRest(t *testing.T) {
	pool := testutil.NewTestDB(t)
	store := webhook.NewStore(pool)
	accountID := seedAccount(t, pool, "cus_def")
	seedActivePM(t, pool, accountID, "pm_def_a", 12, 2099)
	seedActivePM(t, pool, accountID, "pm_def_b", 6, 2099)
	seedActivePM(t, pool, accountID, "pm_def_c", 1, 2099)

	require.NoError(t, store.SetDefaultPaymentMethod(context.Background(), "cus_def", "pm_def_b"))

	defaults := readDefaultFlags(t, pool, accountID)
	require.False(t, defaults["pm_def_a"])
	require.True(t, defaults["pm_def_b"])
	require.False(t, defaults["pm_def_c"])

	// Flip default to pm_def_c.
	require.NoError(t, store.SetDefaultPaymentMethod(context.Background(), "cus_def", "pm_def_c"))

	defaults = readDefaultFlags(t, pool, accountID)
	require.False(t, defaults["pm_def_a"])
	require.False(t, defaults["pm_def_b"], "previous default should flip back to false")
	require.True(t, defaults["pm_def_c"])

	// Clear default everywhere.
	require.NoError(t, store.SetDefaultPaymentMethod(context.Background(), "cus_def", ""))

	defaults = readDefaultFlags(t, pool, accountID)
	require.False(t, defaults["pm_def_a"])
	require.False(t, defaults["pm_def_b"])
	require.False(t, defaults["pm_def_c"])
}

// --- helpers --------------------------------------------------------------

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

func seedActivePM(t *testing.T, pool *pgxpool.Pool, accountID uuid.UUID, pmID string, expMonth, expYear int) {
	t.Helper()
	_, err := pool.Exec(context.Background(),
		`INSERT INTO ms_billing.payment_methods_mirror
		   (account_id, stripe_payment_method_id, brand, last4, exp_month, exp_year, is_default)
		 VALUES ($1, $2, 'visa', '4242', $3, $4, false)`,
		accountID, pmID, expMonth, expYear,
	)
	require.NoError(t, err)
}

func readDefaultFlags(t *testing.T, pool *pgxpool.Pool, accountID uuid.UUID) map[string]bool {
	t.Helper()
	rows, err := pool.Query(context.Background(),
		`SELECT stripe_payment_method_id, is_default FROM ms_billing.payment_methods_mirror
		 WHERE account_id = $1 AND deleted_at IS NULL`,
		accountID,
	)
	require.NoError(t, err)
	defer rows.Close()
	out := map[string]bool{}
	for rows.Next() {
		var pmID string
		var def bool
		require.NoError(t, rows.Scan(&pmID, &def))
		out[pmID] = def
	}
	require.NoError(t, rows.Err())
	return out
}
