//go:build integration

package creditpurchase_test

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/mirrorstack-ai/billing-engine/internal/account/creditpurchase"
	"github.com/mirrorstack-ai/billing-engine/internal/shared/testutil"
)

func TestStoreFailExactVoidCommitsOnceWithoutAdvancingBalance(t *testing.T) {
	pool := testutil.NewTestDB(t)
	ctx := context.Background()
	accountID, ledgerID := uuid.New(), uuid.New()
	_, err := pool.Exec(ctx,
		`INSERT INTO ms_billing.accounts (id, owner_kind, owner_user_id)
		 VALUES ($1, 'user', $2)`,
		accountID,
		uuid.New(),
	)
	require.NoError(t, err)
	_, err = pool.Exec(ctx,
		`INSERT INTO ms_billing.credit_ledger
		   (account_id, amount_micros, type, status, balance_after_micros,
		    actor, idempotency_key)
		 VALUES ($1, 1000000, 'grant', 'settled', 1000000,
		         'system', $2)`,
		accountID,
		"grant:"+uuid.NewString(),
	)
	require.NoError(t, err)
	_, err = pool.Exec(ctx,
		`INSERT INTO ms_billing.credit_ledger
		   (id, account_id, amount_micros, type, status, balance_after_micros,
		    actor, idempotency_key, stripe_invoice_id)
		 VALUES ($1, $2, 5000000, 'purchase', 'pending', 6000000,
		         'self', $3, 'in_manual_void')`,
		ledgerID,
		accountID,
		"purchase:"+ledgerID.String(),
	)
	require.NoError(t, err)

	store := creditpurchase.NewStore(pool)
	attempt, found, err := store.FindByStripeInvoice(ctx, "in_manual_void")
	require.NoError(t, err)
	require.True(t, found)

	first, transitioned, err := store.Fail(
		ctx,
		attempt,
		"https://stripe.test/in_manual_void",
	)
	require.NoError(t, err)
	require.True(t, transitioned)
	require.Equal(t, "failed", first.Status)

	second, transitioned, err := store.Fail(
		ctx,
		attempt,
		"https://stripe.test/in_manual_void",
	)
	require.NoError(t, err)
	require.False(t, transitioned)
	require.Equal(t, "failed", second.Status)

	var (
		status       string
		balanceAfter int64
		receiptURL   string
		settled      int64
	)
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT status, balance_after_micros, COALESCE(receipt_url, '')
		   FROM ms_billing.credit_ledger
		  WHERE id = $1`,
		ledgerID,
	).Scan(&status, &balanceAfter, &receiptURL))
	require.Equal(t, "failed", status)
	require.Equal(t, int64(1_000_000), balanceAfter)
	require.Equal(t, "https://stripe.test/in_manual_void", receiptURL)
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT COALESCE(SUM(amount_micros), 0)::bigint
		   FROM ms_billing.credit_ledger
		  WHERE account_id = $1 AND status = 'settled'`,
		accountID,
	).Scan(&settled))
	require.Equal(t, int64(1_000_000), settled)
}
