//go:build integration

package creditledger_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/mirrorstack-ai/billing-engine/internal/account/creditledger"
	"github.com/mirrorstack-ai/billing-engine/internal/shared/testutil"
)

func TestSettleStripeInvoice_ManualPurchaseExactlyOnce(t *testing.T) {
	pool := testutil.NewTestDB(t)
	store := creditledger.NewStore(pool)
	accountID := seedCreditAccount(t, pool)
	seedCreditGrant(t, pool, accountID, 1_000_000)
	ledgerID := seedManualPurchase(t, pool, accountID, "in_manual", "pending", 5_005_000)

	first, err := store.SettleStripeInvoice(
		context.Background(),
		"in_manual",
		501,
		"USD",
		"https://stripe.test/in_manual",
	)
	require.NoError(t, err)
	require.Equal(t, creditledger.Settlement{
		Found: true, Transitioned: true,
		AccountID: accountID, LedgerID: ledgerID, Type: "purchase",
	}, first)

	second, err := store.SettleStripeInvoice(
		context.Background(),
		"in_manual",
		501,
		"usd",
		"https://stripe.test/in_manual",
	)
	require.NoError(t, err)
	require.True(t, second.Found)
	require.False(t, second.Transitioned)

	status, balanceAfter, failureCode, receiptURL := readCreditAttempt(t, pool, ledgerID)
	require.Equal(t, "settled", status)
	require.Equal(t, int64(6_005_000), balanceAfter)
	require.Empty(t, failureCode)
	require.Equal(t, "https://stripe.test/in_manual", receiptURL)
	require.Equal(t, int64(6_005_000), settledBalance(t, pool, accountID))
}

func TestSettleStripeInvoice_PaidIsHighestForFailedAutoTopUp(t *testing.T) {
	pool := testutil.NewTestDB(t)
	store := creditledger.NewStore(pool)
	accountID := seedCreditAccount(t, pool)
	paymentMethodID := seedCreditPaymentMethod(t, pool, accountID)
	ledgerID := seedFailedAutoTopUp(
		t, pool, accountID, paymentMethodID, "in_late_paid", 5_000_000,
	)

	result, err := store.SettleStripeInvoice(
		context.Background(),
		"in_late_paid",
		500,
		"usd",
		"https://stripe.test/in_late_paid",
	)

	require.NoError(t, err)
	require.True(t, result.Found)
	require.True(t, result.Transitioned)
	require.Equal(t, "auto_topup", result.Type)
	status, balanceAfter, failureCode, _ := readCreditAttempt(t, pool, ledgerID)
	require.Equal(t, "settled", status)
	require.Equal(t, int64(5_000_000), balanceAfter)
	require.Empty(t, failureCode, "paid-is-highest recovery clears terminal failure")
	require.Equal(t, int64(5_000_000), settledBalance(t, pool, accountID))
}

func TestSettleManualStripeInvoice_PaidIsHighestForFailedPurchase(t *testing.T) {
	pool := testutil.NewTestDB(t)
	store := creditledger.NewStore(pool)
	accountID := seedCreditAccount(t, pool)
	ledgerID := seedManualPurchase(
		t,
		pool,
		accountID,
		"in_manual_late_paid",
		"failed",
		5_000_000,
	)

	result, err := store.SettleManualStripeInvoice(
		context.Background(),
		"in_manual_late_paid",
		500,
		"usd",
		"https://stripe.test/in_manual_late_paid",
	)

	require.NoError(t, err)
	require.Equal(t, creditledger.Settlement{
		Found: true, Transitioned: true,
		AccountID: accountID, LedgerID: ledgerID, Type: "purchase",
	}, result)
	status, balanceAfter, failureCode, receiptURL := readCreditAttempt(
		t,
		pool,
		ledgerID,
	)
	require.Equal(t, "settled", status)
	require.Equal(t, int64(5_000_000), balanceAfter)
	require.Empty(t, failureCode)
	require.Equal(t, "https://stripe.test/in_manual_late_paid", receiptURL)
	require.Equal(t, int64(5_000_000), settledBalance(t, pool, accountID))
}

func TestSettleStripeInvoice_AmountCurrencyAndUnknownFailuresDoNotMutate(t *testing.T) {
	pool := testutil.NewTestDB(t)
	store := creditledger.NewStore(pool)
	accountID := seedCreditAccount(t, pool)
	ledgerID := seedManualPurchase(t, pool, accountID, "in_validate", "pending", 5_000_000)

	_, err := store.SettleStripeInvoice(
		context.Background(), "in_validate", 499, "usd", "",
	)
	require.ErrorContains(t, err, "expected 500")
	status, balanceAfter, _, _ := readCreditAttempt(t, pool, ledgerID)
	require.Equal(t, "pending", status)
	require.Zero(t, balanceAfter)
	require.Zero(t, settledBalance(t, pool, accountID))

	_, err = store.SettleStripeInvoice(
		context.Background(), "in_validate", 500, "eur", "",
	)
	require.ErrorContains(t, err, "is not usd")
	status, _, _, _ = readCreditAttempt(t, pool, ledgerID)
	require.Equal(t, "pending", status)

	unknown, err := store.SettleStripeInvoice(
		context.Background(), "in_unknown", 500, "usd", "",
	)
	require.NoError(t, err)
	require.Equal(t, creditledger.Settlement{}, unknown)
}

func TestSettleStripeInvoice_ConcurrentReplaysCommitOneTransition(t *testing.T) {
	pool := testutil.NewTestDB(t)
	store := creditledger.NewStore(pool)
	accountID := seedCreditAccount(t, pool)
	ledgerID := seedManualPurchase(t, pool, accountID, "in_concurrent", "pending", 5_000_000)

	const workers = 12
	start := make(chan struct{})
	results := make(chan creditledger.Settlement, workers)
	errs := make(chan error, workers)
	var wg sync.WaitGroup
	wg.Add(workers)
	for range workers {
		go func() {
			defer wg.Done()
			<-start
			result, err := store.SettleStripeInvoice(
				context.Background(), "in_concurrent", 500, "usd", "",
			)
			results <- result
			errs <- err
		}()
	}
	close(start)
	wg.Wait()
	close(results)
	close(errs)

	for err := range errs {
		require.NoError(t, err)
	}
	transitioned := 0
	for result := range results {
		require.True(t, result.Found)
		require.Equal(t, accountID, result.AccountID)
		require.Equal(t, ledgerID, result.LedgerID)
		if result.Transitioned {
			transitioned++
		}
	}
	require.Equal(t, 1, transitioned)
	require.Equal(t, int64(5_000_000), settledBalance(t, pool, accountID))
}

func seedCreditAccount(t *testing.T, pool *pgxpool.Pool) uuid.UUID {
	t.Helper()
	accountID := uuid.New()
	_, err := pool.Exec(context.Background(),
		`INSERT INTO ms_billing.accounts (id, owner_kind, owner_user_id)
		 VALUES ($1, 'user', $2)`,
		accountID, uuid.New(),
	)
	require.NoError(t, err)
	return accountID
}

func seedCreditGrant(t *testing.T, pool *pgxpool.Pool, accountID uuid.UUID, amount int64) {
	t.Helper()
	_, err := pool.Exec(context.Background(),
		`INSERT INTO ms_billing.credit_ledger
		   (account_id, amount_micros, type, status, balance_after_micros, actor, idempotency_key)
		 VALUES ($1, $2, 'grant', 'settled', $2, 'system', $3)`,
		accountID, amount, "grant:"+uuid.NewString(),
	)
	require.NoError(t, err)
}

func seedManualPurchase(
	t *testing.T,
	pool *pgxpool.Pool,
	accountID uuid.UUID,
	stripeInvoiceID, status string,
	amount int64,
) uuid.UUID {
	t.Helper()
	ledgerID := uuid.New()
	_, err := pool.Exec(context.Background(),
		`INSERT INTO ms_billing.credit_ledger
		   (id, account_id, amount_micros, type, status, balance_after_micros,
		    actor, idempotency_key, stripe_invoice_id)
		 VALUES ($1, $2, $3, 'purchase', $4, 0, 'self', $5, $6)`,
		ledgerID, accountID, amount, status, "purchase:"+ledgerID.String(), stripeInvoiceID,
	)
	require.NoError(t, err)
	return ledgerID
}

func seedCreditPaymentMethod(t *testing.T, pool *pgxpool.Pool, accountID uuid.UUID) uuid.UUID {
	t.Helper()
	paymentMethodID := uuid.New()
	_, err := pool.Exec(context.Background(),
		`INSERT INTO ms_billing.payment_methods_mirror
		   (id, account_id, stripe_payment_method_id, brand, last4, exp_month, exp_year)
		 VALUES ($1, $2, $3, 'visa', '4242', 12, 2099)`,
		paymentMethodID, accountID, "pm_"+paymentMethodID.String(),
	)
	require.NoError(t, err)
	return paymentMethodID
}

func seedFailedAutoTopUp(
	t *testing.T,
	pool *pgxpool.Pool,
	accountID, paymentMethodID uuid.UUID,
	stripeInvoiceID string,
	amount int64,
) uuid.UUID {
	t.Helper()
	ledgerID := uuid.New()
	createdAt := time.Date(2026, time.July, 25, 12, 0, 0, 0, time.UTC)
	_, err := pool.Exec(context.Background(),
		`INSERT INTO ms_billing.credit_ledger
		   (id, account_id, amount_micros, type, status, balance_after_micros,
		    actor, idempotency_key, stripe_invoice_id,
		    attempt_payment_method_id, attempt_stripe_payment_method_id,
		    attempt_stripe_customer_id, attempt_expires_at, failure_code, created_at)
		 VALUES ($1, $2, $3, 'auto_topup', 'failed', 0, 'system', $4, $5,
		         $6, $7, $8, $9, 'card_declined', $10)`,
		ledgerID,
		accountID,
		amount,
		"auto:"+ledgerID.String(),
		stripeInvoiceID,
		paymentMethodID,
		"pm_"+paymentMethodID.String(),
		"cus_"+accountID.String(),
		createdAt.Add(10*time.Minute),
		createdAt,
	)
	require.NoError(t, err)
	return ledgerID
}

func readCreditAttempt(
	t *testing.T,
	pool *pgxpool.Pool,
	ledgerID uuid.UUID,
) (status string, balanceAfter int64, failureCode, receiptURL string) {
	t.Helper()
	require.NoError(t, pool.QueryRow(context.Background(),
		`SELECT status, balance_after_micros, COALESCE(failure_code, ''), COALESCE(receipt_url, '')
		   FROM ms_billing.credit_ledger WHERE id=$1`,
		ledgerID,
	).Scan(&status, &balanceAfter, &failureCode, &receiptURL))
	return status, balanceAfter, failureCode, receiptURL
}

func settledBalance(t *testing.T, pool *pgxpool.Pool, accountID uuid.UUID) int64 {
	t.Helper()
	var balance int64
	require.NoError(t, pool.QueryRow(context.Background(),
		`SELECT COALESCE(SUM(amount_micros), 0)::bigint
		   FROM ms_billing.credit_ledger
		  WHERE account_id=$1 AND status='settled'`,
		accountID,
	).Scan(&balance))
	return balance
}
