//go:build integration

package billing_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/mirrorstack-ai/billing-engine/internal/account/billing"
	"github.com/mirrorstack-ai/billing-engine/internal/shared/testutil"
)

func TestCreditGateSnapshot_Integration_UsesSpendableLotsAndOnlyAutoTopUpGrace(t *testing.T) {
	pool := testutil.NewTestDB(t)
	ctx := context.Background()
	accountID, ownerID := uuid.New(), uuid.New()
	_, err := pool.Exec(ctx, `
		INSERT INTO ms_billing.accounts (
			id, owner_kind, owner_user_id, billing_mode,
			credit_limit_micros, activated_at
		) VALUES ($1, 'user', $2, 'credits', 0, $3)`,
		accountID, ownerID, time.Date(2026, time.July, 1, 0, 0, 0, 0, time.UTC),
	)
	require.NoError(t, err)

	insert := func(amount int64, typ, status string, expiresAt *time.Time) {
		t.Helper()
		id := uuid.New()
		_, err := pool.Exec(ctx, `
			INSERT INTO ms_billing.credit_ledger (
				id, account_id, amount_micros, type, status,
				balance_after_micros, actor, idempotency_key, expires_at
			) VALUES ($1, $2, $3, $4, $5, $3, 'system', $6, $7)`,
			id, accountID, amount, typ, status, "gate:"+id.String(), expiresAt,
		)
		require.NoError(t, err)
	}

	expired := time.Date(2020, time.January, 1, 0, 0, 0, 0, time.UTC)
	insert(100_000, "grant", "settled", &expired)
	insert(50_000, "purchase", "settled", nil)
	insert(-170_000, "adjustment", "settled", nil)
	insert(9_000_000, "purchase", "pending", nil)

	store := billing.NewStore(pool)
	snapshot, err := store.CreditGateSnapshot(ctx, accountID)
	require.NoError(t, err)
	require.Equal(t, ownerID, snapshot.OwnerUserID)
	require.EqualValues(t, -20_000, snapshot.SettledBalanceMicros,
		"the raw posted balance preserves an unsecured residual for the gate")
	require.Zero(t, snapshot.SpendableBalanceMicros,
		"expired remainder and pending manual purchase must not become spendable")
	require.False(t, snapshot.PendingAutoTopUp,
		"a pending manual purchase must not grant auto-top-up grace")

	insert(5_000_000, "auto_topup", "pending", nil)
	snapshot, err = store.CreditGateSnapshot(ctx, accountID)
	require.NoError(t, err)
	require.True(t, snapshot.PendingAutoTopUp)
	require.Zero(t, snapshot.SpendableBalanceMicros,
		"pending auto-top-up grants grace but is not settled balance")
}

func TestCreditGateSnapshot_Integration_StandardAccountNeverReadsCreditLedger(t *testing.T) {
	pool := testutil.NewTestDB(t)
	ctx := context.Background()
	standardID, creditsID := uuid.New(), uuid.New()
	for _, account := range []struct {
		id   uuid.UUID
		mode string
	}{
		{id: standardID, mode: "standard"},
		{id: creditsID, mode: "credits"},
	} {
		_, err := pool.Exec(ctx, `
			INSERT INTO ms_billing.accounts (
				id, owner_kind, owner_user_id, billing_mode,
				credit_limit_micros, activated_at
			) VALUES ($1, 'user', $2, $3, 0, CURRENT_TIMESTAMP)`,
			account.id, uuid.New(), account.mode,
		)
		require.NoError(t, err)
	}

	// Hold an exclusive lock as an executable SQL tripwire. An accidental
	// credit_ledger read blocks behind it; the accounts-only classification
	// query remains available.
	tx, err := pool.Begin(ctx)
	require.NoError(t, err)
	defer func() { _ = tx.Rollback(ctx) }()
	_, err = tx.Exec(ctx, `LOCK TABLE ms_billing.credit_ledger IN ACCESS EXCLUSIVE MODE`)
	require.NoError(t, err)

	store := billing.NewStore(pool)
	standardCtx, cancelStandard := context.WithTimeout(ctx, time.Second)
	defer cancelStandard()
	snapshot, err := store.CreditGateSnapshot(standardCtx, standardID)
	require.NoError(t, err)
	require.Equal(t, "standard", snapshot.BillingMode)

	creditsCtx, cancelCredits := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancelCredits()
	_, err = store.CreditGateSnapshot(creditsCtx, creditsID)
	require.Error(t, err,
		"the credits control account must hit the locked ledger, proving the tripwire is active")
}

func TestFinalizeCreditPurchase_Integration_ConcurrentWinnerTransitionsOnce(t *testing.T) {
	pool := testutil.NewTestDB(t)
	ctx := context.Background()
	accountID := uuid.New()
	_, err := pool.Exec(ctx, `
		INSERT INTO ms_billing.accounts (
			id, owner_kind, owner_user_id, billing_mode,
			credit_limit_micros, activated_at
		) VALUES ($1, 'user', $2, 'credits', 0, CURRENT_TIMESTAMP)`,
		accountID, uuid.New(),
	)
	require.NoError(t, err)

	store := billing.NewStore(pool)
	purchase, err := store.CreatePendingCreditPurchase(
		ctx, accountID, billing.MinCreditPurchaseMicros, "concurrent-finalize",
	)
	require.NoError(t, err)

	start := make(chan struct{})
	results := make(chan billing.CreditPurchaseRow, 2)
	errs := make(chan error, 2)
	for range 2 {
		go func() {
			<-start
			row, finalizeErr := store.FinalizeCreditPurchase(
				ctx, purchase.ID, accountID, "settled", "https://receipt.test",
			)
			results <- row
			errs <- finalizeErr
		}()
	}
	close(start)

	transitions := 0
	for range 2 {
		require.NoError(t, <-errs)
		if (<-results).Transitioned {
			transitions++
		}
	}
	require.Equal(t, 1, transitions,
		"only the row-lock winner may drive the post-settlement observer")
	standing, err := store.CreditStanding(ctx, accountID)
	require.NoError(t, err)
	require.EqualValues(t, billing.MinCreditPurchaseMicros, standing.BalanceMicros,
		"concurrent finalization credits the wallet exactly once")
}
