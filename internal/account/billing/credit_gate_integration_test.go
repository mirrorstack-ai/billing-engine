//go:build integration

package billing_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/mirrorstack-ai/billing-engine/internal/account/billing"
	"github.com/mirrorstack-ai/billing-engine/internal/account/cycle"
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

	paymentMethodID := uuid.New()
	_, err = pool.Exec(ctx, `
		UPDATE ms_billing.accounts
		SET stripe_customer_id = 'cus_gate_snapshot'
		WHERE id = $1`,
		accountID,
	)
	require.NoError(t, err)
	_, err = pool.Exec(ctx, `
		INSERT INTO ms_billing.payment_methods_mirror (
			id, account_id, stripe_payment_method_id, brand, last4,
			exp_month, exp_year, is_default
		) VALUES ($2, $1, 'pm_gate_snapshot', 'visa', '4242', 12, 2099, true)`,
		accountID, paymentMethodID,
	)
	require.NoError(t, err)
	attemptID := uuid.New()
	createdAt := time.Now().UTC()
	_, err = pool.Exec(ctx, `
		INSERT INTO ms_billing.credit_ledger (
			id, account_id, amount_micros, type, status,
			balance_after_micros, actor, idempotency_key,
			attempt_payment_method_id, attempt_stripe_payment_method_id,
			attempt_stripe_customer_id, attempt_expires_at, created_at
		) VALUES (
			$1, $2, 5000000, 'auto_topup', 'pending',
			4980000, 'system', $3, $4, 'pm_gate_snapshot',
			'cus_gate_snapshot', $5, $6
		)`,
		attemptID, accountID, "gate:"+attemptID.String(), paymentMethodID,
		createdAt.Add(5*time.Minute), createdAt,
	)
	require.NoError(t, err)
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

func TestSetCustomerBillingMode_Integration_PendingAutoTopUpGuardsAtomicModeFlip(t *testing.T) {
	pool := testutil.NewTestDB(t)
	ctx := context.Background()
	ownerID, accountID, paymentMethodID, attemptID := uuid.New(), uuid.New(), uuid.New(), uuid.New()
	initialLimit := int64(7_000_000)
	_, err := pool.Exec(ctx, `
		INSERT INTO ms_billing.accounts (
			id, owner_kind, owner_user_id, billing_mode,
			credit_limit_micros, stripe_customer_id, activated_at
		) VALUES ($1, 'user', $2, 'credits', $3, 'cus_mode_guard', CURRENT_TIMESTAMP)`,
		accountID, ownerID, initialLimit,
	)
	require.NoError(t, err)
	_, err = pool.Exec(ctx, `
		INSERT INTO ms_billing.payment_methods_mirror (
			id, account_id, stripe_payment_method_id, brand, last4,
			exp_month, exp_year, is_default
		) VALUES ($1, $2, 'pm_mode_guard', 'visa', '4242', 12, 2099, true)`,
		paymentMethodID, accountID,
	)
	require.NoError(t, err)

	createdAt := time.Now().UTC().Add(-time.Minute)
	_, err = pool.Exec(ctx, `
		INSERT INTO ms_billing.credit_ledger (
			id, account_id, amount_micros, type, status,
			balance_after_micros, actor, idempotency_key,
			attempt_payment_method_id, attempt_stripe_payment_method_id,
			attempt_stripe_customer_id, attempt_expires_at, created_at
		) VALUES (
			$1, $2, $3, 'auto_topup', 'pending',
			$3, 'system', $4, $5, 'pm_mode_guard',
			'cus_mode_guard', $6, $7
		)`,
		attemptID,
		accountID,
		billing.MinCreditPurchaseMicros,
		"mode-guard:"+attemptID.String(),
		paymentMethodID,
		createdAt.Add(5*time.Minute),
		createdAt,
	)
	require.NoError(t, err)

	svc := billing.NewService(billing.NewStore(pool), &fakeStripe{}, "").
		WithCreditWallet(true)
	req := billing.SetCustomerBillingModeRequest{
		OwnerUserID: ownerID,
		BillingMode: billing.BillingModeStandard,
	}

	resp, err := svc.SetCustomerBillingMode(ctx, req)
	require.Nil(t, resp)
	requireBillingErrorCode(t, err, billing.CodeInvalidInput)

	var mode string
	var limit int64
	err = pool.QueryRow(ctx, `
		SELECT billing_mode, credit_limit_micros
		FROM ms_billing.accounts
		WHERE id = $1`,
		accountID,
	).Scan(&mode, &limit)
	require.NoError(t, err)
	require.Equal(t, string(billing.BillingModeCredits), mode)
	require.Equal(t, initialLimit, limit,
		"the rejected transaction must preserve both mode and limit")

	_, err = pool.Exec(ctx, `
		UPDATE ms_billing.credit_ledger
		SET status = 'failed', failure_code = 'payment_failed'
		WHERE id = $1`,
		attemptID,
	)
	require.NoError(t, err)

	resp, err = svc.SetCustomerBillingMode(ctx, req)
	require.NoError(t, err)
	require.Equal(t, billing.BillingModeStandard, resp.BillingMode)

	err = pool.QueryRow(ctx, `
		SELECT billing_mode, credit_limit_micros
		FROM ms_billing.accounts
		WHERE id = $1`,
		accountID,
	).Scan(&mode, &limit)
	require.NoError(t, err)
	require.Equal(t, string(billing.BillingModeStandard), mode)
	require.Equal(t, initialLimit, limit,
		"the exact same request may commit after the attempt becomes terminal")
}

func TestSetCreditBillingMode_Integration_PartialWalletDrawGuardsModeFlipUntilRunInvoiced(t *testing.T) {
	pool := testutil.NewTestDB(t)
	ctx := context.Background()
	accountID, ownerID, runID := uuid.New(), uuid.New(), uuid.New()
	periodStart := time.Date(2026, time.June, 1, 0, 0, 0, 0, time.UTC)
	periodEnd := periodStart.AddDate(0, 1, 0)
	initialLimit := int64(7_000_000)

	_, err := pool.Exec(ctx, `
		INSERT INTO ms_billing.accounts (
			id, owner_kind, owner_user_id, billing_mode,
			credit_limit_micros, activated_at
		) VALUES ($1, 'user', $2, 'standard', $3, CURRENT_TIMESTAMP)`,
		accountID, ownerID, initialLimit,
	)
	require.NoError(t, err)

	cycleStore := cycle.NewStore(pool)
	periodID, err := cycleStore.OpenPeriodForAccount(
		ctx,
		accountID,
		periodStart,
		periodEnd,
	)
	require.NoError(t, err)
	_, err = pool.Exec(ctx, `
		INSERT INTO ms_billing.billing_runs (
			id, account_id, period_start, period_end, status
		) VALUES ($1, $2, $3, $4, 'pending')`,
		runID, accountID, periodStart, periodEnd,
	)
	require.NoError(t, err)

	sourceID := uuid.New()
	_, err = pool.Exec(ctx, `
		INSERT INTO ms_billing.credit_ledger (
			id, account_id, amount_micros, type, status,
			balance_after_micros, actor, idempotency_key, created_at
		) VALUES (
			$1, $2, 400000, 'purchase', 'settled',
			400000, 'system', $3, $4
		)`,
		sourceID,
		accountID,
		"mode-draw-source:"+sourceID.String(),
		periodStart.Add(-time.Hour),
	)
	require.NoError(t, err)

	draw, err := cycleStore.DrawWalletCredits(
		ctx,
		accountID,
		periodStart,
		periodEnd,
		1_000_000,
		true,
	)
	require.NoError(t, err)
	require.Equal(t, cycle.CreditBillingModeStandard, draw.Mode)
	require.EqualValues(t, 400_000, draw.DrawnMicros,
		"the standard boundary must durably draw only the available wallet credit")

	var (
		runStatus       string
		chargeWasFrozen bool
		drawCount       int
	)
	err = pool.QueryRow(ctx, `
		SELECT status, frozen_charge_cents IS NOT NULL
		FROM ms_billing.billing_runs
		WHERE id = $1`,
		runID,
	).Scan(&runStatus, &chargeWasFrozen)
	require.NoError(t, err)
	require.Equal(t, "pending", runStatus)
	require.False(t, chargeWasFrozen,
		"the crash boundary is after the durable draw but before Stripe charge freeze")
	err = pool.QueryRow(ctx, `
		SELECT count(*)
		FROM ms_billing.credit_ledger
		WHERE account_id = $1
		  AND period_id = $2
		  AND type IN ('usage_draw', 'subscription_draw')
		  AND status = 'settled'`,
		accountID, periodID,
	).Scan(&drawCount)
	require.NoError(t, err)
	require.Equal(t, 1, drawCount)

	billingStore := billing.NewStore(pool)
	updatedLimit := int64(9_000_000)
	changed, err := billingStore.SetCreditBillingMode(
		ctx,
		accountID,
		nil,
		billing.BillingModeStandard,
		updatedLimit,
	)
	require.NoError(t, err)
	require.True(t, changed,
		"a credit-limit-only update must remain available during recovery")
	changed, err = billingStore.SetCreditBillingMode(
		ctx,
		accountID,
		nil,
		billing.BillingModeStandard,
		updatedLimit,
	)
	require.NoError(t, err)
	require.False(t, changed, "an exact no-op must not be blocked")

	svc := billing.NewService(billingStore, &fakeStripe{}, "").
		WithCreditWallet(true)
	requestedLimit := billing.DefaultCreditsLimitMicros
	response, err := svc.SetCustomerBillingMode(
		ctx,
		billing.SetCustomerBillingModeRequest{
			OwnerUserID:       ownerID,
			BillingMode:       billing.BillingModeCredits,
			CreditLimitMicros: &requestedLimit,
		},
	)
	require.Nil(t, response)
	requireBillingErrorCode(t, err, billing.CodeInvalidInput)

	var (
		mode  string
		limit int64
	)
	err = pool.QueryRow(ctx, `
		SELECT billing_mode, credit_limit_micros
		FROM ms_billing.accounts
		WHERE id = $1`,
		accountID,
	).Scan(&mode, &limit)
	require.NoError(t, err)
	require.Equal(t, string(billing.BillingModeStandard), mode)
	require.Equal(t, updatedLimit, limit,
		"the rejected mode transaction must preserve the independent limit update")

	_, err = pool.Exec(ctx, `
		UPDATE ms_billing.billing_runs
		SET status = 'invoiced'
		WHERE id = $1`,
		runID,
	)
	require.NoError(t, err)

	response, err = svc.SetCustomerBillingMode(
		ctx,
		billing.SetCustomerBillingModeRequest{
			OwnerUserID:       ownerID,
			BillingMode:       billing.BillingModeCredits,
			CreditLimitMicros: &requestedLimit,
		},
	)
	require.NoError(t, err)
	require.Equal(t, billing.BillingModeCredits, response.BillingMode)

	err = pool.QueryRow(ctx, `
		SELECT billing_mode, credit_limit_micros
		FROM ms_billing.accounts
		WHERE id = $1`,
		accountID,
	).Scan(&mode, &limit)
	require.NoError(t, err)
	require.Equal(t, string(billing.BillingModeCredits), mode)
	require.Equal(t, billing.DefaultCreditsLimitMicros, limit,
		"the mode change may commit after the run reaches non-reclaimable success")
}

func TestSetCreditBillingMode_Integration_ConcurrentModeFlipAndDrawSerializeToCoherentWinner(t *testing.T) {
	pool := testutil.NewTestDB(t)
	ctx := context.Background()
	accountID, runID := uuid.New(), uuid.New()
	periodStart := time.Date(2026, time.May, 1, 0, 0, 0, 0, time.UTC)
	periodEnd := periodStart.AddDate(0, 1, 0)

	_, err := pool.Exec(ctx, `
		INSERT INTO ms_billing.accounts (
			id, owner_kind, owner_user_id, billing_mode,
			credit_limit_micros, activated_at
		) VALUES ($1, 'user', $2, 'standard', 0, CURRENT_TIMESTAMP)`,
		accountID, uuid.New(),
	)
	require.NoError(t, err)

	cycleStore := cycle.NewStore(pool)
	periodID, err := cycleStore.OpenPeriodForAccount(
		ctx,
		accountID,
		periodStart,
		periodEnd,
	)
	require.NoError(t, err)
	_, err = pool.Exec(ctx, `
		INSERT INTO ms_billing.billing_runs (
			id, account_id, period_start, period_end, status
		) VALUES ($1, $2, $3, $4, 'pending')`,
		runID, accountID, periodStart, periodEnd,
	)
	require.NoError(t, err)
	sourceID := uuid.New()
	_, err = pool.Exec(ctx, `
		INSERT INTO ms_billing.credit_ledger (
			id, account_id, amount_micros, type, status,
			balance_after_micros, actor, idempotency_key
		) VALUES (
			$1, $2, 400000, 'purchase', 'settled',
			400000, 'system', $3
		)`,
		sourceID, accountID, "mode-race-source:"+sourceID.String(),
	)
	require.NoError(t, err)

	// Hold the exact parent row before releasing both real operations. Neither
	// may finish while this lock is held; after release, their shared
	// LockWalletAccount serialization permits only two coherent outcomes.
	lockTx, err := pool.Begin(ctx)
	require.NoError(t, err)
	defer func() { _ = lockTx.Rollback(ctx) }()
	_, err = lockTx.Exec(ctx, `
		SELECT billing_mode
		FROM ms_billing.accounts
		WHERE id = $1
		FOR UPDATE`,
		accountID,
	)
	require.NoError(t, err)

	type modeResult struct {
		changed bool
		err     error
	}
	type drawResult struct {
		draw cycle.WalletDrawdown
		err  error
	}
	ready := make(chan struct{})
	modeDone := make(chan modeResult, 1)
	drawDone := make(chan drawResult, 1)
	billingStore := billing.NewStore(pool)
	go func() {
		ready <- struct{}{}
		changed, setErr := billingStore.SetCreditBillingMode(
			ctx,
			accountID,
			nil,
			billing.BillingModeCredits,
			billing.DefaultCreditsLimitMicros,
		)
		modeDone <- modeResult{changed: changed, err: setErr}
	}()
	go func() {
		ready <- struct{}{}
		draw, drawErr := cycleStore.DrawWalletCredits(
			ctx,
			accountID,
			periodStart,
			periodEnd,
			1_000_000,
			true,
		)
		drawDone <- drawResult{draw: draw, err: drawErr}
	}()
	<-ready
	<-ready

	select {
	case result := <-modeDone:
		t.Fatalf("mode change bypassed the held account lock: %+v", result)
	case result := <-drawDone:
		t.Fatalf("wallet draw bypassed the held account lock: %+v", result)
	case <-time.After(100 * time.Millisecond):
	}
	require.NoError(t, lockTx.Commit(ctx))

	modeOutcome := <-modeDone
	drawOutcome := <-drawDone
	require.NoError(t, drawOutcome.err)

	var persistedMode string
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT billing_mode
		FROM ms_billing.accounts
		WHERE id = $1`,
		accountID,
	).Scan(&persistedMode))

	switch drawOutcome.draw.Mode {
	case cycle.CreditBillingModeCredits:
		require.NoError(t, modeOutcome.err)
		require.True(t, modeOutcome.changed)
		require.Equal(t, string(billing.BillingModeCredits), persistedMode)
		require.EqualValues(t, 1_000_000, drawOutcome.draw.DrawnMicros,
			"mode-first winner makes the subsequent draw coherently credits-mode")
	case cycle.CreditBillingModeStandard:
		require.ErrorContains(t, modeOutcome.err, "uninvoiced billing-run wallet draw")
		require.False(t, modeOutcome.changed)
		require.Equal(t, string(billing.BillingModeStandard), persistedMode)
		require.EqualValues(t, 400_000, drawOutcome.draw.DrawnMicros,
			"draw-first winner keeps the partial standard remainder recoverable")
	default:
		t.Fatalf("unexpected draw billing mode %q", drawOutcome.draw.Mode)
	}

	var durableDrawn int64
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT COALESCE(-SUM(amount_micros), 0)
		FROM ms_billing.credit_ledger
		WHERE account_id = $1
		  AND period_id = $2
		  AND type IN ('usage_draw', 'subscription_draw')
		  AND status = 'settled'`,
		accountID, periodID,
	).Scan(&durableDrawn))
	require.Equal(t, drawOutcome.draw.DrawnMicros, durableDrawn)
	require.NotEqual(t,
		struct {
			mode  string
			drawn int64
		}{mode: string(billing.BillingModeCredits), drawn: 400_000},
		struct {
			mode  string
			drawn int64
		}{mode: persistedMode, drawn: durableDrawn},
		"credits mode with only the standard partial draw is the forbidden torn outcome",
	)
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
