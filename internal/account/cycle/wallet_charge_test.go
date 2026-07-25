package cycle_test

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/mirrorstack-ai/billing-engine/internal/account/credit/rollout"
	"github.com/mirrorstack-ai/billing-engine/internal/account/cycle"
)

func seedWalletSource(store *fakeStore, typ string, amount int64, expiresAt, createdAt time.Time) uuid.UUID {
	id := uuid.New()
	store.walletSources[id] = &fakeWalletSource{
		id: id, typ: typ, remaining: amount, expiresAt: expiresAt, createdAt: createdAt,
	}
	return id
}

type boundaryReconcileCall struct {
	accountID   uuid.UUID
	periodStart time.Time
	amount      int64
}

type fakeBoundaryReconciler struct {
	calls  []boundaryReconcileCall
	onCall func()
}

type fakeWalletMutationObserver struct {
	calls []uuid.UUID
	err   error
}

func (f *fakeWalletMutationObserver) ObserveAccount(_ context.Context, accountID uuid.UUID) error {
	f.calls = append(f.calls, accountID)
	return f.err
}

func (f *fakeBoundaryReconciler) ReconcileBoundary(_ context.Context, accountID uuid.UUID, periodStart time.Time, amountMicros int64) error {
	if f.onCall != nil {
		f.onCall()
	}
	f.calls = append(f.calls, boundaryReconcileCall{
		accountID: accountID, periodStart: periodStart, amount: amountMicros,
	})
	return nil
}

func TestRunBillingCycle_ReconcilesNewPeriodAfterDurableCreditsDraw(t *testing.T) {
	store := newFakeStore()
	store.walletMode = cycle.CreditBillingModeCredits
	store.chargedTotal = 1_000_000
	seedApp(store, chargeAccount, 0, false)
	reconciler := &fakeBoundaryReconciler{onCall: func() {
		require.NotEmpty(t, store.walletDraws, "boundary callback must run after the durable wallet draw")
	}}

	_, err := chargeSvc(store, newFakeStripe()).
		WithCreditWallet(true).
		WithBoundaryEstimateReconciler(reconciler).
		RunBillingCycle(context.Background(), chargeAccount, periodStart, periodEnd, 0)

	require.NoError(t, err)
	require.Len(t, reconciler.calls, 1)
	call := reconciler.calls[0]
	require.Equal(t, chargeAccount, call.accountID)
	require.True(t, call.periodStart.Equal(periodEnd), "the cache key is the period that just opened")
	require.Zero(t, call.amount,
		"closed-period arrears and already-drawn advance fees must not leak into unpaid exposure")
}

func TestRunBillingCycle_StandardWalletDrawsThenChargesOnlyRemainder(t *testing.T) {
	store := newFakeStore()
	store.chargedTotal = 1_000_000
	store.hasPM = true
	store.stripeCustomer = "cus_wallet_remainder"
	source := seedWalletSource(store, "purchase", 400_000, time.Time{}, timeUTC(2026, 1, 1, 0))
	sc := newFakeStripe()
	sc.invoiceAmountDue = 60

	resp, err := chargeSvc(store, sc).WithCreditWallet(true).RunBillingCycle(context.Background(), chargeAccount, periodStart, periodEnd, 0)
	require.NoError(t, err)
	require.Equal(t, cycle.RunStatusInvoiced, resp.Status)
	require.EqualValues(t, 400_000, resp.WalletDrawnMicros)
	require.EqualValues(t, 60, resp.ChargedCents)
	require.Len(t, sc.itemCalls, 1)
	require.EqualValues(t, 60, sc.itemCalls[0].amountCfg)
	require.Zero(t, store.walletSources[source].remaining)
}

func TestRunBillingCycle_WalletConsumptionOrderIsDeterministic(t *testing.T) {
	store := newFakeStore()
	store.chargedTotal = 7_000_000
	created := timeUTC(2026, 1, 1, 0)
	purchased := seedWalletSource(store, "purchase", 5_000_000, time.Time{}, created)
	nonExpiringGrant := seedWalletSource(store, "grant", 2_000_000, time.Time{}, created.Add(time.Hour))
	laterGrant := seedWalletSource(store, "grant", 3_000_000, timeUTC(2099, 2, 1, 0), created.Add(2*time.Hour))
	soonerGrant := seedWalletSource(store, "grant", 1_000_000, timeUTC(2099, 1, 1, 0), created.Add(3*time.Hour))
	sc := newFakeStripe()

	resp, err := chargeSvc(store, sc).WithCreditWallet(true).RunBillingCycle(context.Background(), chargeAccount, periodStart, periodEnd, 0)
	require.NoError(t, err)
	require.Equal(t, cycle.RunStatusInvoiced, resp.Status)
	require.EqualValues(t, 7_000_000, resp.WalletDrawnMicros)
	require.Equal(t, []uuid.UUID{soonerGrant, laterGrant, nonExpiringGrant, purchased}, store.walletDrawOrder)
	require.EqualValues(t, 4_000_000, store.walletSources[purchased].remaining)
	require.Empty(t, sc.invoiceCalls, "a fully wallet-covered boundary never reaches Stripe")
}

func TestRunBillingCycle_CreditsModeDebitsFullBoundaryWithoutStripe(t *testing.T) {
	store := newFakeStore()
	store.walletMode = cycle.CreditBillingModeCredits
	store.chargedTotal = 5_000_000
	seedWalletSource(store, "grant", 2_000_000, time.Time{}, timeUTC(2026, 1, 1, 0))
	sc := newFakeStripe()

	resp, err := chargeSvc(store, sc).WithCreditWallet(true).RunBillingCycle(context.Background(), chargeAccount, periodStart, periodEnd, 0)
	require.NoError(t, err)
	require.Equal(t, cycle.RunStatusInvoiced, resp.Status)
	require.EqualValues(t, 5_000_000, resp.WalletDrawnMicros)
	require.EqualValues(t, 3_000_000, store.walletUnallocated, "the configured credit policy owns the negative residual")
	require.Zero(t, resp.ChargedCents)
	require.Empty(t, sc.invoiceCalls)
	require.Empty(t, sc.itemCalls)
}

func TestRunBillingCycle_WalletDrawIsPeriodIdempotentAcrossNoPMReclaim(t *testing.T) {
	store := newFakeStore()
	store.chargedTotal = 1_000_000
	firstSource := seedWalletSource(store, "grant", 400_000, time.Time{}, timeUTC(2026, 1, 1, 0))
	sc := newFakeStripe()

	first, err := chargeSvc(store, sc).WithCreditWallet(true).RunBillingCycle(context.Background(), chargeAccount, periodStart, periodEnd, 0)
	require.NoError(t, err)
	require.Equal(t, cycle.RunStatusSkippedNoPM, first.Status)
	require.EqualValues(t, 400_000, first.WalletDrawnMicros)
	require.Zero(t, store.walletSources[firstSource].remaining)

	// Credit arriving after the skipped attempt belongs to future draws. A
	// reclaim must reuse the period's original debit, not consume it as well.
	lateSource := seedWalletSource(store, "purchase", 900_000, time.Time{}, timeUTC(2026, 2, 1, 0))
	var telemetry bytes.Buffer
	second, err := chargeSvc(store, sc).
		WithCreditWallet(true).
		WithCreditRollout(cycleRolloutController(rollout.ModeShadow, "10000", &telemetry)).
		RunBillingCycle(context.Background(), chargeAccount, periodStart, periodEnd, 0)
	require.NoError(t, err)
	require.Equal(t, cycle.RunStatusSkippedNoPM, second.Status)
	require.Zero(t, second.WalletDrawnMicros,
		"the durable run marker recovers the frozen remainder without re-reading the wallet")
	require.EqualValues(t, 900_000, store.walletSources[lateSource].remaining)
	require.Equal(t, []uuid.UUID{firstSource}, store.walletDrawOrder)
	require.Zero(t, store.walletModeCalls,
		"rollback recovery must not enter the current rollout classifier")
	require.EqualValues(t, 1, store.walletDrawCalls,
		"the reclaim completes from billing_runs and performs no wallet read or debit")
}

func TestRunBillingCycle_PartialWalletCommitCrashThenMasterOffChargesOnlyFrozenRemainder(t *testing.T) {
	store := newFakeStore()
	store.chargedTotal = 1_000_000
	source := seedWalletSource(store, "grant", 400_000, time.Time{}, timeUTC(2026, 1, 1, 0))
	store.errAfterBoundaryWalletCommit = errors.New("process died after wallet transaction commit")
	sc := newFakeStripe()

	_, err := chargeSvc(store, sc).
		WithCreditWallet(true).
		RunBillingCycle(context.Background(), chargeAccount, periodStart, periodEnd, 0)
	require.ErrorContains(t, err, "process died after wallet transaction commit")
	require.Zero(t, store.walletSources[source].remaining)
	require.Len(t, store.frozenCharges, 1)
	for _, frozen := range store.frozenCharges {
		require.EqualValues(t, 60, frozen.Cents,
			"the ledger debit and exact Stripe remainder freeze atomically")
	}

	store.hasPM = true
	store.stripeCustomer = "cus_wallet_crash_remainder"
	sc.invoiceAmountDue = 60
	resp, err := chargeSvc(store, sc).
		WithCreditWallet(false).
		RunBillingCycle(context.Background(), chargeAccount, periodStart, periodEnd, 0)

	require.NoError(t, err)
	require.Equal(t, cycle.RunStatusInvoiced, resp.Status)
	require.Zero(t, resp.WalletDrawnMicros,
		"true off must recover exclusively from the legacy billing_run marker")
	require.EqualValues(t, 60, resp.ChargedCents)
	require.Len(t, sc.itemCalls, 1)
	require.EqualValues(t, 60, sc.itemCalls[0].amountCfg)
	require.EqualValues(t, 1, store.walletDrawCalls,
		"the true-off reclaim performs zero migration-048 wallet operations")
}

func TestRunBillingCycle_PartialWalletCommitCrashThenExcludedReclaimUsesRunMarker(t *testing.T) {
	store := newFakeStore()
	store.chargedTotal = 1_000_000
	source := seedWalletSource(store, "grant", 400_000, time.Time{}, timeUTC(2026, 1, 1, 0))
	store.errAfterBoundaryWalletCommit = errors.New("process died after wallet transaction commit")
	sc := newFakeStripe()

	_, err := chargeSvc(store, sc).
		WithCreditWallet(true).
		RunBillingCycle(context.Background(), chargeAccount, periodStart, periodEnd, 0)
	require.ErrorContains(t, err, "process died after wallet transaction commit")
	require.Zero(t, store.walletSources[source].remaining)

	store.hasPM = true
	store.stripeCustomer = "cus_wallet_excluded_recovery"
	sc.invoiceAmountDue = 60
	var telemetry bytes.Buffer
	resp, err := chargeSvc(store, sc).
		WithCreditWallet(true).
		WithCreditRollout(cycleRolloutController(rollout.ModeEnforce, "0", &telemetry)).
		RunBillingCycle(context.Background(), chargeAccount, periodStart, periodEnd, 0)

	require.NoError(t, err)
	require.Equal(t, cycle.RunStatusInvoiced, resp.Status)
	require.EqualValues(t, 60, resp.ChargedCents)
	require.EqualValues(t, 1, store.walletDrawCalls,
		"an excluded reclaim must not re-enter the wallet graph")
	require.Zero(t, store.walletModeCalls)
	require.EqualValues(t, 1, store.walletStateCalls,
		"only the original selected attempt read wallet state")
	require.Len(t, sc.itemCalls, 1)
	require.EqualValues(t, 60, sc.itemCalls[0].amountCfg)
}

func TestRunBillingCycle_FullWalletCommitCrashThenMasterOffTerminatesWithoutStripeOrPM(t *testing.T) {
	store := newFakeStore()
	store.chargedTotal = 1_000_000
	source := seedWalletSource(store, "grant", 1_000_000, time.Time{}, timeUTC(2026, 1, 1, 0))
	store.errAfterBoundaryWalletCommit = errors.New("process died after wallet transaction commit")
	sc := newFakeStripe()

	_, err := chargeSvc(store, sc).
		WithCreditWallet(true).
		RunBillingCycle(context.Background(), chargeAccount, periodStart, periodEnd, 0)
	require.ErrorContains(t, err, "process died after wallet transaction commit")
	require.Zero(t, store.walletSources[source].remaining)
	require.Len(t, store.frozenCharges, 1)
	for _, frozen := range store.frozenCharges {
		require.Zero(t, frozen.Cents, "a full wallet settlement freezes a zero Stripe remainder")
	}

	// A zero marker is already fully paid. Prove the true-off reclaim cannot
	// touch customer/PM recovery even when those seams would fail loudly.
	store.errPM = errors.New("PM lookup must not run")
	store.errCustomer = errors.New("customer lookup must not run")
	resp, err := chargeSvc(store, sc).
		WithCreditWallet(false).
		RunBillingCycle(context.Background(), chargeAccount, periodStart, periodEnd, 0)

	require.NoError(t, err)
	require.Equal(t, cycle.RunStatusInvoiced, resp.Status)
	require.Zero(t, resp.ChargedCents)
	require.Empty(t, sc.findByRefCalls)
	require.Empty(t, sc.invoiceCalls)
	require.Empty(t, sc.itemCalls)
	require.Empty(t, sc.finalizeCalls)
	require.EqualValues(t, 1, store.walletDrawCalls,
		"the true-off reclaim performs zero migration-048 wallet operations")
}

func TestRunBillingCycle_ConcurrentStripeFreezeWinsBeforeWalletLockForbidsNewDebit(t *testing.T) {
	store := newFakeStore()
	store.chargedTotal = 1_000_000
	store.hasPM = true
	store.stripeCustomer = "cus_concurrent_freeze"
	source := seedWalletSource(store, "grant", 1_000_000, time.Time{}, timeUTC(2026, 1, 1, 0))
	store.beforeBillingRunWalletDraw = func(f *fakeStore, runID uuid.UUID) {
		// Daemon A froze the full Stripe request after daemon B's top-of-run
		// read but before B acquired the run-row lock inside its wallet tx.
		f.frozenCharges[runID] = cycle.FrozenBoundaryCharge{Cents: 100}
	}
	sc := newFakeStripe()
	sc.invoiceAmountDue = 100

	resp, err := chargeSvc(store, sc).
		WithCreditWallet(true).
		RunBillingCycle(context.Background(), chargeAccount, periodStart, periodEnd, 0)

	require.NoError(t, err)
	require.Equal(t, cycle.RunStatusInvoiced, resp.Status)
	require.EqualValues(t, 100, resp.ChargedCents)
	require.Zero(t, resp.WalletDrawnMicros)
	require.EqualValues(t, 1_000_000, store.walletSources[source].remaining,
		"a marker found under the draw transaction's run lock forbids allocation")
	require.Empty(t, store.walletDrawOrder)
	require.Len(t, sc.itemCalls, 1)
	require.EqualValues(t, 100, sc.itemCalls[0].amountCfg)
}

func TestRunBillingCycle_CreditWalletFlagOffReclaimKeepsLegacyZeroWalletSQL(t *testing.T) {
	store := newFakeStore()
	store.chargedTotal = 1_000_000
	source := seedWalletSource(store, "grant", 1_000_000, time.Time{}, timeUTC(2026, 1, 1, 0))
	sc := newFakeStripe()
	svc := chargeSvc(store, sc).WithCreditWallet(false)

	first, err := svc.RunBillingCycle(context.Background(), chargeAccount, periodStart, periodEnd, 0)
	require.NoError(t, err)
	require.Equal(t, cycle.RunStatusSkippedNoPM, first.Status)

	second, err := svc.RunBillingCycle(context.Background(), chargeAccount, periodStart, periodEnd, 0)
	require.NoError(t, err)
	require.Equal(t, cycle.RunStatusSkippedNoPM, second.Status)
	require.Zero(t, second.WalletDrawnMicros)
	require.Zero(t, store.walletModeCalls, "true off must not read accounts.billing_mode on reclaim")
	require.Zero(t, store.walletStateCalls, "true off must not read wallet state on reclaim")
	require.Zero(t, store.walletDrawCalls, "true off must not query or mutate credit_ledger on reclaim")
	require.EqualValues(t, 1_000_000, store.walletSources[source].remaining)
	require.Empty(t, store.walletDrawOrder)
}

func TestRunBillingCycle_FrozenStripeAttemptNeverStartsNewWalletDraw(t *testing.T) {
	store := newFakeStore()
	store.chargedTotal = 1_000_000
	store.hasPM = true
	store.stripeCustomer = "cus_wallet_frozen"
	sc := newFakeStripe()
	sc.errDraft = errors.New("stripe unavailable after boundary freeze")

	_, err := chargeSvc(store, sc).WithCreditWallet(true).RunBillingCycle(context.Background(), chargeAccount, periodStart, periodEnd, 0)
	require.Error(t, err)
	require.NotEmpty(t, store.frozenCharges)

	lateSource := seedWalletSource(store, "grant", 1_000_000, time.Time{}, timeUTC(2026, 2, 1, 0))
	sc.errDraft = nil
	sc.invoiceAmountDue = 100
	resp, err := chargeSvc(store, sc).WithCreditWallet(true).RunBillingCycle(context.Background(), chargeAccount, periodStart, periodEnd, 0)
	require.NoError(t, err)
	require.Equal(t, cycle.RunStatusInvoiced, resp.Status)
	require.Zero(t, resp.WalletDrawnMicros)
	require.EqualValues(t, 1_000_000, store.walletSources[lateSource].remaining)
	require.EqualValues(t, 100, resp.ChargedCents)
}

func TestRunBillingCycle_CreditWalletFlagOffSkipsWalletStateAndKeepsLegacyPrepaidPath(t *testing.T) {
	store := newFakeStore()
	store.collection.Mode = cycle.BillingModePrepaid
	store.chargedTotal = 1_000_000
	source := seedWalletSource(store, "grant", 1_000_000, time.Time{}, timeUTC(2026, 1, 1, 0))

	resp, err := chargeSvc(store, newFakeStripe()).WithCreditWallet(false).RunBillingCycle(
		context.Background(), chargeAccount, periodStart, periodEnd, 0,
	)

	require.NoError(t, err)
	require.Equal(t, cycle.RunStatusSkippedPrepaid, resp.Status)
	require.Zero(t, store.walletModeCalls, "flag OFF must not read accounts.billing_mode")
	require.Zero(t, store.walletStateCalls, "flag OFF must execute no migration-048 wallet-state read")
	require.EqualValues(t, 1_000_000, store.walletSources[source].remaining, "flag OFF must not draw wallet credit")
}
