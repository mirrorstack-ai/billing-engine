package cycle_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/mirrorstack-ai/billing-engine/internal/account/cycle"
)

func seedWalletSource(store *fakeStore, typ string, amount int64, expiresAt, createdAt time.Time) uuid.UUID {
	id := uuid.New()
	store.walletSources[id] = &fakeWalletSource{
		id: id, typ: typ, remaining: amount, expiresAt: expiresAt, createdAt: createdAt,
	}
	return id
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
	second, err := chargeSvc(store, sc).WithCreditWallet(true).RunBillingCycle(context.Background(), chargeAccount, periodStart, periodEnd, 0)
	require.NoError(t, err)
	require.Equal(t, cycle.RunStatusSkippedNoPM, second.Status)
	require.EqualValues(t, 400_000, second.WalletDrawnMicros)
	require.EqualValues(t, 900_000, store.walletSources[lateSource].remaining)
	require.Equal(t, []uuid.UUID{firstSource}, store.walletDrawOrder)
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
	require.Zero(t, store.walletStateCalls, "flag OFF must execute no migration-048 wallet-state read")
	require.EqualValues(t, 1_000_000, store.walletSources[source].remaining, "flag OFF must not draw wallet credit")
}
