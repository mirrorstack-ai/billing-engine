package cycle_test

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/mirrorstack-ai/billing-engine/internal/account/cycle"
)

// billing-engine#217. A frozen remainder above the live boundary is refused by
// splitBoundary — right when a provider was committed to it, wrong when the
// figure was never presented to anyone and never sealed (the 2026-09-11 run
// froze, died on the nil-proposer panic before ProposeGroup, and every
// reclaim since re-derived a lower boundary and was refused for ever).
//
// The discriminator is what the run was BEFORE the reclaim: anything but
// 'proposed' means nothing was sealed, so the freeze is stale and moves to the
// live figure exactly once; 'proposed' keeps the refusal.

func TestRunBillingCycle_StaleFrozenAboveLiveRefreezesOnceWhenNothingWasSealed(t *testing.T) {
	store := newFakeStore()
	store.hasPM = true
	store.stripeCustomer = "cus_stale_freeze"
	sc := newFakeStripe()
	store.chargedTotal = 1_000_000
	runID := seedFrozenRun(t, store, sc, 100) // a crashed attempt froze 100¢ for a $1.00 boundary
	require.NotEqual(t, cycle.RunStatusProposed, store.runStatus[runID], "fixture: the seeded run must not read as sealed")

	// The closed period re-derives LOWER on the reclaim (a re-priced line).
	store.chargedTotal = 800_000

	svc, p := chargeSvcProposing(store, sc)
	resp, err := svc.WithCreditWallet(false).RunBillingCycle(context.Background(), chargeAccount, periodStart, periodEnd, 0)
	require.NoError(t, err, "a never-presented, never-sealed freeze must reconcile, not refuse")
	require.Equal(t, cycle.RunStatusProposed, resp.Status)
	require.EqualValues(t, 1, store.refreezeCalls, "the stale freeze moves by ONE compare-and-set")
	require.EqualValues(t, 80, frozenClaim(t, store).Cents, "the run marker now holds the live figure")
	require.EqualValues(t, 800_000, proposedMicros(t, p), "the boundary sealed is what the period costs now")
	var wallet, remainder int64
	for _, c := range p.groups[0] {
		wallet += c.WalletAllocationMicros
		remainder += c.TotalMicros() - c.WalletAllocationMicros
	}
	require.Zero(t, wallet)
	require.EqualValues(t, 800_000, remainder, "the provider remainder is the re-frozen figure, not the stale 100¢")

	// A later reclaim of the now-proposed run finds frozen == live: no second
	// re-freeze, and the same figure is proposed again.
	resp2, err := svc.WithCreditWallet(false).RunBillingCycle(context.Background(), chargeAccount, periodStart, periodEnd, 0)
	require.NoError(t, err)
	require.Equal(t, cycle.RunStatusProposed, resp2.Status)
	require.EqualValues(t, 1, store.refreezeCalls, "a reconciled freeze is not re-frozen again")
	require.EqualValues(t, 80, frozenClaim(t, store).Cents)
}

// TestRunBillingCycle_Account2ccc7c7bShape_11390FrozenThen11202Live is the
// production case as a named fixture (account 2ccc7c7b…, period 2026-08-11 →
// 2026-09-11): the 2026-09-11T03:00:14Z run froze 11390¢ and died before
// ProposeGroup; the 2026-09-14 reclaim derived 11202¢. Under rule (b) the
// boundary re-freezes at 11202¢, is proposed once at $112.02, and the audit
// line carries both figures.
func TestRunBillingCycle_Account2ccc7c7bShape_11390FrozenThen11202Live(t *testing.T) {
	store := newFakeStore()
	store.hasPM = true
	store.stripeCustomer = "cus_2ccc7c7b"
	sc := newFakeStripe()
	store.chargedTotal = 113_900_000
	runID := seedFrozenRun(t, store, sc, 11390)
	require.EqualValues(t, 11390, store.frozenCharges[runID].Cents)
	store.chargedTotal = 112_020_000

	svc, p := chargeSvcProposing(store, sc)
	resp, err := svc.WithCreditWallet(false).RunBillingCycle(context.Background(), chargeAccount, periodStart, periodEnd, 0)
	require.NoError(t, err, "the refused-for-ever boundary reconciles")
	require.Equal(t, cycle.RunStatusProposed, resp.Status)
	require.EqualValues(t, 11202, frozenClaim(t, store).Cents)
	require.EqualValues(t, 112_020_000, proposedMicros(t, p), "$112.02 sealed — the 188¢ no derivation supports is not billed")
	require.EqualValues(t, 1, store.refreezeCalls)
}

func TestRunBillingCycle_FrozenAboveLiveOnASealedRunIsStillRefused(t *testing.T) {
	store := newFakeStore()
	store.hasPM = true
	store.stripeCustomer = "cus_sealed_freeze"
	sc := newFakeStripe()
	store.chargedTotal = 1_000_000
	runID := seedFrozenRun(t, store, sc, 100)
	// The prior attempt got as far as sealing intents (status 'proposed')
	// before it died: the frozen figure IS committed, in a digest.
	store.runStatus[runID] = cycle.RunStatusProposed
	store.chargedTotal = 800_000

	svc, _ := chargeSvcProposing(store, sc)
	_, err := svc.WithCreditWallet(false).RunBillingCycle(context.Background(), chargeAccount, periodStart, periodEnd, 0)
	require.Error(t, err)
	require.ErrorContains(t, err, "a frozen remainder cannot exceed the boundary it is part of")
	require.Zero(t, store.refreezeCalls, "a sealed run's freeze is never touched")
	require.EqualValues(t, 100, frozenClaim(t, store).Cents)
}

func TestRunBillingCycle_StaleFreezeLostCASReReadsTheSurvivor(t *testing.T) {
	store := newFakeStore()
	store.hasPM = true
	store.stripeCustomer = "cus_lost_cas"
	sc := newFakeStripe()
	store.chargedTotal = 1_000_000
	runID := seedFrozenRun(t, store, sc, 100)
	store.chargedTotal = 800_000
	// Another daemon reconciled first: the row already holds 80, so our CAS
	// from 100 matches nothing; the survivor (80 == live) is used as-is.
	store.beforeBillingRunWalletDraw = func(f *fakeStore, id uuid.UUID) {
		require.Equal(t, runID, id)
		fz := f.frozenCharges[id]
		fz.Cents = 80
		f.frozenCharges[id] = fz
	}

	svc, p := chargeSvcProposing(store, sc)
	resp, err := svc.WithCreditWallet(false).RunBillingCycle(context.Background(), chargeAccount, periodStart, periodEnd, 0)
	require.NoError(t, err)
	require.Equal(t, cycle.RunStatusProposed, resp.Status)
	require.EqualValues(t, 80, frozenClaim(t, store).Cents)
	require.EqualValues(t, 800_000, proposedMicros(t, p))
}
