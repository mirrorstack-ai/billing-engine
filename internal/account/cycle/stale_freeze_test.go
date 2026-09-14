package cycle_test

import (
	"context"
	"errors"
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
	require.False(t, store.proposalAttempted[runID], "fixture: the seeded run was never handed to the proposer")

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

// TestRunBillingCycle_ProposedThenReclaimedThenCrashedStillRefuses is the
// round-2 blocker: attempt A seals ('proposed'); the next reclaim resets the
// status to 'pending' and dies BEFORE ProposeGroup; the reclaim after that
// sees a lower live boundary. Status alone would now say "never sealed" and a
// re-freeze would seal a SECOND digest for one boundary while A's document is
// alive. The migration-083 marker (stamped before the seal, never cleared) is
// what keeps the refusal — proven by the mutant below, which drops it.
func TestRunBillingCycle_ProposedThenReclaimedThenCrashedStillRefuses(t *testing.T) {
	run := func(t *testing.T, dropMarker bool) (*fakeStore, error) {
		t.Helper()
		store := newFakeStore()
		store.hasPM = true
		store.stripeCustomer = "cus_proposed_then_crash"
		sc := newFakeStripe()
		store.chargedTotal = 1_000_000
		svc, _ := chargeSvcProposing(store, sc)

		// Attempt A: proposes and seals at $1.00 (frozen 100¢, status 'proposed').
		resp, err := svc.WithCreditWallet(false).RunBillingCycle(context.Background(), chargeAccount, periodStart, periodEnd, 0)
		require.NoError(t, err)
		require.Equal(t, cycle.RunStatusProposed, resp.Status)
		require.EqualValues(t, 100, frozenClaim(t, store).Cents)
		var runID uuid.UUID
		for _, id := range store.insertedRuns {
			runID = id
		}
		require.True(t, store.proposalAttempted[runID], "the marker is stamped before the seal")

		// Attempt B: reclaimed (status reset to 'pending'), dies before ProposeGroup.
		store.dropProposalMarkerOnReclaim = dropMarker
		store.errMarkRun = errors.New("process died before ProposeGroup")
		_, err = svc.WithCreditWallet(false).RunBillingCycle(context.Background(), chargeAccount, periodStart, periodEnd, 0)
		require.Error(t, err)
		require.Equal(t, cycle.BillingRunStatus("pending"), store.runStatus[runID], "the reclaim reset the status; the seal is now invisible to status alone")
		store.errMarkRun = nil

		// Attempt C: the live boundary re-derives LOWER.
		store.chargedTotal = 800_000
		_, err = svc.WithCreditWallet(false).RunBillingCycle(context.Background(), chargeAccount, periodStart, periodEnd, 0)
		return store, err
	}

	t.Run("with the durable marker: refused, frozen untouched", func(t *testing.T) {
		store, err := run(t, false)
		require.Error(t, err)
		require.ErrorContains(t, err, "a frozen remainder cannot exceed the boundary it is part of")
		require.Zero(t, store.refreezeCalls, "a maybe-sealed run is never re-frozen")
		require.EqualValues(t, 100, frozenClaim(t, store).Cents)
	})
	t.Run("MUTANT — marker dropped on reclaim: the re-freeze goes through (the defect the marker prevents)", func(t *testing.T) {
		store, err := run(t, true)
		require.NoError(t, err, "without the marker the reclaim reads 'never sealed' and reconciles — a second digest for one boundary")
		require.EqualValues(t, 1, store.refreezeCalls)
		require.EqualValues(t, 80, frozenClaim(t, store).Cents)
	})
}
