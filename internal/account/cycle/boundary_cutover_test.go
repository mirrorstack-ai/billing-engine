package cycle_test

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/mirrorstack-ai/billing-engine/internal/account/cycle"
	"github.com/mirrorstack-ai/billing-engine/internal/account/usage"
	"github.com/mirrorstack-ai/billing-engine/internal/intent"
)

// 🔴 THE BOUNDARY LEG, PROPOSING INSTEAD OF COLLECTING.
//
// charge.go had never mentioned a proposer: the period boundary was the last
// leg with no intent seam at all, and it is the one that collects the most.
//
// The assertion that matters is not that intents appeared. It is that NOTHING
// REACHED STRIPE — a cut-over leg holds no write port, and that is what makes
// "cmd/billing-cycle cannot charge anyone" a fact about the code rather than a
// claim about its call graph.
//
// TestWithoutAProposerTheBoundaryStillCharges used to sit below, pinning that
// an unarmed service still collected — the canary that these tests were not
// passing against a leg which had simply stopped working. The collector it
// watched is deleted, so there is no unarmed behaviour left for it to describe.
// Its job passes to TestTheProposedBoundaryCarriesTheLegacyAmounts: a leg that
// stopped working cannot still seal the amounts the legacy path computed.
func TestBoundaryLegProposesInsteadOfCharging(t *testing.T) {
	store := newFakeStore()
	store.chargedTotal = 1_000_000
	store.hasPM = true
	store.stripeCustomer = "cus_boundary_1"
	seedApp(store, chargeAccount, 0, false)
	sc := newFakeStripe()
	p := &capturingProposer{}

	svc := cycle.NewService(store, sc).WithIntentProposer(p)
	resp, err := svc.RunBillingCycle(context.Background(), chargeAccount, periodStart, periodEnd, 0)
	require.NoError(t, err)

	require.Equal(t, cycle.RunStatusProposed, resp.Status,
		"the boundary did not take the cutover branch")

	// The assertion the whole seam exists for.
	require.Empty(t, sc.itemCalls, "a cut-over boundary created a Stripe invoice item")
	require.Zero(t, resp.ChargedCents, "a cut-over boundary reported cents as charged")

	// One GROUP, not two proposals. Two separately-proposed intents are
	// collected as two invoices with two roundings, which is not what the
	// legacy path takes.
	require.Len(t, p.groups, 1, "the boundary's halves were not proposed as one group")
	require.Len(t, p.groups[0], 2, "a boundary with arrears and a base fee sealed other than two intents")

	require.Equal(t, intent.KindModuleUsage, p.groups[0][0].Kind)
	require.Equal(t, intent.KindPlatformBase, p.groups[0][1].Kind)

	// The run must be walkable to the documents that replaced its charge.
	require.Len(t, resp.ProposedDigests, 2,
		"a proposed run carries no digests, so the run cannot be walked to its intents")
	for _, d := range resp.ProposedDigests {
		require.NotEmpty(t, d)
	}
}

// The amounts must survive the cutover. A seam that proposes the WRONG figure
// is worse than one that does not propose at all, because it looks like it
// worked.
func TestTheProposedBoundaryCarriesTheLegacyAmounts(t *testing.T) {
	store := newFakeStore()
	store.chargedTotal = 1_000_000
	store.hasPM = true
	store.stripeCustomer = "cus_boundary_2"
	seedApp(store, chargeAccount, 0, false)
	sc := newFakeStripe()
	p := &capturingProposer{}

	resp, err := cycle.NewService(store, sc).WithIntentProposer(p).
		RunBillingCycle(context.Background(), chargeAccount, periodStart, periodEnd, 0)
	require.NoError(t, err)

	require.EqualValues(t, 1_000_000, resp.ArrearsMicros)
	require.EqualValues(t, usage.BaseFeeMicros, resp.AdvanceBaseMicros)

	var total int64
	for _, c := range p.groups[0] {
		total += c.TotalMicros()
	}
	require.Equal(t, resp.ArrearsMicros+resp.AdvanceBaseMicros, total,
		"the sealed intents do not add up to the boundary the legacy path computed")
}

// A proposal that fails leaves the run PENDING, never 'failed'.
//
// Nothing was attempted at a provider, so there is nothing to reconcile and a
// retry is safe. Marking it failed would strand a boundary that never reached
// Stripe, and marking it proposed would claim intents exist that do not.
func TestAFailedProposalLeavesTheRunUnfinished(t *testing.T) {
	store := newFakeStore()
	store.chargedTotal = 1_000_000
	store.hasPM = true
	store.stripeCustomer = "cus_boundary_4"
	seedApp(store, chargeAccount, 0, false)
	sc := newFakeStripe()
	p := &capturingProposer{err: errors.New("proposal refused")}

	_, err := cycle.NewService(store, sc).WithIntentProposer(p).
		RunBillingCycle(context.Background(), chargeAccount, periodStart, periodEnd, 0)
	require.Error(t, err, "a failed proposal was reported as success")

	require.Empty(t, sc.itemCalls,
		"a boundary whose proposal failed fell back to charging the customer")
	// 🔴 Assert the run was not marked AT ALL, not "not marked failed".
	//
	// Ranging over markedRuns and requiring each entry not to be failed is
	// vacuous when the map is empty — which it is on the correct path — so it
	// passes whether or not the code marks anything. Mutation testing caught
	// exactly that: adding MarkBillingRun(..., RunStatusFailed, ...) to the
	// failure path left this test green. The property is that a proposal
	// failure reaches NO terminal mark, leaving the run pending for the next
	// reclaim, so that is what is asserted.
	require.Empty(t, store.markedRuns,
		"a proposal failure gave the run a terminal status. Nothing reached a provider, so "+
			"there is nothing to reconcile and the run must stay pending and retryable — "+
			"marking it failed strands a boundary that never charged anyone.")
}
