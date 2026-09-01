package cycle_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/mirrorstack-ai/billing-engine/internal/account/cycle"
	"github.com/mirrorstack-ai/billing-engine/internal/intent"
)

// 🔴 THE COMBINED-PRORATION LEG, PROPOSING INSTEAD OF COLLECTING.
//
// The assertion that matters is that NOTHING REACHED STRIPE. A cut-over leg
// holds no write port, and that is what makes "this worker cannot charge
// anyone" a fact about the code rather than a claim about its call graph.
func TestProrationLegProposesInsteadOfCharging(t *testing.T) {
	store := newFakeStore()
	user, _ := registeredAccount(store)
	sc := newFakeStripe()
	p := &capturingProposer{}
	svc := appsSvc(store, sc).WithIntentProposer(p)

	appID := uuid.New()
	registerMirror(t, svc, user, appID, time.Date(2026, 6, 19, 12, 0, 0, 0, time.UTC), 0)

	res, err := svc.ChargeCreationProration(context.Background(), appID)
	require.NoError(t, err)

	require.Equal(t, cycle.ProrationStatusProposed, res.Status,
		"the leg did not take the cutover branch")
	require.NotEmpty(t, res.IntentDigest,
		"a proposed proration carries no digest, so the app row cannot be walked to its intent")

	require.Empty(t, sc.itemCalls, "a cut-over proration created a Stripe invoice item")
	require.Empty(t, sc.finalizeCalls, "a cut-over proration finalized a Stripe invoice")
	// 🔴 The terminal stamp. Without it AppsPendingProration re-selects this
	// app forever — a new intent per sweep for one charge — and on a disarm the
	// legacy branch mints a real Stripe invoice for a period already sealed.
	require.Len(t, store.proposedProrations, 1,
		"the proposed proration wrote no terminal stamp, so the app is re-swept forever")
	require.Equal(t, "intent:"+res.IntentDigest, store.proposedProrations[0],
		"the stamp does not name the intent that replaced this charge")
	require.Equal(t, "intent:"+res.IntentDigest, store.apps[appID].ProrationInvoiceID,
		"the app row still selects as pending proration")

	// ONE intent, not two. Before §12 item 12 the prorated base fee and the
	// prorated module overage were platform_base and module_capacity — two
	// kinds, so two intents and a group. The fold collapsed them.
	require.Len(t, p.charges, 1, "the proration sealed more than one intent")
	require.Empty(t, p.groups, "the proration needed a group; after the fold it is a single kind")
	require.Equal(t, intent.KindPlatformBase, p.charges[0].Kind)
	require.Positive(t, p.charges[0].TotalMicros(),
		"the leg proposed a zero charge; the proration was lost in the cutover")
}

// DELETED with the path it tested: TestWithoutAProposerTheProrationStillCharges
// asserted that a service with NO proposer still finalized a Stripe invoice —
// "or the test above would pass against a leg that had simply stopped
// working". That non-vacuity argument was sound while two paths existed. There
// is one now: the leg has no direct charge path at all, and a service without
// a proposer cannot bill an app's creation period — it says so and fails.
//
// The test above is not vacuous without it. It asserts a POSITIVE outcome —
// a sealed charge with a real amount, a terminal stamp naming that intent's
// digest — which a leg that had stopped working could not produce.
