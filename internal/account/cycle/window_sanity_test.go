package cycle_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/mirrorstack-ai/billing-engine/internal/account/cycle"
	"github.com/mirrorstack-ai/billing-engine/internal/shared/stripe/stripetest"
)

// One reviewer claimed the two shipped legs are immune to the dead-on-arrival
// execution window; another claimed they are not. Rather than believe either,
// ask the leg.
//
// A sealed window that does not contain the seal instant can never be
// collected: predicate.ClauseWithinExecutionWindow refuses it forever, the
// provider leg has already stopped, and the charge evaporates silently.
func TestShippedLegsSealAWindowContainingTheSealInstant(t *testing.T) {
	recorder := stripetest.New()
	store := newFakeStore()
	p := &capturingProposer{}
	cand := seedDomain(t, store)

	at := time.Now().UTC()
	svc := cycle.NewService(store, recorder).WithIntentProposer(p)
	res, err := svc.ChargeDomain(context.Background(), cand, at)
	require.NoError(t, err)
	require.Equal(t, cycle.DomainChargeProposed, res.Status)
	require.Len(t, p.charges, 1)

	c := p.charges[0]
	require.False(t, c.ExecuteNotBefore.IsZero(), "no execution window was sealed")
	require.False(t, c.ExecuteNotAfter.IsZero(), "no execution window was sealed")

	require.False(t, at.Before(c.ExecuteNotBefore),
		"sealed at %s but the window does not open until %s — the intent is not yet collectable",
		at, c.ExecuteNotBefore)
	require.False(t, at.After(c.ExecuteNotAfter),
		"sealed at %s but the window closed at %s — this intent can NEVER be collected",
		at, c.ExecuteNotAfter)
}
