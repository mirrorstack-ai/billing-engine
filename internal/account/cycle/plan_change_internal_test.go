package cycle

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/mirrorstack-ai/billing-engine/internal/account/usage"
)

// TestUpgradeDeltaMicros_IsTheDifferenceNotTheNewPrice pins the owner's rule
// on BOTH rungs of the ladder. The RPC refuses business today, so without
// this a mutant that charged the NEW price in full would survive the whole
// suite: Free → Pro is the only upgrade the RPC can make, and Free's base is
// $0, where "difference" and "new price" are the same number.
func TestUpgradeDeltaMicros_IsTheDifferenceNotTheNewPrice(t *testing.T) {
	t.Parallel()

	periodStart := time.Date(2026, 6, 4, 0, 0, 0, 0, time.UTC)
	periodEnd := time.Date(2026, 7, 4, 0, 0, 0, 0, time.UTC)
	half := time.Date(2026, 6, 19, 15, 0, 0, 0, time.UTC) // 15 of 30 days left, the 19th inclusive

	got, err := upgradeDeltaMicros(usage.PlanFree, usage.PlanPro, half, periodStart, periodEnd)
	require.NoError(t, err)
	require.EqualValues(t, 10_000_000, got, "(20 − 0) × 15/30")

	got, err = upgradeDeltaMicros(usage.PlanPro, usage.PlanBusiness, half, periodStart, periodEnd)
	require.NoError(t, err)
	require.EqualValues(t, 15_000_000, got, "(50 − 20) × 15/30, not 50 × 15/30 = 25")

	got, err = upgradeDeltaMicros(usage.PlanFree, usage.PlanBusiness, periodStart, periodStart, periodEnd)
	require.NoError(t, err)
	require.EqualValues(t, 50_000_000, got, "the whole period left: the full difference")

	got, err = upgradeDeltaMicros(usage.PlanFree, usage.PlanPro, periodEnd, periodStart, periodEnd)
	require.NoError(t, err)
	require.Zero(t, got, "nothing left of the period: nothing to charge")

	_, err = upgradeDeltaMicros(usage.PlanPro, usage.PlanFree, half, periodStart, periodEnd)
	require.Error(t, err, "a downgrade is never priced here")
}

// TestCreationBaseSegments_ChainsFoldedChanges pins the segment builder: the
// first segment starts at created_at on the plan the first fold LEFT, each
// fold opens a segment at its effect instant, and a chain that does not end
// on the row's plan is refused rather than priced.
func TestCreationBaseSegments_ChainsFoldedChanges(t *testing.T) {
	t.Parallel()

	created := time.Date(2026, 6, 10, 9, 0, 0, 0, time.UTC)
	changed := time.Date(2026, 6, 19, 15, 0, 0, 0, time.UTC)
	app := AppMirror{CreatedAt: created, Plan: usage.PlanPro}

	segments, err := creationBaseSegments(app, nil)
	require.NoError(t, err)
	require.Equal(t, []usage.BaseSegment{{From: created, BaseMicros: 20_000_000}}, segments)

	segments, err = creationBaseSegments(app, []PlanChange{{FromPlan: usage.PlanFree, ToPlan: usage.PlanPro, EffectiveAt: changed}})
	require.NoError(t, err)
	require.Equal(t, []usage.BaseSegment{{From: created, BaseMicros: 0}, {From: changed, BaseMicros: 20_000_000}}, segments)

	_, err = creationBaseSegments(app, []PlanChange{{FromPlan: usage.PlanFree, ToPlan: usage.PlanBusiness, EffectiveAt: changed}})
	require.Error(t, err, "the chain ends on business but the row says pro")

	_, err = creationBaseSegments(app, []PlanChange{
		{FromPlan: usage.PlanFree, ToPlan: usage.PlanBusiness, EffectiveAt: changed},
		{FromPlan: usage.PlanFree, ToPlan: usage.PlanPro, EffectiveAt: changed.Add(time.Hour)},
	})
	require.Error(t, err, "the second fold does not start where the first ended")
}
