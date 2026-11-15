package usage_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/mirrorstack-ai/billing-engine/internal/account/usage"
)

// TestPlanFeeMicros_DefaultBasisIsIdentity pins the acceptance criterion: with
// no payer_price_basis row (DefaultPriceBasisBP, 10000), every existing
// amount is byte-identical to pre-258 behavior, for every plan.
func TestPlanFeeMicros_DefaultBasisIsIdentity(t *testing.T) {
	for _, plan := range []usage.Plan{usage.PlanFree, usage.PlanPro, usage.PlanBusiness} {
		list := usage.TermsFor(plan).BaseFeeMicros
		require.Equal(t, list, usage.PlanFeeMicros(usage.DefaultPriceBasisBP, list), "plan %s at 10000bp must equal list", plan)
	}
	// A missing row resolves to 0 at the store layer only as a defensive
	// fallback never expected in practice; PlanFeeMicros itself must still
	// never treat 0 as "free".
	require.EqualValues(t, 20_000_000, usage.PlanFeeMicros(0, 20_000_000), "priceBasisBP<=0 must price at list, never free")
}

// TestPlanFeeMicros_FirstDistributorFixtures pins the exact numbers from
// billing-engine#258 at basis 6000 (D8-wholesale-60, 60% of list for the
// first distributor's plan fees and seats — test payer only, never a
// constant/default/seed in this codebase).
func TestPlanFeeMicros_FirstDistributorFixtures(t *testing.T) {
	const bp = 6000

	// Pro app 20.00 -> 12.00. Business app 50.00 -> 30.00.
	require.EqualValues(t, 12_000_000, usage.PlanFeeMicros(bp, 20_000_000), "Pro app")
	require.EqualValues(t, 30_000_000, usage.PlanFeeMicros(bp, 50_000_000), "Business app")

	// Base seat 4.00 -> 2.40. Enterprise seat 19.00 -> 11.40.
	baseSeat := usage.PlanFeeMicros(bp, 4_000_000)
	enterpriseSeat := usage.PlanFeeMicros(bp, 19_000_000)
	require.EqualValues(t, 2_400_000, baseSeat, "Base seat")
	require.EqualValues(t, 11_400_000, enterpriseSeat, "Enterprise seat")

	// Five Enterprise seats -> 57.00: the UNIT is rounded once, then
	// multiplied by quantity — never the pre-multiplied total rounded.
	require.EqualValues(t, 57_000_000, enterpriseSeat*5, "five Enterprise seats")

	// (Pro app 20.00 + 3 Base seats at 4.00) = 32.00 -> 19.20: each LINE priced
	// (and rounded) independently, then summed.
	comboTotal := usage.PlanFeeMicros(bp, 20_000_000) + baseSeat*3
	require.EqualValues(t, 19_200_000, comboTotal, "Pro app + 3 Base seats")
}

// TestPlanFeeMicros_RoundsHalfUpOncePerLine exercises a basis that does not
// divide the list fee evenly, to pin the rounding rule itself rather than
// fixtures that happen to divide exactly.
func TestPlanFeeMicros_RoundsHalfUpOncePerLine(t *testing.T) {
	// 20_000_000 * 3333 / 10000 = 6,666,000 exactly — pick a basis that forces
	// a genuine half-up decision instead.
	// 1 * 5000 / 10000 = 0.5 -> rounds up to 1.
	require.EqualValues(t, 1, usage.PlanFeeMicros(5000, 1))
	// 1 * 4999 / 10000 = 0.4999 -> rounds down to 0.
	require.EqualValues(t, 0, usage.PlanFeeMicros(4999, 1))
	// 3 * 5000 / 10000 = 1.5 -> rounds up to 2.
	require.EqualValues(t, 2, usage.PlanFeeMicros(5000, 3))
}

// TestPlanFeeMicros_NeverAboveList refuses to charge more than list even if
// miscalled with a basis above 10000 — the function is a discount-only seam.
func TestPlanFeeMicros_NeverAboveList(t *testing.T) {
	require.EqualValues(t, 20_000_000, usage.PlanFeeMicros(15000, 20_000_000))
}
