package usage

// White-box pins for the ACCOUNT-level PaaS credit (accountPaasCreditMicros).
// The RPC-level tests can only observe 0 (the credit is subscription-gated OFF
// in v1), so the earn-path math and the usage-only-offset cap invariant are
// pinned here with the gate forced open.

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestAccountPaasCreditMicros_GatedOffIsZero(t *testing.T) {
	// No ACTIVE SaaS subscription → no credit, regardless of the totals (the
	// platform never grants an unearned credit).
	credit, err := accountPaasCreditMicros(false, 1_000_000, 9_000_000)
	require.NoError(t, err)
	require.Zero(t, credit)
}

func TestAccountPaasCreditMicros_EarnedIsPctOfAccountInfra(t *testing.T) {
	// Gate forced open: the credit is PaasCreditPct% of the ACCOUNT-WIDE infra
	// total — a single account-level magnitude, not a per-app one, so it can
	// exceed any one app's usage (10 here) while staying under the account's
	// usage+infra cap.
	credit, err := accountPaasCreditMicros(true, 10, 200)
	require.NoError(t, err)
	require.EqualValues(t, 60, credit, "30% of the account infra total")
	require.LessOrEqual(t, credit, int64(10+200), "capped at usage + infra")
}

func TestAccountPaasCreditMicros_ZeroInfraZeroCredit(t *testing.T) {
	credit, err := accountPaasCreditMicros(true, 5_000_000, 0)
	require.NoError(t, err)
	require.Zero(t, credit, "the credit is infra-proportional; no infra → nothing to offset")
}

func TestAccountPaasCreditMicros_NeverExceedsUsagePlusInfra(t *testing.T) {
	// The cap invariant (credit ≤ moduleUsageTotal + infraTotal ⇒ the credit
	// can never eat base fees). Under today's pct formula the cap arm cannot
	// bind (pct% of infra ≤ infra); this sweep pins the INVARIANT so a future
	// flat-allowance credit formula inherits it rather than silently
	// discounting base fees.
	for _, tc := range []struct{ usage, infra int64 }{
		{0, 0},
		{0, 1},
		{0, 3},
		{0, 1_000_000_000},
		{1, 1},
		{123, 456_789},
		{5_000_000, 20_000_000},
	} {
		credit, err := accountPaasCreditMicros(true, tc.usage, tc.infra)
		require.NoError(t, err)
		require.GreaterOrEqual(t, credit, int64(0))
		require.LessOrEqual(t, credit, tc.usage+tc.infra,
			"usage=%d infra=%d: credit must never exceed the usage-plane charges", tc.usage, tc.infra)
	}
}

// 🔴 THE FORECAST'S STRADDLE MUST PRICE THE APP'S PLAN, NOT THE FLAT BASE.
//
// unresolvedOneTimeChargeIncrementMicros derives its unit from
// resolveBaseFeeMicros(charge.Plan) and then adds a FULL unit when the grace
// straddles the boundary (accountbill.go, the `amount += unitMicros` and the
// D1d `amount = unitMicros` narrowing). At the default plan the plan base and
// the flat constant are the same $20, so every existing forecast assertion
// passes whether or not the plan is read — a revert to BaseFeeMicros survives.
//
// A BUSINESS row ($50) separates them. Mutation-proved 2026-09-13 by reverting
// resolveBaseFeeMicros to return BaseFeeMicros: this test fails, the rest pass.
func TestUnresolvedOneTimeChargeIncrement_StraddlePricesThePlan(t *testing.T) {
	t.Parallel()

	// Created 2 days before the boundary with a 3-day grace: the grace crosses
	// it, so the full unit is added on top of the prorated slice.
	created := time.Date(2026, 6, 2, 0, 0, 0, 0, time.UTC)
	activated := time.Date(2026, 5, 4, 9, 0, 0, 0, time.UTC)
	projectedStart := time.Date(2026, 6, 4, 0, 0, 0, 0, time.UTC)

	raw := func(plan Plan) UnresolvedOneTimeChargeRaw {
		return UnresolvedOneTimeChargeRaw{
			Kind:           UnresolvedOneTimeChargeCreationBase,
			ChargeID:       uuid.New(),
			AppID:          uuid.New(),
			ChargeAt:       created,
			GraceExpiresAt: GraceExpiry(created),
			ActivatedAt:    activated,
			Plan:           plan,
		}
	}

	pro, err := unresolvedOneTimeChargeIncrementMicros(raw(PlanPro), projectedStart)
	require.NoError(t, err)
	business, err := unresolvedOneTimeChargeIncrementMicros(raw(PlanBusiness), projectedStart)
	require.NoError(t, err)

	require.Positive(t, pro, "the fixture must forecast something, or it cannot discriminate")
	require.Equal(t, TermsFor(PlanPro).BaseFeeMicros, BaseFeeMicros,
		"this test assumes Pro IS the flat base; if that changes, so must the reasoning below")

	// 🔴 THE DISCRIMINATING ASSERTION. Business is 2.5× Pro's base, so its
	// forecast increment must exceed Pro's. A site that reads the flat constant
	// returns the SAME number for both.
	require.Greater(t, business, pro,
		"a Business app's forecast increment must exceed a Pro app's — the flat base was used")
	// NOT asserted as an exact ratio: proration ROUNDS, so scaling one rounded
	// slice by the base ratio lands a micro off the other. (I wrote that
	// assertion twice in this session and it failed both times.) The
	// strict-greater check above is what kills the mutant; the exact figure is
	// pinned where it is computed, not re-derived here.
	require.EqualValues(t, business-pro,
		TermsFor(PlanBusiness).BaseFeeMicros-TermsFor(PlanPro).BaseFeeMicros+
			(ProratedBaseMicros(TermsFor(PlanBusiness).BaseFeeMicros, created, projectedStart.AddDate(0, -1, 0), projectedStart)-
				ProratedBaseMicros(TermsFor(PlanPro).BaseFeeMicros, created, projectedStart.AddDate(0, -1, 0), projectedStart)),
		"the gap between the two forecasts is the gap between their plan bases")
}
