//go:build integration

package cycle_test

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/mirrorstack-ai/billing-engine/internal/account/cycle"
	"github.com/mirrorstack-ai/billing-engine/internal/account/usage"
	"github.com/mirrorstack-ai/billing-engine/internal/shared/testutil"
)

// TestInfraVersionPricing_PostPublishAbsorb_EarlierPeriodNeverRetroactivelyReprices
// is issue #219's exact scenario, end to end: a reserved infra.* metric priced
// through an api-platform SetInfraPriceOverrides publish, version-stamped, must
// resolve VERSION-FIRST from the immutable metric_version_prices snapshot — the
// same table and the same cycle.MetricPriceMicros code path custom metrics
// already use — so a LATER publish (an ms.AbsorbInfra() adoption, or any other
// re-price) can never retroactively change an EARLIER, already-rolled-up
// period's charge on RE-ROLLUP.
//
// Before this fix, SyncInfraPriceOverrides wrote ONLY to the live, mutable
// ms_billing.metric_definitions row (never to metric_version_prices), so an
// infra event's module_version was purely a reporting dimension: every
// re-rollup re-resolved the price from whatever metric_definitions said AT
// THAT MOMENT. This test's second RollupPeriod call is the "a publish
// re-prices closed periods on re-rollup" bug from the issue title; asserting
// the ORIGINAL price survives is the fix.
func TestInfraVersionPricing_PostPublishAbsorb_EarlierPeriodNeverRetroactivelyReprices(t *testing.T) {
	pool := testutil.NewTestDB(t)
	cycleSvc := cycle.NewService(cycle.NewStore(pool), nil)
	usageSvc := usage.NewService(usage.NewStore(pool))
	ctx := context.Background()

	acct := seedAccount(t, pool)
	app, mod := uuid.New(), uuid.New()
	const metric = "infra.mcp.tool_call.count"

	// v1 publishes an explicit override at 100 µ$/call, version-stamped — the
	// billing-engine side of a module's ms.Meter("infra.X", ms.Price(n)).
	_, err := usageSvc.SetInfraPriceOverrides(ctx, usage.SetInfraPriceOverridesRequest{
		ModuleID:      mod,
		ModuleVersion: "1.0.0",
		Overrides:     []usage.InfraPriceOverride{{Metric: metric, UnitPriceMicros: 100}},
	})
	require.NoError(t, err)

	// v1's usage accrues at version "1.0.0", before any later re-price exists.
	seedEventVersion(t, pool, acct, app, mod, metric, usage.KindCount, 5, "2026-06-05T00:00:00Z", "1.0.0")

	resp, err := cycleSvc.RollupPeriod(ctx, acct, mustTime(t, pStart), mustTime(t, pEnd))
	require.NoError(t, err)

	v1 := aggByVersion(resp.Aggregates, metric)["1.0.0"]
	require.EqualValues(t, 100, v1.UnitPriceMicros, "v1 resolves its own version-stamped snapshot")
	// raw cost = 5 * 100 = 500; reserved-metric markup is 12/10 ⇒ 600.
	require.EqualValues(t, 600, v1.ChargedMicros)

	// The module adopts ms.AbsorbInfra() at a LATER version, mid-cycle — this
	// REWRITES the live metric_definitions row for (mod, metric) to 0, exactly
	// as a real re-publish does.
	_, err = usageSvc.SetInfraPriceOverrides(ctx, usage.SetInfraPriceOverridesRequest{
		ModuleID: mod, ModuleVersion: "2.0.0", AbsorbAll: true,
	})
	require.NoError(t, err)

	// Re-rollup the SAME already-closed period — the "re-rollup" the issue
	// names. Without the fix, MetricPriceMicros falls through to the
	// now-mutated metric_definitions row (price 0) for the version-less legacy
	// path, or — if version-first were implemented wrong — could let v2's
	// snapshot bleed into v1. Either way v1's charge would change.
	resp2, err := cycleSvc.RollupPeriod(ctx, acct, mustTime(t, pStart), mustTime(t, pEnd))
	require.NoError(t, err)

	v1Again := aggByVersion(resp2.Aggregates, metric)["1.0.0"]
	require.EqualValues(t, 100, v1Again.UnitPriceMicros,
		"v1's immutable snapshot must not be re-priced by v2's later ms.AbsorbInfra() publish")
	require.EqualValues(t, 600, v1Again.ChargedMicros,
		"re-rollup of an already-closed period must reproduce the ORIGINAL charge, not re-price it live")

	wrongAbsorbedCharge := int64(0)
	require.NotEqual(t, wrongAbsorbedCharge, v1Again.ChargedMicros,
		"the pre-fix bug: a later absorb silently re-prices the earlier period's infra line to 0 on re-rollup")
}
