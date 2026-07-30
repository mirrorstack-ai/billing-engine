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

// A reserved infra./platform. charge is a PLATFORM<->APP transaction: the app
// pays cost x 1.2 and the platform's compensation IS that 0.2. The developer is
// not party to it, so it must never reach ModuleIncomeForPeriod — otherwise the
// margin-share hands 70-85% of the platform's own infra markup back to the
// developer, netting -0.64C..-0.82C per infra dollar against a markup meant to
// earn +0.2C.
//
// This is an integration test on purpose: the defect lived entirely in SQL, so
// the fake-store unit tests in service_test.go could not see it.
func TestSettleDevelopers_Integration_ExcludesInfraFromModuleIncome(t *testing.T) {
	pool := testutil.NewTestDB(t)
	store := cycle.NewStore(pool)
	svc := cycle.NewService(store, nil)
	ctx := context.Background()

	acct := seedAccount(t, pool)
	app, mod := uuid.New(), uuid.New()

	// The developer's own metric: 20 x 50,000 = 1,000,000 µ$, no markup.
	seedMetricDef(t, pool, mod, "orders.placed", usage.KindSum, 50_000)
	seedEvent(t, pool, acct, app, mod, "orders.placed", usage.KindSum, 20, "2026-06-10T00:00:00Z")

	// A reserved metric ATTRIBUTED to that same real module — the shape that
	// caused the leak. No per-module catalog row is seeded: the rollup resolves
	// it through the (module, metric) -> (SENTINEL, metric) fallback against
	// migration 020's seed, then applies the 12/10 reserved markup.
	seedEvent(t, pool, acct, app, mod, "infra.cron.count", usage.KindCount, 100, "2026-06-10T00:00:00Z")

	// Residual infra booked against the platform-infra sentinel. It has no
	// module_visibility row, so unfiltered it settled at the private 30%
	// default and accrued 70% of residual infra revenue to developer_id = NULL.
	seedEvent(t, pool, acct, app, usage.PlatformInfraModuleID(), "infra.cron.count", usage.KindCount, 500, "2026-06-10T00:00:00Z")

	start, end := mustTime(t, pStart), mustTime(t, pEnd)
	period, err := svc.RollupPeriod(ctx, acct, start, end)
	require.NoError(t, err)

	// Sanity: the infra rows really did roll up and really did carry the
	// markup. Without this the test could pass for the wrong reason (no infra
	// aggregate written at all), leaving the exclusion unproven.
	var infraCharged int64
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT COALESCE(SUM(charged_micros), 0)::bigint
		   FROM ms_billing.usage_aggregates
		  WHERE period_id = $1 AND metric LIKE 'infra.%'`,
		period.PeriodID.String()).Scan(&infraCharged))
	require.Equal(t, int64(720), infraCharged,
		"600 raw µ$ of infra.cron.count must bill at 12/10 = 720; if this is 600 the markup regressed, if 0 nothing rolled up and the exclusion below proves nothing")

	sum, err := svc.SettleDevelopers(ctx, acct, period.PeriodID)
	require.NoError(t, err)

	byModule := make(map[uuid.UUID]cycle.ModuleSettlement, len(sum.Settlements))
	for _, s := range sum.Settlements {
		byModule[s.ModuleID] = s
	}

	got, ok := byModule[mod]
	require.True(t, ok, "the developer's module must still settle")

	// 1,000,000 — NOT 1,000,120. The 120 µ$ is the app's infra line plus the
	// platform's own markup on it.
	require.EqualValues(t, 1_000_000, got.IncomeMicros,
		"reserved infra.* revenue leaked into developer income")
	require.EqualValues(t, 300_000, got.PlatformTakeMicros, "30%% private take on custom income only")
	require.EqualValues(t, 700_000, got.DeveloperOwedMicros)

	// The sentinel is not a developer and must not produce a ledger row at all.
	_, sentinelSettled := byModule[usage.PlatformInfraModuleID()]
	require.False(t, sentinelSettled,
		"the platform-infra sentinel settled as if it were a third-party developer")
}
