package cycle_test

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/mirrorstack-ai/billing-engine/internal/account/cycle"
	"github.com/mirrorstack-ai/billing-engine/internal/account/usage"
)

// Migration 073 — dev_served: usage a developer's dev tunnel produced. The rule
// under test is a pair, and both halves have to hold at once:
//
//	PRICED   — charged_micros is computed and snapshotted, because the console
//	           shows a developer what the test would have cost;
//	CHARGED  — never: it contributes nothing to the period total the charge leg
//	NEVER      bills.
//
// A test that only asserts "the total is 0" would pass against an
// implementation that simply refused to price the row, which would take the
// console's number away. Every test below pins BOTH.

// rawAggDev is rawAgg with the migration-073 tunnel flag set.
func rawAggDev(app, mod uuid.UUID, metric string, kind cycle.Kind, qty string) cycle.RawAggregate {
	return cycle.RawAggregate{
		AppID: app, ModuleID: mod, Metric: metric, Kind: kind,
		BillableQuantity: qty, DevServed: true,
	}
}

func TestRollupPeriod_DevServedIsPricedButAddsNothingToTheChargedTotal(t *testing.T) {
	store := newFakeStore()
	app, mod := uuid.New(), uuid.New()
	store.raws = []cycle.RawAggregate{rawAggDev(app, mod, "orders.placed", usage.KindSum, "10")}
	store.prices[priceKey(mod, "orders.placed")] = 50_000 // $0.05/unit

	resp, err := cycle.NewService(store, nil).RollupPeriod(context.Background(), uuid.New(), periodStart, periodEnd)
	require.NoError(t, err)
	require.Len(t, resp.Aggregates, 1)

	a := resp.Aggregates[0]
	require.True(t, a.DevServed, "the flag survives from the raw row onto the aggregate")
	require.EqualValues(t, 500_000, a.RawCostMicros, "priced normally: 10 × 50_000")
	require.EqualValues(t, 500_000, a.ChargedMicros,
		"charged_micros is REAL — it is the figure the console shows the developer")

	require.EqualValues(t, 0, resp.TotalChargedMicros,
		"…and none of it reaches the total the charge leg bills")
	require.EqualValues(t, 500_000, resp.DevServedChargedMicros,
		"it is reported on its own field instead of being silently dropped")
}

func TestRollupPeriod_MixedDevAndDeployedSameMetricAreTwoAggregateRows(t *testing.T) {
	// 🔴 The collision migration 073 widened the unique key for. A module
	// deployed for most of the period and tunnelled for the rest emits BOTH
	// kinds of usage of the SAME metric, in ONE period. They must land as two
	// rows: one billed, one displayed. Under the pre-073 key they were the same
	// row, and whichever the rollup upserted last became the whole period.
	store := newFakeStore()
	app, mod := uuid.New(), uuid.New()
	store.raws = []cycle.RawAggregate{
		rawAgg(app, mod, "orders.placed", usage.KindSum, "10"),    // deployed
		rawAggDev(app, mod, "orders.placed", usage.KindSum, "40"), // tunnelled
	}
	store.prices[priceKey(mod, "orders.placed")] = 50_000

	resp, err := cycle.NewService(store, nil).RollupPeriod(context.Background(), uuid.New(), periodStart, periodEnd)
	require.NoError(t, err)
	require.Len(t, resp.Aggregates, 2, "same metric, one period, two rows — not one overwriting the other")

	// The fake's aggregate map is keyed exactly like the DB's unique index, so
	// two surviving entries is the assertion that the key really did widen.
	require.Len(t, store.aggregates, 2,
		"both rows are upserted under DISTINCT keys; a narrow key would leave one")

	byDev := map[bool]cycle.MetricAggregate{}
	for _, a := range resp.Aggregates {
		byDev[a.DevServed] = a
	}
	require.EqualValues(t, 500_000, byDev[false].ChargedMicros, "deployed: 10 × 50_000")
	require.EqualValues(t, 2_000_000, byDev[true].ChargedMicros, "tunnelled: 40 × 50_000, priced the same way")

	require.EqualValues(t, 500_000, resp.TotalChargedMicros,
		"the customer owes the DEPLOYED part and only the deployed part")
	require.EqualValues(t, 2_000_000, resp.DevServedChargedMicros)
}

func TestRollupPeriod_DevServedInfraMetricStillPricesOnTheInfraPlane(t *testing.T) {
	// dev_served changes WHO PAYS, never HOW A LINE IS PRICED. A reserved
	// metric keeps its 12/10 plane and its loud missing-price guard; only the
	// destination of its charge changes. (Platform infra is never flagged in
	// practice — RecordInfraUsage does not set it — so this pins the pricing
	// path's independence rather than a reachable production shape.)
	store := newFakeStore()
	app, mod := uuid.New(), uuid.New()
	store.raws = []cycle.RawAggregate{rawAggDev(app, mod, "infra.compute.ms", usage.KindSum, "100")}
	store.prices[priceKey(mod, "infra.compute.ms")] = 1_000

	resp, err := cycle.NewService(store, nil).RollupPeriod(context.Background(), uuid.New(), periodStart, periodEnd)
	require.NoError(t, err)

	a := resp.Aggregates[0]
	require.Equal(t, 12, a.MarkupNum)
	require.Equal(t, 10, a.MarkupDen)
	require.EqualValues(t, 120_000, a.ChargedMicros, "still cost × 1.2")
	require.EqualValues(t, 0, resp.TotalChargedMicros)
	require.EqualValues(t, 120_000, resp.DevServedChargedMicros)
}

func TestRollupPeriod_DevServedOnlyPeriodChargesNothing(t *testing.T) {
	// A developer spends a period exercising a paid meter through a tunnel and
	// deploys nothing. The rollup still runs and still writes priced rows — the
	// console has something to show — but the period is not collectable.
	store := newFakeStore()
	app, mod := uuid.New(), uuid.New()
	store.raws = []cycle.RawAggregate{
		rawAggDev(app, mod, "orders.placed", usage.KindSum, "10"),
		rawAggDev(app, mod, "orders.shipped", usage.KindSum, "3"),
	}
	store.prices[priceKey(mod, "orders.placed")] = 50_000
	store.prices[priceKey(mod, "orders.shipped")] = 10_000

	resp, err := cycle.NewService(store, nil).RollupPeriod(context.Background(), uuid.New(), periodStart, periodEnd)
	require.NoError(t, err)
	require.Len(t, resp.Aggregates, 2)
	for _, a := range resp.Aggregates {
		require.Positive(t, a.ChargedMicros, "every row is priced")
	}
	require.EqualValues(t, 0, resp.TotalChargedMicros, "nothing to collect")
	require.EqualValues(t, 530_000, resp.DevServedChargedMicros) // 500_000 + 30_000
}
