package cycle_test

import (
	"context"
	"fmt"
	"strconv"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/mirrorstack-ai/billing-engine/internal/account/cycle"
	"github.com/mirrorstack-ai/billing-engine/internal/account/usage"
)

// core-v2#1665: a keyed peak meter (aggregation_key="subject" — 活躍登入使用者 /
// monthly active users) was charged per module VERSION at 100%, so N versions in
// a period billed N x the price for the same users. These tests pin the owner's
// rule at the pricing seam: each version's line is charged its SHARE OF THE
// PERIOD (window_v / P), and Σ window_v == P, so the versions total one period
// price. The windows themselves come from RollupKeyedPeakKind (SQL, covered by
// the usage package's integration test); here they are supplied by the fake.

const activeUsersMetric = "users.monthly_active"

// junePeriodSeconds is the fixture period [periodStart, periodEnd): 30 days.
const junePeriodSeconds = 30 * 24 * 60 * 60

func rawKeyedPeak(app, mod uuid.UUID, version, qty string, activeSeconds int) cycle.RawAggregate {
	return cycle.RawAggregate{
		AppID: app, ModuleID: mod, Metric: activeUsersMetric, Kind: usage.KindPeak,
		AggregationKey: usage.AggregationKeySubject, ModuleVersion: version,
		BillableQuantity: qty, ActiveSeconds: strconv.Itoa(activeSeconds),
	}
}

func TestRollupPeriod_KeyedPeak_HundredVersionsBillExactlyOnePeriodPrice(t *testing.T) {
	store := newFakeStore()
	app, mod := uuid.New(), uuid.New()
	store.prices[priceKey(mod, activeUsersMetric)] = 20_000 // $0.020 / active user

	const versions = 100
	// The same 3 users stay signed in while the app ships 100 versions, each
	// live for an equal 1/100 of the period.
	for v := 0; v < versions; v++ {
		store.raws = append(store.raws,
			rawKeyedPeak(app, mod, fmt.Sprintf("1.0.%d", v), "3", junePeriodSeconds/versions))
	}

	resp, err := cycle.NewService(store, nil).RollupPeriod(context.Background(), uuid.New(), periodStart, periodEnd)
	require.NoError(t, err)
	require.Len(t, resp.Aggregates, versions)

	for _, a := range resp.Aggregates {
		require.Equal(t, "3", a.BillableQuantity, "the persisted quantity stays the raw per-version user count")
		require.EqualValues(t, 600, a.ChargedMicros, "3 users × 20_000 × 1/100 of the period")
		require.NotNil(t, a.ActiveSeconds, "the window is snapshotted so the line is reproducible")
		require.NotNil(t, a.PeriodDays)
		require.Equal(t, "30", *a.PeriodDays)
	}
	require.EqualValues(t, 3*20_000, resp.TotalChargedMicros,
		"100 versions in one period = exactly 1x the price (it was 100x: 6_000_000)")
}

func TestRollupPeriod_KeyedPeak_VersionLiveHalfThePeriodIsChargedHalf(t *testing.T) {
	store := newFakeStore()
	app, mod := uuid.New(), uuid.New()
	store.versionPrices[versionPriceKey(mod, activeUsersMetric, "1.0.0")] = 20_000
	store.versionPrices[versionPriceKey(mod, activeUsersMetric, "2.0.0")] = 30_000
	store.raws = []cycle.RawAggregate{
		rawKeyedPeak(app, mod, "1.0.0", "4", junePeriodSeconds/2),
		rawKeyedPeak(app, mod, "2.0.0", "4", junePeriodSeconds/2),
	}

	resp, err := cycle.NewService(store, nil).RollupPeriod(context.Background(), uuid.New(), periodStart, periodEnd)
	require.NoError(t, err)
	require.Len(t, resp.Aggregates, 2)

	byVersion := map[string]cycle.MetricAggregate{}
	for _, a := range resp.Aggregates {
		byVersion[a.ModuleVersion] = a
	}
	require.EqualValues(t, 40_000, byVersion["1.0.0"].ChargedMicros, "4 × 20_000 × 1/2 — half of its full-period 80_000")
	require.EqualValues(t, 40_000, byVersion["1.0.0"].RawCostMicros)
	require.EqualValues(t, 20_000, byVersion["1.0.0"].UnitPriceMicros, "the version-keyed price is untouched")
	require.EqualValues(t, 60_000, byVersion["2.0.0"].ChargedMicros, "4 × 30_000 × 1/2 — each half bills at ITS version's price")
	require.EqualValues(t, 100_000, resp.TotalChargedMicros)
}

func TestRollupPeriod_KeyedPeak_SingleVersionWholePeriodIsUnchanged(t *testing.T) {
	store := newFakeStore()
	app, mod := uuid.New(), uuid.New()
	store.prices[priceKey(mod, activeUsersMetric)] = 20_000
	store.raws = []cycle.RawAggregate{rawKeyedPeak(app, mod, "1.0.0", "7", junePeriodSeconds)}

	resp, err := cycle.NewService(store, nil).RollupPeriod(context.Background(), uuid.New(), periodStart, periodEnd)
	require.NoError(t, err)
	require.EqualValues(t, 140_000, resp.TotalChargedMicros,
		"no-regression: window == P is factor 1, the pre-#1665 charge")
}

func TestRollupPeriod_AdditiveMeterAcrossVersionsIsNeverProrated(t *testing.T) {
	store := newFakeStore()
	app, mod := uuid.New(), uuid.New()
	store.prices[priceKey(mod, "orders.placed")] = 50_000

	const versions = 100
	for v := 0; v < versions; v++ {
		for _, kind := range []cycle.Kind{usage.KindSum, usage.KindCount} {
			raw := rawAggVersion(app, mod, "orders.placed", kind, fmt.Sprintf("1.0.%d", v), "2")
			// Even a row that (wrongly) carried a window must not be scaled:
			// additive usage is a sum of real events, every one of them billable.
			raw.ActiveSeconds = strconv.Itoa(junePeriodSeconds / versions)
			if kind == usage.KindCount {
				raw.Metric = "orders.counted"
			}
			store.raws = append(store.raws, raw)
		}
	}
	store.prices[priceKey(mod, "orders.counted")] = 50_000

	resp, err := cycle.NewService(store, nil).RollupPeriod(context.Background(), uuid.New(), periodStart, periodEnd)
	require.NoError(t, err)
	require.Len(t, resp.Aggregates, 2*versions)
	for _, a := range resp.Aggregates {
		require.EqualValues(t, 100_000, a.ChargedMicros, "2 × 50_000 at 100% on every version's line")
	}
	require.EqualValues(t, 2*versions*100_000, resp.TotalChargedMicros)
}
