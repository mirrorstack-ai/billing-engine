//go:build integration

package cycle_test

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/mirrorstack-ai/billing-engine/internal/account/cycle"
	"github.com/mirrorstack-ai/billing-engine/internal/account/usage"
	"github.com/mirrorstack-ai/billing-engine/internal/shared/testutil"
)

// These integration tests pin the usage-time-pricing Phase 1 model
// (docs-temp/usage-time-pricing/design.md): every LEVEL metric (peak,
// time_weighted) now bills PER VERSION using
//
//	charge_v = representative_level_v × (window_v / P) × price_v
//
// where P is the whole rollup period and window_v is the version's active
// duration (the LEAD-segmented stream with module_version kept OUT of the
// PARTITION BY — #58's one surviving insight — so a successor version's
// first sample terminates its predecessor's window at the TRUE handoff
// instant). Price resolves VERSION-FIRST from the immutable
// metric_version_prices snapshot (migration 044), falling back to the
// version-blind metric_definitions catalog.
//
// This supersedes fix/peak-multiversion-overcharge (#58, draft PR, on
// hold): #58 explored collapsing module_version out of the peak/
// time_weighted rollup entirely to stop a Σ-per-version double-count. That
// is correct-but-coarse (see #58's own commit message + the design doc's
// "verdict on #58") — it doesn't fix the deeper mid-period-reprice bug,
// because price was still resolved version-BLIND from the catalog. This
// suite's tests (a)/(c) carry #58's original scenarios forward, adapted to
// assert the NEW per-version-priced behavior instead of collapse.

// seedVersionPrice inserts one immutable metric_version_prices snapshot
// (migration 044) directly — the DB-level equivalent of an
// api-platform SetMetricVersionPrices call at version-publish time.
func seedVersionPrice(t *testing.T, pool *pgxpool.Pool, moduleID uuid.UUID, metric, version string, priceMicros int64) {
	t.Helper()
	_, err := pool.Exec(context.Background(),
		`INSERT INTO ms_billing.metric_version_prices (module_id, metric, module_version, unit_price_micros)
		 VALUES ($1,$2,$3,$4)`,
		moduleID.String(), metric, version, priceMicros)
	require.NoError(t, err)
}

// aggByVersion indexes a RollupSummary's aggregates for one metric by
// module_version, for the multi-version assertions below.
func aggByVersion(aggs []cycle.MetricAggregate, metric string) map[string]cycle.MetricAggregate {
	out := map[string]cycle.MetricAggregate{}
	for _, a := range aggs {
		if a.Metric == metric {
			out[a.ModuleVersion] = a
		}
	}
	return out
}

// TestUsageTimePricing_NoRegression_SingleVersionWholePeriod is the
// load-bearing no-regression invariant: a single version spanning the whole
// period must reduce to the EXACT pre-this-PR number, for both peak and
// time_weighted. window_v == P ⇒ the peak proration factor is 1 (full peak,
// unchanged); time_weighted's byte-hours integral was never prorated by P in
// the first place, so it is trivially unchanged. Mirrors
// TestRollupPeriod_Integration_SumAndTimeWeighted's numbers (store_integration_test.go)
// but with an explicit module_version, proving the version dimension is a
// pure no-op in the single-version common case.
func TestUsageTimePricing_NoRegression_SingleVersionWholePeriod(t *testing.T) {
	pool := testutil.NewTestDB(t)
	svc := cycle.NewService(cycle.NewStore(pool), nil)
	ctx := context.Background()

	acct := seedAccount(t, pool)
	app, mod := uuid.New(), uuid.New()
	seedMetricDef(t, pool, mod, "queue.depth", usage.KindPeak, 1_000)
	seedMetricDef(t, pool, mod, "myapp.bytes", usage.KindTimeWeighted, 3)

	// Peak: single version, MAX = 5 (period-wide, since there's only one
	// version). Pre-this-PR (and pre-#58) number: 5 × 1_000 = 5_000, no
	// window proration.
	seedEventVersion(t, pool, acct, app, mod, "queue.depth", usage.KindPeak, 2, "2026-06-01T00:00:00Z", "1.0.0")
	seedEventVersion(t, pool, acct, app, mod, "queue.depth", usage.KindPeak, 5, "2026-06-02T00:00:00Z", "1.0.0")

	// time_weighted: identical to TestRollupPeriod_Integration_SumAndTimeWeighted's
	// "myapp.bytes" case (100 held 1h, then 200 held to period_end) — 143_900
	// byte-hours × price 3 = 431_700 micros, unchanged by carrying a version.
	seedEventVersion(t, pool, acct, app, mod, "myapp.bytes", usage.KindTimeWeighted, 100, "2026-06-01T00:00:00Z", "1.0.0")
	seedEventVersion(t, pool, acct, app, mod, "myapp.bytes", usage.KindTimeWeighted, 200, "2026-06-01T01:00:00Z", "1.0.0")

	resp, err := svc.RollupPeriod(ctx, acct, mustTime(t, pStart), mustTime(t, pEnd))
	require.NoError(t, err)

	peak := aggByVersion(resp.Aggregates, "queue.depth")
	require.Len(t, peak, 1, "single version → exactly one aggregate row")
	p := peak["1.0.0"]
	require.Equal(t, "5", p.BillableQuantity, "raw MAX, unprorated in storage")
	require.EqualValues(t, 5_000, p.ChargedMicros, "window_v == P ⇒ factor 1 ⇒ pre-this-PR number unchanged")
	require.NotNil(t, p.ActiveSeconds)
	require.Equal(t, "2592000", *p.ActiveSeconds, "window_v spans the whole 30-day period (30×86400s)")
	require.NotNil(t, p.PeriodDays)
	require.Equal(t, "30", *p.PeriodDays)

	tw := aggByVersion(resp.Aggregates, "myapp.bytes")
	require.Len(t, tw, 1)
	twAgg := tw["1.0.0"]
	require.Equal(t, "143900", twAgg.BillableQuantity)
	require.EqualValues(t, 431_700, twAgg.RawCostMicros)
	require.EqualValues(t, 431_700, twAgg.ChargedMicros, "time_weighted is never window-prorated a second time")
}

// TestUsageTimePricing_TwoVersionPeak_WindowProratedDifferentPrices is build
// list test (b): a peak metric spanning two module_versions with a clean
// mid-period handoff. v1 (0.1.2) owns the first half of the period at
// $0.02/unit, v2 (0.1.3) owns the second half at $0.05/unit. Each version's
// OWN MAX is billed at its OWN snapshotted price, window-prorated by its OWN
// active fraction of the period — NOT Σ(per-version MAX at one shared
// price), and NOT a naive average of the two MAXes.
func TestUsageTimePricing_TwoVersionPeak_WindowProratedDifferentPrices(t *testing.T) {
	pool := testutil.NewTestDB(t)
	svc := cycle.NewService(cycle.NewStore(pool), nil)
	ctx := context.Background()

	acct := seedAccount(t, pool)
	app, mod := uuid.New(), uuid.New()
	seedMetricDef(t, pool, mod, "queue.depth", usage.KindPeak, 999) // catalog fallback; version snapshots below win
	seedVersionPrice(t, pool, mod, "queue.depth", "0.1.2", 20_000)  // $0.02/unit
	seedVersionPrice(t, pool, mod, "queue.depth", "0.1.3", 50_000)  // $0.05/unit

	// v1 (0.1.2): days 0–15, larger value emitted SECOND so MAX=5 proves
	// MAX not first-seen. v2 (0.1.3) starts exactly at day 15 (the true
	// handoff) — v1's window is cut there, not bled to period_end.
	seedEventVersion(t, pool, acct, app, mod, "queue.depth", usage.KindPeak, 2, "2026-06-01T00:00:00Z", "0.1.2")
	seedEventVersion(t, pool, acct, app, mod, "queue.depth", usage.KindPeak, 5, "2026-06-05T00:00:00Z", "0.1.2")
	// v2 (0.1.3): days 15–30, MAX=8 (a lower later sample proves MAX not last-seen).
	seedEventVersion(t, pool, acct, app, mod, "queue.depth", usage.KindPeak, 8, "2026-06-16T00:00:00Z", "0.1.3")
	seedEventVersion(t, pool, acct, app, mod, "queue.depth", usage.KindPeak, 3, "2026-06-20T00:00:00Z", "0.1.3")

	resp, err := svc.RollupPeriod(ctx, acct, mustTime(t, pStart), mustTime(t, pEnd))
	require.NoError(t, err)

	byVersion := aggByVersion(resp.Aggregates, "queue.depth")
	require.Len(t, byVersion, 2, "peak keeps splitting per version (superseding #58's collapse)")

	v1, v2 := byVersion["0.1.2"], byVersion["0.1.3"]

	require.Equal(t, "5", v1.BillableQuantity, "v1's OWN MAX (not the period-wide MAX of 8)")
	require.Equal(t, "8", v2.BillableQuantity, "v2's OWN MAX (not the period-wide MAX including v1's 5)")

	require.EqualValues(t, 20_000, v1.UnitPriceMicros, "v1 prices at its OWN snapshot")
	require.EqualValues(t, 50_000, v2.UnitPriceMicros, "v2 prices at its OWN snapshot")

	require.NotNil(t, v1.ActiveSeconds)
	require.NotNil(t, v2.ActiveSeconds)
	require.Equal(t, "1296000", *v1.ActiveSeconds, "v1's window == 15 days (day 0 → the true v2 handoff at day 15)")
	require.Equal(t, "1296000", *v2.ActiveSeconds, "v2's window == the remaining 15 days (day 15 → period_end)")

	// charge_v = MAX_v × (window_v / P) × price_v. P = 30 days, window_v/P = 0.5 each:
	//   v1: 5 × 0.5 × 20_000 = 50_000
	//   v2: 8 × 0.5 × 50_000 = 200_000
	require.EqualValues(t, 50_000, v1.ChargedMicros)
	require.EqualValues(t, 200_000, v2.ChargedMicros)

	// The rejected alternatives, spelled out so a future regression is obvious:
	naiveSharedPriceSum := int64(5+8) * 20_000 // Σ(per-version MAX) at ONE shared price
	naiveAverage := int64((5+8)/2) * 35_000    // average MAX × average price
	require.NotEqual(t, naiveSharedPriceSum, v1.ChargedMicros+v2.ChargedMicros)
	require.NotEqual(t, naiveAverage, v1.ChargedMicros+v2.ChargedMicros)
	require.EqualValues(t, 250_000, resp.TotalChargedMicros, "the true window-prorated sum")
}

// TestUsageTimePricing_TwoVersionTimeWeighted_NoTailBleedDifferentPrices is
// build list test (c): the time_weighted sibling of (b). A level of 100 held
// across a mid-period v1→v2 upgrade at the true handoff (day 15) must NOT
// tail-bleed v1's last sample to period_end (the #58 bug), and each
// version's own integral must price at its OWN snapshot.
func TestUsageTimePricing_TwoVersionTimeWeighted_NoTailBleedDifferentPrices(t *testing.T) {
	pool := testutil.NewTestDB(t)
	svc := cycle.NewService(cycle.NewStore(pool), nil)
	ctx := context.Background()

	acct := seedAccount(t, pool)
	app, mod := uuid.New(), uuid.New()
	seedMetricDef(t, pool, mod, "db.rows_held", usage.KindTimeWeighted, 999) // catalog fallback; version snapshots win
	seedVersionPrice(t, pool, mod, "db.rows_held", "1.0.0", 20_000)          // $0.02/unit-hour
	seedVersionPrice(t, pool, mod, "db.rows_held", "2.0.0", 50_000)          // $0.05/unit-hour

	// Level 100 held the whole period; the module upgrades 1.0.0 → 2.0.0 at
	// the true handoff, exactly day 15 (the ONLY sample the LEAD needs to
	// terminate v1's window there instead of bleeding to period_end).
	seedEventVersion(t, pool, acct, app, mod, "db.rows_held", usage.KindTimeWeighted, 100, "2026-06-01T00:00:00Z", "1.0.0")
	seedEventVersion(t, pool, acct, app, mod, "db.rows_held", usage.KindTimeWeighted, 100, "2026-06-16T00:00:00Z", "2.0.0")

	resp, err := svc.RollupPeriod(ctx, acct, mustTime(t, pStart), mustTime(t, pEnd))
	require.NoError(t, err)

	byVersion := aggByVersion(resp.Aggregates, "db.rows_held")
	require.Len(t, byVersion, 2, "time_weighted keeps splitting per version (superseding #58's collapse)")

	v1, v2 := byVersion["1.0.0"], byVersion["2.0.0"]

	// 15 days × 24h = 360h; NOT 720h (the pre-#58 tail-bleed to period_end).
	require.Equal(t, "36000", v1.BillableQuantity, "v1's window ends AT THE HANDOFF (360h), no tail-bleed to period_end")
	require.Equal(t, "36000", v2.BillableQuantity, "v2 covers the remaining 360h to period_end")

	require.NotNil(t, v1.ActiveSeconds)
	require.NotNil(t, v2.ActiveSeconds)
	require.Equal(t, "1296000", *v1.ActiveSeconds, "v1's window == 15 days, the TRUE handoff instant")
	require.Equal(t, "1296000", *v2.ActiveSeconds)

	require.EqualValues(t, 20_000, v1.UnitPriceMicros, "v1 prices at its OWN snapshot")
	require.EqualValues(t, 50_000, v2.UnitPriceMicros, "v2 prices at its OWN snapshot")

	// charge_v = I_v × price_v (time_weighted is NOT additionally window-
	// prorated — see RollupTimeWeightedKind's query comment):
	//   v1: 36_000 × 20_000 = 720_000_000
	//   v2: 36_000 × 50_000 = 1_800_000_000
	require.EqualValues(t, 720_000_000, v1.ChargedMicros)
	require.EqualValues(t, 1_800_000_000, v2.ChargedMicros)

	// The pre-#58 tail-bled number this test would have produced had the LEAD
	// still partitioned by module_version: v1's last sample would bleed all
	// the way to period_end (720h, not 360h), double-billing the tail once per
	// version. Assert the ACTUAL total is the true 30-day sum, not that.
	tailBledV1Charge := int64(100) * 720 * 20_000 // 100 level × 720h × v1 price
	require.NotEqual(t, tailBledV1Charge, v1.ChargedMicros)
	require.EqualValues(t, 2_520_000_000, resp.TotalChargedMicros)
}

// TestUsageTimePricing_MidPeriodReprice_V1RateNeverRetroactivelyChanges is
// build list test (d) — the exact bug the user is trying to kill. v1
// publishes first at $0.02 and accrues usage; v2 publishes LATER, mid-cycle,
// at a NEW price ($0.05) — simulated the way a real re-price actually
// happens: the version-blind metric_definitions CATALOG row is updated
// in-place (exactly what SetMetricDefinitions does on every publish) AND a
// new immutable metric_version_prices snapshot is written for v2. v1's
// ALREADY-RECORDED events must keep resolving at v1's ORIGINAL snapshotted
// price via the version-first LookupMetricVersionPrice — never the
// now-mutated catalog. Uses a plain SUM metric (additive) since this test's
// subject is price immutability, not window-proration (covered by (b)/(c)).
//
// FAIL-FIRST PROOF (do this by hand, not asserted in CI): temporarily make
// cycle.MetricPriceMicros skip the version-first branch (resolve straight to
// LookupMetricPrice, the OLD version-blind path) — v1.ChargedMicros then
// comes back 2_500 (5 × the now-mutated catalog price 500), failing the
// EqualValues(500, ...) assertion below. That failure IS the regression this
// test guards; restoring the version-first branch passes it again.
func TestUsageTimePricing_MidPeriodReprice_V1RateNeverRetroactivelyChanges(t *testing.T) {
	pool := testutil.NewTestDB(t)
	svc := cycle.NewService(cycle.NewStore(pool), nil)
	ctx := context.Background()

	acct := seedAccount(t, pool)
	app, mod := uuid.New(), uuid.New()

	// v1 publishes at $0.0001/unit (catalog price 100 micros) and its
	// snapshot is written at THAT price — this is what api-platform's
	// SetMetricDefinitions + SetMetricVersionPrices both do at v1's publish.
	seedMetricDef(t, pool, mod, "orders.placed", usage.KindSum, 100)
	seedVersionPrice(t, pool, mod, "orders.placed", "1.0.0", 100)

	// v1's usage accrues FIRST, before any re-price exists.
	seedEventVersion(t, pool, acct, app, mod, "orders.placed", usage.KindSum, 5, "2026-06-05T00:00:00Z", "1.0.0")

	// v2 publishes LATER at a NEW price (500 micros): the catalog row is
	// mutated in place (the REAL production write path — a second
	// SetMetricDefinitions sync) AND v2 gets its own immutable snapshot.
	_, err := pool.Exec(ctx,
		`UPDATE ms_billing.metric_definitions SET unit_price_micros = 500 WHERE module_id = $1 AND metric = $2`,
		mod.String(), "orders.placed")
	require.NoError(t, err)
	seedVersionPrice(t, pool, mod, "orders.placed", "2.0.0", 500)
	seedEventVersion(t, pool, acct, app, mod, "orders.placed", usage.KindSum, 3, "2026-06-20T00:00:00Z", "2.0.0")

	resp, err := svc.RollupPeriod(ctx, acct, mustTime(t, pStart), mustTime(t, pEnd))
	require.NoError(t, err)

	byVersion := aggByVersion(resp.Aggregates, "orders.placed")
	require.Len(t, byVersion, 2)

	v1, v2 := byVersion["1.0.0"], byVersion["2.0.0"]
	require.EqualValues(t, 100, v1.UnitPriceMicros, "v1 resolves its OWN original snapshot, NOT the now-mutated catalog (500)")
	require.EqualValues(t, 500, v1.ChargedMicros, "5 × 100 — v2's re-price never retroactively re-bills v1's already-accrued usage")

	// The wrong number the OLD version-blind LookupMetricPrice path would
	// have produced for v1 (5 × the mutated catalog price 500 = 2_500) —
	// asserting v1 charges anything else proves this isn't accidentally
	// passing because both prices happen to coincide.
	wrongCatalogOnlyCharge := int64(5) * 500
	require.NotEqual(t, wrongCatalogOnlyCharge, v1.ChargedMicros)

	require.EqualValues(t, 500, v2.UnitPriceMicros, "v2 resolves its own new snapshot")
	require.EqualValues(t, 1_500, v2.ChargedMicros, "3 × 500")
}
