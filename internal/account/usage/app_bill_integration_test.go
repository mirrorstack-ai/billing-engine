//go:build integration

package usage_test

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/mirrorstack-ai/billing-engine/internal/account/usage"
	"github.com/mirrorstack-ai/billing-engine/internal/shared/testutil"
)

// These exercise the AppBillLines / ListBillingPeriods / BillingPeriodWindow
// sqlc queries against a real Postgres (gated by the `integration` build tag).
// They verify what the fake-store unit tests can't: the LIVE infra 1.2× markup
// applied INLINE in SQL, the reserved-vs-custom split at the query level, and the
// period listing / window resolution. Reuses the seed helpers +
// appPeriodStart/End constants from app_usage_integration_test.go (same package).

// TestAppBill_Integration_LiveInfraMarkupAndSplit: on the LIVE branch a custom
// metric is charged qty × price (NO markup) while a reserved infra.* metric is
// charged qty × price × 12/10 (the 1.2× infra plane) — both returned by one read
// so the service can split them.
func TestAppBill_Integration_LiveInfraMarkupAndSplit(t *testing.T) {
	pool := testutil.NewTestDB(t)
	store := usage.NewStore(pool)
	ctx := context.Background()

	acct := appSeedAccount(t, pool)
	app := uuid.New()
	modCustom := uuid.New()
	modInfra := uuid.New() // a distinct module carrying the infra metric catalog row

	appSeedMetricDef(t, pool, modCustom, "orders.placed", usage.KindCount, 100)       // 100 µ$/unit
	appSeedMetricDef(t, pool, modInfra, "infra.egress.api.bytes", usage.KindSum, 100) // 100 µ$/unit COGS

	// Custom: 4 units → 400 µ$ (no markup).
	appSeedEvent(t, pool, acct, app, modCustom, "orders.placed", usage.KindCount, 4, "2026-06-05T00:00:00Z", "", "")
	// Infra: 10 units × 100 = 1000 raw → ×1.2 = 1200 µ$ charged.
	appSeedEvent(t, pool, acct, app, modInfra, "infra.egress.api.bytes", usage.KindSum, 10, "2026-06-06T00:00:00Z", "", "")

	rows, err := store.AppBill(ctx, acct, app,
		appMustTime(t, appPeriodStart), appMustTime(t, appPeriodEnd))
	require.NoError(t, err)
	require.Len(t, rows, 2)

	custom, ok := findAppRow(rows, "orders.placed", "", "")
	require.True(t, ok)
	require.EqualValues(t, 400, custom.ChargedMicros, "custom metric: 4 × 100, NO markup")

	infra, ok := findAppRow(rows, "infra.egress.api.bytes", "", "")
	require.True(t, ok)
	require.EqualValues(t, 1200, infra.ChargedMicros, "reserved infra metric: 10 × 100 × 1.2")
}

// TestAppBill_Integration_RolledInfraFrozenNotRemarkedUp: once rolled up, the
// frozen usage_aggregates.charged_micros is read verbatim — the rolled branch
// must NOT re-apply the 1.2× (the markup was already snapshotted at rollup).
func TestAppBill_Integration_RolledInfraFrozenNotRemarkedUp(t *testing.T) {
	pool := testutil.NewTestDB(t)
	store := usage.NewStore(pool)
	ctx := context.Background()

	acct := appSeedAccount(t, pool)
	app := uuid.New()
	modInfra := uuid.New()

	// A live event that would show on the live path — suppressed once rolled.
	appSeedMetricDef(t, pool, modInfra, "infra.egress.api.bytes", usage.KindSum, 100)
	appSeedEvent(t, pool, acct, app, modInfra, "infra.egress.api.bytes", usage.KindSum, 999, "2026-06-05T00:00:00Z", "", "")

	// Frozen record: charged 1200 already includes the 1.2× markup.
	periodID := appSeedPeriod(t, pool, acct, appPeriodStart, appPeriodEnd)
	appSeedAggregate(t, pool, periodID, acct, app, modInfra, "infra.egress.api.bytes", usage.KindSum, "", "",
		10, 100, 1000, 1200)

	rows, err := store.AppBill(ctx, acct, app,
		appMustTime(t, appPeriodStart), appMustTime(t, appPeriodEnd))
	require.NoError(t, err)
	require.Len(t, rows, 1, "rolled branch wins; live events suppressed")
	require.EqualValues(t, 1200, rows[0].ChargedMicros, "frozen charged, not re-marked-up")
}

// countActiveSentinelMetrics returns how many ACTIVE platform-infra sentinel
// metric_definitions rows the migrations seeded — the exact number of rows
// AppInfraBillLines must return (catalog-anchored, one per declared infra metric).
func countActiveSentinelMetrics(t *testing.T, pool *pgxpool.Pool) int {
	t.Helper()
	var n int
	err := pool.QueryRow(context.Background(),
		`SELECT COUNT(*) FROM ms_billing.metric_definitions
		 WHERE module_id = $1 AND active`,
		usage.PlatformInfraModuleID().String()).Scan(&n)
	require.NoError(t, err)
	return n
}

func findInfraLine(lines []usage.AppInfraUsage, metric string) (usage.AppInfraUsage, bool) {
	for _, l := range lines {
		if l.Metric == metric {
			return l, true
		}
	}
	return usage.AppInfraUsage{}, false
}

// TestAppInfraBill_Integration_CatalogAnchoredWithZeros: the LIVE branch returns
// ONE row per active declared infra metric (catalog-anchored) — the used one
// carries qty × price × 1.2, the unused ones render at qty 0 · $0 — and
// InfraTotalMicros reconciles as Σ of the lines' charged. Infra events are seeded
// under the platform-infra SENTINEL module_id (as the real ingest stamps them).
func TestAppInfraBill_Integration_CatalogAnchoredWithZeros(t *testing.T) {
	pool := testutil.NewTestDB(t)
	store := usage.NewStore(pool)
	ctx := context.Background()

	acct := appSeedAccount(t, pool)
	app := uuid.New()
	sentinel := usage.PlatformInfraModuleID()

	// infra.ai.input.tokens is a seeded sentinel row (catalog COGS 1000 µ$/1k).
	// 3 units → 3 × 1000 = 3000 raw → ×1.2 = 3600 µ$ charged (markup once, in SQL).
	appSeedEvent(t, pool, acct, app, sentinel, "infra.ai.input.tokens", usage.KindSum, 3, "2026-06-05T00:00:00Z", "", "")

	lines, err := store.AppInfraBill(ctx, acct, app,
		appMustTime(t, appPeriodStart), appMustTime(t, appPeriodEnd))
	require.NoError(t, err)
	require.Len(t, lines, countActiveSentinelMetrics(t, pool),
		"one line per active declared infra metric — catalog-anchored, unused ones included")

	used, ok := findInfraLine(lines, "infra.ai.input.tokens")
	require.True(t, ok)
	require.EqualValues(t, 3, used.BillableQuantity)
	require.EqualValues(t, 1000, used.UnitPriceMicros, "raw catalog COGS, pre-markup")
	require.EqualValues(t, 3600, used.ChargedMicros, "3 × 1000 × 1.2, markup once")

	unused, ok := findInfraLine(lines, "infra.cron.count")
	require.True(t, ok, "a declared-but-unused infra metric still appears")
	require.Zero(t, unused.BillableQuantity)
	require.Zero(t, unused.ChargedMicros, "unused metric renders at $0")

	var sum int64
	for _, l := range lines {
		sum += l.ChargedMicros
	}
	require.EqualValues(t, 3600, sum, "infra_total == Σ infra_lines[].charged")
}

// TestAppInfraBill_Integration_RolledFrozenNotRemarkedUp: once rolled up, the
// frozen usage_aggregates.charged_micros is read verbatim — the rolled branch must
// NOT re-apply the 1.2× (the markup was already snapshotted at rollup), and the
// live events in-window are suppressed.
func TestAppInfraBill_Integration_RolledFrozenNotRemarkedUp(t *testing.T) {
	pool := testutil.NewTestDB(t)
	store := usage.NewStore(pool)
	ctx := context.Background()

	acct := appSeedAccount(t, pool)
	app := uuid.New()
	sentinel := usage.PlatformInfraModuleID()

	// A live event that would show on the live path — suppressed once rolled.
	appSeedEvent(t, pool, acct, app, sentinel, "infra.ai.input.tokens", usage.KindSum, 999, "2026-06-05T00:00:00Z", "", "")

	// Frozen record: charged 3600 already includes the 1.2× markup.
	periodID := appSeedPeriod(t, pool, acct, appPeriodStart, appPeriodEnd)
	appSeedAggregate(t, pool, periodID, acct, app, sentinel, "infra.ai.input.tokens", usage.KindSum, "", "",
		3, 1000, 3000, 3600)

	lines, err := store.AppInfraBill(ctx, acct, app,
		appMustTime(t, appPeriodStart), appMustTime(t, appPeriodEnd))
	require.NoError(t, err)
	require.Len(t, lines, countActiveSentinelMetrics(t, pool))

	used, ok := findInfraLine(lines, "infra.ai.input.tokens")
	require.True(t, ok)
	require.EqualValues(t, 3, used.BillableQuantity, "frozen quantity, not the live 999")
	require.EqualValues(t, 3600, used.ChargedMicros, "frozen charged, not re-marked-up")
}

// TestAppModuleInfraBill_Integration_DualPriceSentinelFallback: the decision-19
// per-module infra read resolves (module, metric) → (SENTINEL, metric) at the SQL
// level. A module with an ms.Price(n) override prices at n; a module with NO override
// falls back to the SENTINEL default (NO revenue leak); the override price rides the
// wire as a nullable *int64 (nil ⇔ no override). Sentinel-attributed usage of the SAME
// metric stays in the RESIDUAL (AppInfraBill), proving the two queries partition the
// reserved namespace with no double-count.
func TestAppModuleInfraBill_Integration_DualPriceSentinelFallback(t *testing.T) {
	pool := testutil.NewTestDB(t)
	store := usage.NewStore(pool)
	ctx := context.Background()

	acct := appSeedAccount(t, pool)
	app := uuid.New()
	sentinel := usage.PlatformInfraModuleID()
	metric := "infra.compute.walltime.ms"

	// Pin the SENTINEL default COGS to a known, active 20 µ$/unit for exact assertions
	// (the metric is migration-seeded under the sentinel).
	_, err := pool.Exec(ctx,
		`UPDATE ms_billing.metric_definitions SET unit_price_micros = 20, active = true
		 WHERE module_id = $1 AND metric = $2`,
		sentinel.String(), metric)
	require.NoError(t, err)

	modAbsorb := uuid.New() // ms.Price(0) override → full absorb
	modPlain := uuid.New()  // NO override row → sentinel-default fallback
	modMarkup := uuid.New() // override 50 (> default) → module markup

	// Override rows are price-only, keyed (real module_id, metric). These (module,
	// metric) pairs are not migration-seeded, so the plain INSERT is safe.
	appSeedMetricDef(t, pool, modAbsorb, metric, usage.KindSum, 0)
	appSeedMetricDef(t, pool, modMarkup, metric, usage.KindSum, 50)
	// modPlain: intentionally NO override row (exercises the sentinel fallback).

	// Each attributed module incurs 100 units; a sentinel event of the SAME metric is
	// the residual, not a per-module line.
	appSeedEvent(t, pool, acct, app, modAbsorb, metric, usage.KindSum, 100, "2026-06-05T00:00:00Z", "", "")
	appSeedEvent(t, pool, acct, app, modPlain, metric, usage.KindSum, 100, "2026-06-05T00:00:00Z", "", "")
	appSeedEvent(t, pool, acct, app, modMarkup, metric, usage.KindSum, 100, "2026-06-05T00:00:00Z", "", "")
	appSeedEvent(t, pool, acct, app, sentinel, metric, usage.KindSum, 5, "2026-06-05T00:00:00Z", "", "")

	lines, err := store.AppModuleInfraBill(ctx, acct, app,
		appMustTime(t, appPeriodStart), appMustTime(t, appPeriodEnd))
	require.NoError(t, err)
	require.Len(t, lines, 3, "only the three ATTRIBUTED modules; the sentinel event is residual")

	byMod := func(mod uuid.UUID) usage.AppModuleInfraUsage {
		for _, l := range lines {
			if l.ModuleID == mod {
				return l
			}
		}
		t.Fatalf("no module_infra line for module %s", mod)
		return usage.AppModuleInfraUsage{}
	}

	absorb := byMod(modAbsorb)
	require.EqualValues(t, 20, absorb.DefaultUnitPriceMicros, "default is the SENTINEL COGS")
	require.NotNil(t, absorb.ModuleUnitPriceMicros)
	require.EqualValues(t, 0, *absorb.ModuleUnitPriceMicros, "ms.Price(0) override → non-nil 0")
	require.Zero(t, absorb.ChargedMicros, "100 × 0 × 1.2 = 0 (full absorb)")

	plain := byMod(modPlain)
	require.EqualValues(t, 20, plain.DefaultUnitPriceMicros)
	require.Nil(t, plain.ModuleUnitPriceMicros, "no override row → NULL (plain mode on the wire)")
	require.EqualValues(t, 2400, plain.ChargedMicros, "SENTINEL FALLBACK: 100 × 20 × 1.2 — NO revenue leak")
	require.Equal(t, metric, plain.Label, "no friendly-label registry → metric id is the label")
	require.Equal(t, "compute", plain.Group, "kind/unit/group from the SENTINEL row")

	markup := byMod(modMarkup)
	require.NotNil(t, markup.ModuleUnitPriceMicros)
	require.EqualValues(t, 50, *markup.ModuleUnitPriceMicros)
	require.EqualValues(t, 6000, markup.ChargedMicros, "override applied: 100 × 50 × 1.2")

	// RESIDUAL now excludes the attributed events and keeps ONLY the sentinel usage of
	// this metric (5 units) — the reconciliation partition, no double-count.
	residual, err := store.AppInfraBill(ctx, acct, app,
		appMustTime(t, appPeriodStart), appMustTime(t, appPeriodEnd))
	require.NoError(t, err)
	resLine, ok := findInfraLine(residual, metric)
	require.True(t, ok)
	require.EqualValues(t, 5, resLine.BillableQuantity, "residual = the sentinel event only (5), NOT the 305 total")
	require.EqualValues(t, 120, resLine.ChargedMicros, "residual: 5 × 20 × 1.2")
}

// TestAppModuleInfraBill_Integration_RolledFrozen: once rolled up, the per-module read
// serves the FROZEN usage_aggregates.charged_micros (the override price + 1.2× already
// snapshotted at rollup), suppressing the in-window live events; the display prices
// (default + override) are still resolved fresh from the catalog.
func TestAppModuleInfraBill_Integration_RolledFrozen(t *testing.T) {
	pool := testutil.NewTestDB(t)
	store := usage.NewStore(pool)
	ctx := context.Background()

	acct := appSeedAccount(t, pool)
	app := uuid.New()
	sentinel := usage.PlatformInfraModuleID()
	metric := "infra.compute.walltime.ms"
	mod := uuid.New()

	_, err := pool.Exec(ctx,
		`UPDATE ms_billing.metric_definitions SET unit_price_micros = 20, active = true
		 WHERE module_id = $1 AND metric = $2`,
		sentinel.String(), metric)
	require.NoError(t, err)
	appSeedMetricDef(t, pool, mod, metric, usage.KindSum, 5) // override 5

	// A live event that would show on the live path — suppressed once rolled.
	appSeedEvent(t, pool, acct, app, mod, metric, usage.KindSum, 999, "2026-06-05T00:00:00Z", "", "")
	// Frozen record: charged 600 already includes the override price + 1.2× markup.
	periodID := appSeedPeriod(t, pool, acct, appPeriodStart, appPeriodEnd)
	appSeedAggregate(t, pool, periodID, acct, app, mod, metric, usage.KindSum, "", "",
		100, 5, 500, 600)

	lines, err := store.AppModuleInfraBill(ctx, acct, app,
		appMustTime(t, appPeriodStart), appMustTime(t, appPeriodEnd))
	require.NoError(t, err)
	require.Len(t, lines, 1, "rolled branch wins; live events suppressed")
	require.EqualValues(t, 100, lines[0].BillableQuantity, "frozen quantity, not the live 999")
	require.EqualValues(t, 600, lines[0].ChargedMicros, "frozen charged, not re-marked-up")
	require.EqualValues(t, 20, lines[0].DefaultUnitPriceMicros, "default resolved fresh from the SENTINEL row")
	require.NotNil(t, lines[0].ModuleUnitPriceMicros)
	require.EqualValues(t, 5, *lines[0].ModuleUnitPriceMicros, "override resolved fresh from the catalog")
}

// TestListBillingPeriods_Integration: an account's real periods list newest-first
// with is_current flagging the row equal to current_month_start; another
// account's periods are excluded.
func TestListBillingPeriods_Integration(t *testing.T) {
	pool := testutil.NewTestDB(t)
	store := usage.NewStore(pool)
	ctx := context.Background()

	acct := appSeedAccount(t, pool)
	other := appSeedAccount(t, pool)

	appSeedPeriod(t, pool, acct, "2026-05-01T00:00:00Z", "2026-06-01T00:00:00Z")
	junID := appSeedPeriod(t, pool, acct, "2026-06-01T00:00:00Z", "2026-07-01T00:00:00Z")
	appSeedPeriod(t, pool, other, "2026-06-01T00:00:00Z", "2026-07-01T00:00:00Z") // excluded

	currentMonthStart := appMustTime(t, "2026-06-01T00:00:00Z")
	rows, err := store.ListBillingPeriods(ctx, acct, currentMonthStart)
	require.NoError(t, err)
	require.Len(t, rows, 2, "only this account's periods")
	require.True(t, rows[0].PeriodStart.After(rows[1].PeriodStart), "newest-first")
	require.Equal(t, junID, rows[0].ID)
	require.True(t, rows[0].IsCurrent, "June == current_month_start")
	require.False(t, rows[1].IsCurrent, "May is a past period")
}

// TestBillingPeriodWindow_Integration: resolves a period's window by (account,
// id); wrong account or unknown id → found=false.
func TestBillingPeriodWindow_Integration(t *testing.T) {
	pool := testutil.NewTestDB(t)
	store := usage.NewStore(pool)
	ctx := context.Background()

	acct := appSeedAccount(t, pool)
	other := appSeedAccount(t, pool)
	pid := appSeedPeriod(t, pool, acct, appPeriodStart, appPeriodEnd)

	start, end, found, err := store.BillingPeriodWindow(ctx, acct, pid)
	require.NoError(t, err)
	require.True(t, found)
	require.True(t, start.Equal(appMustTime(t, appPeriodStart)))
	require.True(t, end.Equal(appMustTime(t, appPeriodEnd)))

	// Wrong account → not found (no cross-account resolution).
	_, _, found, err = store.BillingPeriodWindow(ctx, other, pid)
	require.NoError(t, err)
	require.False(t, found)

	// Unknown id → not found.
	_, _, found, err = store.BillingPeriodWindow(ctx, acct, uuid.New())
	require.NoError(t, err)
	require.False(t, found)
}
