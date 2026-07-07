//go:build integration

package usage_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/mirrorstack-ai/billing-engine/internal/account/usage"
	"github.com/mirrorstack-ai/billing-engine/internal/shared/testutil"
)

// These exercise the AppUsageSummary sqlc query against a real Postgres (gated
// by the `integration` build tag; run via `make test-integration`, skipped when
// Docker is unavailable). They verify what the fake-store unit tests can't: the
// rolled-up-else-live branch selection, the app_id filter, the account_id payer
// gate, and the live money math (SUM(value × unit_price), no markup).

const (
	appPeriodStart = "2026-06-01T00:00:00Z"
	appPeriodEnd   = "2026-07-01T00:00:00Z"
)

func appMustTime(t *testing.T, s string) time.Time {
	t.Helper()
	ts, err := time.Parse(time.RFC3339, s)
	require.NoError(t, err)
	return ts
}

func appSeedAccount(t *testing.T, pool *pgxpool.Pool) uuid.UUID {
	t.Helper()
	id := uuid.New()
	_, err := pool.Exec(context.Background(),
		`INSERT INTO ms_billing.accounts (id, owner_kind, owner_user_id) VALUES ($1, 'user', $2)`,
		id.String(), uuid.New().String())
	require.NoError(t, err)
	return id
}

func appSeedMetricDef(t *testing.T, pool *pgxpool.Pool, moduleID uuid.UUID, metric string, kind usage.Kind, priceMicros int64) {
	t.Helper()
	_, err := pool.Exec(context.Background(),
		`INSERT INTO ms_billing.metric_definitions (module_id, metric, kind, unit_price_micros) VALUES ($1,$2,$3,$4)`,
		moduleID.String(), metric, string(kind), priceMicros)
	require.NoError(t, err)
}

// appSeedEvent inserts a usage_events row. model/version are stored NULL when
// empty, matching the ingest path's nullable columns.
func appSeedEvent(t *testing.T, pool *pgxpool.Pool, acct, app, mod uuid.UUID, metric string, kind usage.Kind, value float64, at, model, version string) {
	t.Helper()
	var modelArg, versionArg any
	if model != "" {
		modelArg = model
	}
	if version != "" {
		versionArg = version
	}
	_, err := pool.Exec(context.Background(),
		`INSERT INTO ms_billing.usage_events
		   (event_id, account_id, app_id, module_id, metric, kind, value, recorded_at, model, module_version)
		 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)`,
		uuid.NewString(), acct.String(), app.String(), mod.String(), metric, string(kind), value, at, modelArg, versionArg)
	require.NoError(t, err)
}

// appSeedPeriod inserts an 'open' billing_periods row and returns its id.
func appSeedPeriod(t *testing.T, pool *pgxpool.Pool, acct uuid.UUID, start, end string) uuid.UUID {
	t.Helper()
	id := uuid.New()
	_, err := pool.Exec(context.Background(),
		`INSERT INTO ms_billing.billing_periods (id, account_id, period_start, period_end, status)
		 VALUES ($1,$2,$3,$4,'open')`,
		id.String(), acct.String(), start, end)
	require.NoError(t, err)
	return id
}

// appSeedAggregate inserts a snapshotted usage_aggregates row (the frozen
// billable record the rolled branch reads). active_seconds/period_days are
// left NULL (the DB default for these migration-044 columns) — the
// pre-Phase-1 / additive-kind case: every existing caller of this helper
// models a row that legitimately never populates them.
func appSeedAggregate(t *testing.T, pool *pgxpool.Pool, periodID, acct, app, mod uuid.UUID, metric string, kind usage.Kind, model, version string, qty float64, unitPrice, rawCost, charged int64) {
	t.Helper()
	appSeedAggregateWithWindow(t, pool, periodID, acct, app, mod, metric, kind, model, version, qty, unitPrice, rawCost, charged, nil, nil)
}

// appSeedAggregateWithWindow is appSeedAggregate widened to also set the
// migration-044 reproducibility columns (active_seconds/period_days) — the
// usage-time-pricing Phase 1 per-version window snapshot that Phase 2 (this
// PR) exposes on the read path. Passing nil for either leaves it SQL NULL,
// exactly like appSeedAggregate; a non-nil *float64 seeds the exact rolled
// window a peak/time_weighted row would carry post-rollup.
func appSeedAggregateWithWindow(t *testing.T, pool *pgxpool.Pool, periodID, acct, app, mod uuid.UUID, metric string, kind usage.Kind, model, version string, qty float64, unitPrice, rawCost, charged int64, activeSeconds, periodDays *float64) {
	t.Helper()
	_, err := pool.Exec(context.Background(),
		`INSERT INTO ms_billing.usage_aggregates
		   (period_id, account_id, app_id, module_id, metric, kind, model, module_version,
		    billable_quantity, unit_price_micros, raw_cost_micros, charged_micros,
		    active_seconds, period_days)
		 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)`,
		periodID.String(), acct.String(), app.String(), mod.String(), metric, string(kind), model, version,
		qty, unitPrice, rawCost, charged, activeSeconds, periodDays)
	require.NoError(t, err)
}

func findAppRow(rows []usage.AppMetricUsageRaw, metric, model, version string) (usage.AppMetricUsageRaw, bool) {
	for _, r := range rows {
		if r.Metric == metric && r.Model == model && r.ModuleVersion == version {
			return r, true
		}
	}
	return usage.AppMetricUsageRaw{}, false
}

// TestAppUsage_Integration_LivePath: no rollup yet → estimate LIVE from
// usage_events, charged = SUM(value × unit_price) with NO markup, split per
// (module_version). Also proves the app_id filter and account_id payer gate.
func TestAppUsage_Integration_LivePath(t *testing.T) {
	pool := testutil.NewTestDB(t)
	store := usage.NewStore(pool)
	ctx := context.Background()

	acct := appSeedAccount(t, pool)
	otherAcct := appSeedAccount(t, pool)
	app := uuid.New()
	otherApp := uuid.New()
	mod := uuid.New()
	appSeedMetricDef(t, pool, mod, "orders.placed", usage.KindCount, 100) // 100 µ$ / unit

	// This app, two versions: v1 → 4 units, v2 → 6 units.
	appSeedEvent(t, pool, acct, app, mod, "orders.placed", usage.KindCount, 4, "2026-06-05T00:00:00Z", "", "1.0.0")
	appSeedEvent(t, pool, acct, app, mod, "orders.placed", usage.KindCount, 6, "2026-06-06T00:00:00Z", "", "2.0.0")
	// Same account, DIFFERENT app → must be excluded by the app_id filter.
	appSeedEvent(t, pool, acct, otherApp, mod, "orders.placed", usage.KindCount, 99, "2026-06-07T00:00:00Z", "", "1.0.0")
	// DIFFERENT account, same app → must be excluded by the account_id gate.
	appSeedEvent(t, pool, otherAcct, app, mod, "orders.placed", usage.KindCount, 77, "2026-06-08T00:00:00Z", "", "1.0.0")
	// Out of window → excluded.
	appSeedEvent(t, pool, acct, app, mod, "orders.placed", usage.KindCount, 5, "2026-07-02T00:00:00Z", "", "1.0.0")

	rows, err := store.AppUsage(ctx, acct, app,
		appMustTime(t, appPeriodStart), appMustTime(t, appPeriodEnd))
	require.NoError(t, err)
	require.Len(t, rows, 2, "one line per module_version for this app+account, in-window only")

	v1, ok := findAppRow(rows, "orders.placed", "", "1.0.0")
	require.True(t, ok)
	require.Equal(t, mod, v1.ModuleID)
	require.EqualValues(t, 4, v1.BillableQuantity)
	require.EqualValues(t, 100, v1.UnitPriceMicros)
	require.EqualValues(t, 400, v1.ChargedMicros, "no markup: 4 × 100")
	require.Nil(t, v1.ActiveSeconds, "live estimate has no window-segmentation logic — unknown, not 0")
	require.Nil(t, v1.PeriodDays, "live estimate has no window-segmentation logic — unknown, not 0")

	v2, ok := findAppRow(rows, "orders.placed", "", "2.0.0")
	require.True(t, ok)
	require.EqualValues(t, 6, v2.BillableQuantity)
	require.EqualValues(t, 600, v2.ChargedMicros, "no markup: 6 × 100")
	require.Nil(t, v2.ActiveSeconds)
	require.Nil(t, v2.PeriodDays)
}

// TestAppUsage_Integration_AggregatesPath: once the period is rolled up, read
// the FROZEN usage_aggregates record and IGNORE live events (the
// rolled-up-else-live gate flips to the snapshot).
func TestAppUsage_Integration_AggregatesPath(t *testing.T) {
	pool := testutil.NewTestDB(t)
	store := usage.NewStore(pool)
	ctx := context.Background()

	acct := appSeedAccount(t, pool)
	app := uuid.New()
	mod := uuid.New()
	appSeedMetricDef(t, pool, mod, "orders.placed", usage.KindCount, 100)

	// A live event that would show 999 on the live path — it must be ignored
	// once the rolled record exists.
	appSeedEvent(t, pool, acct, app, mod, "orders.placed", usage.KindCount, 999, "2026-06-05T00:00:00Z", "", "1.0.0")

	// The frozen billable record for this period: 10 units, charged 1000 µ$.
	periodID := appSeedPeriod(t, pool, acct, appPeriodStart, appPeriodEnd)
	appSeedAggregate(t, pool, periodID, acct, app, mod, "orders.placed", usage.KindCount, "", "1.0.0",
		10, 100, 1000, 1000)

	rows, err := store.AppUsage(ctx, acct, app,
		appMustTime(t, appPeriodStart), appMustTime(t, appPeriodEnd))
	require.NoError(t, err)
	require.Len(t, rows, 1, "the rolled branch wins; live events are suppressed")

	r := rows[0]
	require.Equal(t, "orders.placed", r.Metric)
	require.Equal(t, "1.0.0", r.ModuleVersion)
	require.EqualValues(t, 10, r.BillableQuantity, "frozen quantity, not the live 999")
	require.EqualValues(t, 100, r.UnitPriceMicros)
	require.EqualValues(t, 1000, r.ChargedMicros, "frozen charged_micros")
	// This is a pre-Phase-1 additive (count) aggregate row: active_seconds/
	// period_days are legitimately NULL in the DB (proration never applies to
	// count/sum kinds — migration 044). Must surface as nil, not 0, not an error.
	require.Nil(t, r.ActiveSeconds, "NULL active_seconds (additive kind) decodes to nil, not 0")
	require.Nil(t, r.PeriodDays, "NULL period_days (additive kind) decodes to nil, not 0")
}

// TestAppUsage_Integration_AggregatesPath_SurfacesActiveWindow: a ROLLED
// (post-rollup) peak/time_weighted aggregate row with active_seconds/
// period_days populated (usage-time-pricing Phase 1) surfaces the EXACT
// values on AppUsage's returned rows — not nil, not rounded/mangled.
func TestAppUsage_Integration_AggregatesPath_SurfacesActiveWindow(t *testing.T) {
	pool := testutil.NewTestDB(t)
	store := usage.NewStore(pool)
	ctx := context.Background()

	acct := appSeedAccount(t, pool)
	app := uuid.New()
	mod := uuid.New()
	appSeedMetricDef(t, pool, mod, "storage.gib_hours", usage.KindTimeWeighted, 100)

	periodID := appSeedPeriod(t, pool, acct, appPeriodStart, appPeriodEnd)
	activeSeconds := 1_296_000.0 // 15 days, in seconds
	periodDays := 30.0
	appSeedAggregateWithWindow(t, pool, periodID, acct, app, mod, "storage.gib_hours", usage.KindTimeWeighted, "", "1.0.0",
		5, 100, 500, 500, &activeSeconds, &periodDays)

	rows, err := store.AppUsage(ctx, acct, app,
		appMustTime(t, appPeriodStart), appMustTime(t, appPeriodEnd))
	require.NoError(t, err)
	require.Len(t, rows, 1)

	r := rows[0]
	require.NotNil(t, r.ActiveSeconds)
	require.NotNil(t, r.PeriodDays)
	require.EqualValues(t, 1_296_000.0, *r.ActiveSeconds, "exact rolled active_seconds, not rounded/mangled")
	require.EqualValues(t, 30.0, *r.PeriodDays, "exact rolled period_days, not rounded/mangled")
}

// TestAppUsage_Integration_EmptyWhenNoUsage: an app with no events and no
// aggregates returns no rows (not an error).
func TestAppUsage_Integration_EmptyWhenNoUsage(t *testing.T) {
	pool := testutil.NewTestDB(t)
	store := usage.NewStore(pool)
	ctx := context.Background()

	acct := appSeedAccount(t, pool)
	rows, err := store.AppUsage(ctx, acct, uuid.New(),
		appMustTime(t, appPeriodStart), appMustTime(t, appPeriodEnd))
	require.NoError(t, err)
	require.Empty(t, rows)
}
