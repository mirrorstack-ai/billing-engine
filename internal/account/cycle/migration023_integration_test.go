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

// These integration tests validate migration 023 (module_version attribution
// dimension) against a real Postgres 17 (gated by the `integration` build
// tag). NewTestDB applies ALL *.up.sql in lexical order, so 023.up is already
// applied on entry; the round-trip test then runs 023.down + 023.up
// explicitly, exercising the SAME "collapse duplicate rows before
// re-narrowing UNIQUE" logic migration 018's down demonstrates for `model`.
// The helpers (seedAccount, seedMetricDef, migrationSQL, mustTime, pStart/
// pEnd) live in store_integration_test.go / migration019_integration_test.go
// in this package.

// seedEventVersion is seedEvent (store_integration_test.go) plus an explicit
// module_version, for tests that need the version-attribution dimension.
func seedEventVersion(t *testing.T, pool *pgxpool.Pool, acct, app, mod uuid.UUID, metric string, kind usage.Kind, value float64, at, version string) {
	t.Helper()
	_, err := pool.Exec(context.Background(),
		`INSERT INTO ms_billing.usage_events (event_id, account_id, app_id, module_id, metric, kind, value, recorded_at, module_version)
		 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)`,
		uuid.NewString(), acct.String(), app.String(), mod.String(), metric, string(kind), value, at, version)
	require.NoError(t, err)
}

// columnExists reports whether the given column exists on ms_billing.<table>.
func columnExists(t *testing.T, pool *pgxpool.Pool, table, column string) bool {
	t.Helper()
	var exists bool
	require.NoError(t, pool.QueryRow(context.Background(),
		`SELECT EXISTS (
		    SELECT 1 FROM information_schema.columns
		    WHERE table_schema = 'ms_billing' AND table_name = $1 AND column_name = $2
		 )`, table, column).Scan(&exists))
	return exists
}

func TestMigration023_Up_DistinctPerModuleVersion(t *testing.T) {
	pool := testutil.NewTestDB(t) // 023.up already applied
	svc := cycle.NewService(cycle.NewStore(pool), nil)
	ctx := context.Background()

	require.True(t, columnExists(t, pool, "usage_events", "module_version"))
	require.True(t, columnExists(t, pool, "usage_aggregates", "module_version"))

	acct := seedAccount(t, pool)
	app, mod := uuid.New(), uuid.New()
	seedMetricDef(t, pool, mod, "orders.placed", usage.KindSum, 50_000)

	// Two versions of the same (app, module, metric) — version is a pure
	// attribution dimension, so both price at the SAME catalog rate, but they
	// must roll up into DISTINCT aggregate rows (the widened UNIQUE key).
	seedEventVersion(t, pool, acct, app, mod, "orders.placed", usage.KindSum, 4, "2026-06-01T00:00:00Z", "1.0.0")
	seedEventVersion(t, pool, acct, app, mod, "orders.placed", usage.KindSum, 6, "2026-06-02T00:00:00Z", "2.0.0")

	resp, err := svc.RollupPeriod(ctx, acct, mustTime(t, pStart), mustTime(t, pEnd))
	require.NoError(t, err)
	require.Len(t, resp.Aggregates, 2, "two module_version events roll up into two distinct aggregate rows")

	byVersion := map[string]cycle.MetricAggregate{}
	for _, a := range resp.Aggregates {
		byVersion[a.ModuleVersion] = a
	}
	require.EqualValues(t, 50_000, byVersion["1.0.0"].UnitPriceMicros)
	require.EqualValues(t, 50_000, byVersion["2.0.0"].UnitPriceMicros, "version never changes the resolved price")
	require.EqualValues(t, 200_000, byVersion["1.0.0"].ChargedMicros) // 4 × 50_000
	require.EqualValues(t, 300_000, byVersion["2.0.0"].ChargedMicros) // 6 × 50_000

	// Re-running the rollup (idempotent) must still upsert exactly 2 rows for
	// this (app, module, metric) — proves the widened UNIQUE constraint
	// actually exists and is enforced by the DB, not just asserted in Go.
	resp2, err := svc.RollupPeriod(ctx, acct, mustTime(t, pStart), mustTime(t, pEnd))
	require.NoError(t, err)
	require.Len(t, resp2.Aggregates, 2)

	var count int
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT count(*) FROM ms_billing.usage_aggregates WHERE app_id=$1 AND module_id=$2 AND metric=$3`,
		app.String(), mod.String(), "orders.placed").Scan(&count))
	require.Equal(t, 2, count)
}

func TestMigration023_UpDownUp_RoundTrips(t *testing.T) {
	pool := testutil.NewTestDB(t) // 023.up already applied
	svc := cycle.NewService(cycle.NewStore(pool), nil)
	ctx := context.Background()

	acct := seedAccount(t, pool)
	app, mod := uuid.New(), uuid.New()
	seedMetricDef(t, pool, mod, "orders.placed", usage.KindSum, 50_000)

	seedEventVersion(t, pool, acct, app, mod, "orders.placed", usage.KindSum, 4, "2026-06-01T00:00:00Z", "1.0.0")
	seedEventVersion(t, pool, acct, app, mod, "orders.placed", usage.KindSum, 6, "2026-06-02T00:00:00Z", "2.0.0")

	resp, err := svc.RollupPeriod(ctx, acct, mustTime(t, pStart), mustTime(t, pEnd))
	require.NoError(t, err)
	require.Len(t, resp.Aggregates, 2, "pre-down: two version-split rows exist")

	// --- down: collapse the version-split rows back into one ---
	_, err = pool.Exec(ctx, migrationSQL(t, "023_usage_module_version.down.sql"))
	require.NoError(t, err)

	require.False(t, columnExists(t, pool, "usage_events", "module_version"), "023.down must drop usage_events.module_version")
	require.False(t, columnExists(t, pool, "usage_aggregates", "module_version"), "023.down must drop usage_aggregates.module_version")

	var rowCount int
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT count(*) FROM ms_billing.usage_aggregates WHERE app_id=$1 AND module_id=$2 AND metric=$3`,
		app.String(), mod.String(), "orders.placed").Scan(&rowCount))
	require.Equal(t, 1, rowCount, "023.down must collapse the two version-split rows into one")

	var qty string
	var raw, charged int64
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT billable_quantity::text, raw_cost_micros, charged_micros
		   FROM ms_billing.usage_aggregates WHERE app_id=$1 AND module_id=$2 AND metric=$3`,
		app.String(), mod.String(), "orders.placed").Scan(&qty, &raw, &charged))
	require.Equal(t, "10", qty, "the collapsed row's quantity is the SUM of the two version rows (4+6)")
	require.EqualValues(t, 500_000, raw, "no money lost on down: 200_000 + 300_000")
	require.EqualValues(t, 500_000, charged)

	// --- up again: re-apply 023 cleanly (idempotent forward path) ---
	_, err = pool.Exec(ctx, migrationSQL(t, "023_usage_module_version.up.sql"))
	require.NoError(t, err)

	require.True(t, columnExists(t, pool, "usage_events", "module_version"))
	require.True(t, columnExists(t, pool, "usage_aggregates", "module_version"))

	// A fresh rollup with two NEW versions (different app/module so it doesn't
	// collide with the now-collapsed historical row) proves the widened UNIQUE
	// constraint is back and functioning post-re-apply.
	app2, mod2 := uuid.New(), uuid.New()
	seedMetricDef(t, pool, mod2, "orders.placed", usage.KindSum, 50_000)
	seedEventVersion(t, pool, acct, app2, mod2, "orders.placed", usage.KindSum, 1, "2026-06-01T00:00:00Z", "1.0.0")
	seedEventVersion(t, pool, acct, app2, mod2, "orders.placed", usage.KindSum, 1, "2026-06-01T00:00:00Z", "2.0.0")

	_, err = svc.RollupPeriod(ctx, acct, mustTime(t, pStart), mustTime(t, pEnd))
	require.NoError(t, err)

	var count2 int
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT count(*) FROM ms_billing.usage_aggregates WHERE app_id=$1 AND module_id=$2 AND metric=$3`,
		app2.String(), mod2.String(), "orders.placed").Scan(&count2))
	require.Equal(t, 2, count2, "post-023.up (re-applied) two new version-split rows can coexist again")
}
