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

// These integration tests validate migration 073 (dev_served — usage a
// developer's dev tunnel produced, priced but never charged) against a real
// Postgres 17, gated by the `integration` build tag. NewTestDB applies ALL
// *.up.sql in lexical order, so 073.up is already applied on entry.
//
// The load-bearing claim is a DATABASE claim, which is why it needs this suite
// rather than the unit tests beside it: migration 055 replaced the aggregate's
// UNIQUE CONSTRAINT with a bare CREATE UNIQUE INDEX, so 073 has to drop the old
// arbiter by pg_index lookup. A DROP CONSTRAINT IF EXISTS would have missed
// silently, recorded 073 applied, and left the narrow key in force — the
// collision would then only appear as an under-billed customer weeks later.
// TestMigration073_Up_MixedDevAndDeployedAreDistinctRows fails against exactly
// that outcome; a Go-level fake cannot.
//
// Helpers (seedAccount, seedMetricDef, seedEvent, migrationSQL, mustTime,
// pStart/pEnd, columnExists) live in store_integration_test.go /
// migration019_integration_test.go / migration023_integration_test.go.

// seedEventDevServed is seedEvent plus the migration-073 tunnel flag.
func seedEventDevServed(t *testing.T, pool *pgxpool.Pool, acct, app, mod uuid.UUID, metric string, kind usage.Kind, value float64, at string, devServed bool) {
	t.Helper()
	_, err := pool.Exec(context.Background(),
		`INSERT INTO ms_billing.usage_events (event_id, account_id, app_id, module_id, metric, kind, value, recorded_at, dev_served)
		 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)`,
		uuid.NewString(), acct.String(), app.String(), mod.String(), metric, string(kind), value, at, devServed)
	require.NoError(t, err)
}

// uniqueIndexColumns returns the column expressions of every non-primary UNIQUE
// index on ms_billing.usage_aggregates, keyed by index name — read from the
// catalog, so it reports what the DATABASE enforces rather than what a
// migration file says it should.
func uniqueIndexColumns(t *testing.T, pool *pgxpool.Pool) map[string]string {
	t.Helper()
	rows, err := pool.Query(context.Background(),
		`SELECT idx.relname, pg_get_indexdef(i.indexrelid)
		   FROM pg_index i
		   JOIN pg_class idx ON idx.oid = i.indexrelid
		   JOIN pg_class tbl ON tbl.oid = i.indrelid
		   JOIN pg_namespace ns ON ns.oid = tbl.relnamespace
		  WHERE ns.nspname = 'ms_billing'
		    AND tbl.relname = 'usage_aggregates'
		    AND i.indisunique
		    AND NOT i.indisprimary`)
	require.NoError(t, err)
	defer rows.Close()

	out := map[string]string{}
	for rows.Next() {
		var name, def string
		require.NoError(t, rows.Scan(&name, &def))
		out[name] = def
	}
	require.NoError(t, rows.Err())
	return out
}

func TestMigration073_Up_MixedDevAndDeployedAreDistinctRows(t *testing.T) {
	pool := testutil.NewTestDB(t) // 073.up already applied
	svc := cycle.NewService(cycle.NewStore(pool), nil)
	ctx := context.Background()

	require.True(t, columnExists(t, pool, "usage_events", "dev_served"))
	require.True(t, columnExists(t, pool, "usage_aggregates", "dev_served"))

	// 🔴 EXACTLY ONE uniqueness arbiter, and it is the wide one. Two would mean
	// 073's lookup-drop missed the 055 index and left the narrow key beside the
	// new one — the failure mode a name-guessed DROP CONSTRAINT produces
	// silently.
	indexes := uniqueIndexColumns(t, pool)
	require.Len(t, indexes, 1, "the pre-073 arbiter must be GONE, not merely joined by a wider one: %v", indexes)
	def, ok := indexes["usage_aggregates_period_line_dev_served_key"]
	require.True(t, ok, "the surviving arbiter is 073's: %v", indexes)
	require.Contains(t, def, "dev_served")

	acct := seedAccount(t, pool)
	app, mod := uuid.New(), uuid.New()
	seedMetricDef(t, pool, mod, "orders.placed", usage.KindSum, 50_000)

	// One module, one metric, one period, both kinds of usage — the module was
	// deployed for most of the period and tunnelled for the rest.
	seedEventDevServed(t, pool, acct, app, mod, "orders.placed", usage.KindSum, 4, "2026-06-01T00:00:00Z", false)
	seedEventDevServed(t, pool, acct, app, mod, "orders.placed", usage.KindSum, 6, "2026-06-02T00:00:00Z", true)

	resp, err := svc.RollupPeriod(ctx, acct, mustTime(t, pStart), mustTime(t, pEnd))
	require.NoError(t, err)
	require.Len(t, resp.Aggregates, 2, "the two kinds roll up into two distinct aggregate rows")

	byDev := map[bool]cycle.MetricAggregate{}
	for _, a := range resp.Aggregates {
		byDev[a.DevServed] = a
	}
	require.EqualValues(t, 200_000, byDev[false].ChargedMicros) // 4 × 50_000
	require.EqualValues(t, 300_000, byDev[true].ChargedMicros,  // 6 × 50_000
		"the tunnel line is PRICED — that figure is what the console shows the developer")

	require.EqualValues(t, 200_000, resp.TotalChargedMicros, "only the deployed part is collectable")
	require.EqualValues(t, 300_000, resp.DevServedChargedMicros)

	// Both rows really are in the table under distinct keys (a narrow arbiter
	// would have left one), and a re-run upserts the same two.
	countRows := func() int {
		var n int
		require.NoError(t, pool.QueryRow(ctx,
			`SELECT count(*) FROM ms_billing.usage_aggregates WHERE app_id=$1 AND module_id=$2 AND metric=$3`,
			app.String(), mod.String(), "orders.placed").Scan(&n))
		return n
	}
	require.Equal(t, 2, countRows())

	resp2, err := svc.RollupPeriod(ctx, acct, mustTime(t, pStart), mustTime(t, pEnd))
	require.NoError(t, err)
	require.Len(t, resp2.Aggregates, 2)
	require.Equal(t, 2, countRows(), "the widened ON CONFLICT target keeps the rollup idempotent")

	// The money readers stop at the flag: the arrears total the charge leg bills
	// is the deployed part alone, read back through the real SQL.
	total, err := cycle.NewStore(pool).PeriodChargedTotal(ctx, acct, mustTime(t, pStart), mustTime(t, pEnd))
	require.NoError(t, err)
	require.EqualValues(t, 200_000, total, "PeriodChargedTotal excludes dev_served")
}

func TestMigration073_UpDownUp_RoundTrips(t *testing.T) {
	pool := testutil.NewTestDB(t) // 073.up already applied
	svc := cycle.NewService(cycle.NewStore(pool), nil)
	ctx := context.Background()

	acct := seedAccount(t, pool)
	app, mod := uuid.New(), uuid.New()
	seedMetricDef(t, pool, mod, "orders.placed", usage.KindSum, 50_000)

	seedEventDevServed(t, pool, acct, app, mod, "orders.placed", usage.KindSum, 4, "2026-06-01T00:00:00Z", false)
	seedEventDevServed(t, pool, acct, app, mod, "orders.placed", usage.KindSum, 6, "2026-06-02T00:00:00Z", true)

	_, err := svc.RollupPeriod(ctx, acct, mustTime(t, pStart), mustTime(t, pEnd))
	require.NoError(t, err)

	rowCount := func() int {
		var n int
		require.NoError(t, pool.QueryRow(ctx,
			`SELECT count(*) FROM ms_billing.usage_aggregates WHERE app_id=$1 AND module_id=$2 AND metric=$3`,
			app.String(), mod.String(), "orders.placed").Scan(&n))
		return n
	}
	require.Equal(t, 2, rowCount(), "pre-down: two dev-split rows exist")

	// --- down: LOSSY BY DESIGN. The narrow key cannot be restored while both
	// rows exist, and merging them would fold never-charged tunnel usage into a
	// billable line — the one outcome the feature exists to prevent — so the
	// dev_served rows are deleted. The charged row must survive untouched.
	_, err = pool.Exec(ctx, migrationSQL(t, "073_dev_served_usage.down.sql"))
	require.NoError(t, err)

	require.False(t, columnExists(t, pool, "usage_events", "dev_served"), "073.down drops usage_events.dev_served")
	require.False(t, columnExists(t, pool, "usage_aggregates", "dev_served"), "073.down drops usage_aggregates.dev_served")
	require.Equal(t, 1, rowCount(), "073.down deletes the dev_served row and keeps the charged one")

	var qty string
	var raw, charged int64
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT billable_quantity::text, raw_cost_micros, charged_micros
		   FROM ms_billing.usage_aggregates WHERE app_id=$1 AND module_id=$2 AND metric=$3`,
		app.String(), mod.String(), "orders.placed").Scan(&qty, &raw, &charged))
	require.Equal(t, "4", qty, "the surviving row is the DEPLOYED one, unmerged")
	require.EqualValues(t, 200_000, raw, "no charged money is moved or lost by the rollback")
	require.EqualValues(t, 200_000, charged)

	// The pre-073 arbiter is back, alone.
	afterDown := uniqueIndexColumns(t, pool)
	require.Len(t, afterDown, 1, "%v", afterDown)
	require.Contains(t, afterDown, "usage_aggregates_period_line_aggregation_key")

	// --- up again: 073.up must re-apply cleanly onto the restored 055 shape,
	// which is what CI's down-then-up idempotency sweep exercises.
	_, err = pool.Exec(ctx, migrationSQL(t, "073_dev_served_usage.up.sql"))
	require.NoError(t, err)

	require.True(t, columnExists(t, pool, "usage_events", "dev_served"))
	require.True(t, columnExists(t, pool, "usage_aggregates", "dev_served"))
	afterUp := uniqueIndexColumns(t, pool)
	require.Len(t, afterUp, 1, "the re-applied up drops the restored narrow arbiter again: %v", afterUp)
	require.Contains(t, afterUp, "usage_aggregates_period_line_dev_served_key")

	// A fresh mixed rollup under a different app proves the widened arbiter is
	// functioning post-re-apply, not merely present.
	app2, mod2 := uuid.New(), uuid.New()
	seedMetricDef(t, pool, mod2, "orders.placed", usage.KindSum, 50_000)
	seedEventDevServed(t, pool, acct, app2, mod2, "orders.placed", usage.KindSum, 1, "2026-06-01T00:00:00Z", false)
	seedEventDevServed(t, pool, acct, app2, mod2, "orders.placed", usage.KindSum, 1, "2026-06-01T00:00:00Z", true)

	_, err = svc.RollupPeriod(ctx, acct, mustTime(t, pStart), mustTime(t, pEnd))
	require.NoError(t, err)

	var count2 int
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT count(*) FROM ms_billing.usage_aggregates WHERE app_id=$1 AND module_id=$2 AND metric=$3`,
		app2.String(), mod2.String(), "orders.placed").Scan(&count2))
	require.Equal(t, 2, count2, "post-re-apply, dev and deployed rows coexist again")
}
