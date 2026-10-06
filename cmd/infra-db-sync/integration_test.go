//go:build integration

package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/mirrorstack-ai/billing-engine/internal/account/usage"
	"github.com/mirrorstack-ai/billing-engine/internal/shared/testutil"
)

const sentinelModule = "00000000-0000-0000-0000-000000000000"

// DATABASE claims of migration 089: the catalog row exists at the ruled raw
// price in the database group, a re-run changes nothing (and never undoes a
// finance UPDATE), and the down file removes exactly that row.
func TestMigration089_SeedsTheDBSizeMeterAndIsRerunnable(t *testing.T) {
	pool := testutil.NewTestDB(t)
	ctx := context.Background()

	type row struct {
		kind, unit, group string
		price             int64
		active            bool
	}
	read := func() (row, bool) {
		var r row
		err := pool.QueryRow(ctx,
			`SELECT kind::text, unit, display_group::text, unit_price_micros, active
			 FROM ms_billing.metric_definitions WHERE module_id = $1 AND metric = 'infra.db.gib_hours'`,
			sentinelModule).Scan(&r.kind, &r.unit, &r.group, &r.price, &r.active)
		return r, err == nil
	}
	got, ok := read()
	require.True(t, ok, "089 seeds the row")
	require.Equal(t, row{"time_weighted", "GiB-hour", "database", 137, true}, got,
		"089 seeds 114; 090 corrects it to 137 raw (cost basis) x1.2 = $0.12 per GiB-month at 730 h")

	up := readMigration(t, "089_db_size_metric.up.sql")
	down := readMigration(t, "089_db_size_metric.down.sql")

	_, err := pool.Exec(ctx, `UPDATE ms_billing.metric_definitions SET unit_price_micros = 150
		WHERE module_id = $1 AND metric = 'infra.db.gib_hours'`, sentinelModule)
	require.NoError(t, err)
	_, err = pool.Exec(ctx, up)
	require.NoError(t, err, "rerunnable")
	got, _ = read()
	require.EqualValues(t, 150, got.price, "a re-run must not undo a finance UPDATE")

	_, err = pool.Exec(ctx, down)
	require.NoError(t, err)
	_, ok = read()
	require.False(t, ok, "down removes the row")
	_, err = pool.Exec(ctx, up)
	require.NoError(t, err)
	got, ok = read()
	require.True(t, ok)
	require.EqualValues(t, 114, got.price, "089 alone seeds the original raw price")
}

// DATABASE claims of migration 090: the raw price is 137 after the full chain, a
// re-run is a no-op, only a row still at 114 moves, and the down restores 114.
func TestMigration090_DBSizePriceIsTheCostBasisAndRerunnable(t *testing.T) {
	pool := testutil.NewTestDB(t)
	ctx := context.Background()
	price := func() int64 {
		var p int64
		require.NoError(t, pool.QueryRow(ctx,
			`SELECT unit_price_micros FROM ms_billing.metric_definitions WHERE module_id = $1 AND metric = 'infra.db.gib_hours'`,
			sentinelModule).Scan(&p))
		return p
	}
	set := func(p int64) {
		_, err := pool.Exec(ctx, `UPDATE ms_billing.metric_definitions SET unit_price_micros = $2
			WHERE module_id = $1 AND metric = 'infra.db.gib_hours'`, sentinelModule, p)
		require.NoError(t, err)
	}
	up := readMigration(t, "090_db_size_price_cost_basis.up.sql")
	down := readMigration(t, "090_db_size_price_cost_basis.down.sql")

	require.EqualValues(t, 137, price(), "the migration chain ends at the cost-basis price")
	_, err := pool.Exec(ctx, up)
	require.NoError(t, err)
	require.EqualValues(t, 137, price(), "rerun is a no-op")

	set(150)
	_, err = pool.Exec(ctx, up)
	require.NoError(t, err)
	require.EqualValues(t, 150, price(), "a finance UPDATE is never overwritten")

	set(137)
	_, err = pool.Exec(ctx, down)
	require.NoError(t, err)
	require.EqualValues(t, 114, price(), "down restores 114")
	_, err = pool.Exec(ctx, up)
	require.NoError(t, err)
	require.EqualValues(t, 137, price(), "up from 114 gives 137")
}

func readMigration(t *testing.T, name string) string {
	t.Helper()
	b, err := os.ReadFile(filepath.Join("..", "..", "migrations", "billing", name))
	require.NoError(t, err)
	return string(b)
}

// seedAppSchema creates app_<id> with a module_install table and the given
// per-module tables, filled so every relation has a measurable size.
func seedAppSchema(t *testing.T, pool *pgxpool.Pool, app uuid.UUID, installs []uuid.UUID, tables []string) {
	t.Helper()
	ctx := context.Background()
	schema := appSchemaName(app)
	_, err := pool.Exec(ctx, fmt.Sprintf(`CREATE SCHEMA %s`, schema))
	require.NoError(t, err)
	_, err = pool.Exec(ctx, fmt.Sprintf(`CREATE TABLE %s.module_install (module_id uuid PRIMARY KEY, prefix text NOT NULL, version_id uuid NULL)`, schema))
	require.NoError(t, err)
	for _, m := range installs {
		// prefix is the platform's display value, deliberately NOT the physical one.
		_, err = pool.Exec(ctx, fmt.Sprintf(`INSERT INTO %s.module_install (module_id, prefix) VALUES ($1, 'acme_display_')`, schema), m)
		require.NoError(t, err)
	}
	_, err = pool.Exec(ctx, fmt.Sprintf(`CREATE TABLE %s.members (user_id uuid PRIMARY KEY)`, schema))
	require.NoError(t, err)
	for _, tb := range tables {
		_, err = pool.Exec(ctx, fmt.Sprintf(`CREATE TABLE %s.%s (id serial PRIMARY KEY, body text)`, schema, tb))
		require.NoError(t, err)
		_, err = pool.Exec(ctx, fmt.Sprintf(`INSERT INTO %s.%s (body) SELECT repeat('x', 1000) FROM generate_series(1, 2000)`, schema, tb))
		require.NoError(t, err)
	}
}

// The two fixed queries against a real Postgres, under a role that holds NO
// grant on any customer table: the catalog size query must still answer, and the
// install read needs only its own SELECT.
func TestPGReader_ReadsSizesFromTheCatalogUnderAReadOnlyRole(t *testing.T) {
	pool := testutil.NewTestDB(t)
	ctx := context.Background()
	app, mod := uuid.New(), uuid.New()
	seedAppSchema(t, pool, app, []uuid.UUID{mod}, []string{phys(mod) + "attempts"})
	// A view and a partitioned parent+child: only relations with storage count,
	// each exactly once.
	schema := appSchemaName(app)
	for _, ddl := range []string{
		fmt.Sprintf(`CREATE VIEW %s.%sv AS SELECT * FROM %s.%sattempts`, schema, phys(mod), schema, phys(mod)),
		fmt.Sprintf(`CREATE TABLE %s.%spart (id int, d date) PARTITION BY RANGE (d)`, schema, phys(mod)),
		fmt.Sprintf(`CREATE TABLE %s.%spart_1 PARTITION OF %s.%spart FOR VALUES FROM ('2026-01-01') TO ('2027-01-01')`, schema, phys(mod), schema, phys(mod)),
		`CREATE SCHEMA mod_m0123456789abcdef0123456789abcdef`,
		`CREATE TABLE mod_m0123456789abcdef0123456789abcdef.t (id int)`,
		`CREATE ROLE dbsize_test NOLOGIN`,
		fmt.Sprintf(`GRANT USAGE ON SCHEMA %s TO dbsize_test`, schema),
		fmt.Sprintf(`GRANT SELECT (module_id) ON %s.module_install TO dbsize_test`, schema),
	} {
		_, err := pool.Exec(ctx, ddl)
		require.NoError(t, err, ddl)
	}

	cfg := pool.Config().Copy()
	cfg.ConnConfig.RuntimeParams["role"] = "dbsize_test"
	ro, err := pgxpool.NewWithConfig(ctx, cfg)
	require.NoError(t, err)
	t.Cleanup(ro.Close)
	rd := pgReader{pool: ro}

	tables, err := rd.TableSizes(ctx)
	require.NoError(t, err)
	got := map[string]int64{}
	for _, tb := range tables {
		require.Equal(t, schema, tb.Schema, "only app schemas are returned; ms_billing and mod_* never")
		got[tb.Table] = tb.Bytes
	}
	require.Contains(t, got, phys(mod)+"attempts")
	require.Contains(t, got, "members")
	require.Contains(t, got, "module_install")
	require.Contains(t, got, phys(mod)+"part_1", "a partition child is a relation with storage")
	require.NotContains(t, got, phys(mod)+"v", "a view has no storage")
	require.NotContains(t, got, phys(mod)+"part", "a partitioned parent has none; its children count as themselves")
	require.Greater(t, got[phys(mod)+"attempts"], int64(1_000_000), "heap + index + TOAST of 2000 x 1 kB rows")

	installs, err := rd.Installs(ctx, app)
	require.NoError(t, err)
	require.Equal(t, []install{{Module: mod}}, installs)

	// An app with no schema (or no grant) is an error, never an empty answer.
	_, err = rd.Installs(ctx, uuid.New())
	require.Error(t, err)

	// The reads run under statement_timeout and lock_timeout, scoped to the tx.
	require.NoError(t, rd.readOnly(ctx, func(tx pgx.Tx) error {
		var st, lt string
		require.NoError(t, tx.QueryRow(ctx, `SHOW statement_timeout`).Scan(&st))
		require.NoError(t, tx.QueryRow(ctx, `SHOW lock_timeout`).Scan(&lt))
		require.Equal(t, "20s", st)
		require.Equal(t, "3s", lt)
		return nil
	}))

	// READ ONLY is enforced by the server, not just by the code.
	_, werr := ro.Exec(ctx, fmt.Sprintf(`DELETE FROM %s.module_install`, schema))
	require.Error(t, werr)
}

// The whole path, writer included: real usage.Service, real catalog, real rows.
// A capability test that never reaches the writer passes vacuously.
func TestSyncDB_EndToEndRecordsTheModuleLevelInUsageEvents(t *testing.T) {
	pool := testutil.NewTestDB(t)
	ctx := context.Background()
	app, quiz, empty := uuid.New(), uuid.New(), uuid.New()
	seedAppSchema(t, pool, app, []uuid.UUID{quiz, empty}, []string{phys(quiz) + "attempts"})

	svc := usage.NewService(usage.NewStore(pool))
	at := time.Date(2026, 10, 5, 12, 30, 0, 0, time.UTC)
	res := syncDB(ctx, svc, pgReader{pool: pool}, billingHistory{pool: pool}, at)
	require.False(t, res.Failed, "%v", res.Err)
	require.Equal(t, 2*lookbackHours, res.Recorded)
	require.Greater(t, res.UnattributedBytes, int64(0), "members and module_install are the platform's")

	hour := at.Truncate(time.Hour).Add(-time.Hour)
	read := func(mod uuid.UUID) (float64, string, string) {
		var v float64
		var kind, metric string
		require.NoError(t, pool.QueryRow(ctx,
			`SELECT value::float8, kind::text, metric FROM ms_billing.usage_events WHERE event_id = $1 AND app_id = $2 AND module_id = $3`,
			dbEventID(app, mod, hour), app, mod).Scan(&v, &kind, &metric))
		return v, kind, metric
	}
	v, kind, metric := read(quiz)
	require.Equal(t, "infra.db.gib_hours", metric)
	require.Equal(t, "time_weighted", kind)
	require.Greater(t, v, 0.0)
	require.Less(t, v, 1.0, "a few MB is a fraction of a GiB: the level, not bytes")
	v, _, _ = read(empty)
	require.EqualValues(t, 0, v, "an installed module with no tables records an explicit 0 level")

	again := syncDB(ctx, svc, pgReader{pool: pool}, billingHistory{pool: pool}, at)
	require.Equal(t, 0, again.Recorded, "a re-run dedupes on the deterministic event_id")
	require.Equal(t, 2*lookbackHours, again.Deduped)
}

// D3 against real rows: a module sampled positive, then uninstalled, is read back
// from usage_events by billingHistory and zeroed; a pair already at 0 is not
// returned again.
func TestSyncDB_EndToEndZeroesAnUninstalledModule(t *testing.T) {
	pool := testutil.NewTestDB(t)
	ctx := context.Background()
	app, keep, gone := uuid.New(), uuid.New(), uuid.New()
	seedAppSchema(t, pool, app, []uuid.UUID{keep, gone}, []string{phys(keep) + "t", phys(gone) + "t"})
	svc := usage.NewService(usage.NewStore(pool))
	h := billingHistory{pool: pool}

	t0 := time.Date(2026, 10, 5, 12, 30, 0, 0, time.UTC)
	res := syncDB(ctx, svc, pgReader{pool: pool}, h, t0)
	require.False(t, res.Failed, "%v", res.Err)

	_, err := pool.Exec(ctx, fmt.Sprintf(`DELETE FROM %s.module_install WHERE module_id = $1`, appSchemaName(app)), gone)
	require.NoError(t, err)

	t1 := t0.Add(2 * time.Hour)
	res = syncDB(ctx, svc, pgReader{pool: pool}, h, t1)
	require.False(t, res.Failed, "%v", res.Err)
	require.Equal(t, 1, res.Zeroed)

	newest := t1.Truncate(time.Hour).Add(-time.Hour)
	var v float64
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT value::float8 FROM ms_billing.usage_events WHERE event_id = $1`, dbEventID(app, gone, newest)).Scan(&v))
	require.EqualValues(t, 0, v, "the vanished module's level ends at an explicit 0")

	pairs, err := h.LivePairs(ctx, t0.Add(-historyWindow))
	require.NoError(t, err)
	require.Equal(t, []pair{{app, keep}}, pairs, "a zeroed pair is not returned again")
}
