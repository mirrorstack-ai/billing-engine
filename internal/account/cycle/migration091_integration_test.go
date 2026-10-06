//go:build integration

package cycle_test

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/mirrorstack-ai/billing-engine/internal/shared/testutil"
)

// Migration 091 applies the owner rule (infra price = cost x 1.2) to the WP13
// audit gaps. The claims are about the DATABASE: the price (and unit) of each
// metric after the full chain, a re-run is a no-op, a row that is no longer at the
// audited value (a finance UPDATE) is never overwritten, and the down restores.

type m091Row struct {
	unit  string
	price int64
}

var m091After = map[string]m091Row{
	"infra.db.gib_hours":             {"GiB-hour", 164},
	"infra.request.count":            {"1k requests", 1490},
	"infra.cron.count":               {"1k fires", 1250},
	"infra.storage.put.count":        {"1k requests", 4700},
	"infra.storage.list.count":       {"1k requests", 4700},
	"infra.compute.walltime.ms":      {"millisecond", 0},
	"infra.task.ephemeral.gib_hours": {"GiB-hour", 133},
}

var m091Before = map[string]m091Row{
	"infra.db.gib_hours":        {"GiB-hour", 137},
	"infra.request.count":       {"request", 1},
	"infra.cron.count":          {"fire", 1},
	"infra.storage.put.count":   {"1k requests", 5},
	"infra.storage.list.count":  {"1k requests", 5},
	"infra.compute.walltime.ms": {"millisecond", 1},
}

// m091Untouched are the rows the owner told this migration to leave alone.
var m091Untouched = map[string]int64{
	"infra.egress.cdn.bytes":         122406,
	"infra.egress.api.bytes":         122406,
	"infra.compute.ssr.egress.bytes": 122406,
	"infra.storage.gib_hours":        37,
	"infra.task.gpu.hours":           566900,
	"infra.ai.requests":              0,
}

func m091Read(t *testing.T, pool *pgxpool.Pool, metric string) (m091Row, bool) {
	t.Helper()
	_, unit, price, _, ok := metricRow(t, pool, metric)
	if !ok {
		return m091Row{}, false
	}
	require.NotNil(t, price, metric)
	return m091Row{unit, *price}, true
}

func m091Assert(t *testing.T, pool *pgxpool.Pool, want map[string]m091Row, why string) {
	t.Helper()
	for metric, w := range want {
		got, ok := m091Read(t, pool, metric)
		require.True(t, ok, "%s: %s row", why, metric)
		require.Equal(t, w, got, "%s: %s", why, metric)
	}
}

func m091Exec(t *testing.T, pool *pgxpool.Pool, sql string) {
	t.Helper()
	_, err := pool.Exec(context.Background(), sql)
	require.NoError(t, err)
}

func TestMigration091_PricesAfterTheChainAndRerunIsANoOp(t *testing.T) {
	pool := testutil.NewTestDB(t) // the FULL chain, 091 included
	up := migrationSQL(t, "091_infra_price_cost_basis_audit_gaps.up.sql")

	m091Assert(t, pool, m091After, "after the chain")
	kind, _, _, active, ok := metricRow(t, pool, "infra.task.ephemeral.gib_hours")
	require.True(t, ok)
	require.Equal(t, "sum", kind)
	require.True(t, active)
	for metric, price := range m091Untouched {
		got, ok := m091Read(t, pool, metric)
		require.True(t, ok, metric)
		require.EqualValues(t, price, got.price, "091 leaves %s alone", metric)
	}

	m091Exec(t, pool, up)
	m091Assert(t, pool, m091After, "rerun")
}

func TestMigration091_OnlyMovesARowStillAtTheAuditedValue(t *testing.T) {
	pool := testutil.NewTestDB(t)
	up := migrationSQL(t, "091_infra_price_cost_basis_audit_gaps.up.sql")
	down := migrationSQL(t, "091_infra_price_cost_basis_audit_gaps.down.sql")

	// Back to the audited state, then a finance UPDATE on every moved metric
	// (a price, and for the unit changes a price on the old unit).
	m091Exec(t, pool, down)
	m091Assert(t, pool, m091Before, "down")
	finance := map[string]int64{
		"infra.db.gib_hours":        150,
		"infra.request.count":       3,
		"infra.cron.count":          2,
		"infra.storage.put.count":   9,
		"infra.storage.list.count":  8,
		"infra.compute.walltime.ms": 7,
	}
	for metric, p := range finance {
		_, err := pool.Exec(context.Background(),
			`UPDATE ms_billing.metric_definitions SET unit_price_micros = $2 WHERE module_id = $1 AND metric = $3`,
			sentinelModuleID, p, metric)
		require.NoError(t, err)
	}
	m091Exec(t, pool, up)
	for metric, p := range finance {
		got, ok := m091Read(t, pool, metric)
		require.True(t, ok, metric)
		require.Equal(t, p, got.price, "a finance UPDATE on %s is never overwritten", metric)
		require.Equal(t, m091Before[metric].unit, got.unit, "%s keeps its unit with its price", metric)
	}

	// A row at the audited price but on a unit someone already changed keeps both.
	m091Exec(t, pool, down)
	_, err := pool.Exec(context.Background(),
		`UPDATE ms_billing.metric_definitions SET unit = 'per-request', unit_price_micros = 1
		  WHERE module_id = $1 AND metric = 'infra.request.count'`, sentinelModuleID)
	require.NoError(t, err)
	m091Exec(t, pool, up)
	got, _ := m091Read(t, pool, "infra.request.count")
	require.Equal(t, m091Row{"per-request", 1}, got, "unit and price move together or not at all")
}

func TestMigration091_DownRestoresTheAuditedState(t *testing.T) {
	pool := testutil.NewTestDB(t)
	up := migrationSQL(t, "091_infra_price_cost_basis_audit_gaps.up.sql")
	down := migrationSQL(t, "091_infra_price_cost_basis_audit_gaps.down.sql")

	m091Exec(t, pool, down)
	m091Assert(t, pool, m091Before, "down")
	_, ok := m091Read(t, pool, "infra.task.ephemeral.gib_hours")
	require.False(t, ok, "down removes the ephemeral row 091 added")
	for metric, price := range m091Untouched {
		got, ok := m091Read(t, pool, metric)
		require.True(t, ok, metric)
		require.EqualValues(t, price, got.price, "down leaves %s alone", metric)
	}

	m091Exec(t, pool, up)
	m091Assert(t, pool, m091After, "up again")
}
