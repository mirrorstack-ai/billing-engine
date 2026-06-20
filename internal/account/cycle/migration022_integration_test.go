//go:build integration

package cycle_test

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/mirrorstack-ai/billing-engine/internal/shared/testutil"
)

// These integration tests validate migration 022 (drop the deprecated
// infra.compute.ms alias) against a real Postgres 17 (gated by the `integration`
// build tag). NewTestDB applies ALL *.up.sql in lexical order, so 022.up is
// already applied on entry; the round-trip test then runs 022.down + 022.up
// explicitly. The helpers (sentinelModuleID, migrationSQL, metricRow) live in
// migration019_integration_test.go in this package.

func computeRowCount(t *testing.T, pool *pgxpool.Pool) int {
	t.Helper()
	var n int
	require.NoError(t, pool.QueryRow(context.Background(),
		`SELECT count(*) FROM ms_billing.metric_definitions
		  WHERE module_id = $1
		    AND metric IN ('infra.compute.ms', 'infra.compute.walltime.ms')`,
		sentinelModuleID).Scan(&n))
	return n
}

func TestMigration022_Up_DropsAlias(t *testing.T) {
	pool := testutil.NewTestDB(t) // applies through 022.up

	// The deprecated alias row is gone.
	_, _, _, _, ok := metricRow(t, pool, "infra.compute.ms")
	require.False(t, ok, "022.up must DELETE the deprecated infra.compute.ms alias row")

	// The authoritative walltime row is untouched (sum, ms, placeholder 1 µ$).
	kind, unit, price, active, ok := metricRow(t, pool, "infra.compute.walltime.ms")
	require.True(t, ok, "infra.compute.walltime.ms must survive 022.up as the primary compute row")
	require.Equal(t, "sum", kind)
	require.Equal(t, "millisecond", unit)
	require.NotNil(t, price)
	require.EqualValues(t, 1, *price)
	require.True(t, active)

	// Exactly one compute row remains (just walltime; no alias).
	require.Equal(t, 1, computeRowCount(t, pool), "post-022.up exactly one compute row (walltime) exists")
}

func TestMigration022_UpDownUp_RoundTrips(t *testing.T) {
	pool := testutil.NewTestDB(t) // 022.up already applied → alias dropped
	ctx := context.Background()

	// --- down: restore the deprecated alias (019's shape) ---
	_, err := pool.Exec(ctx, migrationSQL(t, "022_drop_compute_alias.down.sql"))
	require.NoError(t, err)

	// The alias is back: same sentinel, kind=sum, unit=ms, placeholder 1 µ$, active.
	kind, unit, price, active, ok := metricRow(t, pool, "infra.compute.ms")
	require.True(t, ok, "022.down must re-insert the infra.compute.ms alias row")
	require.Equal(t, "sum", kind)
	require.Equal(t, "millisecond", unit)
	require.NotNil(t, price)
	require.EqualValues(t, 1, *price)
	require.True(t, active)

	// The down also restores the 021 display_group ('compute') on the alias.
	var group string
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT display_group FROM ms_billing.metric_definitions
		  WHERE module_id = $1 AND metric = 'infra.compute.ms'`,
		sentinelModuleID).Scan(&group))
	require.Equal(t, "compute", group, "022.down restores the 021 display_group on the alias")

	// The walltime row is still present and untouched by the down.
	_, _, _, _, ok = metricRow(t, pool, "infra.compute.walltime.ms")
	require.True(t, ok, "022.down must not touch the authoritative walltime row")

	// Both compute rows exist post-down (alias + walltime), no collision.
	require.Equal(t, 2, computeRowCount(t, pool), "post-022.down both compute rows exist (alias restored)")

	// --- up again: re-apply 022 cleanly (idempotent forward path) ---
	_, err = pool.Exec(ctx, migrationSQL(t, "022_drop_compute_alias.up.sql"))
	require.NoError(t, err)

	_, _, _, _, ok = metricRow(t, pool, "infra.compute.ms")
	require.False(t, ok, "re-applied 022.up must drop the alias again")
	_, _, _, _, ok = metricRow(t, pool, "infra.compute.walltime.ms")
	require.True(t, ok, "walltime row still present after 022.up re-apply")
	require.Equal(t, 1, computeRowCount(t, pool), "post-022.up (re-applied) exactly one compute row")
}
