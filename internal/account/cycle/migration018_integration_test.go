//go:build integration

package cycle_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/mirrorstack-ai/billing-engine/internal/account/cycle"
	"github.com/mirrorstack-ai/billing-engine/internal/account/usage"
	"github.com/mirrorstack-ai/billing-engine/internal/shared/testutil"
)

// These integration tests validate migration 018 (infra catalog hygiene)
// against a real Postgres 17 (gated by the `integration` build tag; run via
// `make test-integration`, skipped when Docker is unavailable). NewTestDB
// applies ALL *.up.sql in lexical order, so 018.up is already applied on entry;
// the round-trip test then runs 018.down + 018.up explicitly. No Stripe.

const sentinelModuleID = usage.PlatformInfraModuleIDString

// migrationSQL reads a migration file body relative to the repo root (walked up
// from the test's cwd to go.mod), so the round-trip test exercises the SAME SQL
// the migration ships — not a hand-copied duplicate that could drift.
func migrationSQL(t *testing.T, name string) string {
	t.Helper()
	dir, err := os.Getwd()
	require.NoError(t, err)
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			break
		}
		parent := filepath.Dir(dir)
		require.NotEqual(t, parent, dir, "go.mod not found above %s", dir)
		dir = parent
	}
	body, err := os.ReadFile(filepath.Join(dir, "migrations", "billing", name))
	require.NoError(t, err)
	return string(body)
}

// metricRow reads back a single sentinel-keyed metric_definitions row. ok=false
// when no row exists for that name.
func metricRow(t *testing.T, pool *pgxpool.Pool, metric string) (kind, unit string, price *int64, active bool, ok bool) {
	t.Helper()
	row := pool.QueryRow(context.Background(),
		`SELECT kind, unit, unit_price_micros, active
		   FROM ms_billing.metric_definitions
		  WHERE module_id = $1 AND metric = $2`,
		sentinelModuleID, metric)
	err := row.Scan(&kind, &unit, &price, &active)
	if err != nil {
		return "", "", nil, false, false
	}
	return kind, unit, price, active, true
}

func assertHygieneApplied(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()

	// (1) compute.ms renamed → walltime.ms (sum, ms, placeholder 1 µ$, active).
	kind, unit, price, active, ok := metricRow(t, pool, "infra.compute.walltime.ms")
	require.True(t, ok, "infra.compute.walltime.ms row must exist after 018.up")
	require.Equal(t, "sum", kind)
	require.Equal(t, "millisecond", unit)
	require.NotNil(t, price)
	require.EqualValues(t, 1, *price)
	require.True(t, active)

	// (2) deprecated alias compute.ms still present (same sentinel/kind/price).
	kind, unit, price, active, ok = metricRow(t, pool, "infra.compute.ms")
	require.True(t, ok, "deprecated alias infra.compute.ms row must exist after 018.up")
	require.Equal(t, "sum", kind)
	require.Equal(t, "millisecond", unit)
	require.NotNil(t, price)
	require.EqualValues(t, 1, *price)
	require.True(t, active)

	// (3) egress.bytes retired → price 0 (NOT NULL), still active.
	_, _, price, active, ok = metricRow(t, pool, "infra.egress.bytes")
	require.True(t, ok, "infra.egress.bytes row must remain as the unpriced parent")
	require.NotNil(t, price, "retired egress price must be 0, NOT NULL (NULL would loud-fail the cycle)")
	require.EqualValues(t, 0, *price)
	require.True(t, active)
}

func TestMigration018_Up_AppliesCatalogHygiene(t *testing.T) {
	pool := testutil.NewTestDB(t) // applies through 018.up
	assertHygieneApplied(t, pool)
}

func TestMigration018_UpDownUp_RoundTrips(t *testing.T) {
	pool := testutil.NewTestDB(t) // 018.up already applied
	ctx := context.Background()

	// --- down: reverse to 017's seeded state ---
	_, err := pool.Exec(ctx, migrationSQL(t, "018_infra_catalog_hygiene.down.sql"))
	require.NoError(t, err)

	// walltime.ms gone; compute.ms back to the single 017 seed (price 1);
	// egress.bytes restored to 017's price 1.
	_, _, _, _, ok := metricRow(t, pool, "infra.compute.walltime.ms")
	require.False(t, ok, "down must remove the renamed walltime.ms row")

	kind, unit, price, active, ok := metricRow(t, pool, "infra.compute.ms")
	require.True(t, ok, "down must restore the 017-named infra.compute.ms row")
	require.Equal(t, "sum", kind)
	require.Equal(t, "millisecond", unit)
	require.NotNil(t, price)
	require.EqualValues(t, 1, *price)
	require.True(t, active)

	_, _, price, _, ok = metricRow(t, pool, "infra.egress.bytes")
	require.True(t, ok)
	require.NotNil(t, price)
	require.EqualValues(t, 1, *price, "down must restore 017's egress price")

	// exactly one compute row exists post-down (no leftover alias colliding).
	var computeCount int
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT count(*) FROM ms_billing.metric_definitions
		  WHERE module_id = $1 AND metric IN ('infra.compute.ms','infra.compute.walltime.ms')`,
		sentinelModuleID).Scan(&computeCount))
	require.Equal(t, 1, computeCount, "post-down there is exactly one compute row (the 017 original)")

	// --- up again: re-apply 018 cleanly (validates idempotent forward path) ---
	_, err = pool.Exec(ctx, migrationSQL(t, "018_infra_catalog_hygiene.up.sql"))
	require.NoError(t, err)
	assertHygieneApplied(t, pool)
}

func TestMigration018_WalltimeMSRollupPricesViaNewRow(t *testing.T) {
	pool := testutil.NewTestDB(t) // 018.up applied → walltime.ms seeded at 1 µ$
	store := cycle.NewStore(pool)
	svc := cycle.NewService(store, nil)
	ctx := context.Background()

	acct := seedAccount(t, pool)
	app := uuid.New()
	sentinel := usage.PlatformInfraModuleID()
	// Event under the NEW name + sentinel; price comes from the migration-renamed
	// row (no extra seedMetricDef). 100 ms × 1 µ$ × 12/10 = 120 µ$.
	seedEvent(t, pool, acct, app, sentinel, "infra.compute.walltime.ms", usage.KindSum, 100, "2026-06-10T00:00:00Z")

	resp, err := svc.RollupPeriod(ctx, acct, mustTime(t, pStart), mustTime(t, pEnd))
	require.NoError(t, err)
	require.Len(t, resp.Aggregates, 1)
	a := resp.Aggregates[0]
	require.Equal(t, "infra.compute.walltime.ms", a.Metric)
	require.Equal(t, 12, a.MarkupNum)
	require.EqualValues(t, 100, a.RawCostMicros) // 100 × 1
	require.EqualValues(t, 120, a.ChargedMicros) // × 1.2
	require.EqualValues(t, 120, resp.TotalChargedMicros)
}

func TestMigration018_EgressBytesRollsUpToZero(t *testing.T) {
	pool := testutil.NewTestDB(t) // 018.up applied → egress.bytes price 0
	store := cycle.NewStore(pool)
	svc := cycle.NewService(store, nil)
	ctx := context.Background()

	acct := seedAccount(t, pool)
	app := uuid.New()
	sentinel := usage.PlatformInfraModuleID()
	// A large egress event must roll up to 0 — and MUST NOT loud-fail, proving
	// the retired (price=0) reserved metric is distinguished from the
	// missing-seed (NULL) loud-fail path.
	seedEvent(t, pool, acct, app, sentinel, "infra.egress.bytes", usage.KindSum, 5_000_000, "2026-06-10T00:00:00Z")

	resp, err := svc.RollupPeriod(ctx, acct, mustTime(t, pStart), mustTime(t, pEnd))
	require.NoError(t, err, "retired egress.bytes (price 0) must roll up cleanly, not loud-fail")
	require.Len(t, resp.Aggregates, 1)
	a := resp.Aggregates[0]
	require.Equal(t, "infra.egress.bytes", a.Metric)
	require.EqualValues(t, 0, a.UnitPriceMicros)
	require.EqualValues(t, 0, a.ChargedMicros)
	require.EqualValues(t, 0, resp.TotalChargedMicros)
}
