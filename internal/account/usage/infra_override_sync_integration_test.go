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

// SyncInfraPriceOverrides against a real Postgres. The unit tests exercise a
// fake that MIRRORS these semantics; only this file proves the three sqlc
// queries behind them actually parse and behave that way.
//
// 🔴 THE EMPTY-KEEP CASE IS THE POINT. The delete filters `NOT (metric = ANY(
// @keep))`, and in Postgres `x = ANY('{}')` is false while `x = ANY(NULL)` is
// NULL — so an empty keep-set deletes everything (correct: the module withdrew
// its last declaration) but a NIL one would delete nothing and silently keep
// charging. The store builds `keep` with make(...,0,n) so it is never nil; this
// test is what would catch that changing.

func overrideStore(t *testing.T, pool *pgxpool.Pool) usage.Store {
	t.Helper()
	return usage.NewStore(pool)
}

// countModuleReserved returns how many reserved-namespace metric_definitions
// rows this module owns, and the price of one of them.
func moduleOverride(t *testing.T, pool *pgxpool.Pool, mod uuid.UUID, metric string) (int64, bool) {
	t.Helper()
	var price int64
	err := pool.QueryRow(context.Background(),
		`SELECT unit_price_micros FROM ms_billing.metric_definitions
		 WHERE module_id = $1 AND metric = $2`, mod.String(), metric).Scan(&price)
	if err != nil {
		return 0, false
	}
	return price, true
}

func countModuleReserved(t *testing.T, pool *pgxpool.Pool, mod uuid.UUID) int {
	t.Helper()
	var n int
	require.NoError(t, pool.QueryRow(context.Background(),
		`SELECT count(*) FROM ms_billing.metric_definitions
		 WHERE module_id = $1 AND (metric LIKE 'infra.%' OR metric LIKE 'platform.%')`,
		mod.String()).Scan(&n))
	return n
}

func TestSyncInfraPriceOverrides_AbsorbAllExpandsFromTheSeededCatalog(t *testing.T) {
	pool := testutil.NewTestDB(t)
	store := overrideStore(t, pool)
	mod := uuid.New()

	require.NoError(t, store.SyncInfraPriceOverrides(context.Background(), mod, true, nil))

	// The expansion is the SEEDED sentinel catalog — the caller named nothing.
	var active int
	require.NoError(t, pool.QueryRow(context.Background(),
		`SELECT count(*) FROM ms_billing.metric_definitions
		 WHERE module_id = '00000000-0000-0000-0000-000000000000' AND active`).Scan(&active))
	require.Positive(t, active, "migrations 017/018/020 seed the sentinel catalog")
	require.Equal(t, active, countModuleReserved(t, pool, mod),
		"one absorbed row per active sentinel metric, no list in code")

	price, ok := moduleOverride(t, pool, mod, "infra.compute.walltime.ms")
	require.True(t, ok)
	require.Equal(t, int64(0), price)

	// kind + unit are INHERITED, never supplied: they match the sentinel row.
	var kind, unit string
	require.NoError(t, pool.QueryRow(context.Background(),
		`SELECT s.kind::text, s.unit FROM ms_billing.metric_definitions m
		 JOIN ms_billing.metric_definitions s
		   ON s.module_id = '00000000-0000-0000-0000-000000000000' AND s.metric = m.metric
		 WHERE m.module_id = $1 AND m.metric = 'infra.storage.gib_hours'
		   AND m.kind = s.kind AND m.unit = s.unit`, mod.String()).Scan(&kind, &unit))
	require.Equal(t, "GiB-hour", unit)
}

func TestSyncInfraPriceOverrides_ExplicitOverrideBeatsAbsorbAll(t *testing.T) {
	pool := testutil.NewTestDB(t)
	store := overrideStore(t, pool)
	mod := uuid.New()

	require.NoError(t, store.SyncInfraPriceOverrides(context.Background(), mod, true,
		[]usage.InfraPriceOverride{{Metric: "infra.egress.api.bytes", UnitPriceMicros: 120_000}}))

	egress, ok := moduleOverride(t, pool, mod, "infra.egress.api.bytes")
	require.True(t, ok)
	require.Equal(t, int64(120_000), egress, "the named metric wins over the absorbed 0")

	compute, ok := moduleOverride(t, pool, mod, "infra.compute.walltime.ms")
	require.True(t, ok)
	require.Equal(t, int64(0), compute, "everything else is still absorbed")
}

func TestSyncInfraPriceOverrides_EmptyPayloadWithdrawsEverything(t *testing.T) {
	pool := testutil.NewTestDB(t)
	store := overrideStore(t, pool)
	ctx := context.Background()
	mod := uuid.New()

	require.NoError(t, store.SyncInfraPriceOverrides(ctx, mod, false,
		[]usage.InfraPriceOverride{{Metric: "infra.compute.walltime.ms", UnitPriceMicros: 5}}))
	require.Equal(t, 1, countModuleReserved(t, pool, mod))

	// The author deletes the declaration and republishes. An empty keep-array
	// must delete — see the file header on = ANY('{}') vs = ANY(NULL).
	require.NoError(t, store.SyncInfraPriceOverrides(ctx, mod, false, nil))
	require.Equal(t, 0, countModuleReserved(t, pool, mod),
		"the withdrawn override is gone; the line reverts to the platform default")
}

func TestSyncInfraPriceOverrides_WithdrawalIsScopedToTheModuleAndTheReservedNamespace(t *testing.T) {
	pool := testutil.NewTestDB(t)
	store := overrideStore(t, pool)
	ctx := context.Background()
	mine, other := uuid.New(), uuid.New()

	// A custom metric of MINE, and another module's override.
	require.NoError(t, store.UpsertMetricDefinitions(ctx, []usage.MetricDeclaration{{
		ModuleID: mine, Metric: "video.publish", Kind: usage.KindCount, Unit: "video",
		UnitPriceMicros: 30_000, Priced: true, Active: true,
	}}))
	require.NoError(t, store.SyncInfraPriceOverrides(ctx, other, false,
		[]usage.InfraPriceOverride{{Metric: "infra.compute.walltime.ms", UnitPriceMicros: 7}}))

	require.NoError(t, store.SyncInfraPriceOverrides(ctx, mine, false,
		[]usage.InfraPriceOverride{{Metric: "infra.compute.walltime.ms", UnitPriceMicros: 5}}))
	require.NoError(t, store.SyncInfraPriceOverrides(ctx, mine, false, nil))

	// My custom metric survives — it belongs to SetMetricDefinitions, not here.
	var customPrice int64
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT unit_price_micros FROM ms_billing.metric_definitions
		 WHERE module_id = $1 AND metric = 'video.publish'`, mine.String()).Scan(&customPrice))
	require.Equal(t, int64(30_000), customPrice)

	// So does the other module's override.
	otherPrice, ok := moduleOverride(t, pool, other, "infra.compute.walltime.ms")
	require.True(t, ok)
	require.Equal(t, int64(7), otherPrice)

	// And the platform's own sentinel row is untouched.
	var sentinel int
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT count(*) FROM ms_billing.metric_definitions
		 WHERE module_id = '00000000-0000-0000-0000-000000000000'`).Scan(&sentinel))
	require.Positive(t, sentinel)
}

func TestSyncInfraPriceOverrides_AbsorbAllSweepsARetiredMetric(t *testing.T) {
	pool := testutil.NewTestDB(t)
	store := overrideStore(t, pool)
	ctx := context.Background()
	mod := uuid.New()

	require.NoError(t, store.SyncInfraPriceOverrides(ctx, mod, true, nil))
	_, ok := moduleOverride(t, pool, mod, "infra.cron.count")
	require.True(t, ok, "precondition: absorbed")

	// The platform retires the metric.
	_, err := pool.Exec(ctx,
		`UPDATE ms_billing.metric_definitions SET active = false
		 WHERE module_id = '00000000-0000-0000-0000-000000000000' AND metric = 'infra.cron.count'`)
	require.NoError(t, err)

	require.NoError(t, store.SyncInfraPriceOverrides(ctx, mod, true, nil))
	_, still := moduleOverride(t, pool, mod, "infra.cron.count")
	require.False(t, still, "a retired metric is neither re-absorbed nor left behind")
}
