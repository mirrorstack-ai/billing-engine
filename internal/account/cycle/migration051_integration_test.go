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

var migration051GPUPrices = map[string]int64{
	"g4dn.xlarge": 710000, "g4dn.2xlarge": 1015000,
	"g4dn.4xlarge": 1625000, "g4dn.8xlarge": 2938000,
	"g4dn.12xlarge": 5281000, "g4dn.16xlarge": 5875000,
	"g5.xlarge": 1459000, "g5.2xlarge": 1757760,
	"g5.4xlarge": 2355280, "g5.8xlarge": 3550330,
	"g5.12xlarge": 8226090, "g5.16xlarge": 5940420,
	"g5.24xlarge": 11811230, "g5.48xlarge": 23622460,
}

func assertMigration051HeavyRows(t *testing.T, pool *pgxpool.Pool, gpuPrice int64) {
	t.Helper()
	want := []struct {
		metric, unit string
		price        int64
	}{
		{"infra.task.vcpu.hours", "vCPU-hour", 40450},
		{"infra.task.memory.gib_hours", "GiB-hour", 4420},
		{"infra.task.gpu.hours", "instance-hour", gpuPrice},
	}
	for _, w := range want {
		kind, unit, price, active, ok := metricRow(t, pool, w.metric)
		require.True(t, ok, w.metric)
		require.Equal(t, "sum", kind)
		require.Equal(t, w.unit, unit)
		require.NotNil(t, price)
		require.EqualValues(t, w.price, *price)
		require.True(t, active)

		var group string
		require.NoError(t, pool.QueryRow(context.Background(),
			`SELECT display_group FROM ms_billing.metric_definitions
			  WHERE module_id=$1 AND metric=$2`, sentinelModuleID, w.metric).Scan(&group))
		require.Equal(t, "compute", group)
	}
}

func assertMigration051Reprices(t *testing.T, pool *pgxpool.Pool, api, storage int64) {
	t.Helper()
	prices := map[string]int64{
		"infra.egress.api.bytes":  api,
		"infra.storage.gib_hours": storage,
	}
	for metric, want := range prices {
		_, _, price, _, ok := metricRow(t, pool, metric)
		require.True(t, ok, metric)
		require.NotNil(t, price)
		require.EqualValues(t, want, *price)
	}
}

func assertMigration051GPUPrices(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	for model, want := range migration051GPUPrices {
		var price int64
		var active bool
		require.NoError(t, pool.QueryRow(context.Background(),
			`SELECT unit_price_micros, active
			   FROM ms_billing.metric_model_prices
			  WHERE metric='infra.task.gpu.hours' AND model=$1`, model).Scan(&price, &active))
		require.EqualValues(t, want, price, model)
		require.True(t, active, model)
	}
}

func TestMigration051_Up_SeedsHeavyTierRates(t *testing.T) {
	assertMigration051HeavyRows(t, testutil.NewTestDB(t), 566900)
}

func TestMigration051_Up_RepricesEgressAndStorage(t *testing.T) {
	pool := testutil.NewTestDB(t)
	assertMigration051Reprices(t, pool, 122406, 37)
}

func TestMigration051_Up_SeedsGPUInstancePrices(t *testing.T) {
	assertMigration051GPUPrices(t, testutil.NewTestDB(t))
}

func TestMigration051_UpDownUp_RoundTrips(t *testing.T) {
	pool := testutil.NewTestDB(t)
	ctx := context.Background()

	_, err := pool.Exec(ctx, migrationSQL(t, "051_heavy_tier_rates.down.sql"))
	require.NoError(t, err)
	for _, metric := range []string{
		"infra.task.vcpu.hours", "infra.task.memory.gib_hours", "infra.task.gpu.hours",
	} {
		_, _, _, _, ok := metricRow(t, pool, metric)
		require.False(t, ok, metric)
	}
	var gpuCount int
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT count(*) FROM ms_billing.metric_model_prices
		  WHERE metric='infra.task.gpu.hours'`).Scan(&gpuCount))
	require.Zero(t, gpuCount)
	assertMigration051Reprices(t, pool, 90000, 32)

	_, err = pool.Exec(ctx, migrationSQL(t, "051_heavy_tier_rates.up.sql"))
	require.NoError(t, err)
	assertMigration051HeavyRows(t, pool, 710000)
	assertMigration051GPUPrices(t, pool)
	assertMigration051Reprices(t, pool, 122406, 37)
}

func TestMigration051_VcpuHoursRollsUpAtTokyoARMRate(t *testing.T) {
	pool := testutil.NewTestDB(t)
	svc := cycle.NewService(cycle.NewStore(pool), nil)
	acct := seedAccount(t, pool)
	seedEvent(t, pool, acct, uuid.New(), usage.PlatformInfraModuleID(),
		"infra.task.vcpu.hours", usage.KindSum, 5, "2026-06-10T00:00:00Z")

	resp, err := svc.RollupPeriod(context.Background(), acct, mustTime(t, pStart), mustTime(t, pEnd))
	require.NoError(t, err)
	require.Len(t, resp.Aggregates, 1)
	a := resp.Aggregates[0]
	require.EqualValues(t, 202250, a.RawCostMicros) // 5 x 40,450 RAW COGS.
	require.EqualValues(t, 242700, a.ChargedMicros) // x 12/10 at rollup, not in the seed.
	require.Equal(t, 12, a.MarkupNum)
}
