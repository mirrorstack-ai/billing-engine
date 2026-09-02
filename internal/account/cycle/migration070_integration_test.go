//go:build integration

package cycle_test

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/mirrorstack-ai/billing-engine/internal/account/cycle"
	"github.com/mirrorstack-ai/billing-engine/internal/account/usage"
	"github.com/mirrorstack-ai/billing-engine/internal/shared/testutil"
)

const g5gRawTokyoHourlyMicros int64 = 566900

func TestMigration070_AdmittedTaskGPUModelsHaveExactPriceRows(t *testing.T) {
	pool := testutil.NewTestDB(t)
	ctx := context.Background()
	models := usage.AdmittedTaskGPUModels()
	require.Equal(t, []string{usage.TaskGPUModelG5GXlarge}, models)

	for _, model := range models {
		var count int
		var price int64
		var active bool
		require.NoError(t, pool.QueryRow(ctx,
			`SELECT count(*), min(unit_price_micros), bool_and(active)
			   FROM ms_billing.metric_model_prices
			  WHERE metric='infra.task.gpu.hours' AND model=$1`, model).Scan(&count, &price, &active))
		require.Equal(t, 1, count, model)
		require.EqualValues(t, g5gRawTokyoHourlyMicros, price, model)
		require.True(t, active, model)
	}

	_, _, catalogPrice, active, ok := metricRow(t, pool, "infra.task.gpu.hours")
	require.True(t, ok)
	require.True(t, active)
	require.NotNil(t, catalogPrice)
	require.EqualValues(t, g5gRawTokyoHourlyMicros, *catalogPrice)
}

func TestMigration070_UpDownUp_RoundTrips(t *testing.T) {
	pool := testutil.NewTestDB(t)
	ctx := context.Background()

	_, err := pool.Exec(ctx, migrationSQL(t, "070_g5g_task_gpu_price.down.sql"))
	require.NoError(t, err)
	var count int
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT count(*) FROM ms_billing.metric_model_prices
		  WHERE metric='infra.task.gpu.hours' AND model='g5g.xlarge'`).Scan(&count))
	require.Zero(t, count)
	_, _, catalogPrice, _, ok := metricRow(t, pool, "infra.task.gpu.hours")
	require.True(t, ok)
	require.NotNil(t, catalogPrice)
	require.EqualValues(t, 710000, *catalogPrice)

	_, err = pool.Exec(ctx, migrationSQL(t, "070_g5g_task_gpu_price.up.sql"))
	require.NoError(t, err)
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT count(*) FROM ms_billing.metric_model_prices
		  WHERE metric='infra.task.gpu.hours' AND model='g5g.xlarge'
		    AND unit_price_micros=566900 AND active`).Scan(&count))
	require.Equal(t, 1, count)
	_, _, catalogPrice, _, ok = metricRow(t, pool, "infra.task.gpu.hours")
	require.True(t, ok)
	require.NotNil(t, catalogPrice)
	require.EqualValues(t, g5gRawTokyoHourlyMicros, *catalogPrice)
}

func TestMigration070_TaskGPUExactModelPriceWinsOverVersionAndCatalog(t *testing.T) {
	pool := testutil.NewTestDB(t)
	ctx := context.Background()
	acct, app, mod := seedAccount(t, pool), uuid.New(), uuid.New()

	_, err := pool.Exec(ctx,
		`INSERT INTO ms_billing.metric_version_prices
		    (module_id, metric, module_version, unit_price_micros)
		 VALUES ($1, 'infra.task.gpu.hours', 'v1', 1)`, mod.String())
	require.NoError(t, err)
	_, err = pool.Exec(ctx,
		`INSERT INTO ms_billing.usage_events
		    (event_id, account_id, app_id, module_id, metric, kind, value, model, module_version, recorded_at)
		 VALUES ($1,$2,$3,$4,'infra.task.gpu.hours','sum',1,$5,'v1',$6)`,
		uuid.NewString(), acct.String(), app.String(), mod.String(), usage.TaskGPUModelG5GXlarge, "2026-06-10T00:00:00Z")
	require.NoError(t, err)

	resp, err := cycle.NewService(cycle.NewStore(pool), nil).RollupPeriod(
		ctx, acct, mustTime(t, pStart), mustTime(t, pEnd))
	require.NoError(t, err)
	require.Len(t, resp.Aggregates, 1)
	require.EqualValues(t, g5gRawTokyoHourlyMicros, resp.Aggregates[0].RawCostMicros)
	require.EqualValues(t, 680280, resp.Aggregates[0].ChargedMicros)
}

func TestMigration070_UnknownTaskGPUModelCannotBillThroughHistoricalOrCatalogFallback(t *testing.T) {
	pool := testutil.NewTestDB(t)
	ctx := context.Background()
	acct, app := seedAccount(t, pool), uuid.New()
	_, err := pool.Exec(ctx,
		`INSERT INTO ms_billing.usage_events
		    (event_id, account_id, app_id, module_id, metric, kind, value, model, recorded_at)
		 VALUES ($1,$2,$3,$4,'infra.task.gpu.hours','sum',1,'g4dn.xlarge',$5)`,
		uuid.NewString(), acct.String(), app.String(), sentinelModuleID, "2026-06-10T00:00:00Z")
	require.NoError(t, err)

	_, err = cycle.NewService(cycle.NewStore(pool), nil).RollupPeriod(
		ctx, acct, mustTime(t, pStart), mustTime(t, pEnd))
	require.Error(t, err)
	require.Contains(t, err.Error(), "active exact price")
}

func TestMigration070_RemovingG5GPriceMakesBillingFailClosed(t *testing.T) {
	pool := testutil.NewTestDB(t)
	ctx := context.Background()
	acct, app := seedAccount(t, pool), uuid.New()
	_, err := pool.Exec(ctx,
		`DELETE FROM ms_billing.metric_model_prices
		  WHERE metric='infra.task.gpu.hours' AND model='g5g.xlarge'`)
	require.NoError(t, err)
	_, err = pool.Exec(ctx,
		`INSERT INTO ms_billing.usage_events
		    (event_id, account_id, app_id, module_id, metric, kind, value, model, recorded_at)
		 VALUES ($1,$2,$3,$4,'infra.task.gpu.hours','sum',1,$5,$6)`,
		uuid.NewString(), acct.String(), app.String(), sentinelModuleID, usage.TaskGPUModelG5GXlarge, "2026-06-10T00:00:00Z")
	require.NoError(t, err)

	_, err = cycle.NewService(cycle.NewStore(pool), nil).RollupPeriod(
		ctx, acct, mustTime(t, pStart), mustTime(t, pEnd))
	require.Error(t, err)
	require.Contains(t, err.Error(), "active exact price")
}

func TestMigration070_ZeroG5GPriceMakesBillingFailClosed(t *testing.T) {
	pool := testutil.NewTestDB(t)
	ctx := context.Background()
	acct, app := seedAccount(t, pool), uuid.New()
	_, err := pool.Exec(ctx,
		`UPDATE ms_billing.metric_model_prices SET unit_price_micros=0
		  WHERE metric='infra.task.gpu.hours' AND model='g5g.xlarge'`)
	require.NoError(t, err)
	_, err = pool.Exec(ctx,
		`INSERT INTO ms_billing.usage_events
		    (event_id, account_id, app_id, module_id, metric, kind, value, model, recorded_at)
		 VALUES ($1,$2,$3,$4,'infra.task.gpu.hours','sum',1,$5,$6)`,
		uuid.NewString(), acct.String(), app.String(), sentinelModuleID, usage.TaskGPUModelG5GXlarge, "2026-06-10T00:00:00Z")
	require.NoError(t, err)

	_, err = cycle.NewService(cycle.NewStore(pool), nil).RollupPeriod(
		ctx, acct, mustTime(t, pStart), mustTime(t, pEnd))
	require.Error(t, err)
	require.Contains(t, err.Error(), "positive active exact price")
}

func TestMigration070_ModelPriceSchemaRejectsNegativePrice(t *testing.T) {
	pool := testutil.NewTestDB(t)
	_, err := pool.Exec(context.Background(),
		`UPDATE ms_billing.metric_model_prices SET unit_price_micros=-1
		  WHERE metric='infra.task.gpu.hours' AND model='g5g.xlarge'`)
	require.Error(t, err)
	require.Contains(t, err.Error(), "metric_model_prices_unit_price_micros_check")
}
