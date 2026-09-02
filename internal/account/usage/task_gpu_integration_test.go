//go:build integration

package usage_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/mirrorstack-ai/billing-engine/internal/account/billing"
	"github.com/mirrorstack-ai/billing-engine/internal/account/usage"
	"github.com/mirrorstack-ai/billing-engine/internal/shared/testutil"
)

func taskGPURequest(ownerUserID uuid.UUID) usage.RecordInfraUsageRequest {
	return usage.RecordInfraUsageRequest{
		EventID:     "task-attempt-1/infra.task.gpu.hours",
		AppID:       uuid.New(),
		OwnerUserID: ownerUserID,
		Metric:      "infra.task.gpu.hours",
		Model:       usage.TaskGPUModelG5GXlarge,
		Value:       1,
		RecordedAt:  time.Date(2026, 8, 14, 0, 0, 0, 0, time.UTC),
	}
}

func TestRecordInfraUsage_TaskGPUExactPriceAndDuplicateEvent_Integration(t *testing.T) {
	pool := testutil.NewTestDB(t)
	ctx := context.Background()
	accountID, ownerUserID := uuid.New(), uuid.New()
	_, err := pool.Exec(ctx,
		`INSERT INTO ms_billing.accounts (id, owner_kind, owner_user_id)
		 VALUES ($1, 'user', $2)`, accountID.String(), ownerUserID.String())
	require.NoError(t, err)
	svc := usage.NewService(usage.NewStore(pool))
	req := taskGPURequest(ownerUserID)

	first, err := svc.RecordInfraUsage(ctx, req)
	require.NoError(t, err)
	require.True(t, first.Recorded)
	second, err := svc.RecordInfraUsage(ctx, req)
	require.NoError(t, err)
	require.False(t, second.Recorded)

	var count int
	var model string
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT count(*), min(model)
		   FROM ms_billing.usage_events
		  WHERE event_id=$1 AND account_id=$2`, req.EventID, accountID.String()).Scan(&count, &model))
	require.Equal(t, 1, count)
	require.Equal(t, usage.TaskGPUModelG5GXlarge, model)
}

func TestRecordInfraUsage_TaskGPUPriceRowRemovalFailsAdmission_Integration(t *testing.T) {
	pool := testutil.NewTestDB(t)
	ctx := context.Background()
	accountID, ownerUserID := uuid.New(), uuid.New()
	_, err := pool.Exec(ctx,
		`INSERT INTO ms_billing.accounts (id, owner_kind, owner_user_id)
		 VALUES ($1, 'user', $2)`, accountID.String(), ownerUserID.String())
	require.NoError(t, err)
	_, err = pool.Exec(ctx,
		`DELETE FROM ms_billing.metric_model_prices
		  WHERE metric='infra.task.gpu.hours' AND model='g5g.xlarge'`)
	require.NoError(t, err)
	req := taskGPURequest(ownerUserID)

	_, err = usage.NewService(usage.NewStore(pool)).RecordInfraUsage(ctx, req)
	requireCode(t, err, billing.CodeInternal)
	var count int
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT count(*) FROM ms_billing.usage_events WHERE event_id=$1`, req.EventID).Scan(&count))
	require.Zero(t, count)
}

func TestRecordInfraUsage_TaskGPUZeroPriceFailsAdmission_Integration(t *testing.T) {
	pool := testutil.NewTestDB(t)
	ctx := context.Background()
	accountID, ownerUserID := uuid.New(), uuid.New()
	_, err := pool.Exec(ctx,
		`INSERT INTO ms_billing.accounts (id, owner_kind, owner_user_id)
		 VALUES ($1, 'user', $2)`, accountID.String(), ownerUserID.String())
	require.NoError(t, err)
	_, err = pool.Exec(ctx,
		`UPDATE ms_billing.metric_model_prices SET unit_price_micros=0
		  WHERE metric='infra.task.gpu.hours' AND model='g5g.xlarge'`)
	require.NoError(t, err)
	req := taskGPURequest(ownerUserID)

	_, err = usage.NewService(usage.NewStore(pool)).RecordInfraUsage(ctx, req)
	requireCode(t, err, billing.CodeInternal)
	var count int
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT count(*) FROM ms_billing.usage_events WHERE event_id=$1`, req.EventID).Scan(&count))
	require.Zero(t, count)
}
