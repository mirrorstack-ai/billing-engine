//go:build integration

package cycle_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/mirrorstack-ai/billing-engine/internal/account/cycle"
	"github.com/mirrorstack-ai/billing-engine/internal/account/usage"
	"github.com/mirrorstack-ai/billing-engine/internal/shared/testutil"
)

// The rosters the charge legs price from carry each app's plan
// (billing-engine#202): the SQL selects apps.plan and both stores map it.

func seedOneAppPerPlan(t *testing.T, pool *pgxpool.Pool, acct uuid.UUID) map[uuid.UUID]usage.Plan {
	t.Helper()
	want := map[uuid.UUID]usage.Plan{}
	for _, plan := range []usage.Plan{usage.PlanFree, usage.PlanPro, usage.PlanBusiness} {
		appID := seedPlanApp(t, pool, acct, false)
		_, err := pool.Exec(context.Background(), `UPDATE ms_billing.apps SET plan = $2 WHERE app_id = $1`, appID.String(), string(plan))
		require.NoError(t, err)
		want[appID] = plan
	}
	return want
}

func TestLiveAppsCreatedBefore_CarriesEachAppsPlan(t *testing.T) {
	pool := testutil.NewTestDB(t)
	acct := seedAccount(t, pool)
	want := seedOneAppPerPlan(t, pool, acct)

	// Past every app's creation grace, so the whole roster qualifies.
	apps, err := cycle.NewStore(pool).LiveAppsCreatedBefore(context.Background(), acct, time.Now().Add(96*time.Hour), usage.GraceDays)
	require.NoError(t, err)
	got := map[uuid.UUID]usage.Plan{}
	for _, a := range apps {
		got[a.AppID] = a.Plan
	}
	require.Equal(t, want, got, "the advance leg prices each app from the plan the roster read carries")
}

func TestPendingNewCreationCharges_CarriesEachAppsPlan(t *testing.T) {
	pool := testutil.NewTestDB(t)
	acct := seedAccount(t, pool)
	want := seedOneAppPerPlan(t, pool, acct)

	now := time.Now()
	rows, err := usage.NewStore(pool).PendingNewCreationCharges(context.Background(), acct, now.Add(-time.Hour), now.Add(time.Hour), now.Add(-72*time.Hour))
	require.NoError(t, err)
	got := map[uuid.UUID]usage.Plan{}
	for _, r := range rows {
		got[r.AppID] = r.Plan
	}
	require.Equal(t, want, got, "the pending creation preview prices each app from the plan the read carries")
}
