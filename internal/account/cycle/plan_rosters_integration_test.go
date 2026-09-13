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
	"github.com/mirrorstack-ai/billing-engine/internal/account/db"
	"github.com/mirrorstack-ai/billing-engine/internal/account/usage"
	"github.com/mirrorstack-ai/billing-engine/internal/shared/testutil"
)

// The reads the charge legs and the bill forecast price from carry each app's
// plan (billing-engine#202): the SQL selects apps.plan and the stores map it.

func seedOneAppPerPlan(t *testing.T, pool *pgxpool.Pool, acct uuid.UUID) map[uuid.UUID]usage.Plan {
	t.Helper()
	want := map[uuid.UUID]usage.Plan{}
	for _, pb := range planBases {
		appID := seedPlanApp(t, pool, acct, false)
		n, err := db.New(pool).SetAppPlan(context.Background(), db.SetAppPlanParams{AppID: appID.String(), Plan: string(pb.plan)})
		require.NoError(t, err)
		require.EqualValues(t, 1, n)
		want[appID] = pb.plan
	}
	return want
}

func plansByApp[T any](rows []T, of func(T) (uuid.UUID, usage.Plan)) map[uuid.UUID]usage.Plan {
	got := map[uuid.UUID]usage.Plan{}
	for _, r := range rows {
		id, plan := of(r)
		got[id] = plan
	}
	return got
}

func TestLiveAppsCreatedBefore_CarriesEachAppsPlan(t *testing.T) {
	pool := testutil.NewTestDB(t)
	acct := seedAccount(t, pool)
	want := seedOneAppPerPlan(t, pool, acct)

	// Past every app's creation grace, so the whole roster qualifies.
	apps, err := cycle.NewStore(pool).LiveAppsCreatedBefore(context.Background(), acct, time.Now().Add(96*time.Hour), usage.GraceDays)
	require.NoError(t, err)
	require.Equal(t, want, plansByApp(apps, func(a cycle.AppModuleCount) (uuid.UUID, usage.Plan) { return a.AppID, a.Plan }),
		"the advance leg prices each app from the plan the roster read carries")
}

func TestPendingNewCreationCharges_CarriesEachAppsPlan(t *testing.T) {
	pool := testutil.NewTestDB(t)
	acct := seedAccount(t, pool)
	want := seedOneAppPerPlan(t, pool, acct)

	now := time.Now()
	rows, err := usage.NewStore(pool).PendingNewCreationCharges(context.Background(), acct, now.Add(-time.Hour), now.Add(time.Hour), now.Add(-72*time.Hour))
	require.NoError(t, err)
	require.Equal(t, want, plansByApp(rows, func(r usage.PendingNewCreationChargeRaw) (uuid.UUID, usage.Plan) { return r.AppID, r.Plan }),
		"the pending creation preview prices each app from the plan the read carries")
}

func TestUnresolvedOneTimeCharges_CarriesEachAppsPlan(t *testing.T) {
	pool := testutil.NewTestDB(t)
	acct := seedAccount(t, pool)
	_, err := pool.Exec(context.Background(), `UPDATE ms_billing.accounts SET activated_at = now() WHERE id = $1`, acct.String())
	require.NoError(t, err)
	want := seedOneAppPerPlan(t, pool, acct)

	rows, err := usage.NewStore(pool).UnresolvedOneTimeCharges(context.Background(), acct, usage.IncludedModules, usage.GraceDays*24)
	require.NoError(t, err)
	require.Equal(t, want, plansByApp(rows, func(r usage.UnresolvedOneTimeChargeRaw) (uuid.UUID, usage.Plan) { return r.AppID, r.Plan }),
		"every uncharged creation carries its app's plan into the forecast")
}
