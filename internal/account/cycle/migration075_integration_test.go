//go:build integration

package cycle_test

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/mirrorstack-ai/billing-engine/internal/account/db"
	"github.com/mirrorstack-ai/billing-engine/internal/shared/testutil"
)

// Migration 075 (the per-app billing plan, core-v2#1412). The claims here are
// DATABASE claims — the column default every existing and new app lands on, the
// CHECK that keeps retired vocabularies out, and the SetAppPlan query's refusal
// to move a deleted row — so they need Postgres, not the unit fakes.

func seedPlanApp(t *testing.T, pool *pgxpool.Pool, acct uuid.UUID, deleted bool) uuid.UUID {
	t.Helper()
	appID := uuid.New()
	_, err := pool.Exec(context.Background(),
		`INSERT INTO ms_billing.apps (app_id, account_id, module_count, created_module_count, created_at, deleted_at)
		 VALUES ($1, $2, 1, 1, now(), CASE WHEN $3 THEN now() END)`,
		appID.String(), acct.String(), deleted)
	require.NoError(t, err)
	return appID
}

func TestMigration075_AppsDefaultToPro(t *testing.T) {
	pool := testutil.NewTestDB(t)
	appID := seedPlanApp(t, pool, seedAccount(t, pool), false)

	row, err := db.New(pool).SelectAppMirror(context.Background(), appID.String())
	require.NoError(t, err)
	require.Equal(t, "pro", row.Plan, "every app lands on pro, whose base fee is the pre-plan flat fee")
}

func TestMigration075_OnlyKnownPlansFit(t *testing.T) {
	pool := testutil.NewTestDB(t)
	appID := seedPlanApp(t, pool, seedAccount(t, pool), false)

	for _, plan := range []string{"enterprise", "hobby", "default", ""} {
		_, err := pool.Exec(context.Background(), `UPDATE ms_billing.apps SET plan = $2 WHERE app_id = $1`, appID.String(), plan)
		var pgErr *pgconn.PgError
		require.True(t, errors.As(err, &pgErr), "plan %q: err = %v, want a check violation", plan, err)
		require.Equal(t, "23514", pgErr.Code, "plan %q must violate apps_plan_known", plan)
	}
	for _, plan := range []string{"free", "pro", "business"} {
		_, err := pool.Exec(context.Background(), `UPDATE ms_billing.apps SET plan = $2 WHERE app_id = $1`, appID.String(), plan)
		require.NoError(t, err, "plan %q is a known plan", plan)
	}
}

func TestSetAppPlanQuery_MovesLiveAppsOnly(t *testing.T) {
	pool := testutil.NewTestDB(t)
	acct := seedAccount(t, pool)
	live, gone := seedPlanApp(t, pool, acct, false), seedPlanApp(t, pool, acct, true)
	q := db.New(pool)
	ctx := context.Background()

	n, err := q.SetAppPlan(ctx, db.SetAppPlanParams{AppID: live.String(), Plan: "business"})
	require.NoError(t, err)
	require.EqualValues(t, 1, n)
	row, err := q.SelectAppMirror(ctx, live.String())
	require.NoError(t, err)
	require.Equal(t, "business", row.Plan)

	n, err = q.SetAppPlan(ctx, db.SetAppPlanParams{AppID: gone.String(), Plan: "business"})
	require.NoError(t, err)
	require.EqualValues(t, 0, n, "a deleted app's plan is frozen")
	row, err = q.SelectAppMirror(ctx, gone.String())
	require.NoError(t, err)
	require.Equal(t, "pro", row.Plan)
}
