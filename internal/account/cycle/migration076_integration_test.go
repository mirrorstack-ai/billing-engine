//go:build integration

package cycle_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/mirrorstack-ai/billing-engine/internal/account/cycle"
	"github.com/mirrorstack-ai/billing-engine/internal/account/db"
	"github.com/mirrorstack-ai/billing-engine/internal/account/usage"
	"github.com/mirrorstack-ai/billing-engine/internal/shared/testutil"
)

// Migrations 076 (the plan-change ledger) and 077 (the member count). The
// claims here are DATABASE claims — one open change per app, the kind/status
// CHECK, the plan flip committed with the row under the app lock, the wallet
// decision taken once and drawn from lots only, the boundary apply — so they
// need Postgres, not the unit fakes.

func seedPlanChangeApp(t *testing.T, pool *pgxpool.Pool, acct uuid.UUID, plan usage.Plan) uuid.UUID {
	t.Helper()
	appID := uuid.New()
	_, err := pool.Exec(context.Background(),
		`INSERT INTO ms_billing.apps (app_id, account_id, module_count, created_module_count, created_at, plan, member_count)
		 VALUES ($1, $2, 1, 1, now() - interval '10 days', $3, 12)`,
		appID.String(), acct.String(), string(plan))
	require.NoError(t, err)
	return appID
}

func upgradeParams(appID, acct uuid.UUID, amount int64, status cycle.PlanChangeStatus) cycle.OpenPlanChangeParams {
	now := time.Date(2026, 6, 19, 15, 0, 0, 0, time.UTC)
	return cycle.OpenPlanChangeParams{
		AppID: appID, AccountID: acct, FromPlan: usage.PlanFree, ToPlan: usage.PlanPro,
		Kind: cycle.PlanChangeUpgrade, RequestedAt: now, EffectiveAt: now,
		PeriodStart: time.Date(2026, 6, 4, 0, 0, 0, 0, time.UTC), PeriodEnd: time.Date(2026, 7, 4, 0, 0, 0, 0, time.UTC),
		AmountMicros: amount, Status: status,
	}
}

func TestMigration076_OpenPlanChangeFlipsThePlanWithTheRowOnce(t *testing.T) {
	pool := testutil.NewTestDB(t)
	store := cycle.NewStore(pool)
	ctx := context.Background()
	acct := seedAccount(t, pool)
	appID := seedPlanChangeApp(t, pool, acct, usage.PlanFree)

	change, outcome, err := store.OpenPlanChange(ctx, upgradeParams(appID, acct, 10_000_000, cycle.PlanChangePending))
	require.NoError(t, err)
	require.Equal(t, cycle.PlanChangeOpened, outcome)
	require.Equal(t, cycle.PlanChangePending, change.Status)
	require.False(t, change.WalletDecided)
	row, err := db.New(pool).SelectAppMirror(ctx, appID.String())
	require.NoError(t, err)
	require.Equal(t, "pro", row.Plan, "the flip commits with the row")
	require.EqualValues(t, 12, row.MemberCount, "migration 077's count reads back")

	// A retry finds the open row instead of opening a second one — and the
	// derivation it carries (FromPlan free) is now stale against the flipped
	// row, which is exactly why the open row is checked first.
	again, outcome, err := store.OpenPlanChange(ctx, upgradeParams(appID, acct, 10_000_000, cycle.PlanChangePending))
	require.NoError(t, err)
	require.Equal(t, cycle.PlanChangeExisting, outcome)
	require.Equal(t, change.ID, again.ID)

	// A derivation whose FromPlan the locked row no longer carries is stale.
	settled, err := store.SettlePlanChangeCard(ctx, change.ID, 10_000_000, "intent:abc", time.Now())
	require.NoError(t, err)
	require.True(t, settled)
	_, outcome, err = store.OpenPlanChange(ctx, upgradeParams(appID, acct, 10_000_000, cycle.PlanChangePending))
	require.NoError(t, err)
	require.Equal(t, cycle.PlanChangeAppStale, outcome)

	// The partial unique index is the database's own statement of "one open
	// change per app": a second pending row cannot be inserted around the
	// store either.
	_, err = pool.Exec(ctx, `INSERT INTO ms_billing.app_plan_changes
		(app_id, account_id, from_plan, to_plan, kind, requested_at, effective_at, period_start, period_end, amount_micros, status)
		VALUES ($1, $2, 'pro', 'free', 'downgrade', now(), now(), now(), now(), 0, 'scheduled')`, appID.String(), acct.String())
	require.NoError(t, err)
	_, err = pool.Exec(ctx, `INSERT INTO ms_billing.app_plan_changes
		(app_id, account_id, from_plan, to_plan, kind, requested_at, effective_at, period_start, period_end, amount_micros, status)
		VALUES ($1, $2, 'pro', 'free', 'downgrade', now(), now(), now(), now(), 0, 'scheduled')`, appID.String(), acct.String())
	var pgErr *pgconn.PgError
	require.True(t, errors.As(err, &pgErr))
	require.Equal(t, "23505", pgErr.Code, "app_plan_changes_open_uidx refuses a second open change")
}

func TestMigration076_KindAndStatusAreCheckedTogether(t *testing.T) {
	pool := testutil.NewTestDB(t)
	ctx := context.Background()
	acct := seedAccount(t, pool)
	appID := seedPlanChangeApp(t, pool, acct, usage.PlanPro)

	for _, bad := range [][2]string{{"upgrade", "scheduled"}, {"upgrade", "applied"}, {"downgrade", "pending"}, {"downgrade", "settled"}, {"sideways", "settled"}, {"upgrade", "done"}} {
		_, err := pool.Exec(ctx, `INSERT INTO ms_billing.app_plan_changes
			(app_id, account_id, from_plan, to_plan, kind, requested_at, effective_at, period_start, period_end, amount_micros, status)
			VALUES ($1, $2, 'pro', 'free', $3, now(), now(), now(), now(), 0, $4)`, appID.String(), acct.String(), bad[0], bad[1])
		var pgErr *pgconn.PgError
		require.True(t, errors.As(err, &pgErr), "%v: err = %v, want a check violation", bad, err)
		require.Equal(t, "23514", pgErr.Code, "%v must violate a CHECK", bad)
	}
}

func TestMigration076_WalletDecisionIsTakenOnceFromLotsOnly(t *testing.T) {
	pool := testutil.NewTestDB(t)
	store := cycle.NewStore(pool)
	ctx := context.Background()
	acct := seedAccount(t, pool)
	_, err := pool.Exec(ctx, `UPDATE ms_billing.accounts SET billing_mode = 'credits' WHERE id = $1`, acct.String())
	require.NoError(t, err)
	grant := uuid.New()
	insertWalletEntry(t, pool, acct, grant, 3_000_000, "grant", "settled", nil, time.Now().Add(-time.Hour))
	appID := seedPlanChangeApp(t, pool, acct, usage.PlanFree)
	change, _, err := store.OpenPlanChange(ctx, upgradeParams(appID, acct, 10_000_000, cycle.PlanChangePending))
	require.NoError(t, err)

	// Short and no card remainder allowed: nothing written.
	outcome, drawn, err := store.DrawPlanChangeFromWallet(ctx, change, false, time.Now())
	require.NoError(t, err)
	require.Equal(t, cycle.PlanChangeWalletShort, outcome)
	require.Zero(t, drawn)
	latest, _, err := store.PlanChange(ctx, change.ID)
	require.NoError(t, err)
	require.False(t, latest.WalletDecided)

	// With the card allowed: the lot is drawn, and ONLY the lot — no
	// unsecured remainder row, unlike a credits-mode boundary draw.
	outcome, drawn, err = store.DrawPlanChangeFromWallet(ctx, change, true, time.Now())
	require.NoError(t, err)
	require.Equal(t, cycle.PlanChangeWalletDecided, outcome)
	require.EqualValues(t, 3_000_000, drawn)
	latest, _, err = store.PlanChange(ctx, change.ID)
	require.NoError(t, err)
	require.True(t, latest.WalletDecided)
	require.EqualValues(t, 3_000_000, latest.WalletMicros)
	require.Equal(t, cycle.PlanChangePending, latest.Status, "the card remainder is still owed")
	var rows int
	var unsecured int
	require.NoError(t, pool.QueryRow(ctx, `SELECT count(*), count(*) FILTER (WHERE source_credit_id IS NULL)
		FROM ms_billing.credit_ledger WHERE account_id = $1 AND type = 'subscription_draw'`, acct.String()).Scan(&rows, &unsecured))
	require.Equal(t, 1, rows)
	require.Zero(t, unsecured, "never an unsecured remainder for an upgrade")
	var balance int64
	require.NoError(t, pool.QueryRow(ctx, `SELECT COALESCE(SUM(amount_micros), 0) FROM ms_billing.credit_ledger WHERE account_id = $1 AND status = 'settled'`, acct.String()).Scan(&balance))
	require.Zero(t, balance, "the grant is spent to exactly zero")

	// Decided once: a retry writes nothing more.
	outcome, drawn, err = store.DrawPlanChangeFromWallet(ctx, change, true, time.Now())
	require.NoError(t, err)
	require.Equal(t, cycle.PlanChangeWalletAlreadyDecided, outcome)
	require.Zero(t, drawn)
	require.NoError(t, pool.QueryRow(ctx, `SELECT count(*) FROM ms_billing.credit_ledger WHERE account_id = $1 AND type = 'subscription_draw'`, acct.String()).Scan(&rows))
	require.Equal(t, 1, rows)

	// A covering wallet settles in the decision itself.
	acct2 := seedAccount(t, pool)
	_, err = pool.Exec(ctx, `UPDATE ms_billing.accounts SET billing_mode = 'credits' WHERE id = $1`, acct2.String())
	require.NoError(t, err)
	insertWalletEntry(t, pool, acct2, uuid.New(), 50_000_000, "grant", "settled", nil, time.Now().Add(-time.Hour))
	app2 := seedPlanChangeApp(t, pool, acct2, usage.PlanFree)
	change2, _, err := store.OpenPlanChange(ctx, upgradeParams(app2, acct2, 10_000_000, cycle.PlanChangePending))
	require.NoError(t, err)
	outcome, drawn, err = store.DrawPlanChangeFromWallet(ctx, change2, false, time.Now())
	require.NoError(t, err)
	require.Equal(t, cycle.PlanChangeWalletDecided, outcome)
	require.EqualValues(t, 10_000_000, drawn)
	latest, _, err = store.PlanChange(ctx, change2.ID)
	require.NoError(t, err)
	require.Equal(t, cycle.PlanChangeSettled, latest.Status)

	// A standard account decides 0 and touches no ledger row.
	acct3 := seedAccount(t, pool)
	insertWalletEntry(t, pool, acct3, uuid.New(), 50_000_000, "grant", "settled", nil, time.Now().Add(-time.Hour))
	app3 := seedPlanChangeApp(t, pool, acct3, usage.PlanFree)
	change3, _, err := store.OpenPlanChange(ctx, upgradeParams(app3, acct3, 10_000_000, cycle.PlanChangePending))
	require.NoError(t, err)
	outcome, drawn, err = store.DrawPlanChangeFromWallet(ctx, change3, true, time.Now())
	require.NoError(t, err)
	require.Equal(t, cycle.PlanChangeWalletDecided, outcome)
	require.Zero(t, drawn)
	require.NoError(t, pool.QueryRow(ctx, `SELECT count(*) FROM ms_billing.credit_ledger WHERE account_id = $1 AND type = 'subscription_draw'`, acct3.String()).Scan(&rows))
	require.Zero(t, rows)
}

func TestMigration076_DowngradeAppliesAtTheBoundaryAndCancelsBefore(t *testing.T) {
	pool := testutil.NewTestDB(t)
	store := cycle.NewStore(pool)
	ctx := context.Background()
	acct := seedAccount(t, pool)
	appID := seedPlanChangeApp(t, pool, acct, usage.PlanPro)
	boundary := time.Date(2026, 7, 4, 0, 0, 0, 0, time.UTC)
	params := cycle.OpenPlanChangeParams{
		AppID: appID, AccountID: acct, FromPlan: usage.PlanPro, ToPlan: usage.PlanFree,
		Kind: cycle.PlanChangeDowngrade, RequestedAt: boundary.AddDate(0, 0, -15), EffectiveAt: boundary,
		PeriodStart: boundary.AddDate(0, -1, 0), PeriodEnd: boundary, Status: cycle.PlanChangeScheduled,
	}
	change, outcome, err := store.OpenPlanChange(ctx, params)
	require.NoError(t, err)
	require.Equal(t, cycle.PlanChangeOpened, outcome)
	row, err := db.New(pool).SelectAppMirror(ctx, appID.String())
	require.NoError(t, err)
	require.Equal(t, "pro", row.Plan, "a downgrade does not flip the plan when opened")
	open, has, err := store.OpenPlanChangeForApp(ctx, appID)
	require.NoError(t, err)
	require.True(t, has)
	require.Equal(t, change.ID, open.ID)

	// Not due yet: nothing applies.
	n, err := store.ApplyDuePlanChanges(ctx, acct, boundary.Add(-time.Second))
	require.NoError(t, err)
	require.Zero(t, n)

	// Cancel, then nothing to apply at the boundary either.
	cancelled, err := store.CancelScheduledPlanChange(ctx, appID, time.Now())
	require.NoError(t, err)
	require.True(t, cancelled)
	cancelled, err = store.CancelScheduledPlanChange(ctx, appID, time.Now())
	require.NoError(t, err)
	require.False(t, cancelled, "nothing left to cancel")
	n, err = store.ApplyDuePlanChanges(ctx, acct, boundary)
	require.NoError(t, err)
	require.Zero(t, n)

	// Schedule again; the boundary applies it once.
	_, _, err = store.OpenPlanChange(ctx, params)
	require.NoError(t, err)
	n, err = store.ApplyDuePlanChanges(ctx, acct, boundary)
	require.NoError(t, err)
	require.Equal(t, 1, n)
	row, err = db.New(pool).SelectAppMirror(ctx, appID.String())
	require.NoError(t, err)
	require.Equal(t, "free", row.Plan)
	n, err = store.ApplyDuePlanChanges(ctx, acct, boundary)
	require.NoError(t, err)
	require.Zero(t, n, "applied once")
	_, has, err = store.OpenPlanChangeForApp(ctx, appID)
	require.NoError(t, err)
	require.False(t, has)
}

func TestMigration077_CountLiveAppsOnPlanScopesByOwnerKind(t *testing.T) {
	pool := testutil.NewTestDB(t)
	store := cycle.NewStore(pool)
	ctx := context.Background()
	acct := seedAccount(t, pool)
	org := uuid.New()
	user1 := seedPlanChangeApp(t, pool, acct, usage.PlanFree)
	_ = seedPlanChangeApp(t, pool, acct, usage.PlanFree)
	_ = seedPlanChangeApp(t, pool, acct, usage.PlanPro)
	orgApp := uuid.New()
	_, err := pool.Exec(ctx, `INSERT INTO ms_billing.apps (app_id, account_id, owner_org_id, module_count, created_module_count, created_at, plan)
		VALUES ($1, $2, $3, 0, 0, now(), 'free')`, orgApp.String(), acct.String(), org.String())
	require.NoError(t, err)
	deleted := uuid.New()
	_, err = pool.Exec(ctx, `INSERT INTO ms_billing.apps (app_id, account_id, module_count, created_module_count, created_at, deleted_at, plan)
		VALUES ($1, $2, 0, 0, now(), now(), 'free')`, deleted.String(), acct.String())
	require.NoError(t, err)

	n, err := store.CountLiveAppsOnPlan(ctx, acct, uuid.Nil, usage.PlanFree, uuid.Nil)
	require.NoError(t, err)
	require.Equal(t, 2, n, "the personal count is the account's user-owned live Free apps")
	n, err = store.CountLiveAppsOnPlan(ctx, acct, uuid.Nil, usage.PlanFree, user1)
	require.NoError(t, err)
	require.Equal(t, 1, n, "the app being asked about is excluded")
	n, err = store.CountLiveAppsOnPlan(ctx, acct, org, usage.PlanFree, uuid.Nil)
	require.NoError(t, err)
	require.Equal(t, 1, n, "the org count is keyed by owner_org_id")

	require.NoError(t, store.SetAppMemberCount(ctx, user1, 7))
	row, err := db.New(pool).SelectAppMirror(ctx, user1.String())
	require.NoError(t, err)
	require.EqualValues(t, 7, row.MemberCount)
	require.NoError(t, store.SetAppMemberCount(ctx, deleted, 7))
	row, err = db.New(pool).SelectAppMirror(ctx, deleted.String())
	require.NoError(t, err)
	require.Zero(t, row.MemberCount, "a deleted app's count is frozen")
	_, err = pool.Exec(ctx, `UPDATE ms_billing.apps SET member_count = -1 WHERE app_id = $1`, user1.String())
	var pgErr *pgconn.PgError
	require.True(t, errors.As(err, &pgErr))
	require.Equal(t, "23514", pgErr.Code, "member_count is non-negative")
}
