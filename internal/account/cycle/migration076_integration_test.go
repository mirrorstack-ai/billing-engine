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

	"github.com/mirrorstack-ai/billing-engine/internal/account/billing"
	"github.com/mirrorstack-ai/billing-engine/internal/account/cycle"
	"github.com/mirrorstack-ai/billing-engine/internal/account/db"
	"github.com/mirrorstack-ai/billing-engine/internal/account/usage"
	"github.com/mirrorstack-ai/billing-engine/internal/shared/testutil"
)

// Migrations 076 (the plan-change ledger) and 077 (the member history and the
// created plan). The claims here are DATABASE claims — one open change per
// app, the kind/status CHECK, the fold decided from the locked markers, the
// plan flip and wallet draw committed with the row, the cap counting scheduled
// downgrades under the owner lock, the balance-capped lots-only draw, the
// boundary apply, the member high-water read, and the transfer refusals — so
// they need Postgres, not the unit fakes.

var (
	pcReq   = time.Date(2026, 6, 19, 15, 0, 0, 0, time.UTC)
	pcStart = time.Date(2026, 6, 4, 0, 0, 0, 0, time.UTC)
	pcEnd   = time.Date(2026, 7, 4, 0, 0, 0, 0, time.UTC)
)

func seedPlanChangeApp(t *testing.T, pool *pgxpool.Pool, acct uuid.UUID, plan usage.Plan, billed bool) uuid.UUID {
	t.Helper()
	appID := uuid.New()
	_, err := pool.Exec(context.Background(),
		`INSERT INTO ms_billing.apps (app_id, account_id, module_count, created_module_count, created_at, plan, created_plan, member_count, proration_invoice_id)
		 VALUES ($1, $2, 1, 1, now() - interval '10 days', $3, $3, 12, CASE WHEN $4 THEN 'intent:test' END)`,
		appID.String(), acct.String(), string(plan), billed)
	require.NoError(t, err)
	return appID
}

func upgradeParams(appID, acct uuid.UUID, amount int64, wallet *cycle.PlanChangeWalletParams) cycle.OpenPlanChangeParams {
	return cycle.OpenPlanChangeParams{
		AppID: appID, AccountID: acct, FromPlan: usage.PlanFree, ToPlan: usage.PlanPro,
		Kind: cycle.PlanChangeUpgrade, RequestedAt: pcReq,
		Folded:        &cycle.PlanChangeShape{EffectiveAt: pcReq, PeriodStart: pcStart, PeriodEnd: pcEnd},
		Charged:       &cycle.PlanChangeShape{EffectiveAt: pcReq, PeriodStart: pcStart, PeriodEnd: pcEnd, AmountMicros: amount},
		Wallet:        wallet,
		ChargeAllowed: true,
	}
}

func downgradeParams(appID, acct uuid.UUID) cycle.OpenPlanChangeParams {
	return cycle.OpenPlanChangeParams{
		AppID: appID, AccountID: acct, FromPlan: usage.PlanPro, ToPlan: usage.PlanFree,
		Kind: cycle.PlanChangeDowngrade, RequestedAt: pcReq,
		Downgrade: &cycle.PlanChangeShape{EffectiveAt: pcEnd, PeriodStart: pcStart, PeriodEnd: pcEnd},
	}
}

func TestMigration076_OpenPlanChangeDecidesFoldOrChargeUnderTheLock(t *testing.T) {
	pool := testutil.NewTestDB(t)
	store := cycle.NewStore(pool)
	ctx := context.Background()
	acct := seedAccount(t, pool)
	q := db.New(pool)

	// Creation unbilled → the fold shape, settled at 0, plan flipped.
	unbilled := seedPlanChangeApp(t, pool, acct, usage.PlanFree, false)
	change, outcome, err := store.OpenPlanChange(ctx, upgradeParams(unbilled, acct, 10_000_000, nil))
	require.NoError(t, err)
	require.Equal(t, cycle.PlanChangeOpened, outcome)
	require.True(t, change.FoldedIntoCreation)
	require.Equal(t, cycle.PlanChangeSettled, change.Status)
	require.Zero(t, change.AmountMicros)
	row, err := q.SelectAppMirror(ctx, unbilled.String())
	require.NoError(t, err)
	require.Equal(t, "pro", row.Plan, "the flip commits with the row")
	require.Equal(t, "free", row.CreatedPlan, "the created plan never moves")
	require.EqualValues(t, 12, row.MemberCount, "migration 077's count reads back")

	// Creation billed → the charged shape, pending, wallet decided (0: not a
	// credits account).
	billed := seedPlanChangeApp(t, pool, acct, usage.PlanFree, true)
	change, outcome, err = store.OpenPlanChange(ctx, upgradeParams(billed, acct, 10_000_000, &cycle.PlanChangeWalletParams{Credits: false, AllowRemainder: true}))
	require.NoError(t, err)
	require.Equal(t, cycle.PlanChangeOpened, outcome)
	require.False(t, change.FoldedIntoCreation)
	require.Equal(t, cycle.PlanChangePending, change.Status)
	require.EqualValues(t, 10_000_000, change.AmountMicros)
	require.True(t, change.WalletDecided)
	require.Zero(t, change.WalletMicros)

	// A retry finds the open row instead of opening a second one — and the
	// derivation it carries (FromPlan free) is now stale against the flipped
	// row, which is exactly why the open row is checked first.
	again, outcome, err := store.OpenPlanChange(ctx, upgradeParams(billed, acct, 10_000_000, nil))
	require.NoError(t, err)
	require.Equal(t, cycle.PlanChangeExisting, outcome)
	require.Equal(t, change.ID, again.ID)

	// The charged decision with the gates not passed writes nothing.
	refused := seedPlanChangeApp(t, pool, acct, usage.PlanFree, true)
	p := upgradeParams(refused, acct, 10_000_000, nil)
	p.ChargeAllowed = false
	_, outcome, err = store.OpenPlanChange(ctx, p)
	require.NoError(t, err)
	require.Equal(t, cycle.PlanChangeChargeRefused, outcome)
	row, err = q.SelectAppMirror(ctx, refused.String())
	require.NoError(t, err)
	require.Equal(t, "free", row.Plan, "nothing written")
	_, err = q.OpenPlanChangeForApp(ctx, refused.String())
	require.Error(t, err, "no row")

	// The window anchor is stored once — and replaced only once its window
	// has closed.
	first, err := store.EnsurePlanChangeCardWindow(ctx, change.ID, pcReq, pcReq.AddDate(0, 0, -30))
	require.NoError(t, err)
	second, err := store.EnsurePlanChangeCardWindow(ctx, change.ID, pcReq.Add(48*time.Hour), pcReq.Add(48*time.Hour).AddDate(0, 0, -30))
	require.NoError(t, err)
	require.True(t, first.Equal(second), "the first anchor survives while its window is open")
	late := pcReq.AddDate(0, 0, 40)
	third, err := store.EnsurePlanChangeCardWindow(ctx, change.ID, late, late.AddDate(0, 0, -30))
	require.NoError(t, err)
	require.True(t, third.Equal(late), "a closed window is re-anchored")

	// Settle, then a derivation whose FromPlan the locked row no longer
	// carries is stale.
	settled, err := store.SettlePlanChangeCard(ctx, change.ID, 10_000_000, "intent:abc", time.Now())
	require.NoError(t, err)
	require.True(t, settled)
	_, outcome, err = store.OpenPlanChange(ctx, upgradeParams(billed, acct, 10_000_000, nil))
	require.NoError(t, err)
	require.Equal(t, cycle.PlanChangeAppStale, outcome)

	// The partial unique index is the database's own statement of "one open
	// change per app".
	_, err = pool.Exec(ctx, `INSERT INTO ms_billing.app_plan_changes
		(app_id, account_id, from_plan, to_plan, kind, requested_at, effective_at, period_start, period_end, amount_micros, status)
		VALUES ($1, $2, 'pro', 'free', 'downgrade', now(), now(), now(), now(), 0, 'scheduled')`, billed.String(), acct.String())
	require.NoError(t, err)
	_, err = pool.Exec(ctx, `INSERT INTO ms_billing.app_plan_changes
		(app_id, account_id, from_plan, to_plan, kind, requested_at, effective_at, period_start, period_end, amount_micros, status)
		VALUES ($1, $2, 'pro', 'free', 'downgrade', now(), now(), now(), now(), 0, 'scheduled')`, billed.String(), acct.String())
	var pgErr *pgconn.PgError
	require.True(t, errors.As(err, &pgErr))
	require.Equal(t, "23505", pgErr.Code, "app_plan_changes_open_uidx refuses a second open change")
}

func TestMigration076_KindAndStatusAreCheckedTogether(t *testing.T) {
	pool := testutil.NewTestDB(t)
	ctx := context.Background()
	acct := seedAccount(t, pool)
	appID := seedPlanChangeApp(t, pool, acct, usage.PlanPro, true)

	for _, bad := range [][2]string{{"upgrade", "scheduled"}, {"upgrade", "applied"}, {"downgrade", "pending"}, {"downgrade", "settled"}, {"sideways", "settled"}, {"upgrade", "done"}} {
		_, err := pool.Exec(ctx, `INSERT INTO ms_billing.app_plan_changes
			(app_id, account_id, from_plan, to_plan, kind, requested_at, effective_at, period_start, period_end, amount_micros, status)
			VALUES ($1, $2, 'pro', 'free', $3, now(), now(), now(), now(), 0, $4)`, appID.String(), acct.String(), bad[0], bad[1])
		var pgErr *pgconn.PgError
		require.True(t, errors.As(err, &pgErr), "%v: err = %v, want a check violation", bad, err)
		require.Equal(t, "23514", pgErr.Code, "%v must violate a CHECK", bad)
	}
}

func TestMigration076_WalletDrawIsLotsOnlyBalanceCappedAndInsideTheOpen(t *testing.T) {
	pool := testutil.NewTestDB(t)
	store := cycle.NewStore(pool)
	ctx := context.Background()
	acct := seedAccount(t, pool)
	_, err := pool.Exec(ctx, `UPDATE ms_billing.accounts SET billing_mode = 'credits' WHERE id = $1`, acct.String())
	require.NoError(t, err)
	insertWalletEntry(t, pool, acct, uuid.New(), 3_000_000, "grant", "settled", nil, time.Now().Add(-time.Hour))
	appID := seedPlanChangeApp(t, pool, acct, usage.PlanFree, true)
	q := db.New(pool)

	// Short and no card remainder allowed: the WHOLE open rolls back.
	_, outcome, err := store.OpenPlanChange(ctx, upgradeParams(appID, acct, 10_000_000, &cycle.PlanChangeWalletParams{Credits: true, AllowRemainder: false}))
	require.NoError(t, err)
	require.Equal(t, cycle.PlanChangeOpenWalletShort, outcome)
	row, err := q.SelectAppMirror(ctx, appID.String())
	require.NoError(t, err)
	require.Equal(t, "free", row.Plan, "no flip")
	_, err = q.OpenPlanChangeForApp(ctx, appID.String())
	require.Error(t, err, "no row")
	var rows int
	require.NoError(t, pool.QueryRow(ctx, `SELECT count(*) FROM ms_billing.credit_ledger WHERE account_id = $1 AND type = 'subscription_draw'`, acct.String()).Scan(&rows))
	require.Zero(t, rows, "no draw")

	// With the card allowed: the lot is drawn, and ONLY the lot — no
	// unsecured remainder row, unlike a credits-mode boundary draw — in the
	// same transaction as the row and the flip.
	change, outcome, err := store.OpenPlanChange(ctx, upgradeParams(appID, acct, 10_000_000, &cycle.PlanChangeWalletParams{Credits: true, AllowRemainder: true}))
	require.NoError(t, err)
	require.Equal(t, cycle.PlanChangeOpened, outcome)
	require.True(t, change.WalletDecided)
	require.EqualValues(t, 3_000_000, change.WalletMicros)
	require.Equal(t, cycle.PlanChangePending, change.Status, "the card remainder is still owed")
	var unsecured int
	require.NoError(t, pool.QueryRow(ctx, `SELECT count(*), count(*) FILTER (WHERE source_credit_id IS NULL)
		FROM ms_billing.credit_ledger WHERE account_id = $1 AND type = 'subscription_draw'`, acct.String()).Scan(&rows, &unsecured))
	require.Equal(t, 1, rows)
	require.Zero(t, unsecured, "never an unsecured remainder for an upgrade")
	var balance int64
	require.NoError(t, pool.QueryRow(ctx, `SELECT COALESCE(SUM(amount_micros), 0) FROM ms_billing.credit_ledger WHERE account_id = $1 AND status = 'settled'`, acct.String()).Scan(&balance))
	require.Zero(t, balance, "the grant is spent to exactly zero")

	// Decided once: a retry through the legacy decision path writes nothing.
	outcome2, drawn, err := store.DrawPlanChangeFromWallet(ctx, change, true, time.Now())
	require.NoError(t, err)
	require.Equal(t, cycle.PlanChangeWalletAlreadyDecided, outcome2)
	require.Zero(t, drawn)

	// (h1) The posted balance caps the draw: 5e6 in lots but a settled
	// −3e6 adjustment → 2e6 drawn, and the wallet never goes negative.
	acct2 := seedAccount(t, pool)
	_, err = pool.Exec(ctx, `UPDATE ms_billing.accounts SET billing_mode = 'credits' WHERE id = $1`, acct2.String())
	require.NoError(t, err)
	insertWalletEntry(t, pool, acct2, uuid.New(), 5_000_000, "grant", "settled", nil, time.Now().Add(-2*time.Hour))
	insertWalletEntry(t, pool, acct2, uuid.New(), -3_000_000, "adjustment", "settled", nil, time.Now().Add(-time.Hour))
	app2 := seedPlanChangeApp(t, pool, acct2, usage.PlanFree, true)
	change2, outcome, err := store.OpenPlanChange(ctx, upgradeParams(app2, acct2, 10_000_000, &cycle.PlanChangeWalletParams{Credits: true, AllowRemainder: true}))
	require.NoError(t, err)
	require.Equal(t, cycle.PlanChangeOpened, outcome)
	require.EqualValues(t, 2_000_000, change2.WalletMicros, "capped at the posted balance")
	require.NoError(t, pool.QueryRow(ctx, `SELECT COALESCE(SUM(amount_micros), 0) FROM ms_billing.credit_ledger WHERE account_id = $1 AND status = 'settled'`, acct2.String()).Scan(&balance))
	require.Zero(t, balance, "never negative")

	// A covering wallet settles in the open itself; a standard account
	// decides 0 and touches no ledger row.
	acct3 := seedAccount(t, pool)
	_, err = pool.Exec(ctx, `UPDATE ms_billing.accounts SET billing_mode = 'credits' WHERE id = $1`, acct3.String())
	require.NoError(t, err)
	insertWalletEntry(t, pool, acct3, uuid.New(), 50_000_000, "grant", "settled", nil, time.Now().Add(-time.Hour))
	app3 := seedPlanChangeApp(t, pool, acct3, usage.PlanFree, true)
	change3, _, err := store.OpenPlanChange(ctx, upgradeParams(app3, acct3, 10_000_000, &cycle.PlanChangeWalletParams{Credits: true, AllowRemainder: false}))
	require.NoError(t, err)
	require.Equal(t, cycle.PlanChangeSettled, change3.Status)
	require.EqualValues(t, 10_000_000, change3.WalletMicros)
	acct4 := seedAccount(t, pool)
	insertWalletEntry(t, pool, acct4, uuid.New(), 50_000_000, "grant", "settled", nil, time.Now().Add(-time.Hour))
	app4 := seedPlanChangeApp(t, pool, acct4, usage.PlanFree, true)
	change4, _, err := store.OpenPlanChange(ctx, upgradeParams(app4, acct4, 10_000_000, &cycle.PlanChangeWalletParams{Credits: false, AllowRemainder: true}))
	require.NoError(t, err)
	require.True(t, change4.WalletDecided)
	require.Zero(t, change4.WalletMicros)
	require.NoError(t, pool.QueryRow(ctx, `SELECT count(*) FROM ms_billing.credit_ledger WHERE account_id = $1 AND type = 'subscription_draw'`, acct4.String()).Scan(&rows))
	require.Zero(t, rows)
}

func TestMigration076_DowngradeAppliesAtTheBoundaryCancelsBeforeAndRespectsTheCap(t *testing.T) {
	pool := testutil.NewTestDB(t)
	store := cycle.NewStore(pool)
	ctx := context.Background()
	acct := seedAccount(t, pool)
	q := db.New(pool)
	appID := seedPlanChangeApp(t, pool, acct, usage.PlanPro, true)

	change, outcome, err := store.OpenPlanChange(ctx, downgradeParams(appID, acct))
	require.NoError(t, err)
	require.Equal(t, cycle.PlanChangeOpened, outcome)
	row, err := q.SelectAppMirror(ctx, appID.String())
	require.NoError(t, err)
	require.Equal(t, "pro", row.Plan, "a downgrade does not flip the plan when opened")
	open, has, err := store.OpenPlanChangeForApp(ctx, appID)
	require.NoError(t, err)
	require.True(t, has)
	require.Equal(t, change.ID, open.ID)

	// Not due yet: nothing applies. Cancel, then nothing to apply either.
	n, err := store.ApplyDuePlanChanges(ctx, acct, pcEnd.Add(-time.Second))
	require.NoError(t, err)
	require.Zero(t, n)
	cancelled, err := store.CancelScheduledPlanChange(ctx, appID, time.Now())
	require.NoError(t, err)
	require.True(t, cancelled)
	n, err = store.ApplyDuePlanChanges(ctx, acct, pcEnd)
	require.NoError(t, err)
	require.Zero(t, n)

	// Schedule again; the GLOBAL apply moves it once.
	_, _, err = store.OpenPlanChange(ctx, downgradeParams(appID, acct))
	require.NoError(t, err)
	applied, cancelledN, err := store.ApplyAllDuePlanChanges(ctx, pcEnd)
	require.NoError(t, err)
	require.Equal(t, 1, applied)
	require.Zero(t, cancelledN)
	row, err = q.SelectAppMirror(ctx, appID.String())
	require.NoError(t, err)
	require.Equal(t, "free", row.Plan)
	applied, _, err = store.ApplyAllDuePlanChanges(ctx, pcEnd)
	require.NoError(t, err)
	require.Zero(t, applied, "applied once")

	// (c) The Free cap counts apps already Free AND downgrades scheduled to
	// Free: with 2 Free + 1 scheduled, a further schedule is refused; and a
	// slot taken meanwhile cancels the due row at apply instead of breaching.
	acct2 := seedAccount(t, pool)
	seedPlanChangeApp(t, pool, acct2, usage.PlanFree, true)
	seedPlanChangeApp(t, pool, acct2, usage.PlanFree, true)
	pro1 := seedPlanChangeApp(t, pool, acct2, usage.PlanPro, true)
	pro2 := seedPlanChangeApp(t, pool, acct2, usage.PlanPro, true)
	_, outcome, err = store.OpenPlanChange(ctx, downgradeParams(pro1, acct2))
	require.NoError(t, err)
	require.Equal(t, cycle.PlanChangeOpened, outcome)
	_, outcome, err = store.OpenPlanChange(ctx, downgradeParams(pro2, acct2))
	require.NoError(t, err)
	require.Equal(t, cycle.PlanChangeCapReached, outcome, "2 Free + 1 scheduled = the cap of 3")
	full, err := store.InsertFreeAppMirror(ctx, uuid.New(), acct2, uuid.Nil, 0, 0, time.Now(), "")
	require.NoError(t, err)
	require.True(t, full, "RegisterApp on Free counts the same commitments")
	_, err = pool.Exec(ctx, `UPDATE ms_billing.apps SET plan = 'free' WHERE app_id = $1`, pro2.String()) // a slot taken meanwhile
	require.NoError(t, err)
	applied, cancelledN, err = store.ApplyAllDuePlanChanges(ctx, pcEnd)
	require.NoError(t, err)
	require.Zero(t, applied)
	require.Equal(t, 1, cancelledN, "the due downgrade is cancelled, not applied over the cap")
	row, err = q.SelectAppMirror(ctx, pro1.String())
	require.NoError(t, err)
	require.Equal(t, "pro", row.Plan)
}

func TestMigration077_MemberHistoryAndHighWater(t *testing.T) {
	pool := testutil.NewTestDB(t)
	store := cycle.NewStore(pool)
	ctx := context.Background()
	acct := seedAccount(t, pool)
	appID := uuid.New()
	created := pcStart.AddDate(0, 0, 6)
	require.NoError(t, store.InsertAppMirror(ctx, appID, acct, uuid.Nil, 0, 12, created, "", usage.PlanPro))
	require.NoError(t, store.SetAppMemberCount(ctx, appID, 15, pcStart.AddDate(0, 0, 16)))
	require.NoError(t, store.SetAppMemberCount(ctx, appID, 11, pcStart.AddDate(0, 0, 21)))
	require.NoError(t, store.SetAppMemberCount(ctx, appID, 20, pcEnd.AddDate(0, 0, 1))) // next period

	hwm, err := store.MemberHighWater(ctx, acct, pcStart, pcEnd)
	require.NoError(t, err)
	require.Equal(t, 15, hwm[appID], "the mark inside the period, not the count at the boundary (11) nor after it (20)")
	hwm, err = store.MemberHighWater(ctx, acct, pcEnd, pcEnd.AddDate(0, 1, 0))
	require.NoError(t, err)
	require.Equal(t, 20, hwm[appID], "the next period: the count in force at its start (11) vs the 20 recorded inside")

	var rows int
	require.NoError(t, pool.QueryRow(ctx, `SELECT count(*) FROM ms_billing.app_member_counts WHERE app_id = $1`, appID.String()).Scan(&rows))
	require.Equal(t, 4, rows, "the initial row plus three changes")

	// A deleted app's count is frozen and gets no history row.
	_, err = pool.Exec(ctx, `UPDATE ms_billing.apps SET deleted_at = now() WHERE app_id = $1`, appID.String())
	require.NoError(t, err)
	require.NoError(t, store.SetAppMemberCount(ctx, appID, 99, time.Now()))
	require.NoError(t, pool.QueryRow(ctx, `SELECT count(*) FROM ms_billing.app_member_counts WHERE app_id = $1`, appID.String()).Scan(&rows))
	require.Equal(t, 4, rows)
	_, err = pool.Exec(ctx, `UPDATE ms_billing.apps SET member_count = -1 WHERE app_id = $1`, appID.String())
	var pgErr *pgconn.PgError
	require.True(t, errors.As(err, &pgErr))
	require.Equal(t, "23514", pgErr.Code, "member_count is non-negative")
}

// (e) TransferApp honours the ledger: a pending upgrade refuses, a scheduled
// downgrade is cancelled atomically and reported, and a Free app needs a slot
// under the destination owner's cap.
func TestTransferApp_HonoursThePlanLedgerAndTheFreeCap(t *testing.T) {
	ctx := context.Background()

	t.Run("pending upgrade refuses", func(t *testing.T) {
		f := seedTransferFixture(t)
		f.giveOldAccountACard(t)
		_, err := f.pool.Exec(ctx, `UPDATE ms_billing.apps SET plan = 'pro', created_plan = 'free', proration_invoice_id = 'intent:test' WHERE app_id = $1`, f.appID.String())
		require.NoError(t, err)
		_, err = f.pool.Exec(ctx, `INSERT INTO ms_billing.app_plan_changes
			(app_id, account_id, from_plan, to_plan, kind, requested_at, effective_at, period_start, period_end, amount_micros, wallet_decided_at, status)
			VALUES ($1, $2, 'free', 'pro', 'upgrade', now(), now(), now(), now(), 10000000, now(), 'pending')`, f.appID.String(), f.oldAcct.String())
		require.NoError(t, err)
		_, err = transferSvc(t, f).TransferApp(ctx, cycle.TransferAppRequest{AppID: f.appID, OwnerUserID: f.newOwner, Mode: cycle.TransferModeKeep, RequestID: uuid.New()})
		require.Error(t, err)
		var be *billing.Error
		require.True(t, errors.As(err, &be))
		require.Equal(t, billing.CodeConflict, be.Code)
		require.Contains(t, be.Message, "app_transfer_plan_change_pending")
		require.Equal(t, f.oldAcct, f.rosterAccount(t), "not moved")
	})

	t.Run("scheduled downgrade is cancelled and reported", func(t *testing.T) {
		f := seedTransferFixture(t)
		f.giveOldAccountACard(t)
		_, err := f.pool.Exec(ctx, `UPDATE ms_billing.apps SET proration_invoice_id = 'intent:test' WHERE app_id = $1`, f.appID.String())
		require.NoError(t, err)
		var changeID string
		require.NoError(t, f.pool.QueryRow(ctx, `INSERT INTO ms_billing.app_plan_changes
			(app_id, account_id, from_plan, to_plan, kind, requested_at, effective_at, period_start, period_end, amount_micros, status)
			VALUES ($1, $2, 'pro', 'free', 'downgrade', now(), now() + interval '10 days', now(), now() + interval '10 days', 0, 'scheduled') RETURNING id`,
			f.appID.String(), f.oldAcct.String()).Scan(&changeID))
		resp, err := transferSvc(t, f).TransferApp(ctx, cycle.TransferAppRequest{AppID: f.appID, OwnerUserID: f.newOwner, Mode: cycle.TransferModeKeep, RequestID: uuid.New()})
		require.NoError(t, err)
		require.Equal(t, changeID, resp.CancelledPlanChangeID.String(), "reported")
		var status string
		require.NoError(t, f.pool.QueryRow(ctx, `SELECT status FROM ms_billing.app_plan_changes WHERE id = $1`, changeID).Scan(&status))
		require.Equal(t, "cancelled", status)
		require.Equal(t, f.newAcct, f.rosterAccount(t), "moved")
	})

	t.Run("free app needs a destination slot", func(t *testing.T) {
		f := seedTransferFixture(t)
		f.giveOldAccountACard(t)
		_, err := f.pool.Exec(ctx, `UPDATE ms_billing.apps SET plan = 'free', created_plan = 'free', proration_invoice_id = 'intent:test' WHERE app_id = $1`, f.appID.String())
		require.NoError(t, err)
		for i := 0; i < 3; i++ {
			seedPlanChangeApp(t, f.pool, f.newAcct, usage.PlanFree, true)
		}
		_, err = transferSvc(t, f).TransferApp(ctx, cycle.TransferAppRequest{AppID: f.appID, OwnerUserID: f.newOwner, Mode: cycle.TransferModeKeep, RequestID: uuid.New()})
		require.Error(t, err)
		var be *billing.Error
		require.True(t, errors.As(err, &be))
		require.Equal(t, billing.CodePlanLimit, be.Code)
		require.Equal(t, f.oldAcct, f.rosterAccount(t), "not moved")
	})
}
