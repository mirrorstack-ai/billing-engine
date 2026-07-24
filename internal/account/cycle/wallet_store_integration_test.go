//go:build integration

package cycle_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/mirrorstack-ai/billing-engine/internal/account/cycle"
	"github.com/mirrorstack-ai/billing-engine/internal/shared/testutil"
)

type persistedWalletDraw struct {
	sourceID     *uuid.UUID
	amount       int64
	balanceAfter int64
	key          string
	status       string
	actor        string
}

func insertWalletEntry(t *testing.T, pool *pgxpool.Pool, accountID, id uuid.UUID, amount int64, typ, status string, expiresAt *time.Time, createdAt time.Time) {
	t.Helper()
	_, err := pool.Exec(context.Background(), `
		INSERT INTO ms_billing.credit_ledger (
			id, account_id, amount_micros, type, status,
			balance_after_micros, actor, idempotency_key,
			expires_at, created_at
		) VALUES ($1, $2, $3, $4, $5, $6, 'system', $7, $8, $9)`,
		id.String(), accountID.String(), amount, typ, status, amount,
		"wallet-test:"+id.String(), expiresAt, createdAt,
	)
	require.NoError(t, err)
}

func persistedWalletDraws(t *testing.T, pool *pgxpool.Pool, accountID, periodID uuid.UUID) []persistedWalletDraw {
	t.Helper()
	rows, err := pool.Query(context.Background(), `
		SELECT source_credit_id::text, -amount_micros, balance_after_micros,
		       idempotency_key, status, actor
		FROM ms_billing.credit_ledger
		WHERE account_id = $1
		  AND period_id = $2
		  AND type = 'usage_draw'
		ORDER BY balance_after_micros DESC`, accountID.String(), periodID.String())
	require.NoError(t, err)
	defer rows.Close()

	var out []persistedWalletDraw
	for rows.Next() {
		var source *string
		var row persistedWalletDraw
		require.NoError(t, rows.Scan(
			&source, &row.amount, &row.balanceAfter, &row.key, &row.status, &row.actor,
		))
		if source != nil {
			parsed, err := uuid.Parse(*source)
			require.NoError(t, err)
			row.sourceID = &parsed
		}
		out = append(out, row)
	}
	require.NoError(t, rows.Err())
	return out
}

func TestDrawWalletCredits_Integration_OrderBalancesAndPeriodIdempotency(t *testing.T) {
	pool := testutil.NewTestDB(t)
	store := cycle.NewStore(pool)
	ctx := context.Background()
	accountID := seedAccount(t, pool)
	start, end := mustTime(t, pStart), mustTime(t, pEnd)
	periodID, err := store.OpenPeriodForAccount(ctx, accountID, start, end)
	require.NoError(t, err)

	created := mustTime(t, "2026-01-01T00:00:00Z")
	soonExpiry := mustTime(t, "2099-01-01T00:00:00Z")
	laterExpiry := mustTime(t, "2099-02-01T00:00:00Z")
	expired := mustTime(t, "2020-01-01T00:00:00Z")
	soonGrant, laterGrant := uuid.New(), uuid.New()
	nonExpiringGrant, preallocation := uuid.New(), uuid.New()
	purchase, expiredGrant, pendingGrant := uuid.New(), uuid.New(), uuid.New()
	insertWalletEntry(t, pool, accountID, purchase, 500_000, "purchase", "settled", nil, created.Add(5*time.Hour))
	insertWalletEntry(t, pool, accountID, preallocation, 400_000, "preallocation", "settled", nil, created.Add(4*time.Hour))
	insertWalletEntry(t, pool, accountID, nonExpiringGrant, 300_000, "grant", "settled", nil, created.Add(3*time.Hour))
	insertWalletEntry(t, pool, accountID, laterGrant, 200_000, "grant", "settled", &laterExpiry, created.Add(2*time.Hour))
	insertWalletEntry(t, pool, accountID, soonGrant, 100_000, "grant", "settled", &soonExpiry, created.Add(time.Hour))
	insertWalletEntry(t, pool, accountID, expiredGrant, 700_000, "grant", "settled", &expired, created)
	insertWalletEntry(t, pool, accountID, pendingGrant, 800_000, "grant", "pending", nil, created)

	state, err := store.WalletCreditState(ctx, accountID, start, end)
	require.NoError(t, err)
	require.Equal(t, cycle.CreditBillingModeStandard, state.Mode)
	require.EqualValues(t, 1_500_000, state.SpendableBalanceMicros)
	require.Zero(t, state.PeriodDrawnMicros)

	draw, err := store.DrawWalletCredits(ctx, accountID, start, end, 1_200_000, true)
	require.NoError(t, err)
	require.Equal(t, cycle.CreditBillingModeStandard, draw.Mode)
	require.EqualValues(t, 1_200_000, draw.DrawnMicros)

	rows := persistedWalletDraws(t, pool, accountID, periodID)
	require.Len(t, rows, 5)
	require.Equal(t, []uuid.UUID{
		soonGrant, laterGrant, nonExpiringGrant, preallocation, purchase,
	}, []uuid.UUID{
		*rows[0].sourceID, *rows[1].sourceID, *rows[2].sourceID,
		*rows[3].sourceID, *rows[4].sourceID,
	})
	require.Equal(t, []int64{100_000, 200_000, 300_000, 400_000, 200_000}, []int64{
		rows[0].amount, rows[1].amount, rows[2].amount, rows[3].amount, rows[4].amount,
	})
	require.Equal(t, []int64{2_100_000, 1_900_000, 1_600_000, 1_200_000, 1_000_000}, []int64{
		rows[0].balanceAfter, rows[1].balanceAfter, rows[2].balanceAfter,
		rows[3].balanceAfter, rows[4].balanceAfter,
	})
	for _, row := range rows {
		require.Equal(t, "settled", row.status)
		require.Equal(t, "system", row.actor)
		require.Equal(t,
			fmt.Sprintf("wallet-draw:%s:%s:usage_draw:%s", accountID, periodID, *row.sourceID),
			row.key,
		)
	}

	state, err = store.WalletCreditState(ctx, accountID, start, end)
	require.NoError(t, err)
	require.EqualValues(t, 300_000, state.SpendableBalanceMicros)
	require.EqualValues(t, 1_200_000, state.PeriodDrawnMicros)

	// Credit arriving after the first attempt belongs to future periods. The
	// same-period retry recovers the original rows even with allowNew=false.
	latePurchase := uuid.New()
	insertWalletEntry(t, pool, accountID, latePurchase, 900_000, "purchase", "settled", nil, created.Add(24*time.Hour))
	retry, err := store.DrawWalletCredits(ctx, accountID, start, end, 1_500_000, false)
	require.NoError(t, err)
	require.EqualValues(t, 1_200_000, retry.DrawnMicros)
	require.Len(t, persistedWalletDraws(t, pool, accountID, periodID), 5)
}

func TestDrawWalletCredits_Integration_CreditsResidualAndAllowNewGuard(t *testing.T) {
	pool := testutil.NewTestDB(t)
	store := cycle.NewStore(pool)
	ctx := context.Background()
	start, end := mustTime(t, pStart), mustTime(t, pEnd)

	creditsAccount := seedAccount(t, pool)
	_, err := pool.Exec(ctx, `UPDATE ms_billing.accounts SET billing_mode = 'credits' WHERE id = $1`, creditsAccount.String())
	require.NoError(t, err)
	periodID, err := store.OpenPeriodForAccount(ctx, creditsAccount, start, end)
	require.NoError(t, err)
	sourceID := uuid.New()
	insertWalletEntry(t, pool, creditsAccount, sourceID, 200_000, "grant", "settled", nil, mustTime(t, "2026-01-01T00:00:00Z"))

	draw, err := store.DrawWalletCredits(ctx, creditsAccount, start, end, 500_000, true)
	require.NoError(t, err)
	require.Equal(t, cycle.CreditBillingModeCredits, draw.Mode)
	require.EqualValues(t, 500_000, draw.DrawnMicros)
	rows := persistedWalletDraws(t, pool, creditsAccount, periodID)
	require.Len(t, rows, 2)
	require.NotNil(t, rows[0].sourceID)
	require.Equal(t, sourceID, *rows[0].sourceID)
	require.EqualValues(t, 200_000, rows[0].amount)
	require.Zero(t, rows[0].balanceAfter)
	require.Nil(t, rows[1].sourceID)
	require.EqualValues(t, 300_000, rows[1].amount)
	require.EqualValues(t, -300_000, rows[1].balanceAfter)
	require.Equal(t,
		fmt.Sprintf("wallet-draw:%s:%s:usage_draw:unsecured", creditsAccount, periodID),
		rows[1].key,
	)

	retry, err := store.DrawWalletCredits(ctx, creditsAccount, start, end, 700_000, false)
	require.NoError(t, err)
	require.EqualValues(t, 500_000, retry.DrawnMicros)
	require.Len(t, persistedWalletDraws(t, pool, creditsAccount, periodID), 2)

	guardedAccount := seedAccount(t, pool)
	guardedPeriodID, err := store.OpenPeriodForAccount(ctx, guardedAccount, start, end)
	require.NoError(t, err)
	guardedSource := uuid.New()
	insertWalletEntry(t, pool, guardedAccount, guardedSource, 400_000, "grant", "settled", nil, mustTime(t, "2026-01-01T00:00:00Z"))
	guarded, err := store.DrawWalletCredits(ctx, guardedAccount, start, end, 400_000, false)
	require.NoError(t, err)
	require.Zero(t, guarded.DrawnMicros)
	require.Empty(t, persistedWalletDraws(t, pool, guardedAccount, guardedPeriodID))

	// Expired journal credit cannot offset a negative adjustment and thereby
	// make an unrelated active lot spendable. Posted sum is +30k here, but after
	// removing the expired +100k remainder the effective standard balance is 0.
	expiryAccount := seedAccount(t, pool)
	expiryPeriodID, err := store.OpenPeriodForAccount(ctx, expiryAccount, start, end)
	require.NoError(t, err)
	past := mustTime(t, "2020-01-01T00:00:00Z")
	created := mustTime(t, "2019-01-01T00:00:00Z")
	insertWalletEntry(t, pool, expiryAccount, uuid.New(), 100_000, "grant", "settled", &past, created)
	insertWalletEntry(t, pool, expiryAccount, uuid.New(), 50_000, "purchase", "settled", nil, created)
	insertWalletEntry(t, pool, expiryAccount, uuid.New(), -120_000, "adjustment", "settled", nil, created)
	state, err := store.WalletCreditState(ctx, expiryAccount, start, end)
	require.NoError(t, err)
	require.Zero(t, state.SpendableBalanceMicros)
	expiryDraw, err := store.DrawWalletCredits(ctx, expiryAccount, start, end, 50_000, true)
	require.NoError(t, err)
	require.Zero(t, expiryDraw.DrawnMicros)
	require.Empty(t, persistedWalletDraws(t, pool, expiryAccount, expiryPeriodID))
}

func TestDrawCreationProrationFromWallet_Integration_AttemptedDefersBeforeDraw(t *testing.T) {
	pool := testutil.NewTestDB(t)
	store := cycle.NewStore(pool)
	ctx := context.Background()

	accountID := seedAccount(t, pool)
	_, err := pool.Exec(ctx, `UPDATE ms_billing.accounts SET billing_mode = 'credits' WHERE id = $1`, accountID.String())
	require.NoError(t, err)
	appID := uuid.New()
	createdAt := mustTime(t, "2026-06-19T12:00:00Z")
	require.NoError(t, store.InsertAppMirror(ctx, appID, accountID, uuid.Nil, 0, createdAt, "race app"))
	require.NoError(t, store.MarkAppProrationAttempted(ctx, appID, createdAt.Add(4*time.Hour)))
	insertWalletEntry(t, pool, accountID, uuid.New(), 5_000_000, "grant", "settled", nil, createdAt)

	periodStart := mustTime(t, "2026-06-04T00:00:00Z")
	periodEnd := mustTime(t, "2026-07-04T00:00:00Z")
	outcome, ref, err := store.DrawCreationProrationFromWallet(ctx, appID, cycle.ProrationWalletCharge{
		Ref:          "wallet:app-proration:" + appID.String(),
		AmountMicros: 3_250_123,
		Snapshot: cycle.AppBaseSnapshot{
			AppID: appID, PeriodStart: periodStart, PeriodEnd: periodEnd,
			BaseMicros: 3_250_123,
		},
	})
	require.NoError(t, err)
	require.Equal(t, cycle.ProrationWalletDeferToStripe, outcome)
	require.Empty(t, ref)

	var draws, snapshots int
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT count(*) FROM ms_billing.credit_ledger
		  WHERE account_id = $1 AND type = 'usage_draw'`, accountID.String()).Scan(&draws))
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT count(*) FROM ms_billing.app_base_snapshots WHERE app_id = $1`, appID.String()).Scan(&snapshots))
	require.Zero(t, draws, "the attempted marker must win before any ledger write")
	require.Zero(t, snapshots, "the deferred wallet transaction persists no snapshot")

	app, found, err := store.AppMirror(ctx, appID)
	require.NoError(t, err)
	require.True(t, found)
	require.True(t, app.ProrationAttempted)
	require.Empty(t, app.ProrationInvoiceID, "Stripe recovery, not the wallet, must arm the guard")
}

// Regression: both mid-period wallet legs classify the account before entering
// their transaction. A concurrent credits→standard update can commit while the
// draw is waiting for the account lock. PostgreSQL's locked read must then expose
// standard as the authoritative mode, defer the whole charge to Stripe, and make
// no ledger/guard write even when a grant could fully cover the amount.
func TestMidPeriodWalletDraws_Integration_ModeFlipSerializesAndDefersBeforeLedger(t *testing.T) {
	pool := testutil.NewTestDB(t)
	store := cycle.NewStore(pool)
	ctx := context.Background()

	waitForLockedWalletModeRead := func(t *testing.T) {
		t.Helper()
		var probeErr error
		require.Eventually(t, func() bool {
			var waiting bool
			probeErr = pool.QueryRow(ctx, `
				SELECT EXISTS (
					SELECT 1
					FROM pg_stat_activity
					WHERE datname = current_database()
					  AND wait_event_type = 'Lock'
					  AND query ILIKE '%SELECT billing_mode%'
					  AND query ILIKE '%FOR UPDATE%'
				)`).Scan(&waiting)
			return probeErr == nil && waiting
		}, 5*time.Second, 10*time.Millisecond,
			"the wallet draw must wait behind the concurrent account-mode update")
		require.NoError(t, probeErr)
	}

	t.Run("creation proration", func(t *testing.T) {
		accountID := seedAccount(t, pool)
		_, err := pool.Exec(ctx,
			`UPDATE ms_billing.accounts SET billing_mode = 'credits' WHERE id = $1`,
			accountID.String())
		require.NoError(t, err)
		appID := uuid.New()
		createdAt := mustTime(t, "2026-06-19T12:00:00Z")
		require.NoError(t, store.InsertAppMirror(ctx, appID, accountID, uuid.Nil, 0, createdAt, "mode-race app"))
		insertWalletEntry(t, pool, accountID, uuid.New(), 50_000_000, "grant", "settled", nil, createdAt)

		periodStart := mustTime(t, "2026-06-04T00:00:00Z")
		periodEnd := mustTime(t, "2026-07-04T00:00:00Z")
		classified, err := store.WalletCreditState(ctx, accountID, periodStart, periodEnd)
		require.NoError(t, err)
		require.Equal(t, cycle.CreditBillingModeCredits, classified.Mode,
			"the unlocked caller snapshot selects the wallet rail before the flip")

		flipTx, err := pool.Begin(ctx)
		require.NoError(t, err)
		defer func() { _ = flipTx.Rollback(ctx) }()
		_, err = flipTx.Exec(ctx,
			`UPDATE ms_billing.accounts SET billing_mode = 'standard' WHERE id = $1`,
			accountID.String())
		require.NoError(t, err)

		type drawResult struct {
			outcome cycle.ProrationOutcome
			ref     string
			err     error
		}
		done := make(chan drawResult, 1)
		go func() {
			outcome, ref, drawErr := store.DrawCreationProrationFromWallet(ctx, appID, cycle.ProrationWalletCharge{
				Ref:          "wallet:app-proration:" + appID.String(),
				AmountMicros: 3_250_123,
				Snapshot: cycle.AppBaseSnapshot{
					AppID: appID, PeriodStart: periodStart, PeriodEnd: periodEnd,
					BaseMicros: 3_250_123,
				},
			})
			done <- drawResult{outcome: outcome, ref: ref, err: drawErr}
		}()

		waitForLockedWalletModeRead(t)
		require.NoError(t, flipTx.Commit(ctx))

		var got drawResult
		select {
		case got = <-done:
		case <-time.After(5 * time.Second):
			t.Fatal("creation wallet draw did not resume after the mode flip committed")
		}
		require.NoError(t, got.err)
		require.Equal(t, cycle.ProrationWalletDeferToStripe, got.outcome)
		require.Empty(t, got.ref)

		var draws, snapshots int
		var guard *string
		require.NoError(t, pool.QueryRow(ctx, `
			SELECT count(*)
			FROM ms_billing.credit_ledger
			WHERE account_id = $1 AND type = 'usage_draw'`,
			accountID.String()).Scan(&draws))
		require.NoError(t, pool.QueryRow(ctx,
			`SELECT count(*) FROM ms_billing.app_base_snapshots WHERE app_id = $1`,
			appID.String()).Scan(&snapshots))
		require.NoError(t, pool.QueryRow(ctx,
			`SELECT proration_invoice_id FROM ms_billing.apps WHERE app_id = $1`,
			appID.String()).Scan(&guard))
		require.Zero(t, draws, "the locked standard mode must win before any ledger write")
		require.Zero(t, snapshots, "a mode-change defer freezes no wallet snapshot")
		require.Nil(t, guard, "Stripe, not the wallet transaction, owns the one-shot guard")
	})

	t.Run("module overage", func(t *testing.T) {
		accountID := seedAccount(t, pool)
		_, err := pool.Exec(ctx,
			`UPDATE ms_billing.accounts SET billing_mode = 'credits' WHERE id = $1`,
			accountID.String())
		require.NoError(t, err)
		appID := uuid.New()
		installedAt := mustTime(t, "2026-06-10T12:00:00Z")
		require.NoError(t, store.InsertAppMirror(ctx, appID, accountID, uuid.Nil, 1, installedAt, "mode-race module"))
		require.NoError(t, store.InsertModuleOverageTimers(
			ctx, accountID, appID, installedAt, installedAt.AddDate(0, 0, 3), 1,
		))
		insertWalletEntry(t, pool, accountID, uuid.New(), 50_000_000, "grant", "settled", nil, installedAt)

		var timerID uuid.UUID
		require.NoError(t, pool.QueryRow(ctx, `
			SELECT id
			FROM ms_billing.app_module_overage_timers
			WHERE app_id = $1`,
			appID.String()).Scan(&timerID))
		periodStart := mustTime(t, "2026-06-04T00:00:00Z")
		periodEnd := mustTime(t, "2026-07-04T00:00:00Z")
		classified, err := store.WalletCreditState(ctx, accountID, periodStart, periodEnd)
		require.NoError(t, err)
		require.Equal(t, cycle.CreditBillingModeCredits, classified.Mode,
			"the unlocked caller snapshot selects the wallet rail before the flip")

		flipTx, err := pool.Begin(ctx)
		require.NoError(t, err)
		defer func() { _ = flipTx.Rollback(ctx) }()
		_, err = flipTx.Exec(ctx,
			`UPDATE ms_billing.accounts SET billing_mode = 'standard' WHERE id = $1`,
			accountID.String())
		require.NoError(t, err)

		type drawResult struct {
			outcome cycle.ModuleOverageWalletOutcome
			ref     string
			err     error
		}
		done := make(chan drawResult, 1)
		go func() {
			outcome, ref, drawErr := store.DrawModuleOverageFromWallet(ctx, timerID, cycle.ModuleOverageWalletCharge{
				Ref:          "wallet:module-overage:" + timerID.String(),
				AmountMicros: 2_400_000,
				ChargedAt:    installedAt.AddDate(0, 0, 4),
			})
			done <- drawResult{outcome: outcome, ref: ref, err: drawErr}
		}()

		waitForLockedWalletModeRead(t)
		require.NoError(t, flipTx.Commit(ctx))

		var got drawResult
		select {
		case got = <-done:
		case <-time.After(5 * time.Second):
			t.Fatal("module-overage wallet draw did not resume after the mode flip committed")
		}
		require.NoError(t, got.err)
		require.Equal(t, cycle.ModuleOverageWalletDeferToStripe, got.outcome)
		require.Empty(t, got.ref)

		var draws int
		var resolved bool
		var guard *string
		require.NoError(t, pool.QueryRow(ctx, `
			SELECT count(*)
			FROM ms_billing.credit_ledger
			WHERE account_id = $1 AND type = 'usage_draw'`,
			accountID.String()).Scan(&draws))
		require.NoError(t, pool.QueryRow(ctx, `
			SELECT grace_resolved, grace_invoice_id
			FROM ms_billing.app_module_overage_timers
			WHERE id = $1`,
			timerID.String()).Scan(&resolved, &guard))
		require.Zero(t, draws, "the locked standard mode must win before any ledger write")
		require.False(t, resolved, "Stripe, not the wallet transaction, owns the terminal guard")
		require.Nil(t, guard)
	})
}
