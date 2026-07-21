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
