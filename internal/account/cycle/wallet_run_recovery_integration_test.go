//go:build integration

package cycle_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/mirrorstack-ai/billing-engine/internal/account/cycle"
	"github.com/mirrorstack-ai/billing-engine/internal/account/usage"
	"github.com/mirrorstack-ai/billing-engine/internal/shared/testutil"
)

// TestBillingRunWalletDraw_Integration_CrashThenTrueOffReclaim proves the
// production transaction boundary, not merely the in-memory model:
//
//   - the period ledger debit and billing_runs Stripe-remainder marker commit
//     atomically;
//   - a simulated process death immediately after that commit leaves the run
//     pending and reclaimable;
//   - a true wallet-off service finishes from billing_runs alone, charging only
//     the frozen partial remainder or terminally adopting a full wallet draw.
func TestBillingRunWalletDraw_Integration_CrashThenTrueOffReclaim(t *testing.T) {
	tests := []struct {
		name              string
		walletMicros      int64
		wantFrozenCents   int64
		wantStripeCalls   int
		wantStripeCents   int64
		installPayment    bool
		wantDrawnMicros   int64
		wantRunTotalCents int64
	}{
		{
			name:              "partial draw charges only frozen remainder",
			walletMicros:      400_000,
			wantFrozenCents:   60,
			wantStripeCalls:   1,
			wantStripeCents:   60,
			installPayment:    true,
			wantDrawnMicros:   400_000,
			wantRunTotalCents: 60,
		},
		{
			name:              "full draw terminates without Stripe or PM",
			walletMicros:      1_000_000,
			wantFrozenCents:   0,
			wantStripeCalls:   0,
			wantStripeCents:   0,
			installPayment:    false,
			wantDrawnMicros:   1_000_000,
			wantRunTotalCents: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pool := testutil.NewTestDB(t)
			store := cycle.NewStore(pool)
			ctx := context.Background()
			accountID := seedAccount(t, pool)
			start, end := mustTime(t, pStart), mustTime(t, pEnd)

			// Price one exact $1 usage event into the closed period.
			appID, moduleID := uuid.New(), uuid.New()
			seedMetricDef(t, pool, moduleID, "wallet.crash.test", usage.KindCount, 1_000_000)
			seedEvent(
				t,
				pool,
				accountID,
				appID,
				moduleID,
				"wallet.crash.test",
				usage.KindCount,
				1,
				"2026-06-10T00:00:00Z",
			)
			rollup, err := cycle.NewService(store, nil).RollupPeriod(ctx, accountID, start, end)
			require.NoError(t, err)
			require.EqualValues(t, 1_000_000, rollup.TotalChargedMicros)

			sourceID := uuid.New()
			insertWalletEntry(
				t,
				pool,
				accountID,
				sourceID,
				tt.walletMicros,
				"grant",
				"settled",
				nil,
				mustTime(t, "2026-01-01T00:00:00Z"),
			)
			runID, shouldCharge, reclaimed, err := store.InsertBillingRun(ctx, accountID, start, end)
			require.NoError(t, err)
			require.True(t, shouldCharge)
			require.False(t, reclaimed)

			// This call is the last successful instruction before the simulated
			// process death. Deliberately do not mark the run afterward.
			draw, err := store.DrawBillingRunWalletCredits(
				ctx,
				runID,
				accountID,
				start,
				end,
				1_000_000,
				false,
				true,
			)
			require.NoError(t, err)
			require.EqualValues(t, tt.wantDrawnMicros, draw.DrawnMicros)
			require.True(t, draw.BoundaryChargeFrozen)
			require.EqualValues(t, tt.wantFrozenCents, draw.BoundaryCharge.Cents)

			var (
				status         string
				frozenCents    *int64
				frozenWithBase *bool
			)
			require.NoError(t, pool.QueryRow(ctx, `
				SELECT status, frozen_charge_cents, frozen_charge_with_base
				FROM ms_billing.billing_runs
				WHERE id = $1`,
				runID,
			).Scan(&status, &frozenCents, &frozenWithBase))
			require.Equal(t, "pending", status, "the simulated crash precedes every terminal mark")
			require.NotNil(t, frozenCents)
			require.EqualValues(t, tt.wantFrozenCents, *frozenCents)
			require.NotNil(t, frozenWithBase)
			require.False(t, *frozenWithBase)
			require.Len(t, persistedWalletDraws(t, pool, accountID, rollup.PeriodID), 1)

			if tt.installPayment {
				_, err = pool.Exec(ctx, `
					UPDATE ms_billing.accounts
					SET stripe_customer_id = 'cus_wallet_atomic_recovery'
					WHERE id = $1`,
					accountID,
				)
				require.NoError(t, err)
				_, err = pool.Exec(ctx, `
					INSERT INTO ms_billing.payment_methods_mirror (
						id, account_id, stripe_payment_method_id, brand, last4,
						exp_month, exp_year, is_default
					) VALUES ($1, $2, $3, 'visa', '4242', 12, 2099, true)`,
					uuid.New(),
					accountID,
					"pm_wallet_atomic_"+uuid.NewString(),
				)
				require.NoError(t, err)
			}

			sc := newFakeStripe()
			sc.invoiceAmountDue = tt.wantStripeCents
			resp, err := cycle.NewService(store, sc).
				WithCreditWallet(false).
				RunBillingCycle(ctx, accountID, start, end, 0)
			require.NoError(t, err)
			require.Equal(t, cycle.RunStatusInvoiced, resp.Status)
			require.Zero(t, resp.WalletDrawnMicros,
				"true off recovers through billing_runs, never the wallet graph")
			require.EqualValues(t, tt.wantStripeCents, resp.ChargedCents)
			require.Len(t, sc.itemCalls, tt.wantStripeCalls)
			require.Len(t, sc.invoiceCalls, tt.wantStripeCalls)
			require.Len(t, sc.finalizeCalls, tt.wantStripeCalls)
			if tt.wantStripeCalls == 1 {
				require.EqualValues(t, tt.wantStripeCents, sc.itemCalls[0].amountCfg)
			} else {
				require.Empty(t, sc.findByRefCalls,
					"a zero marker is wallet-terminal and never searches Stripe")
			}

			// The reclaim neither duplicated nor enlarged the committed debit.
			drawRows := persistedWalletDraws(t, pool, accountID, rollup.PeriodID)
			require.Len(t, drawRows, 1)
			require.EqualValues(t, tt.wantDrawnMicros, drawRows[0].amount)
			var gotRunTotalCents int64
			require.NoError(t, pool.QueryRow(ctx, `
				SELECT status, total_amount::bigint
				FROM ms_billing.billing_runs
				WHERE id = $1`,
				runID,
			).Scan(&status, &gotRunTotalCents))
			require.Equal(t, "invoiced", status)
			require.EqualValues(t, tt.wantRunTotalCents, gotRunTotalCents)
		})
	}
}

func TestBillingRunWalletDraw_Integration_ConcurrentStripeFreezeWinsBeforeAllocation(t *testing.T) {
	pool := testutil.NewTestDB(t)
	store := cycle.NewStore(pool)
	ctx := context.Background()
	accountID := seedAccount(t, pool)
	start, end := mustTime(t, pStart), mustTime(t, pEnd)
	periodID, err := store.OpenPeriodForAccount(ctx, accountID, start, end)
	require.NoError(t, err)
	runID, shouldCharge, reclaimed, err := store.InsertBillingRun(ctx, accountID, start, end)
	require.NoError(t, err)
	require.True(t, shouldCharge)
	require.False(t, reclaimed)
	sourceID := uuid.New()
	insertWalletEntry(
		t,
		pool,
		accountID,
		sourceID,
		1_000_000,
		"grant",
		"settled",
		nil,
		mustTime(t, "2026-01-01T00:00:00Z"),
	)

	// Daemon A owns the run row while freezing a full $1 Stripe request.
	// Daemon B may already have read "unfrozen" outside this transaction, but
	// its boundary draw must wait for the authoritative row lock and then
	// refuse to allocate beside A's marker.
	freezeTx, err := pool.Begin(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { _ = freezeTx.Rollback(ctx) })
	_, err = freezeTx.Exec(ctx, `
		UPDATE ms_billing.billing_runs run
		SET frozen_charge_cents = 100,
		    frozen_charge_with_base = false,
		    charge_funding_account_id = funding.funding_account_id,
		    charge_funding_generation = funding.generation
		FROM ms_billing.account_funding_authorizations funding
		WHERE run.id = $1
		  AND funding.account_id = run.account_id`,
		runID,
	)
	require.NoError(t, err)

	type drawResult struct {
		draw cycle.WalletDrawdown
		err  error
	}
	result := make(chan drawResult, 1)
	go func() {
		draw, drawErr := store.DrawBillingRunWalletCredits(
			ctx,
			runID,
			accountID,
			start,
			end,
			1_000_000,
			false,
			true,
		)
		result <- drawResult{draw: draw, err: drawErr}
	}()

	select {
	case got := <-result:
		t.Fatalf("wallet draw bypassed the held billing_run lock: %+v", got)
	case <-time.After(100 * time.Millisecond):
	}
	require.NoError(t, freezeTx.Commit(ctx))

	select {
	case got := <-result:
		require.NoError(t, got.err)
		require.Zero(t, got.draw.DrawnMicros)
		require.True(t, got.draw.BoundaryChargeFrozen)
		require.EqualValues(t, 100, got.draw.BoundaryCharge.Cents)
	case <-time.After(5 * time.Second):
		t.Fatal("wallet draw did not resume after the Stripe marker committed")
	}

	require.Empty(t, persistedWalletDraws(t, pool, accountID, periodID))
	var sourceBalance int64
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT amount_micros
		FROM ms_billing.credit_ledger
		WHERE id = $1`,
		sourceID,
	).Scan(&sourceBalance))
	require.EqualValues(t, 1_000_000, sourceBalance)
}
