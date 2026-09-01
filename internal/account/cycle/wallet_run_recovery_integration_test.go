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
//   - a true wallet-off service finishes from billing_runs alone, sealing only
//     the frozen partial remainder or terminally adopting a full wallet draw.
//
// 🔴 THE SAME BILLING FACTS, ON THE INTENT RAIL.
//
// The reclaim's subject is unchanged by the collector's deletion: a run that
// crashed after freezing its charge is picked up again, and the amount it
// committed to is what settles — never a figure re-derived from live state.
//
// What changed is where that figure is read. A sealed boundary states the
// GROSS on its lines and carries the wallet credit already spent as a
// WalletAllocationMicros, so the provider REMAINDER — gross minus allocation —
// is the number the deleted draft→item→finalize flow used to send to Stripe.
// The partial case therefore pins remainder == the frozen 60 cents, which is
// the same claim the old `sc.itemCalls[0].amountCfg == 60` made.
func TestBillingRunWalletDraw_Integration_CrashThenTrueOffReclaim(t *testing.T) {
	tests := []struct {
		name            string
		walletMicros    int64
		wantFrozenCents int64
		installPayment  bool
		wantDrawnMicros int64

		// The boundary as the intent rail states it.
		wantProposed                bool
		wantStatus                  cycle.BillingRunStatus
		wantProposedGrossMicros     int64
		wantProposedWalletMicros    int64
		wantProposedRemainderMicros int64
		wantRunTotalCents           int64
	}{
		{
			name:            "partial draw seals only frozen remainder",
			walletMicros:    400_000,
			wantFrozenCents: 60,
			installPayment:  true,
			wantDrawnMicros: 400_000,

			wantProposed:             true,
			wantStatus:               cycle.RunStatusProposed,
			wantProposedGrossMicros:  1_000_000,
			wantProposedWalletMicros: 400_000,
			// 600_000 micros == the 60 frozen cents == what Stripe was sent.
			wantProposedRemainderMicros: 600_000,
			// A proposed run collected nothing, so it mirrors no total.
			wantRunTotalCents: 0,
		},
		{
			name:            "full draw terminates without a proposal or a PM",
			walletMicros:    1_000_000,
			wantFrozenCents: 0,
			installPayment:  false,
			wantDrawnMicros: 1_000_000,

			wantProposed:      false,
			wantStatus:        cycle.RunStatusInvoiced,
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
			svc, p := boundarySvcProposing(store, sc)
			resp, err := svc.WithCreditWallet(false).RunBillingCycle(ctx, accountID, start, end, 0)
			require.NoError(t, err)
			require.Equal(t, tt.wantStatus, resp.Status)
			require.Zero(t, resp.WalletDrawnMicros,
				"true off recovers through billing_runs, never the wallet graph")
			require.Zero(t, resp.ChargedCents,
				"nothing was collected, so nothing may report cents as charged")

			// 🔴 Nothing reached the provider — the drop's central claim.
			require.Empty(t, sc.invoiceCalls)
			require.Empty(t, sc.itemCalls)
			require.Empty(t, sc.finalizeCalls)

			if tt.wantProposed {
				require.Len(t, p.groups, 1,
					"the boundary must seal exactly one group — one boundary, one rounding")
				require.EqualValues(t, tt.wantProposedGrossMicros, proposedMicros(t, p),
					"the sealed boundary must state the gross this run priced")

				var wallet int64
				for _, c := range p.groups[0] {
					wallet += c.WalletAllocationMicros
				}
				require.EqualValues(t, tt.wantProposedWalletMicros, wallet,
					"the committed wallet debit must ride the intents as an allocation, "+
						"or the boundary collects it a second time")

				// The figure the deleted collector sent to Stripe, read off the
				// intents instead: gross minus the credit already spent.
				require.EqualValues(t, tt.wantProposedRemainderMicros, proposedRemainderMicros(t, p),
					"the provider remainder must equal what the crashed attempt froze")
				require.EqualValues(t, tt.wantFrozenCents*10_000, proposedRemainderMicros(t, p),
					"the reclaim must reuse the durable amount, not a live re-derivation")
			} else {
				require.Empty(t, p.groups,
					"a full wallet draw is already settled — it must seal no intent")
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
			require.EqualValues(t, tt.wantStatus, status,
				"the run must reach its terminal state for this worker")
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
