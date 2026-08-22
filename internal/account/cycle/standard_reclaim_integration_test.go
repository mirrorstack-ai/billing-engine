//go:build integration

package cycle_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/mirrorstack-ai/billing-engine/internal/account/cycle"
	"github.com/mirrorstack-ai/billing-engine/internal/account/db"
	"github.com/mirrorstack-ai/billing-engine/internal/shared/testutil"
)

type standardRunSnapshot struct {
	status         string
	stripeInvoice  *string
	totalCents     int64
	frozenCents    *int64
	frozenWithBase *bool
}

func readStandardRun(t *testing.T, pool *pgxpool.Pool, runID uuid.UUID) standardRunSnapshot {
	t.Helper()
	var got standardRunSnapshot
	require.NoError(t, pool.QueryRow(context.Background(), `
		SELECT
			status,
			stripe_invoice_id,
			total_amount::bigint,
			frozen_charge_cents,
			frozen_charge_with_base
		FROM ms_billing.billing_runs
		WHERE id = $1`,
		runID,
	).Scan(
		&got.status,
		&got.stripeInvoice,
		&got.totalCents,
		&got.frozenCents,
		&got.frozenWithBase,
	))
	return got
}

func installStandardPaymentMethod(t *testing.T, pool *pgxpool.Pool, accountID uuid.UUID, customerID string) {
	t.Helper()
	ctx := context.Background()
	_, err := pool.Exec(ctx, `
		UPDATE ms_billing.accounts
		SET stripe_customer_id = $2
		WHERE id = $1`,
		accountID,
		customerID,
	)
	require.NoError(t, err)
	_, err = pool.Exec(ctx, `
		INSERT INTO ms_billing.payment_methods_mirror (
			id,
			account_id,
			stripe_payment_method_id,
			brand,
			last4,
			exp_month,
			exp_year,
			is_default
		) VALUES ($1, $2, $3, 'visa', '4242', 12, 2099, true)`,
		uuid.New(),
		accountID,
		"pm_standard_reclaim_"+uuid.NewString(),
	)
	require.NoError(t, err)
}

// TestStandardModeReclaim_Integration_Matrix proves the unflagged billing-run
// gate against real PostgreSQL while the credit-wallet capability is explicitly
// off. These are the five states every existing standard-mode customer can
// cross at a post-deploy boundary.
func TestStandardModeReclaim_Integration_Matrix(t *testing.T) {
	pool := testutil.NewTestDB(t)
	ctx := context.Background()
	start, end := mustTime(t, pStart), mustTime(t, pEnd)

	t.Run("fresh run", func(t *testing.T) {
		store := cycle.NewStore(pool)
		accountID := seedAccount(t, pool)

		runID, shouldCharge, reclaimed, err := store.InsertBillingRun(ctx, accountID, start, end)
		require.NoError(t, err)
		require.True(t, shouldCharge)
		require.False(t, reclaimed)
		require.NotEqual(t, uuid.Nil, runID)

		got := readStandardRun(t, pool, runID)
		require.Equal(t, "pending", got.status)
		require.Nil(t, got.stripeInvoice)
		require.Zero(t, got.totalCents)
		require.Nil(t, got.frozenCents)
		require.Nil(t, got.frozenWithBase)

		marked, err := store.MarkBillingRunInvoicedIfUnfrozen(ctx, runID)
		require.NoError(t, err)
		require.True(t, marked, "a genuinely fresh zero run may terminate")

		blockedID, shouldCharge, reclaimed, err := store.InsertBillingRun(ctx, accountID, start, end)
		require.NoError(t, err)
		require.Equal(t, uuid.Nil, blockedID)
		require.False(t, shouldCharge, "an invoiced window is terminal")
		require.False(t, reclaimed)
	})

	t.Run("reclaimed pending", func(t *testing.T) {
		store := cycle.NewStore(pool)
		accountID := seedAccount(t, pool)

		firstID, shouldCharge, reclaimed, err := store.InsertBillingRun(ctx, accountID, start, end)
		require.NoError(t, err)
		require.True(t, shouldCharge)
		require.False(t, reclaimed)

		reclaimedID, shouldCharge, reclaimed, err := store.InsertBillingRun(ctx, accountID, start, end)
		require.NoError(t, err)
		require.True(t, shouldCharge)
		require.True(t, reclaimed)
		require.Equal(t, firstID, reclaimedID, "reclaim must preserve Stripe idempotency identity")

		sc := newFakeStripe()
		resp, err := cycle.NewService(store, sc).
			WithCreditWallet(false).
			RunBillingCycle(ctx, accountID, start, end, 0)
		require.NoError(t, err)
		require.True(t, resp.FirstRun, "a reclaimed pending row is an active attempt")
		require.Equal(t, cycle.RunStatusInvoiced, resp.Status)
		require.Empty(t, sc.findByRefCalls)
		require.Empty(t, sc.invoiceCalls)
		require.Empty(t, sc.itemCalls)
		require.Empty(t, sc.finalizeCalls)

		got := readStandardRun(t, pool, firstID)
		require.Equal(t, "invoiced", got.status)
		require.Nil(t, got.frozenCents,
			"the standard zero path terminally marks only while still unfrozen")
		require.Zero(t, got.totalCents)
	})

	t.Run("reclaimed frozen greater than zero", func(t *testing.T) {
		store := cycle.NewStore(pool)
		accountID := seedAccount(t, pool)
		installStandardPaymentMethod(t, pool, accountID, "cus_standard_frozen_positive")

		runID, shouldCharge, reclaimed, err := store.InsertBillingRun(ctx, accountID, start, end)
		require.NoError(t, err)
		require.True(t, shouldCharge)
		require.False(t, reclaimed)

		const frozenCents int64 = 137
		frozen, claim, err := store.FreezeBillingRunCharge(ctx, runID, cycle.FrozenBoundaryCharge{
			Cents:    frozenCents,
			WithBase: true,
		})
		require.NoError(t, err)
		require.Equal(t, cycle.StripeRailClaimed, claim)
		require.EqualValues(t, frozenCents, frozen.Cents)
		require.True(t, frozen.WithBase)
		require.NoError(t, store.MarkBillingRun(ctx, runID, cycle.RunStatusFailed, "in_stale", 999))

		sc := newFakeStripe()
		sc.invoiceAmountDue = frozenCents
		resp, err := cycle.NewService(store, sc).
			WithCreditWallet(false).
			RunBillingCycle(ctx, accountID, start, end, 0)
		require.NoError(t, err)
		require.True(t, resp.FirstRun)
		require.Equal(t, cycle.RunStatusInvoiced, resp.Status)
		require.EqualValues(t, frozenCents, resp.ChargedCents)
		require.Len(t, sc.findByRefCalls, 1)
		require.Len(t, sc.invoiceCalls, 1)
		require.Len(t, sc.itemCalls, 1)
		require.Len(t, sc.finalizeCalls, 1)
		require.Equal(t, "run:"+runID.String(), sc.findByRefCalls[0])
		require.Equal(t, "inv-"+runID.String(), sc.invoiceCalls[0].idemKey)
		require.Equal(t, "ii-"+runID.String(), sc.itemCalls[0].idemKey)
		require.Equal(t, "fin-"+runID.String(), sc.finalizeCalls[0].idemKey)
		require.EqualValues(t, frozenCents, sc.itemCalls[0].amountCfg,
			"the reclaim must reuse the durable amount, not the live zero total")

		got := readStandardRun(t, pool, runID)
		require.Equal(t, "invoiced", got.status)
		require.NotNil(t, got.stripeInvoice)
		require.Equal(t, sc.invoiceID, *got.stripeInvoice)
		require.EqualValues(t, frozenCents, got.totalCents)
		require.NotNil(t, got.frozenCents)
		require.EqualValues(t, frozenCents, *got.frozenCents)
		require.NotNil(t, got.frozenWithBase)
		require.True(t, *got.frozenWithBase)
	})

	t.Run("reclaimed frozen equal to zero", func(t *testing.T) {
		store := cycle.NewStore(pool)
		accountID := seedAccount(t, pool)
		installStandardPaymentMethod(t, pool, accountID, "cus_standard_frozen_zero")

		runID, shouldCharge, reclaimed, err := store.InsertBillingRun(ctx, accountID, start, end)
		require.NoError(t, err)
		require.True(t, shouldCharge)
		require.False(t, reclaimed)
		_, claim, err := store.FreezeBillingRunCharge(ctx, runID, cycle.FrozenBoundaryCharge{})
		require.NoError(t, err)
		require.Equal(t, cycle.StripeRailClaimed, claim)
		require.NoError(t, store.MarkBillingRun(ctx, runID, cycle.RunStatusFailed, "", 0))

		sc := newFakeStripe()
		resp, err := cycle.NewService(store, sc).
			WithCreditWallet(false).
			RunBillingCycle(ctx, accountID, start, end, 0)
		require.NoError(t, err)
		require.True(t, resp.FirstRun)
		require.Equal(t, cycle.RunStatusInvoiced, resp.Status)
		require.Zero(t, resp.ChargedCents)
		require.Empty(t, sc.findByRefCalls,
			"a durable zero is already settled and must not search Stripe")
		require.Empty(t, sc.invoiceCalls)
		require.Empty(t, sc.itemCalls)
		require.Empty(t, sc.finalizeCalls)

		got := readStandardRun(t, pool, runID)
		require.Equal(t, "invoiced", got.status)
		require.Nil(t, got.stripeInvoice)
		require.Zero(t, got.totalCents)
		require.NotNil(t, got.frozenCents)
		require.Zero(t, *got.frozenCents)
		require.NotNil(t, got.frozenWithBase)
		require.False(t, *got.frozenWithBase)
	})

	t.Run("concurrent invoiced", func(t *testing.T) {
		t.Run("terminal mark wins before stale freeze", func(t *testing.T) {
			store := cycle.NewStore(pool)
			accountID := seedAccount(t, pool)
			runID, shouldCharge, reclaimed, err := store.InsertBillingRun(ctx, accountID, start, end)
			require.NoError(t, err)
			require.True(t, shouldCharge)
			require.False(t, reclaimed)

			tx, err := pool.Begin(ctx)
			require.NoError(t, err)
			t.Cleanup(func() { _ = tx.Rollback(ctx) })
			rows, err := db.New(tx).MarkBillingRunInvoicedIfUnfrozen(ctx, runID.String())
			require.NoError(t, err)
			require.EqualValues(t, 1, rows)

			type freezeResult struct {
				charge cycle.FrozenBoundaryCharge
				claim  cycle.StripeRailClaimOutcome
				err    error
			}
			started := make(chan struct{})
			result := make(chan freezeResult, 1)
			go func() {
				close(started)
				charge, claim, freezeErr := store.FreezeBillingRunCharge(
					ctx,
					runID,
					cycle.FrozenBoundaryCharge{Cents: 211, WithBase: true},
				)
				result <- freezeResult{charge: charge, claim: claim, err: freezeErr}
			}()
			<-started
			select {
			case got := <-result:
				t.Fatalf("stale freeze bypassed the uncommitted terminal row lock: %+v", got)
			case <-time.After(100 * time.Millisecond):
			}

			require.NoError(t, tx.Commit(ctx))
			select {
			case got := <-result:
				require.NoError(t, got.err)
				require.Equal(t, cycle.StripeRailStale, got.claim,
					"a freeze that loses to terminal invoiced must not manufacture a charge")
			case <-time.After(5 * time.Second):
				t.Fatal("stale freeze did not resume after terminal commit")
			}

			got := readStandardRun(t, pool, runID)
			require.Equal(t, "invoiced", got.status)
			require.Nil(t, got.frozenCents)
			require.Nil(t, got.frozenWithBase)
		})

		t.Run("freeze wins before stale terminal mark", func(t *testing.T) {
			store := cycle.NewStore(pool)
			accountID := seedAccount(t, pool)
			runID, shouldCharge, reclaimed, err := store.InsertBillingRun(ctx, accountID, start, end)
			require.NoError(t, err)
			require.True(t, shouldCharge)
			require.False(t, reclaimed)

			tx, err := pool.Begin(ctx)
			require.NoError(t, err)
			t.Cleanup(func() { _ = tx.Rollback(ctx) })
			qtx := db.New(tx)
			fundingAuth, err := qtx.StripeFundingAuthorization(ctx, accountID.String())
			require.NoError(t, err)
			err = qtx.FreezeBillingRunCharge(ctx, db.FreezeBillingRunChargeParams{
				ID:                      runID.String(),
				FrozenChargeCents:       pgtype.Int8{Int64: 311, Valid: true},
				FrozenChargeWithBase:    pgtype.Bool{Bool: false, Valid: true},
				ChargeFundingAccountID:  fundingAuth.FundingAccountID,
				ChargeFundingGeneration: fundingAuth.Generation,
			})
			require.NoError(t, err)

			type markResult struct {
				marked bool
				err    error
			}
			started := make(chan struct{})
			result := make(chan markResult, 1)
			go func() {
				close(started)
				marked, markErr := store.MarkBillingRunInvoicedIfUnfrozen(ctx, runID)
				result <- markResult{marked: marked, err: markErr}
			}()
			<-started
			select {
			case got := <-result:
				t.Fatalf("stale terminal mark bypassed the uncommitted freeze row lock: %+v", got)
			case <-time.After(100 * time.Millisecond):
			}

			require.NoError(t, tx.Commit(ctx))
			select {
			case got := <-result:
				require.NoError(t, got.err)
				require.False(t, got.marked,
					"a zero terminal mark that loses to freeze must leave the run reclaimable")
			case <-time.After(5 * time.Second):
				t.Fatal("stale terminal mark did not resume after freeze commit")
			}

			got := readStandardRun(t, pool, runID)
			require.Equal(t, "pending", got.status)
			require.NotNil(t, got.frozenCents)
			require.EqualValues(t, 311, *got.frozenCents)
			require.NotNil(t, got.frozenWithBase)
			require.False(t, *got.frozenWithBase)

			reclaimedID, shouldCharge, reclaimed, err := store.InsertBillingRun(ctx, accountID, start, end)
			require.NoError(t, err)
			require.True(t, shouldCharge)
			require.True(t, reclaimed)
			require.Equal(t, runID, reclaimedID)
			got = readStandardRun(t, pool, runID)
			require.NotNil(t, got.frozenCents)
			require.EqualValues(t, 311, *got.frozenCents,
				"reclaim must preserve the winning freeze")
		})
	})
}
