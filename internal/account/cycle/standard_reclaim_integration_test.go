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

		runID, shouldCharge, reclaimed, _, err := store.InsertBillingRun(ctx, accountID, start, end)
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

		blockedID, shouldCharge, reclaimed, _, err := store.InsertBillingRun(ctx, accountID, start, end)
		require.NoError(t, err)
		require.Equal(t, uuid.Nil, blockedID)
		require.False(t, shouldCharge, "an invoiced window is terminal")
		require.False(t, reclaimed)
	})

	t.Run("reclaimed pending", func(t *testing.T) {
		store := cycle.NewStore(pool)
		accountID := seedAccount(t, pool)

		firstID, shouldCharge, reclaimed, _, err := store.InsertBillingRun(ctx, accountID, start, end)
		require.NoError(t, err)
		require.True(t, shouldCharge)
		require.False(t, reclaimed)

		reclaimedID, shouldCharge, reclaimed, _, err := store.InsertBillingRun(ctx, accountID, start, end)
		require.NoError(t, err)
		require.True(t, shouldCharge)
		require.True(t, reclaimed)
		require.Equal(t, firstID, reclaimedID, "reclaim must preserve Stripe idempotency identity")

		sc := newFakeStripe()
		svc, p := boundarySvcProposing(store, sc)
		resp, err := svc.WithCreditWallet(false).RunBillingCycle(ctx, accountID, start, end, 0)
		require.NoError(t, err)
		require.True(t, resp.FirstRun, "a reclaimed pending row is an active attempt")
		require.Equal(t, cycle.RunStatusInvoiced, resp.Status)
		require.Zero(t, resp.ChargedCents)
		require.Empty(t, p.groups,
			"a genuinely fresh zero boundary must seal no intent — a $0 document is still a document")
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

	// 🔴 DRIFT — A STALE FREEZE IS RE-DERIVED; A SEALED ONE IS REFUSED (billing-engine#217).
	//
	// Both subtests build a run whose durable commitment and live state
	// DISAGREE: a crashed attempt froze 137 cents, while live state (no usage,
	// no live apps, no domains) derives 0. The deleted collector closed that
	// gap by REUSING the frozen cents verbatim (charge.go:398-402); the intent
	// rail cannot (a frozen total cannot be split back into lines), so it used
	// to refuse unconditionally — and refused for ever, on a number nothing
	// external had ever seen (the 2026-09-11 production run).
	//
	// Owner rule (b): what makes a frozen figure binding is a provider invoice
	// or a sealed intent. Without either the freeze is a stale intention and
	// moves to the live derivation (compare-and-set, audit-logged); with the
	// migration-083 marker (the run was ever handed to the proposer) the
	// refusal stands, naming both numbers.
	t.Run("reclaimed frozen greater than zero, never handed to the proposer: re-frozen at live", func(t *testing.T) {
		store := cycle.NewStore(pool)
		accountID := seedAccount(t, pool)
		installStandardPaymentMethod(t, pool, accountID, "cus_standard_frozen_positive")

		runID, shouldCharge, reclaimed, everProposed, err := store.InsertBillingRun(ctx, accountID, start, end)
		require.NoError(t, err)
		require.True(t, shouldCharge)
		require.False(t, reclaimed)
		require.False(t, everProposed)

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
		svc, p := boundarySvcProposing(store, sc)
		resp, err := svc.WithCreditWallet(false).RunBillingCycle(ctx, accountID, start, end, 0)
		require.NoError(t, err, "a never-presented, never-sealed freeze reconciles instead of refusing")
		require.True(t, resp.FirstRun)

		// The freeze moved to the live figure — 0 — so this is a zero
		// boundary: nothing is sealed (a $0 document is still a document) and
		// the run terminally marks.
		require.Equal(t, cycle.RunStatusInvoiced, resp.Status)
		require.Zero(t, resp.ChargedCents)
		require.Empty(t, p.groups, "a zero boundary seals no intent")
		require.Empty(t, p.charges)

		// Nothing reached the provider; the crash-recovery READ still ran
		// under this run's own charge ref (it is what proved no invoice exists).
		require.Empty(t, sc.invoiceCalls)
		require.Empty(t, sc.itemCalls)
		require.Empty(t, sc.finalizeCalls)
		require.Len(t, sc.findByRefCalls, 1)
		require.Equal(t, "run:"+runID.String(), sc.findByRefCalls[0])

		got := readStandardRun(t, pool, runID)
		require.Equal(t, "invoiced", got.status)
		require.Nil(t, got.stripeInvoice)
		require.Zero(t, got.totalCents)
		require.NotNil(t, got.frozenCents)
		require.Zero(t, *got.frozenCents, "the run marker holds the live figure after the compare-and-set")
	})

	t.Run("reclaimed frozen greater than zero, previously handed to the proposer: refused", func(t *testing.T) {
		store := cycle.NewStore(pool)
		accountID := seedAccount(t, pool)
		installStandardPaymentMethod(t, pool, accountID, "cus_standard_frozen_sealed")

		runID, shouldCharge, reclaimed, everProposed, err := store.InsertBillingRun(ctx, accountID, start, end)
		require.NoError(t, err)
		require.True(t, shouldCharge)
		require.False(t, reclaimed)
		require.False(t, everProposed)

		const frozenCents int64 = 137
		_, claim, err := store.FreezeBillingRunCharge(ctx, runID, cycle.FrozenBoundaryCharge{Cents: frozenCents, WithBase: true})
		require.NoError(t, err)
		require.Equal(t, cycle.StripeRailClaimed, claim)
		// The prior attempt got as far as the proposer (migration-083 marker),
		// then died before the 'proposed' mark: status says nothing, the marker
		// says "maybe sealed". The reclaim's prior CTE must read the marker.
		require.NoError(t, store.MarkBillingRunProposalAttempted(ctx, runID))
		require.NoError(t, store.MarkBillingRun(ctx, runID, cycle.RunStatusFailed, "in_stale", 999))

		sc := newFakeStripe()
		svc, p := boundarySvcProposing(store, sc)
		resp, err := svc.WithCreditWallet(false).RunBillingCycle(ctx, accountID, start, end, 0)

		// The refusal names BOTH numbers: what this run committed to, and what
		// live state now derives.
		require.Error(t, err)
		require.ErrorContains(t, err, "this run froze 137 cents but live state now derives 0",
			"a drifted reclaim on a maybe-sealed run must name the durable amount and the live one")
		require.Nil(t, resp)
		require.Empty(t, p.groups, "a drifted sealed reclaim sealed an amount nobody derived")
		require.Empty(t, p.charges)
		require.Empty(t, sc.invoiceCalls)
		require.Empty(t, sc.itemCalls)
		require.Empty(t, sc.finalizeCalls)

		// The run is left RECLAIMABLE with its commitment intact — 'pending'
		// from the reclaim's own reset, no invoice, the frozen 137 still on the
		// row, and the marker still set for the next reclaim to read.
		got := readStandardRun(t, pool, runID)
		require.Equal(t, "pending", got.status, "a refused reclaim must stay reclaimable, never terminal")
		require.Nil(t, got.stripeInvoice)
		require.Zero(t, got.totalCents)
		require.NotNil(t, got.frozenCents)
		require.EqualValues(t, frozenCents, *got.frozenCents, "the durable commitment must survive the refusal")
		require.NotNil(t, got.frozenWithBase)
		require.True(t, *got.frozenWithBase)
		_, _, _, everProposed, err = store.InsertBillingRun(ctx, accountID, start, end)
		require.NoError(t, err)
		require.True(t, everProposed, "the marker survives every reclaim")
	})

	t.Run("reclaimed frozen equal to zero", func(t *testing.T) {
		store := cycle.NewStore(pool)
		accountID := seedAccount(t, pool)
		installStandardPaymentMethod(t, pool, accountID, "cus_standard_frozen_zero")

		runID, shouldCharge, reclaimed, _, err := store.InsertBillingRun(ctx, accountID, start, end)
		require.NoError(t, err)
		require.True(t, shouldCharge)
		require.False(t, reclaimed)
		_, claim, err := store.FreezeBillingRunCharge(ctx, runID, cycle.FrozenBoundaryCharge{})
		require.NoError(t, err)
		require.Equal(t, cycle.StripeRailClaimed, claim)
		require.NoError(t, store.MarkBillingRun(ctx, runID, cycle.RunStatusFailed, "", 0))

		sc := newFakeStripe()
		svc, p := boundarySvcProposing(store, sc)
		resp, err := svc.WithCreditWallet(false).RunBillingCycle(ctx, accountID, start, end, 0)
		require.NoError(t, err)
		require.True(t, resp.FirstRun)
		require.Equal(t, cycle.RunStatusInvoiced, resp.Status)
		require.Zero(t, resp.ChargedCents)
		require.Empty(t, p.groups,
			"a durable zero is already settled — it must seal no intent either")
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
			runID, shouldCharge, reclaimed, _, err := store.InsertBillingRun(ctx, accountID, start, end)
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
			runID, shouldCharge, reclaimed, _, err := store.InsertBillingRun(ctx, accountID, start, end)
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

			reclaimedID, shouldCharge, reclaimed, _, err := store.InsertBillingRun(ctx, accountID, start, end)
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
