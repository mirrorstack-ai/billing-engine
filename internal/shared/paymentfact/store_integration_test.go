//go:build integration

package paymentfact

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/mirrorstack-ai/billing-engine/internal/shared/testutil"
)

func seedAccount(t *testing.T, pool *pgxpool.Pool) uuid.UUID {
	t.Helper()
	id := uuid.New()
	_, err := pool.Exec(context.Background(),
		`INSERT INTO ms_billing.accounts (id, owner_kind, owner_user_id) VALUES ($1, 'user', $2)`,
		id, uuid.New())
	require.NoError(t, err)
	return id
}

// A NewebPay notice has no event id (billing-engine#296) — the ingress is
// at-least-once and the whole reason (rail, rail_ref, mode) is the identity.
// This is the acceptance criterion verbatim: "a replay of the same notice
// produces one fact".
func TestRecordTxReplayProducesOneFact(t *testing.T) {
	pool := testutil.NewTestDB(t)
	store := NewStore(pool)
	ctx := context.Background()
	accountID := seedAccount(t, pool)

	n := New{
		Rail:              RailNewebPay,
		RailRef:           "MS0001-MerchantOrderNo-TradeNo",
		Mode:              ModeLive,
		SourceKind:        SourceRailNotice,
		PayerAccountID:    accountID,
		Currency:          "twd",
		NetAmountMicros:   950_000,
		TaxAmountMicros:   50_000,
		GrossAmountMicros: 1_000_000,
		PaidAt:            time.Now().UTC().Truncate(time.Second),
		InvoiceDecision:   DecisionPending,
	}

	first, created, err := store.Record(ctx, n)
	require.NoError(t, err)
	require.True(t, created)

	second, created, err := store.Record(ctx, n)
	require.NoError(t, err)
	require.False(t, created, "a replayed notice must not mint a second fact")
	require.Equal(t, first.ID, second.ID)

	var count int
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT count(*) FROM ms_billing.payment_facts WHERE rail = $1 AND rail_ref = $2 AND mode = $3`,
		string(RailNewebPay), n.RailRef, string(ModeLive)).Scan(&count))
	require.Equal(t, 1, count, "exactly one fact must exist for this (rail, rail_ref, mode)")
}

// The same rail_ref under a DIFFERENT mode is a different payment (a sandbox
// test of the same merchant order number must never collide with the live
// one), so it must mint its own fact.
func TestRecordTxDifferentModeIsADifferentFact(t *testing.T) {
	pool := testutil.NewTestDB(t)
	store := NewStore(pool)
	ctx := context.Background()
	accountID := seedAccount(t, pool)

	base := New{
		Rail: RailTapPay, RailRef: "rec_trade_id_1", SourceKind: SourceRailNotice,
		PayerAccountID: accountID, Currency: "twd",
		NetAmountMicros: 100, TaxAmountMicros: 0, GrossAmountMicros: 100,
		PaidAt: time.Now(), InvoiceDecision: DecisionPending,
	}

	live := base
	live.Mode = ModeLive
	_, created, err := store.Record(ctx, live)
	require.NoError(t, err)
	require.True(t, created)

	sandbox := base
	sandbox.Mode = ModeSandbox
	_, created, err = store.Record(ctx, sandbox)
	require.NoError(t, err)
	require.True(t, created, "sandbox and live must never collapse into one fact")
}

// A Stripe payment in this tree is always a USD charge by the US C-corp, which
// cannot produce a 統一發票 — the consumer writing it already knows that, so
// the fact is written DECIDED, not pending.
func TestRecordTxAStripePaymentWritesNotApplicableWithItsReason(t *testing.T) {
	pool := testutil.NewTestDB(t)
	store := NewStore(pool)
	ctx := context.Background()
	accountID := seedAccount(t, pool)

	f, created, err := store.Record(ctx, New{
		Rail: RailStripe, RailRef: "in_1A2B3C", Mode: ModeLive,
		SourceKind: SourceEngineSettlement, PayerAccountID: accountID,
		Currency: "usd", NetAmountMicros: 9_000_000, TaxAmountMicros: 1_000_000,
		GrossAmountMicros: 10_000_000, PaidAt: time.Now(),
		InvoiceDecision: DecisionNotApplicable, DecisionReason: "foreign_seller_usd",
	})
	require.NoError(t, err)
	require.True(t, created)
	require.Equal(t, DecisionNotApplicable, f.InvoiceDecision)
	require.Equal(t, "foreign_seller_usd", f.DecisionReason)

	loaded, err := store.Load(ctx, RailStripe, "in_1A2B3C", ModeLive)
	require.NoError(t, err)
	require.Equal(t, "foreign_seller_usd", loaded.DecisionReason)
}

// billing-engine#267's operator paid-mark verb: a manual_paid_mark fact is
// written once recorded payments reach the statement's total, never before.
// #267 has not merged in this tree, so this stands in for it with a fake
// statement total and StatementPaidMarkDue, the hook that writer adopts.
func TestManualPaidMarkWritesOnlyWhenTheStatementTotalIsReached(t *testing.T) {
	pool := testutil.NewTestDB(t)
	store := NewStore(pool)
	ctx := context.Background()
	accountID := seedAccount(t, pool)

	const statementTotal = 5_000_000
	attempt := func(recorded int64) (created bool) {
		if !StatementPaidMarkDue(recorded, statementTotal) {
			return false
		}
		_, created, err := store.Record(ctx, New{
			Rail: RailBankTransfer, RailRef: "statement-7", Mode: ModeLive,
			SourceKind: SourceManualPaidMark, PayerAccountID: accountID,
			Currency: "twd", NetAmountMicros: statementTotal, TaxAmountMicros: 0,
			GrossAmountMicros: statementTotal, PaidAt: time.Now(),
			InvoiceDecision: DecisionDeferred, DecisionReason: "pre_registration_statement",
		})
		require.NoError(t, err)
		return created
	}

	require.False(t, attempt(3_000_000), "short of the total must not write a fact")

	var count int
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT count(*) FROM ms_billing.payment_facts WHERE rail_ref = 'statement-7'`).Scan(&count))
	require.Equal(t, 0, count)

	require.True(t, attempt(5_000_000), "reaching the total must write the fact")
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT count(*) FROM ms_billing.payment_facts WHERE rail_ref = 'statement-7'`).Scan(&count))
	require.Equal(t, 1, count)
}

// A fact-write failure must fail the settlement transaction — the vendor call
// is never inside it, but the fact and the money-real step commit or roll
// back together.
func TestAForcedFactInsertFailureRollsBackTheWholeTransaction(t *testing.T) {
	pool := testutil.NewTestDB(t)
	ctx := context.Background()
	accountID := seedAccount(t, pool)

	// Force an actual database-level failure INSIDE a transaction that also
	// does the "the money is real" write (here, a marker account row standing
	// in for a settlement UPDATE), and prove the marker never lands either.
	markerID := uuid.New()
	err := pgx.BeginFunc(ctx, pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx,
			`INSERT INTO ms_billing.accounts (id, owner_kind, owner_user_id) VALUES ($1, 'user', $2)`,
			markerID, uuid.New()); err != nil {
			return err
		}
		// Violates payment_facts_gross_is_net_plus_tax at the database —
		// written as raw SQL, bypassing RecordTx's own Go-side validate(), to
		// prove the DATABASE constraint is what a writer is relying on, not
		// only the Go guard in front of it.
		_, err := tx.Exec(ctx, `
			INSERT INTO ms_billing.payment_facts
			  (rail, rail_ref, mode, source_kind, payer_account_id, currency,
			   net_amount_micros, tax_amount_micros, gross_amount_micros, paid_at,
			   invoice_decision, decision_reason)
			VALUES ('stripe', 'in_bad_math', 'live', 'engine_settlement', $1, 'usd',
			        100, 100, 999, now(), 'not_applicable', 'foreign_seller_usd')`,
			accountID)
		return err
	})
	require.Error(t, err, "an insert that violates the fact's own invariant must fail")

	var markerCount int
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT count(*) FROM ms_billing.accounts WHERE id = $1`, markerID).Scan(&markerCount))
	require.Equal(t, 0, markerCount,
		"the settlement half of the transaction must have rolled back along with the failed fact insert")

	var factCount int
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT count(*) FROM ms_billing.payment_facts WHERE rail_ref = 'in_bad_math'`).Scan(&factCount))
	require.Equal(t, 0, factCount)
}

// The append-only guard: nothing may rewrite a sealed column, and only the
// decision pair may move.
func TestPaymentFactsAreAppendOnlyExceptTheDecisionPair(t *testing.T) {
	pool := testutil.NewTestDB(t)
	store := NewStore(pool)
	ctx := context.Background()
	accountID := seedAccount(t, pool)

	f, _, err := store.Record(ctx, New{
		Rail: RailStripe, RailRef: "in_seal_test", Mode: ModeLive,
		SourceKind: SourceEngineSettlement, PayerAccountID: accountID,
		Currency: "usd", NetAmountMicros: 90, TaxAmountMicros: 10, GrossAmountMicros: 100,
		PaidAt: time.Now(), InvoiceDecision: DecisionPending,
	})
	require.NoError(t, err)

	_, err = pool.Exec(ctx,
		`UPDATE ms_billing.payment_facts SET invoice_decision = 'not_applicable', decision_reason = 'foreign_seller_usd' WHERE id = $1`,
		f.ID)
	require.NoError(t, err, "the decision pair must be writable")

	_, err = pool.Exec(ctx,
		`UPDATE ms_billing.payment_facts SET gross_amount_micros = 999 WHERE id = $1`, f.ID)
	require.Error(t, err, "a sealed column must refuse an edit")

	_, err = pool.Exec(ctx, `DELETE FROM ms_billing.payment_facts WHERE id = $1`, f.ID)
	require.Error(t, err, "a payment fact must never be deleted")
}
