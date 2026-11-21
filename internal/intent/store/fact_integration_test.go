//go:build integration

package store

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/mirrorstack-ai/billing-engine/internal/intent/evidence/evidencetest"
	"github.com/mirrorstack-ai/billing-engine/internal/shared/paymentfact"
	"github.com/mirrorstack-ai/billing-engine/internal/shared/testutil"
)

func validFact(accountID uuid.UUID) paymentfact.New {
	return paymentfact.New{
		Rail: paymentfact.RailStripe, RailRef: "in_store_fact_test", Mode: paymentfact.ModeLive,
		SourceKind: paymentfact.SourceEngineSettlement, PayerAccountID: accountID,
		Currency: "usd", NetAmountMicros: 900, TaxAmountMicros: 100, GrossAmountMicros: 1_000,
		PaidAt: time.Now(), InvoiceDecision: paymentfact.DecisionNotApplicable,
		DecisionReason: "foreign_seller_usd",
	}
}

// billing-engine#313's own rule: a succeeded settlement, its evidence and its
// payment fact commit in ONE transaction.
func TestRecordSettlementWithFactWritesAllThreeTogether(t *testing.T) {
	pool := testutil.NewTestDB(t)
	s := New(pool)
	ctx := context.Background()

	accountID := uuid.New()
	_, err := pool.Exec(ctx,
		`INSERT INTO ms_billing.accounts (id, owner_kind, owner_user_id) VALUES ($1, 'user', $2)`,
		accountID, uuid.New())
	require.NoError(t, err)

	sealed := sealedFixture(t, 7)
	require.NoError(t, s.SaveIntent(ctx, sealed))
	require.NoError(t, s.ClaimSettlement(ctx, sealed.Digest(), "fact-test"))

	require.NoError(t, s.RecordSettlementWithFact(ctx, evidencetest.Recorder(t),
		sealed.Digest(), "in_store_fact_test", settlementEvent(sealed), validFact(accountID)))

	var outcome, ref *string
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT outcome, provider_reference FROM ms_billing.intent_settlement_claims WHERE intent_digest = $1`,
		sealed.Digest()).Scan(&outcome, &ref))
	require.NotNil(t, outcome)
	require.Equal(t, "succeeded", *outcome)
	require.Equal(t, "in_store_fact_test", *ref)

	var evidenceCount int
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT count(*) FROM ms_billing.evidence_records WHERE intent_digest = $1`,
		sealed.Digest()).Scan(&evidenceCount))
	require.Equal(t, 1, evidenceCount)

	var factCount int
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT count(*) FROM ms_billing.payment_facts WHERE rail_ref = 'in_store_fact_test'`).
		Scan(&factCount))
	require.Equal(t, 1, factCount)
}

// A fact-write failure must fail the whole settlement — there is never a
// settled payment without a fact.
func TestAFactWriteFailureRollsBackTheSettlementAndItsEvidence(t *testing.T) {
	pool := testutil.NewTestDB(t)
	s := New(pool)
	ctx := context.Background()

	sealed := sealedFixture(t, 8)
	require.NoError(t, s.SaveIntent(ctx, sealed))
	require.NoError(t, s.ClaimSettlement(ctx, sealed.Digest(), "fact-test"))

	// payer_account_id has no matching accounts row: the fact insert fails
	// its FK, and that must take the outcome and evidence rows with it.
	badFact := validFact(uuid.New())
	err := s.RecordSettlementWithFact(ctx, evidencetest.Recorder(t),
		sealed.Digest(), "in_rollback_test", settlementEvent(sealed), badFact)
	require.Error(t, err)

	var outcome *string
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT outcome FROM ms_billing.intent_settlement_claims WHERE intent_digest = $1`,
		sealed.Digest()).Scan(&outcome))
	require.Nil(t, outcome, "the outcome was recorded despite the fact write failing")

	var evidenceCount int
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT count(*) FROM ms_billing.evidence_records WHERE intent_digest = $1`,
		sealed.Digest()).Scan(&evidenceCount))
	require.Zero(t, evidenceCount, "the evidence record survived a rolled-back settlement")
}
