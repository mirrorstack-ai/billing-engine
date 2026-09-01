//go:build integration

package store

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/mirrorstack-ai/billing-engine/internal/intent"
	"github.com/mirrorstack-ai/billing-engine/internal/intent/evidence"
	"github.com/mirrorstack-ai/billing-engine/internal/intent/evidence/evidencetest"
	"github.com/mirrorstack-ai/billing-engine/internal/shared/testutil"
)

// 🔴 A SETTLEMENT MUST NAME WHAT SETTLED IT.
//
// The executor returned Outcome{Reference: result.Reference} — the Stripe
// invoice id the money moved through — and RecordOutcomeWithEvidence stored
// only the outcome and its timestamp. So after a successful collection nothing
// mapped the provider's object back to the sealed document that authorised it.
//
// That is the concrete blocker for §6's collect_receivable: a receivable is
// CollectRemainderOf(source) and links to a SOURCE INTENT, so retrying an
// unpaid invoice requires knowing which intent raised it.
func TestASucceededSettlementRecordsItsProviderObject(t *testing.T) {
	pool := testutil.NewTestDB(t)
	s := New(pool)
	ctx := context.Background()

	sealed := sealedFixture(t, 7)
	require.NoError(t, s.SaveIntent(ctx, sealed))
	require.NoError(t, s.ClaimSettlement(ctx, sealed.Digest(), "reference-test"))
	require.NoError(t, s.RecordOutcomeWithEvidence(ctx, evidencetest.Recorder(t),
		sealed.Digest(), "succeeded", "in_provider_1", settlementEvent(sealed)))

	var ref *string
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT provider_reference FROM ms_billing.intent_settlement_claims WHERE intent_digest = $1`,
		sealed.Digest()).Scan(&ref))
	require.NotNil(t, ref, "the settlement recorded no provider object; nothing can walk from the "+
		"invoice back to the intent that authorised it")
	require.Equal(t, "in_provider_1", *ref)
}

// And it must REFUSE a settlement that cannot name one, rather than writing a
// row that claims money moved through nothing.
func TestASucceededSettlementWithNoReferenceIsRefused(t *testing.T) {
	pool := testutil.NewTestDB(t)
	s := New(pool)
	ctx := context.Background()

	sealed := sealedFixture(t, 7)
	require.NoError(t, s.SaveIntent(ctx, sealed))
	require.NoError(t, s.ClaimSettlement(ctx, sealed.Digest(), "reference-test"))

	err := s.RecordOutcomeWithEvidence(ctx, evidencetest.Recorder(t),
		sealed.Digest(), "succeeded", "", settlementEvent(sealed))
	require.Error(t, err, "a settlement with no provider reference was recorded as succeeded")
	require.Contains(t, err.Error(), "provider reference")

	var outcome *string
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT outcome FROM ms_billing.intent_settlement_claims WHERE intent_digest = $1`,
		sealed.Digest()).Scan(&outcome))
	require.Nil(t, outcome, "the refused settlement still wrote an outcome")
}

// A FAILED attempt may never have reached a provider, so it is exempt — and
// must still be recordable, or a refusal at the rail could not be written down.
func TestAFailedSettlementNeedsNoReference(t *testing.T) {
	pool := testutil.NewTestDB(t)
	s := New(pool)
	ctx := context.Background()

	sealed := sealedFixture(t, 7)
	require.NoError(t, s.SaveIntent(ctx, sealed))
	require.NoError(t, s.ClaimSettlement(ctx, sealed.Digest(), "reference-test"))
	require.NoError(t, s.RecordOutcomeWithEvidence(ctx, evidencetest.Recorder(t),
		sealed.Digest(), "failed", "", settlementEvent(sealed)))

	var outcome string
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT outcome FROM ms_billing.intent_settlement_claims WHERE intent_digest = $1`,
		sealed.Digest()).Scan(&outcome))
	require.Equal(t, "failed", outcome)
}

func settlementEvent(sealed intent.ChargeIntent) evidence.Event {
	return evidence.Event{
		Kind:         evidence.KindSettlement,
		Subject:      sealed.Payer(),
		IntentDigest: sealed.Digest(),
		Detail:       "test",
		OccurredAt:   evidencetest.At,
	}
}
