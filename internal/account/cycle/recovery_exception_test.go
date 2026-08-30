package cycle_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/mirrorstack-ai/billing-engine/internal/account/cycle"
	billingstripe "github.com/mirrorstack-ai/billing-engine/internal/shared/stripe"
	"github.com/mirrorstack-ai/billing-engine/internal/shared/stripe/stripetest"
)

// 🔴 The exception to "a cut-over leg reaches no provider", pinned so it
// cannot drift into a surprise.
//
// Every leg's crash-recovery path runs BEFORE the `s.proposer != nil`
// branch — domain_charges.go:100 against the cutover at :166. A row
// armed by a LEGACY run, which left a draft or finalized invoice at the
// provider, is therefore still completed after the proposer is armed.
//
// That is deliberate, and the alternative is worse: abandoning a
// finalized invoice strands a charge the customer can see on their card
// statement and nobody can finish or prove. The exception drains — once
// no row carries an unresolved charge-attempt marker, recovery has
// nothing left to complete, which is one of the questions
// scripts/legacy-drop-preconditions.sql asks production.
//
// The test exists because the opposite belief is easy to hold and
// expensive: cmd/billing-cycle's arming log used to claim the worker
// would "collect nothing", which was not true of this path.
func TestRecoveryStillCompletesInFlightLegacyChargesWithAProposerArmed(t *testing.T) {
	// Branch 1: a legacy run left a DRAFT invoice at the provider.
	// Recovery must finish it, and must NOT propose over the top — a
	// proposal here would double-count: once on the customer's card,
	// once in the intent ledger.
	t.Run("an abandoned legacy invoice is completed, not proposed over", func(t *testing.T) {
		recorder := stripetest.New()
		store := newFakeStore()
		p := &capturingProposer{}
		cand := seedDomain(t, store)

		cand.ChargeAttemptedAt = time.Now().UTC().Add(-time.Minute)
		cand.ChargeFundingAccountID = cand.AccountID
		cand.ChargeFundingGeneration = cand.AccountID

		// What the legacy run left behind.
		recorder.Stubs["FindInvoiceByRef"] = billingstripe.Invoice{
			ID: "in_abandoned_by_legacy", Status: "draft", AmountDue: 0,
		}

		svc := cycle.NewService(store, recorder).WithIntentProposer(p)
		_, err := svc.ChargeDomain(context.Background(), cand, time.Now().UTC())
		require.NoError(t, err)

		require.Empty(t, p.charges,
			"the leg proposed a NEW intent for a row whose legacy charge was still in flight at "+
				"the provider; that double-counts the charge — once on the card, once in the ledger")

		require.NotEmpty(t, recorder.CallsWithEffect(stripetest.EffectCollect),
			"recovery abandoned a finalized-or-finalizable legacy invoice; that strands a charge "+
				"the customer can see and nobody can finish or prove")
	})

	// Branch 2: the marker was stamped but nothing reached the provider
	// — a crash between arming and the provider call. There is no
	// in-flight charge to complete, so proposing is the correct answer
	// and no provider may be touched.
	t.Run("a marker with no provider invoice proposes and collects nothing", func(t *testing.T) {
		recorder := stripetest.New()
		store := newFakeStore()
		p := &capturingProposer{}
		cand := seedDomain(t, store)

		cand.ChargeAttemptedAt = time.Now().UTC().Add(-time.Minute)
		cand.ChargeFundingAccountID = cand.AccountID
		cand.ChargeFundingGeneration = cand.AccountID
		// No FindInvoiceByRef stub: the provider has nothing.

		svc := cycle.NewService(store, recorder).WithIntentProposer(p)
		res, err := svc.ChargeDomain(context.Background(), cand, time.Now().UTC())
		require.NoError(t, err)

		require.Equal(t, cycle.DomainChargeProposed, res.Status)
		require.Len(t, p.charges, 1,
			"nothing was in flight at the provider, so the leg should have proposed")
		recorder.RequireNoProviderMutation(t,
			"a cut-over leg recovering a marker with no provider invoice")
	})
}

// The helper test in seal_rounding_test.go proves collectableMicros is
// correct. It proves nothing about whether the seal sites CALL it — which is
// the mistake the reviewers found repeatedly in this migration. This asserts
// the property end-to-end, through the real leg.
func TestASealedDomainChargeIsAlwaysWholeCents(t *testing.T) {
	recorder := stripetest.New()
	store := newFakeStore()
	p := &capturingProposer{}

	// A MID-PERIOD activation, deliberately. The shared seedDomain fixture
	// activates at the period start, so its proration is the whole $20.00 fee
	// — already a round number of cents, which makes it blind to the exact
	// defect this test exists for. Anchoring the account to the 1st and
	// activating on the 13th prorates to 1_225_806 micros: 122.5806 cents.
	cand := seedDomain(t, store)
	acctActivated := time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)
	activated := time.Date(2026, 8, 13, 5, 17, 0, 0, time.UTC)
	cand.AccountActivatedAt = acctActivated
	cand.ActivatedAt = activated
	store.domains[cand.ID].domain.ActivatedAt = activated

	svc := cycle.NewService(store, recorder).WithIntentProposer(p)
	res, err := svc.ChargeDomain(context.Background(), cand, time.Date(2026, 8, 20, 0, 0, 0, 0, time.UTC))
	require.NoError(t, err)
	require.Equal(t, cycle.DomainChargeProposed, res.Status)
	require.Len(t, p.charges, 1)

	sealed := p.charges[0].AmountMicros

	// Non-vacuity guard: if the fixture ever stops producing a fractional
	// derivation, this test silently stops testing rounding. Fail loudly
	// instead — that is how the previous version of this test was hollow.
	require.NotZero(t, res.ChargedCents, "fixture produced a zero charge")
	require.NotEqual(t, int64(1_225_806), sealed,
		"the raw derived micros were sealed — a collection cannot take 122.5806 cents")

	// The invariant: a sealed figure a card cannot be charged is a figure the
	// customer's bundle would attest to and never see on their statement.
	require.Zero(t, sealed%10_000,
		"sealed %d micros is not a whole number of cents", sealed)
	require.Equal(t, res.ChargedCents*10_000, sealed,
		"the sealed amount and the leg's own reported cents disagree")
}

// A proposed domain charge must be walkable back to its intent, and must not
// look like a forgiveness.
//
// The proposed path originally called MarkDomainChargeResolved — the terminal
// NO-CHARGE verdict the period-closed and zero-cent branches take. That
// recorded a sealed obligation as a domain nobody was ever going to bill, and
// left the digest existing only in the return value of a function that had
// already returned. The module-overage leg had it right; the domain leg did
// not, and nothing compared them.
func TestAProposedDomainChargeRecordsItsIntentAndIsNotAForgiveness(t *testing.T) {
	recorder := stripetest.New()
	store := newFakeStore()
	p := &capturingProposer{}
	cand := seedDomain(t, store)

	svc := cycle.NewService(store, recorder).WithIntentProposer(p)
	res, err := svc.ChargeDomain(context.Background(), cand, time.Now().UTC())
	require.NoError(t, err)
	require.Equal(t, cycle.DomainChargeProposed, res.Status)
	require.NotEmpty(t, res.IntentDigest)

	dom := store.domains[cand.ID]
	require.False(t, dom.chargedAt.IsZero(),
		"no charge instant was recorded, so the row reads as a no-charge forgiveness "+
			"rather than a sealed obligation")

	require.Equal(t, "intent:"+res.IntentDigest, dom.chargeInvoiceID,
		"the domain row does not carry the intent reference, so the charge cannot be "+
			"walked back to its document")
	require.True(t, strings.HasPrefix(dom.chargeInvoiceID, "intent:"),
		"an unprefixed digest can be read downstream as a provider invoice id")
	require.Empty(t, dom.chargeInvoiceItemID,
		"a proposed charge has no provider invoice item to name")
}
