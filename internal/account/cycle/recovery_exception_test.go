package cycle_test

import (
	"context"
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
