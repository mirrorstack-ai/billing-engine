package creditpurchase

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/mirrorstack-ai/billing-engine/internal/intent"
	"github.com/mirrorstack-ai/billing-engine/internal/intent/proposer"
	billingstripe "github.com/mirrorstack-ai/billing-engine/internal/shared/stripe"
)

// capturingProposer records what the leg sealed without needing a database.
type capturingProposer struct {
	charges []proposer.Charge
	err     error
}

func (p *capturingProposer) Propose(_ context.Context, c proposer.Charge) (intent.ChargeIntent, error) {
	if p.err != nil {
		return intent.ChargeIntent{}, p.err
	}
	p.charges = append(p.charges, c)
	lines := make([]intent.Line, 0, len(c.Lines))
	facts := make([]string, 0, len(c.Lines))
	for _, l := range c.Lines {
		lines = append(lines, intent.NewLine(l.Description, l.SourceRef, "1", 1, l.AmountMicros))
		facts = append(facts, l.SourceRef)
	}
	return intent.Seal(intent.Draft{
		Payer:                 intent.Subject{Kind: "user", ID: "owner-of-" + c.AccountID},
		Currency:              c.Currency,
		Lines:                 lines,
		Kind:                  c.Kind,
		PriceBookRevision:     c.PriceBookRevision,
		TermsRevision:         c.TermsRevision,
		Tax:                   c.Tax,
		AuthorizationID:       c.AuthorizationID,
		NoticePolicy:          c.NoticePolicy,
		SelectedRail:          c.SelectedRail,
		RoutingPolicyRevision: c.RoutingPolicyRevision,
		ExecuteNotBefore:      c.ExecuteNotBefore,
		ExecuteNotAfter:       c.ExecuteNotAfter,
		SourceFactKeys:        facts,
	})
}

func pendingPurchase() Attempt {
	return Attempt{
		ID:                uuid.New(),
		AccountID:         uuid.New(),
		AmountMicros:      25_000_000,
		Status:            "pending",
		FundingAccountID:  uuid.New(),
		FundingGeneration: uuid.New(),
		StripeCustomerID:  "cus_credit_1",
	}
}

func cutoverExecutor(t *testing.T, a Attempt, p chargeProposer) (*Executor, *fakeStore, *fakeStripe) {
	t.Helper()
	store := &fakeStore{attempt: a}
	sc := &fakeStripe{}
	e := NewExecutor(store, &fakeSettler{}, sc).
		WithNow(func() time.Time { return time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC) }).
		WithIntentProposer(p)
	return e, store, sc
}

// 🔴 THE LAST LEG, AND THE ONLY ONE WHERE THE CUSTOMER IS WAITING.
//
// The assertion that matters is the same as every other leg's: NOTHING REACHED
// STRIPE. A cut-over leg holds no write port.
func TestTheCreditPurchaseLegProposesInsteadOfCharging(t *testing.T) {
	a := pendingPurchase()
	p := &capturingProposer{}
	e, _, sc := cutoverExecutor(t, a, p)

	res, err := e.Resume(context.Background(), a)
	require.NoError(t, err)

	require.Equal(t, "proposed", res.Attempt.Status, "the leg did not take the cutover branch")
	require.NotEmpty(t, res.Attempt.ProposedReference,
		"a proposed purchase carries no reference, so the row cannot be walked to its intent")
	require.Empty(t, res.Attempt.StripeInvoiceID, "a proposed purchase reported a Stripe invoice")

	require.Zero(t, sc.createInvoiceCalls, "a cut-over credit purchase created a Stripe invoice")
	require.Zero(t, sc.finalizeCalls, "a cut-over credit purchase finalized a Stripe invoice")

	require.Len(t, p.charges, 1)
	c := p.charges[0]
	require.Equal(t, intent.KindCreditPurchase, c.Kind)
	require.EqualValues(t, a.AmountMicros, c.TotalMicros(),
		"the amount changed in the cutover")
}

// 🔴 §6: buying credit is not a service you consumed. walletFunding = 0.
//
// A purchase funded from the wallet it is topping up is circular — it would
// spend the balance to increase it — and the funding choice would move the
// taxable basis. The whole obligation is the provider's to collect.
func TestACreditPurchaseIsNeverFundedFromTheWallet(t *testing.T) {
	a := pendingPurchase()
	p := &capturingProposer{}
	e, _, _ := cutoverExecutor(t, a, p)

	_, err := e.Resume(context.Background(), a)
	require.NoError(t, err)

	require.Zero(t, p.charges[0].WalletAllocationMicros,
		"the credit purchase allocated wallet credit to itself; §6 requires "+
			"walletFunding = 0 and providerRemainder = grossObligation")
}

// The reference is prefixed so nothing downstream can read a digest as a
// provider object id — migration 057's own reason for the column.
func TestTheProposedReferenceIsPrefixed(t *testing.T) {
	a := pendingPurchase()
	e, store, _ := cutoverExecutor(t, a, &capturingProposer{})

	res, err := e.Resume(context.Background(), a)
	require.NoError(t, err)

	require.Regexp(t, `^intent:[0-9a-f]{64}$`, res.Attempt.ProposedReference,
		"the reference is not a prefixed digest, so a reader could mistake it for a "+
			"Stripe object id")
	require.Len(t, store.proposedRefs, 1)
	require.Equal(t, res.Attempt.ProposedReference, store.proposedRefs[0],
		"the durable marker and the returned attempt disagree about which intent this is")
}

// An attempt that already reached Stripe must be FINISHED there, never
// proposed. Abandoning a finalized invoice strands a charge the customer can
// see and nobody can prove.
func TestAPurchaseThatAlreadyReachedStripeIsNeverProposed(t *testing.T) {
	a := pendingPurchase()
	a.StripeInvoiceID = "in_already_there"
	p := &capturingProposer{}
	e, _, _ := cutoverExecutor(t, a, p)

	_, _ = e.Resume(context.Background(), a)

	require.Empty(t, p.charges,
		"a purchase with an invoice at the provider was proposed instead of finished; "+
			"the recovery guard is what keeps that charge provable")
}

// The inverse of the test this replaces, which asserted that an unarmed leg
// still collected. That path is deleted, so the assertion is now that an
// unarmed leg REFUSES — loudly, without touching Stripe and without moving the
// row — rather than silently doing nothing, which is the failure the old test
// was written to catch.
func TestWithoutAProposerAFreshPurchaseIsRefusedNotCharged(t *testing.T) {
	a := pendingPurchase()
	store := &fakeStore{attempt: a}
	sc := &fakeStripe{}
	e := NewExecutor(store, &fakeSettler{}, sc)

	_, err := e.Resume(context.Background(), a)

	require.Error(t, err, "an unarmed leg reported success without proposing or charging")
	require.Zero(t, sc.getCalls, "a fresh purchase reached Stripe at all")
	require.Zero(t, sc.createInvoiceCalls, "the legacy collector is still reachable")
	require.Equal(t, "pending", store.attempt.Status,
		"the row moved without either a sealed intent or a charge")
}

// 🔴 The charge that ALREADY REACHED THE PROVIDER must still finish.
//
// This is what the in-flight guard is for: the money moved, and this is the
// only path that can prove it into the wallet. An armed leg must settle it
// rather than seal a second obligation beside it.
func TestAnInFlightPaidPurchaseStillSettlesWhileArmed(t *testing.T) {
	attempt := testAttempt("pending")
	invoice := exactInvoice(attempt, "paid")
	store := &fakeStore{attempt: attempt}
	settler := &fakeSettler{store: store}
	sc := &fakeStripe{
		invoice:  invoice,
		items:    []billingstripe.InvoiceItem{exactItem(attempt)},
		payments: []billingstripe.InvoicePaymentProof{exactPayment(attempt, invoice)},
	}
	p := &capturingProposer{}
	e := NewExecutor(store, settler, sc).WithIntentProposer(p)

	result, err := e.Resume(context.Background(), attempt)

	require.NoError(t, err)
	require.True(t, result.Settlement.Transitioned,
		"a paid in-flight charge was not settled; the customer paid and the wallet did not move")
	require.Equal(t, 1, settler.calls)
	require.Empty(t, p.charges,
		"a charge already collected at the provider was also sealed as an intent")
}

// A failed proposal must not fall back to charging. The customer is waiting,
// so the temptation to "just collect it" is real, and taking it would mean a
// charge with no sealed document behind it.
func TestAFailedProposalDoesNotFallBackToCharging(t *testing.T) {
	a := pendingPurchase()
	p := &capturingProposer{err: errors.New("proposal refused")}
	e, store, sc := cutoverExecutor(t, a, p)

	_, err := e.Resume(context.Background(), a)
	require.Error(t, err, "a failed proposal was reported as success")

	require.Zero(t, sc.createInvoiceCalls, "a failed proposal fell back to charging the customer")
	require.Equal(t, "pending", store.attempt.Status,
		"the row moved despite the proposal failing; it must stay pending and retryable")
	require.Empty(t, store.proposedRefs)
}

// A lost race is not a fault. Another worker moving the row first leaves the
// intent sealed either way — Propose is idempotent on the digest.
func TestALostRaceIsNotAnError(t *testing.T) {
	a := pendingPurchase()
	p := &capturingProposer{}
	store := &fakeStore{attempt: a}
	store.attempt.Status = "settled" // the winner already moved it
	e := NewExecutor(store, &fakeSettler{}, &fakeStripe{}).WithIntentProposer(p)

	_, err := e.Resume(context.Background(), Attempt{
		ID: a.ID, AccountID: a.AccountID, AmountMicros: a.AmountMicros,
		Status: "pending", FundingAccountID: a.FundingAccountID,
		FundingGeneration: a.FundingGeneration, StripeCustomerID: a.StripeCustomerID,
	})
	require.NoError(t, err, "a lost race was reported as a fault")
}

// Proposing without arming is impossible, and armed is observable.
func TestTheSeamIsObservable(t *testing.T) {
	require.False(t, NewExecutor(&fakeStore{}, &fakeSettler{}, &fakeStripe{}).IntentProposerArmed())
	require.True(t, NewExecutor(&fakeStore{}, &fakeSettler{}, &fakeStripe{}).
		WithIntentProposer(&capturingProposer{}).IntentProposerArmed())
}
