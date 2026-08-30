package autotopup

import (
	"context"
	"testing"
	"time"

	"github.com/mirrorstack-ai/billing-engine/internal/intent"
	"github.com/mirrorstack-ai/billing-engine/internal/intent/proposer"
)

// capturingProposer stands in for the real proposer so the cutover can be
// observed without a database.
type capturingProposer struct{ charges []proposer.Charge }

func (p *capturingProposer) Propose(_ context.Context, c proposer.Charge) (intent.ChargeIntent, error) {
	p.charges = append(p.charges, c)
	return intent.Seal(intent.Draft{
		Payer:             c.Payer,
		Currency:          c.Currency,
		Lines:             []intent.Line{intent.NewLine(c.Description, c.SourceRef, "1", 1, c.AmountMicros)},
		Kind:              c.Kind,
		PriceBookRevision: c.PriceBookRevision,
		TermsRevision:     c.TermsRevision,
		Tax:               c.Tax,
		AuthorizationID:   c.AuthorizationID,
		NoticePolicy:      c.NoticePolicy,
		ExecuteNotBefore:  c.ExecuteNotBefore,
		ExecuteNotAfter:   c.ExecuteNotAfter,
		SourceFactKeys:    []string{c.SourceRef},
	})
}

// 🔴 With a proposer installed the leg seals an intent and reaches NO provider.
func TestAutoTopUpProposesInsteadOfCharging(t *testing.T) {
	now := time.Date(2026, time.July, 25, 1, 2, 3, 0, time.UTC)
	attempt := testAttempt(now, 5_005_000)
	store := newMemoryStore(attempt, AcquireNew)
	stripe := &scriptedStripe{}
	p := &capturingProposer{}

	e := NewExecutor(store, &memorySettler{store: store}, stripe).
		WithNow(func() time.Time { return now }).
		WithIntentProposer(p)

	res, err := e.Trigger(context.Background(), attempt.AccountID, 1_000_000)
	if err != nil {
		t.Fatalf("Trigger: %v", err)
	}
	if res.Status != "proposed" {
		t.Fatalf("status = %q, want proposed — the leg did not take the cutover branch", res.Status)
	}
	if len(p.charges) != 1 {
		t.Fatalf("proposed %d charges, want 1", len(p.charges))
	}
	// The assertion the whole migration exists for.
	if len(stripe.createCalls) != 0 || len(stripe.payCalls) != 0 {
		t.Fatalf("a cut-over auto-top-up reached the provider: %d create, %d pay",
			len(stripe.createCalls), len(stripe.payCalls))
	}

	c := p.charges[0]
	if c.Kind != intent.KindAutoTopUp {
		t.Fatalf("kind = %q, want %q", c.Kind, intent.KindAutoTopUp)
	}
	// The window must contain the seal instant, or the document can never be
	// executed — the defect the boundary leg shipped by reusing a coverage
	// period that had already closed.
	if now.Before(c.ExecuteNotBefore) || now.After(c.ExecuteNotAfter) {
		t.Fatalf("sealed a window [%s, %s] that does not contain the seal instant %s",
			c.ExecuteNotBefore, c.ExecuteNotAfter, now)
	}
	// A figure a card cannot be charged is one the customer's bundle would
	// attest to and never see on their statement.
	if c.AmountMicros%microsPerCent != 0 {
		t.Fatalf("sealed %d micros, which is not a whole number of cents", c.AmountMicros)
	}
	if c.AmountMicros != microsToCentsRoundHalfUp(attempt.AmountMicros)*microsPerCent {
		t.Fatalf("sealed %d micros, not the amount a collection would take", c.AmountMicros)
	}
}

// 🔴 The exception that keeps the boundary leg's bug from repeating: an
// attempt that ALREADY has a provider invoice must not be proposed over. The
// rail that started the charge is the rail that finishes it.
func TestAnAttemptWithAProviderInvoiceIsNotProposedOver(t *testing.T) {
	now := time.Date(2026, time.July, 25, 1, 2, 3, 0, time.UTC)
	attempt := testAttempt(now, 5_005_000)
	attempt.StripeInvoiceID = "in_already_at_stripe"
	store := newMemoryStore(attempt, AcquireExisting)
	stripe := &scriptedStripe{}
	p := &capturingProposer{}

	e := NewExecutor(store, &memorySettler{store: store}, stripe).
		WithNow(func() time.Time { return now }).
		WithIntentProposer(p)

	_, _ = e.Trigger(context.Background(), attempt.AccountID, 1_000_000)

	if len(p.charges) != 0 {
		t.Fatal("the leg sealed an intent for an attempt that already had an invoice at the " +
			"provider — that is a second obligation for the same money")
	}
}
