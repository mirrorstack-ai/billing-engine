package autotopup

import (
	"context"
	"errors"
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
		Payer:                 intent.Subject{Kind: "user", ID: "owner-of-" + c.AccountID},
		Currency:              c.Currency,
		Lines:                 chargeLines(c),
		Kind:                  c.Kind,
		PriceBookRevision:     c.PriceBookRevision,
		TermsRevision:         c.TermsRevision,
		Tax:                   c.Tax,
		AuthorizationID:       c.AuthorizationID,
		NoticePolicy:          c.NoticePolicy,
		ExecuteNotBefore:      c.ExecuteNotBefore,
		ExecuteNotAfter:       c.ExecuteNotAfter,
		SourceFactKeys:        chargeFacts(c),
		SelectedRail:          "stripe",
		RoutingPolicyRevision: "routing-2026-08",
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
	if c.TotalMicros()%microsPerCent != 0 {
		t.Fatalf("sealed %d micros, which is not a whole number of cents", c.TotalMicros())
	}
	if c.TotalMicros() != microsToCentsRoundHalfUp(attempt.AmountMicros)*microsPerCent {
		t.Fatalf("sealed %d micros, not the amount a collection would take", c.TotalMicros())
	}
}

// 🔴 The exception that keeps the boundary leg's bug from repeating: an
// attempt that ALREADY has a provider invoice must not be proposed over —
// that would be a second obligation for the same money. It is finished on the
// rail that started it, which after the drop means settled if Stripe already
// took the money and closed if it did not. Never paid.
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

// 🔴 With NO proposer installed the leg REFUSES. It does not fall back to
// collecting, because the collector it used to fall back to is deleted — and a
// leg that quietly did nothing would be indistinguishable from an account that
// did not need topping up.
func TestAnUnarmedExecutorRefusesInsteadOfCollecting(t *testing.T) {
	now := time.Date(2026, time.July, 25, 1, 2, 3, 0, time.UTC)
	attempt := testAttempt(now, 5_005_000)
	store := newMemoryStore(attempt, AcquireNew)
	stripe := &scriptedStripe{}

	e := NewExecutor(store, &memorySettler{store: store}, stripe).
		WithNow(func() time.Time { return now })

	res, err := e.Trigger(context.Background(), attempt.AccountID, 1_000_000)

	if !errors.Is(err, ErrProposerUnarmed) {
		t.Fatalf("err = %v, want ErrProposerUnarmed", err)
	}
	if len(stripe.sequence) != 0 {
		t.Fatalf("an unarmed leg reached the provider: %v", stripe.sequence)
	}
	if res.Status != "pending" || store.mustGet(attempt.ID).Status != "pending" {
		t.Fatalf("a refused trigger must leave the durable attempt recoverable, got %q/%q",
			res.Status, store.mustGet(attempt.ID).Status)
	}
}

// chargeLines and chargeFacts mirror what the real proposer builds, so a
// capturing fake cannot drift from the seam it stands in for.
func chargeLines(c proposer.Charge) []intent.Line {
	out := make([]intent.Line, 0, len(c.Lines))
	for _, l := range c.Lines {
		out = append(out, intent.NewLine(l.Description, l.SourceRef, "1", 1, l.AmountMicros))
	}
	return out
}

func chargeFacts(c proposer.Charge) []string {
	out := make([]string, 0, len(c.Lines))
	for _, l := range c.Lines {
		out = append(out, l.SourceRef)
	}
	return out
}
