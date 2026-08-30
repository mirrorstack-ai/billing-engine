// Package stripeadapter implements the executor's Collector port
// against Stripe.
//
// It is the first adapter, and it is written to be replaceable:
// docs/DESIGN.md §11 step 7 asks for a second rail built against the
// same conformance suite, which only works if the port stays narrow
// enough that a different provider could satisfy it.
//
// The narrowness is doing real work here. The port is one method taking
// one sealed amount, so this package cannot assemble a charge out of
// parts — it turns one intent into one invoice with one line for the
// sealed total, and finalizes. There is nowhere for a second line, a
// discount, or a differently-presented breakdown to enter, because
// nothing in the request describes one.
package stripeadapter

import (
	"context"
	"errors"
	"fmt"

	"github.com/mirrorstack-ai/billing-engine/internal/intent/executor"
	billingstripe "github.com/mirrorstack-ai/billing-engine/internal/shared/stripe"
)

// CustomerResolver maps a payer to the provider's customer identity.
//
// A port rather than a field on the request. If the caller supplied the
// customer id, a caller could point a sealed intent at somebody else's
// card — the amount would still be the one that was sealed, and it
// would come out of the wrong account. Resolving it here from the
// intent's own payer keeps who-pays derived rather than told.
type CustomerResolver interface {
	StripeCustomerFor(ctx context.Context, payerKind, payerID string) (string, error)
}

// Adapter collects a sealed intent through Stripe.
type Adapter struct {
	client   billingstripe.Client
	resolver CustomerResolver
	// Resolved per collection rather than cached: a payer whose
	// provider identity changed between sealing and collecting should
	// fail to resolve rather than charge the old one.
	descriptionFor func(intentDigest string) string
}

// New returns an Adapter.
func New(client billingstripe.Client, resolver CustomerResolver) *Adapter {
	return &Adapter{
		client:   client,
		resolver: resolver,
		descriptionFor: func(digest string) string {
			// The line description names the intent, so an invoice in
			// the provider's own dashboard can be traced back to the
			// document a customer approved.
			return "MirrorStack charge " + shortDigest(digest)
		},
	}
}

// ErrNoCustomer is returned when a payer has no provider identity.
var ErrNoCustomer = errors.New("stripeadapter: payer has no Stripe customer")

// Collect turns one sealed intent into one invoice and finalizes it.
//
// Every step carries an idempotency key derived from the intent digest,
// so a retried collection is the same three requests at the provider
// rather than three more objects. The digest is the identity of the
// document, which makes it exactly the right thing to key on: two
// collections of one intent are the same collection.
//
// Micro-dollars are converted to whole cents here and nowhere else.
// Stripe amounts are integer minor units, and doing the rounding at the
// boundary means the sealed total stays in the engine's own unit right
// up to the wire.
func (a *Adapter) Collect(ctx context.Context, d executor.Debit) (executor.CollectResult, error) {
	customerID, err := a.resolver.StripeCustomerFor(ctx, d.Payer.Kind, d.Payer.ID)
	if err != nil {
		return executor.CollectResult{}, fmt.Errorf("%w: %w", ErrNoCustomer, err)
	}
	if customerID == "" {
		return executor.CollectResult{}, ErrNoCustomer
	}

	cents := centsFromMicros(d.AmountMicros)
	ref := "intent:" + d.IntentDigest

	invoice, err := a.client.CreateDraftInvoice(ctx, customerID, ref, "inv-"+d.IdempotencyKey)
	if err != nil {
		// Nothing has been finalized, so nothing has been collected.
		// A draft that leaks is inert: it can never charge anyone.
		return executor.CollectResult{}, fmt.Errorf("create draft: %w", err)
	}

	if _, err := a.client.CreateInvoiceItem(
		ctx, customerID, invoice.ID, cents, d.Currency,
		a.descriptionFor(d.IntentDigest), billingstripe.LinePeriod{},
		"ii-"+d.IdempotencyKey,
	); err != nil {
		return executor.CollectResult{}, fmt.Errorf("create line: %w", err)
	}

	finalized, err := a.client.FinalizeInvoice(ctx, invoice.ID, "fin-"+d.IdempotencyKey)
	if err != nil {
		// 🔴 This is the ambiguous case, and it is reported as such
		// rather than as a failure.
		//
		// Finalize is the step that moves money. An error here means
		// the request may have arrived and charged the customer, or may
		// not have. The executor retains its claim on an ambiguous
		// result and records no outcome, so the one thing that must not
		// happen — a second attempt — cannot.
		return executor.CollectResult{Ambiguous: true},
			fmt.Errorf("finalize (outcome unknown): %w", err)
	}

	// Stripe's own status is the evidence. A finalized invoice that is
	// not paid has not collected, whatever the call returned.
	switch finalized.Status {
	case "paid":
		return executor.CollectResult{Succeeded: true, Reference: finalized.ID}, nil
	case "open", "draft":
		// Finalized but not settled: Stripe is still trying, or will
		// report through a callback. Not a failure and not a success.
		return executor.CollectResult{Ambiguous: true, Reference: finalized.ID}, nil
	default:
		// void, uncollectible, or a status this adapter has not been
		// taught. An unrecognised status is NOT read as success.
		return executor.CollectResult{Reference: finalized.ID}, nil
	}
}

// centsFromMicros converts micro-dollars to whole cents, rounding half
// up.
//
// Round-half-up rather than Go's truncation, because truncating always
// favours one party and doing so silently on every line is how a
// systematic difference appears in reconciliation with no single cause
// to point at.
func centsFromMicros(micros int64) int64 {
	if micros < 0 {
		return -((-micros + 5_000) / 10_000)
	}
	return (micros + 5_000) / 10_000
}

func shortDigest(digest string) string {
	if len(digest) <= 12 {
		return digest
	}
	return digest[:12]
}
