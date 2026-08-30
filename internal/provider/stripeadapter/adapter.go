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
	"strings"

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
	// ResolvePayer returns the provider customer and the payment
	// method to charge.
	//
	// The instrument is resolved here for the same reason the customer
	// is: which card gets charged must be derived from the payer, not
	// supplied alongside the amount. It is returned rather than left to
	// the provider's own default so the pay step can be keyed — see
	// Collect.
	ResolvePayer(ctx context.Context, payerKind, payerID string) (customerID, paymentMethodID string, err error)
}

// InvoiceRail is the slice of the provider client this adapter needs.
//
// Declared here rather than reusing billingstripe.Client, because that
// interface also carries customer administration and provider reads —
// and an adapter holding those could do things the port it implements
// cannot express. Naming exactly four methods is what makes "this
// package can create an invoice and pay it, and nothing else" a fact a
// reader can check.
type InvoiceRail interface {
	CreateDraftInvoice(ctx context.Context, custID, ref, idemKey string) (billingstripe.Invoice, error)
	CreateInvoiceItem(ctx context.Context, custID, invoiceID string, amountCents int64, currency, desc string, period billingstripe.LinePeriod, idemKey string) (billingstripe.InvoiceItem, error)
	FinalizeInvoiceWithoutAutoAdvance(ctx context.Context, invoiceID, idemKey string) (billingstripe.Invoice, error)
	PayInvoiceWithMethod(ctx context.Context, invoiceID, paymentMethodID, idemKey string) (billingstripe.Invoice, error)
}

// Adapter collects a sealed intent through Stripe.
type Adapter struct {
	client   InvoiceRail
	resolver CustomerResolver
	// Resolved per collection rather than cached: a payer whose
	// provider identity changed between sealing and collecting should
	// fail to resolve rather than charge the old one.
	descriptionFor func(intentDigest string) string
}

// New returns an Adapter.
func New(client InvoiceRail, resolver CustomerResolver) *Adapter {
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
	customerID, paymentMethodID, err := a.resolver.ResolvePayer(ctx, d.Payer.Kind, d.Payer.ID)
	if err != nil {
		return executor.CollectResult{}, fmt.Errorf("%w: %w", ErrNoCustomer, err)
	}
	if customerID == "" || paymentMethodID == "" {
		return executor.CollectResult{}, ErrNoCustomer
	}

	cents := centsFromMicros(d.AmountMicros)
	ref := "intent:" + d.IntentDigest

	// Stripe takes the currency lowercase, and every legacy call site in
	// this repo sends the lowercase constant. Seal stores the canonical
	// upper-case ISO 4217 code, which is right INSIDE the intent and wrong
	// on the wire — so the normalization belongs here, at the provider
	// boundary, and not in the document.
	//
	// Without it the intent rail sends "USD" where the legacy rail sends
	// "usd": the same money described two ways to the same provider, which
	// is the kind of difference that shows up as an unexplained
	// reconciliation mismatch rather than an error.
	currency := strings.ToLower(strings.TrimSpace(d.Currency))

	invoice, err := a.client.CreateDraftInvoice(ctx, customerID, ref, "inv-"+d.IdempotencyKey)
	if err != nil {
		// Nothing has been finalized, so nothing has been collected.
		// A draft that leaks is inert: it can never charge anyone.
		return executor.CollectResult{}, fmt.Errorf("create draft: %w", err)
	}

	if _, err := a.client.CreateInvoiceItem(
		ctx, customerID, invoice.ID, cents, currency,
		a.descriptionFor(d.IntentDigest), billingstripe.LinePeriod{},
		"ii-"+d.IdempotencyKey,
	); err != nil {
		return executor.CollectResult{}, fmt.Errorf("create line: %w", err)
	}

	// Finalize WITHOUT auto-advance, then pay explicitly.
	//
	// Handing the invoice to Stripe's automatic collection would make
	// the money-moving step something that happens later, on Stripe's
	// schedule, with no answer to return. Measured: an auto-advancing
	// test-mode invoice sat `open` indefinitely.
	//
	// It is also the wrong shape for an intent. docs/DESIGN.md §4 wants
	// one permit and one request, with the permit spent by the send —
	// which requires a send this code makes and an answer it receives.
	// The engine's existing auto-top-up path reached the same
	// conclusion (internal/account/autotopup/executor.go).
	finalized, err := a.client.FinalizeInvoiceWithoutAutoAdvance(
		ctx, invoice.ID, "fin-"+d.IdempotencyKey)
	if err != nil {
		// Unambiguous: a finalized-but-unpaid invoice has collected
		// nothing, and one that failed to finalize collected less.
		return executor.CollectResult{}, fmt.Errorf("finalize: %w", err)
	}
	_ = finalized

	// Named the payment method, and KEYED.
	//
	// The unkeyed Invoices.Pay would work, and internal/architecture
	// flagged it: it is the one collecting call in this tree with no
	// deterministic key, so a retry after an ambiguous answer is a
	// second charge the provider cannot deduplicate. An intent's whole
	// point is that retrying it is safe, so the intent path uses the
	// keyed form and carries the instrument to do so.
	paid, err := a.client.PayInvoiceWithMethod(
		ctx, invoice.ID, paymentMethodID, "pay-"+d.IdempotencyKey)
	if err != nil {
		// 🔴 The ambiguous case, reported as such rather than as a
		// failure. Pay is the step that moves money, so an error here
		// means the request may have arrived and charged the customer,
		// or may not have. The executor retains its claim on ambiguity
		// and records no outcome, so the one thing that must not
		// happen — a second attempt — cannot.
		return executor.CollectResult{Ambiguous: true, Reference: invoice.ID},
			fmt.Errorf("pay (outcome unknown): %w", err)
	}
	finalized = paid

	// Stripe's own status is the evidence. A finalized invoice that is
	// not paid has not collected, whatever the call returned.
	switch finalized.Status {
	case "paid":
		return executor.CollectResult{Succeeded: true, Reference: finalized.ID}, nil
	case "open", "draft":
		// Finalized and not yet settled. Stripe's finalize returns
		// BEFORE it collects, so this is the ordinary path rather than
		// an anomaly — measured against the sandbox, an invoice is
		// routinely `open` at the moment the call comes back.
		//
		// Reported as in-progress rather than ambiguous. Both retain
		// the claim, and the difference is what anyone reading the
		// state later can conclude: in-progress means the rail knows
		// and will say, ambiguous means nobody does.
		return executor.CollectResult{InProgress: true, Reference: finalized.ID}, nil
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
