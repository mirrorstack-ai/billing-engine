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

	"github.com/mirrorstack-ai/billing-engine/internal/intent"
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
}

// New returns an Adapter.
func New(client InvoiceRail, resolver CustomerResolver) *Adapter {
	// 🔴 There is deliberately no description function here any more.
	//
	// This adapter used to substitute "MirrorStack charge <short digest>" for
	// every line, discarding what the intent sealed — so the verifiable rail's
	// invoice was strictly LESS informative than the legacy one it replaces.
	// The description a leg wrote is inside the document (the line's Meter),
	// and lineDescription reads it from there.
	//
	// The field is gone rather than left unused, because an unused seam is an
	// invitation: the next reader wiring it back would silently restore the
	// defect, and nothing would fail.
	return &Adapter{client: client, resolver: resolver}
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

	// One invoice item per SEALED line, so the customer's statement says
	// what the document they accepted says.
	//
	// 🔴 This used to be a single item carrying the whole amount under
	// "MirrorStack charge <short digest>". The description the leg wrote is
	// inside the intent — it is the line's Meter — and discarding it made
	// the verifiable rail's invoice strictly less informative than the
	// legacy one it replaces.
	//
	// The amounts still sum to `cents` and not to the lines' own rounding:
	// see splitCents. The provider is handed the sealed provider remainder
	// once, apportioned, never the gross and never a per-line rounding.
	items := splitCents(cents, d.Lines)
	for i, item := range items {
		if _, err := a.client.CreateInvoiceItem(
			ctx, customerID, invoice.ID, item.cents, currency,
			item.description, billingstripe.LinePeriod{},
			fmt.Sprintf("ii-%s-%d", d.IdempotencyKey, i),
		); err != nil {
			return executor.CollectResult{}, fmt.Errorf("create line %d: %w", i, err)
		}
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

// invoiceItem is one line as it will appear at the provider.
type invoiceItem struct {
	cents       int64
	description string
}

// splitCents apportions the collected total across the sealed lines.
//
// 🔴 The TOTAL is authoritative, not the lines.
//
// The engine rounds micros to cents ONCE, on the provider remainder, exactly
// as the legacy boundary collector rounds once on its net
// (internal/account/cycle/charge.go:595). Rounding each line independently and
// summing would give a different integer — measured elsewhere in this repo at
// one cent on a two-component proration — so the lines are apportioned
// largest-remainder within the total the intent sealed. The invoice therefore
// adds up to the sealed charge by construction, whatever the lines are.
//
// A charge with no lines, or one whose lines sum to nothing, collapses to a
// single item for the whole amount rather than producing an invoice with no
// items, which Stripe would refuse to finalize.
func splitCents(total int64, lines []intent.Line) []invoiceItem {
	var gross int64
	for _, l := range lines {
		gross += l.AmountMicros()
	}
	if len(lines) == 0 || gross <= 0 || total <= 0 {
		return []invoiceItem{{cents: total, description: fallbackDescription(lines)}}
	}

	items := make([]invoiceItem, len(lines))
	remainders := make([]int64, len(lines))
	var assigned int64
	for i, l := range lines {
		// Integer floor of the line's share, and the remainder kept so the
		// leftover cents go to the lines that lost the most to truncation.
		num := l.AmountMicros() * total
		items[i] = invoiceItem{cents: num / gross, description: lineDescription(l)}
		remainders[i] = num % gross
		assigned += items[i].cents
	}

	// Largest-remainder: hand out the leftover one cent at a time, highest
	// remainder first, ties to the earlier line so the result is
	// deterministic and a retry produces the same invoice.
	for leftover := total - assigned; leftover > 0; leftover-- {
		best := -1
		for i := range items {
			if remainders[i] > 0 && (best == -1 || remainders[i] > remainders[best]) {
				best = i
			}
		}
		if best == -1 {
			// Every remainder is zero, which can only happen if the division
			// was exact. Give the rest to the first line rather than
			// silently dropping cents.
			items[0].cents += leftover
			break
		}
		items[best].cents++
		remainders[best] = 0
	}
	return items
}

// lineDescription is what the customer reads for one sealed line.
//
// The proposer puts the leg's description in Meter and its source reference
// in Module (proposer.Propose), so Meter is the customer-facing half.
func lineDescription(l intent.Line) string {
	if d := strings.TrimSpace(l.Meter); d != "" {
		return d
	}
	return "MirrorStack charge"
}

func fallbackDescription(lines []intent.Line) string {
	if len(lines) == 1 {
		return lineDescription(lines[0])
	}
	return "MirrorStack charge"
}
