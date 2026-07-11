// Package stripe is the thin wrapper around stripe-go that
// billing-engine handlers depend on. The wrapper exists for two
// reasons: (a) testability via the Client / Verifier interfaces,
// and (b) keeping stripe-go imports out of every consumer.
//
// We intentionally do NOT translate stripe-go's types into a domain
// model — Client methods return *stripego.Customer / *stripego.CheckoutSession
// directly. Callers consume what they need; nothing is hidden.
package stripe

import (
	"context"

	stripego "github.com/stripe/stripe-go/v85"
)

// Client is the Stripe API surface billing-engine uses to create
// Customers and card-on-file Checkout Sessions. Implementations:
//
//   - Production: NewClient(secretKey) — calls the real Stripe API.
//   - Tests: pass a fake satisfying this interface.
type Client interface {
	// CreateCustomer creates a Stripe Customer with the platform's
	// canonical metadata anchor and the account email. Stripe requires
	// an email to confirm a setup-mode Checkout Session (and uses it for
	// receipts/dunning); empty email is tolerated but blocks confirm.
	// The returned *stripego.Customer.ID is what callers persist as
	// accounts.stripe_customer_id.
	CreateCustomer(ctx context.Context, billingAccountID, email string) (*stripego.Customer, error)

	// UpdateCustomerEmail sets the email on an existing Stripe Customer.
	// Used to backfill Customers created before the email was captured —
	// a setup-mode Checkout Session can't be confirmed without one.
	UpdateCustomerEmail(ctx context.Context, stripeCustomerID, email string) error

	// CreateCheckoutSession creates a setup-mode Checkout Session
	// (ui_mode=elements) against an existing Stripe Customer. The
	// returned CheckoutSession.ClientSecret is what web-account passes
	// to Stripe's CheckoutElementsProvider to drive the card-attach
	// flow. returnURL is where Stripe redirects after redirect-based
	// confirmation (required by elements mode even when card-only
	// confirmation stays in-page).
	CreateCheckoutSession(ctx context.Context, stripeCustomerID, returnURL string) (*stripego.CheckoutSession, error)

	// DetachPaymentMethod detaches a saved card from its Customer. The
	// resulting payment_method.detached webhook soft-deletes the mirror row.
	DetachPaymentMethod(ctx context.Context, stripePaymentMethodID string) error

	// SetDefaultPaymentMethod points the Customer's invoice-settings
	// default at the given payment method. The resulting customer.updated
	// webhook syncs is_default across the account's mirror rows.
	SetDefaultPaymentMethod(ctx context.Context, stripeCustomerID, stripePaymentMethodID string) error

	// CreateDraftInvoice creates an EMPTY draft invoice
	// (collection_method=charge_automatically, auto_advance=false,
	// pending_invoice_items_behavior=exclude) that line items are then PINNED
	// to via CreateInvoiceItem, and that charges only once FinalizeInvoice
	// runs. The exclude behavior is load-bearing (review 2026-07-06, C2): the
	// legacy include behavior swept up ALL of the Customer's pending items, so
	// with several independent item→invoice sequences per account (boundary +
	// per-timer Leg 1 + combined creation) an orphaned item from any crashed
	// leg leaked onto the NEXT leg's unrelated invoice — money collected on the
	// wrong invoice and a permanent idempotency wedge for the crashed leg. ref
	// is the deterministic charge identity ("run:<id>" / "timer:<id>" /
	// "app-proration:<id>"), stamped as metadata for crash reconciliation.
	// idemKey (inv-<id>) makes a re-run reuse the SAME draft.
	CreateDraftInvoice(ctx context.Context, custID, ref, idemKey string) (Invoice, error)

	// CreateInvoiceItem creates an invoice item PINNED to the given draft
	// invoice — never a floating customer-level pending item. amountCents is
	// the whole-cent customer charge (micro-dollars are converted to cents
	// round-half-up by the caller BEFORE reaching Stripe — Stripe amounts are
	// integer minor units, never float). desc is the line description shown on
	// the invoice. idemKey is a deterministic Stripe Idempotency-Key
	// (ii-<run> / mod-overage-ii-<timer> / app-ii-<app>) so a re-run /
	// partial-failure resume never creates a duplicate line (the replayed item
	// is already pinned to the same replayed draft). Returns a plain
	// InvoiceItem so the cycle consumer stays free of stripe-go imports.
	CreateInvoiceItem(ctx context.Context, custID, invoiceID string, amountCents int64, currency, desc, idemKey string) (InvoiceItem, error)

	// FinalizeInvoice finalizes a draft invoice with auto_advance=true: Stripe
	// runs the off-session PaymentIntent against the Customer's default payment
	// method (the metered auto-charge). This is the ONLY step that moves money —
	// a crash before it leaves an inert draft that can never charge and never
	// pollute another leg's invoice. idemKey (fin-<id>) makes a re-run replay
	// the original finalization. Returns the finalized invoice projection
	// (id/status/amounts) for the mirror.
	FinalizeInvoice(ctx context.Context, invoiceID, idemKey string) (Invoice, error)

	// RetrieveCharge fetches a charge by id and projects the card-identifying
	// fields the fraud webhook needs. The charge.dispute.created /
	// radar.early_fraud_warning.created events carry only a charge id (no pm id,
	// no fingerprint), so resolving the disputed card to a mirror row requires
	// this one retrieve. A retrieved charge returns both the payment_method id
	// and payment_method_details.card.fingerprint by default. Rare + off the hot
	// path, so a synchronous call in the webhook handler is fine.
	RetrieveCharge(ctx context.Context, chargeID string) (ChargeCardRef, error)

	// PayInvoice pays a finalized Stripe invoice off-session with the
	// Customer's default payment method — the customer-initiated "Pay"
	// action behind billing.PayInvoice (funding-gates design). idemKey is
	// the deterministic "payinv-<mirror uuid>" so a client retry replays the
	// original pay attempt instead of double-charging. Returns the post-pay
	// invoice projection; the mirror settles via the invoice.paid webhook,
	// never from this return value.
	PayInvoice(ctx context.Context, stripeInvoiceID, idemKey string) (Invoice, error)

	// FindInvoiceByRef looks a Customer's invoice up by its ms_charge_ref
	// metadata anchor (stamped by CreateDraftInvoice) — the crash-recovery read
	// (review 2026-07-06, H5): Stripe prunes idempotency keys after ~24h, so a
	// charge leg retrying past that window can no longer rely on key replay to
	// find what a crashed attempt created. found=false when no invoice carries
	// the ref. Backed by the Stripe Search API; its indexing lags writes by up
	// to ~1 minute, which the retry cadences (daily sweeps) sit far above —
	// short-window retries are still covered by idem-key replay.
	FindInvoiceByRef(ctx context.Context, custID, ref string) (Invoice, bool, error)
}

// InvoiceItem is the trust-boundary-edge projection of a Stripe invoice item
// the charge path needs: just the id (callers correlate, they don't read the
// rest). Kept stripe-go-free so the cycle consumer doesn't import the SDK.
type InvoiceItem struct {
	ID string
}

// Invoice is the trust-boundary-edge projection of a Stripe invoice the charge
// path mirrors into ms_billing.invoices: id, status, and the amounts (whole
// cents — Stripe minor units). Kept stripe-go-free so the cycle consumer stays
// off the SDK; the webhook reconciliation path (PR #7) reads the full stripe-go
// Event separately.
type Invoice struct {
	ID         string
	Status     string
	AmountDue  int64
	AmountPaid int64
	Currency   string
}

// ChargeCardRef is the trust-boundary-edge projection of a Stripe charge the
// fraud webhook needs to resolve a disputed/warned card to a mirror row:
// the payment_method id, the card fingerprint (the canonical "same physical
// card" identity, preferred for matching), and the owning Stripe customer id
// (to scope the flag to one account). Kept stripe-go-free like Invoice. Any
// field may be empty — a non-card charge has no card block, and Stripe tags
// fingerprint omitempty (wallet-tokenized cards may also omit it).
type ChargeCardRef struct {
	PaymentMethodID  string
	Fingerprint      string
	StripeCustomerID string
}

// Verifier verifies Stripe webhook signatures. Kept separate from
// Client because the API surface is independent: webhooks use a
// distinct STRIPE_WEBHOOK_SECRET, and signature verification doesn't
// need (or use) the main Stripe secret key.
type Verifier interface {
	// Verify parses + signature-verifies a webhook request body.
	// signature is the raw value of the Stripe-Signature header.
	// On signature mismatch / replay-window expiry / malformed payload,
	// returns a non-nil error and the zero Event.
	Verify(payload []byte, signature string) (stripego.Event, error)
}
