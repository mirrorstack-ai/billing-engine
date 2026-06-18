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

	// CreateInvoiceItem creates a pending invoice item on the Customer — one
	// per metered metric line — that the next CreateInvoice draft sweeps up.
	// amountCents is the whole-cent customer charge (micro-dollars are
	// converted to cents round-half-up by the caller BEFORE reaching Stripe —
	// Stripe amounts are integer minor units, never float). desc is the line
	// description shown on the invoice. idemKey is a deterministic Stripe
	// Idempotency-Key (ii-<run>-<metric>) so a re-run / partial-failure resume
	// never creates a duplicate line. Unlike the card-management methods this
	// returns a plain InvoiceItem (not a stripe-go type) so the cycle consumer
	// stays free of stripe-go imports — the charge path is the trust-boundary
	// edge and the consumer needs only the id.
	CreateInvoiceItem(ctx context.Context, custID string, amountCents int64, currency, desc, idemKey string) (InvoiceItem, error)

	// CreateInvoice creates a draft invoice with
	// collection_method=charge_automatically that sweeps up the Customer's
	// pending invoice items. When autoAdvance is true, Stripe finalizes the
	// draft and attempts an off-session PaymentIntent on the Customer's default
	// payment method automatically (the off-session metered charge). idemKey is
	// the deterministic per-run Stripe Idempotency-Key (inv-<run>). Returns a
	// plain Invoice (id + status + amounts) so the cycle consumer can mirror it
	// without importing stripe-go.
	CreateInvoice(ctx context.Context, custID string, autoAdvance bool, idemKey string) (Invoice, error)
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
