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
	// canonical metadata anchor. The returned *stripego.Customer.ID
	// is what callers persist as accounts.stripe_customer_id.
	CreateCustomer(ctx context.Context, billingAccountID string) (*stripego.Customer, error)

	// CreateCheckoutSession creates a setup-mode Checkout Session
	// (ui_mode=elements) against an existing Stripe Customer. The
	// returned CheckoutSession.ClientSecret is what web-account passes
	// to Stripe's CheckoutElementsProvider to drive the card-attach
	// flow. returnURL is where Stripe redirects after redirect-based
	// confirmation (required by elements mode even when card-only
	// confirmation stays in-page).
	CreateCheckoutSession(ctx context.Context, stripeCustomerID, returnURL string) (*stripego.CheckoutSession, error)
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
