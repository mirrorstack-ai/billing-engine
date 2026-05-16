package stripe

import (
	"context"

	stripego "github.com/stripe/stripe-go/v85"
	stripeclient "github.com/stripe/stripe-go/v85/client"
	stripewebhook "github.com/stripe/stripe-go/v85/webhook"
)

// NewClient returns a Client backed by the real Stripe API. The
// returned client uses an isolated *client.API instance (rather than
// stripe-go's package-level stripego.Key) so multiple secret keys
// can coexist if billing-engine ever runs against test + live in
// the same process (it currently doesn't, but the API supports it).
//
// secretKey is required; an empty string causes Stripe SDK calls to
// fail at the next API request with an authentication error. Callers
// should fail-fast at startup if the secret is empty.
func NewClient(secretKey string) Client {
	sc := &stripeclient.API{}
	sc.Init(secretKey, nil)
	return &realClient{sc: sc}
}

type realClient struct {
	sc *stripeclient.API
}

// CreateCustomer creates a Stripe Customer carrying our canonical
// metadata anchor (billing_account_id). The metadata is what makes
// owner-migration (user → org) safe without re-keying Stripe — the
// metadata value never changes once set; only the Postgres row's
// owner_kind / owner_user_id / owner_org_id can shift.
func (c *realClient) CreateCustomer(ctx context.Context, billingAccountID string) (*stripego.Customer, error) {
	params := &stripego.CustomerParams{}
	params.Context = ctx
	params.AddMetadata("billing_account_id", billingAccountID)
	return c.sc.Customers.New(params)
}

// CreateSetupIntent creates an off-session SetupIntent so the user
// can attach a card via Stripe Elements client-side. usage=off_session
// signals to Stripe that future charges may be initiated by the
// platform (i.e. the user won't be present), enabling card-network
// pre-authorization where applicable.
func (c *realClient) CreateSetupIntent(ctx context.Context, stripeCustomerID string) (*stripego.SetupIntent, error) {
	params := &stripego.SetupIntentParams{
		Customer: stripego.String(stripeCustomerID),
		Usage:    stripego.String(string(stripego.SetupIntentUsageOffSession)),
	}
	params.Context = ctx
	return c.sc.SetupIntents.New(params)
}

// NewVerifier returns a Verifier for the configured webhook signing
// secret. webhookSecret is distinct from the main Stripe secret key
// and is rotated independently (Stripe Dashboard → Developers →
// Webhooks → signing secret).
func NewVerifier(webhookSecret string) Verifier {
	return &realVerifier{secret: webhookSecret}
}

type realVerifier struct {
	secret string
}

// Verify wraps stripe-go's webhook.ConstructEvent. The package
// enforces the default 5-minute replay window; events older than
// that are rejected even if the signature is valid (defense against
// captured-payload replay attacks).
func (v *realVerifier) Verify(payload []byte, signature string) (stripego.Event, error) {
	return stripewebhook.ConstructEvent(payload, signature, v.secret)
}
