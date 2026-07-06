package stripe

import (
	"context"
	"fmt"

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
func (c *realClient) CreateCustomer(ctx context.Context, billingAccountID, email string) (*stripego.Customer, error) {
	params := &stripego.CustomerParams{}
	params.Context = ctx
	params.AddMetadata("billing_account_id", billingAccountID)
	// Stripe requires an email to confirm a setup-mode Checkout Session
	// (and uses it for receipts/dunning). Only set when present so an
	// empty value doesn't get sent.
	if email != "" {
		params.Email = stripego.String(email)
	}
	return c.sc.Customers.New(params)
}

// UpdateCustomerEmail backfills the email on an existing Customer (one
// created before email capture). Idempotent — setting the same value is
// a no-op on Stripe's side.
func (c *realClient) UpdateCustomerEmail(ctx context.Context, stripeCustomerID, email string) error {
	params := &stripego.CustomerParams{Email: stripego.String(email)}
	params.Context = ctx
	_, err := c.sc.Customers.Update(stripeCustomerID, params)
	return err
}

// CreateCheckoutSession creates a setup-mode Checkout Session
// (ui_mode=elements) so the user can attach a card via Stripe's
// CheckoutElementsProvider client-side. mode=setup saves the payment
// method for future off-session charges (subscription + metered
// usage) without collecting a payment now — the card-on-file flow.
//
// This replaces the older SetupIntent + Elements integration per
// Stripe's recommended migration to the Checkout Sessions API
// (docs.stripe.com/payments/payment-element/migration-ewcs).
//
// Payment method selection follows the Stripe dashboard (Settings →
// Payment methods) — the Payment Element renders whatever is enabled
// there and adapts per-device (Apple Pay on Safari, Google Pay on
// Chrome, etc.). returnURL is where Stripe redirects after a
// redirect-based confirmation; elements mode requires it even though
// card-only confirmation stays in-page. currency=usd scopes which
// region-specific methods are eligible.
func (c *realClient) CreateCheckoutSession(ctx context.Context, stripeCustomerID, returnURL string) (*stripego.CheckoutSession, error) {
	params := &stripego.CheckoutSessionParams{
		Mode:      stripego.String(string(stripego.CheckoutSessionModeSetup)),
		UIMode:    stripego.String(string(stripego.CheckoutSessionUIModeElements)),
		Customer:  stripego.String(stripeCustomerID),
		Currency:  stripego.String("usd"),
		ReturnURL: stripego.String(returnURL),
	}
	// Expand the underlying SetupIntent so the caller can read
	// session.SetupIntent.ID directly. StartAddPaymentMethod stamps
	// this id onto ms_billing.add_card_requests so setup_intent.succeeded
	// webhook events can correlate back to the originating request.
	params.Expand = []*string{stripego.String("setup_intent")}
	params.Context = ctx
	return c.sc.CheckoutSessions.New(params)
}

// DetachPaymentMethod detaches a payment method from its Customer. The
// mirror row is soft-deleted by the payment_method.detached webhook, not
// here — this call only performs the Stripe-side detach.
func (c *realClient) DetachPaymentMethod(ctx context.Context, stripePaymentMethodID string) error {
	params := &stripego.PaymentMethodDetachParams{}
	params.Context = ctx
	_, err := c.sc.PaymentMethods.Detach(stripePaymentMethodID, params)
	return err
}

// SetDefaultPaymentMethod sets the Customer's invoice-settings default
// payment method. The resulting customer.updated webhook syncs the
// mirror's is_default flags for the account.
func (c *realClient) SetDefaultPaymentMethod(ctx context.Context, stripeCustomerID, stripePaymentMethodID string) error {
	params := &stripego.CustomerParams{
		InvoiceSettings: &stripego.CustomerInvoiceSettingsParams{
			DefaultPaymentMethod: stripego.String(stripePaymentMethodID),
		},
	}
	params.Context = ctx
	_, err := c.sc.Customers.Update(stripeCustomerID, params)
	return err
}

// CreateDraftInvoice creates an EMPTY draft invoice line items are then
// pinned to (CreateInvoiceItem) and that charges only on FinalizeInvoice.
// PendingInvoiceItemsBehavior=exclude is load-bearing (review 2026-07-06, C2):
// it guarantees this invoice can never sweep up another charge leg's orphaned
// customer-level pending item — with several independent item→invoice
// sequences per account, the legacy include behavior pooled a crashed leg's
// item onto the next leg's unrelated invoice. ref is stamped as the
// ms_charge_ref metadata anchor for crash reconciliation. The deterministic
// Idempotency-Key (inv-<id>) makes a re-run reuse the original draft.
func (c *realClient) CreateDraftInvoice(ctx context.Context, custID, ref, idemKey string) (Invoice, error) {
	params := &stripego.InvoiceParams{
		Customer:                    stripego.String(custID),
		CollectionMethod:            stripego.String(string(stripego.InvoiceCollectionMethodChargeAutomatically)),
		AutoAdvance:                 stripego.Bool(false),
		PendingInvoiceItemsBehavior: stripego.String("exclude"),
	}
	if ref != "" {
		params.AddMetadata("ms_charge_ref", ref)
	}
	params.Context = ctx
	params.SetIdempotencyKey(idemKey)
	inv, err := c.sc.Invoices.New(params)
	if err != nil {
		return Invoice{}, err
	}
	return projectInvoice(inv), nil
}

// CreateInvoiceItem creates an invoice item PINNED to the given draft invoice
// (never a floating customer-level pending item — see CreateDraftInvoice).
// amountCents is whole cents (the caller converts micro-dollars → cents
// round-half-up before this call; Stripe amounts are integer minor units). The
// deterministic Idempotency-Key makes a re-run safe — Stripe returns the
// original item (already pinned to the same replayed draft) instead of
// creating a second one. We project to a plain InvoiceItem (id only) so
// consumers stay off stripe-go.
func (c *realClient) CreateInvoiceItem(ctx context.Context, custID, invoiceID string, amountCents int64, currency, desc, idemKey string) (InvoiceItem, error) {
	params := &stripego.InvoiceItemParams{
		Customer: stripego.String(custID),
		Invoice:  stripego.String(invoiceID),
		Amount:   stripego.Int64(amountCents),
		Currency: stripego.String(currency),
	}
	if desc != "" {
		params.Description = stripego.String(desc)
	}
	params.Context = ctx
	params.SetIdempotencyKey(idemKey)
	item, err := c.sc.InvoiceItems.New(params)
	if err != nil {
		return InvoiceItem{}, err
	}
	return InvoiceItem{ID: item.ID}, nil
}

// FinalizeInvoice finalizes the draft with auto_advance=true — Stripe runs the
// off-session PaymentIntent against the default PM (the metered auto-charge).
// The ONLY money-moving step of the draft→items→finalize flow. The
// deterministic Idempotency-Key (fin-<id>) makes a re-run replay the original
// finalization instead of double-charging. Projected to a plain Invoice
// (id/status/amounts) for the mirror.
func (c *realClient) FinalizeInvoice(ctx context.Context, invoiceID, idemKey string) (Invoice, error) {
	params := &stripego.InvoiceFinalizeInvoiceParams{
		AutoAdvance: stripego.Bool(true),
	}
	params.Context = ctx
	params.SetIdempotencyKey(idemKey)
	inv, err := c.sc.Invoices.FinalizeInvoice(invoiceID, params)
	if err != nil {
		return Invoice{}, err
	}
	return projectInvoice(inv), nil
}

// FindInvoiceByRef searches the Customer's invoices for the ms_charge_ref
// metadata anchor — the crash-recovery read for retries past Stripe's ~24h
// idempotency-key window (see the interface comment). Uses the Stripe Search
// API; at most one invoice can carry a given ref (the ref is the deterministic
// charge identity and the draft that carries it is created under an idem key).
func (c *realClient) FindInvoiceByRef(ctx context.Context, custID, ref string) (Invoice, bool, error) {
	params := &stripego.InvoiceSearchParams{
		SearchParams: stripego.SearchParams{
			Query:   fmt.Sprintf(`customer:"%s" AND metadata["ms_charge_ref"]:"%s"`, custID, ref),
			Limit:   stripego.Int64(1),
			Context: ctx,
		},
	}
	it := c.sc.Invoices.Search(params)
	if it.Next() {
		return projectInvoice(it.Invoice()), true, nil
	}
	if err := it.Err(); err != nil {
		return Invoice{}, false, err
	}
	return Invoice{}, false, nil
}

// projectInvoice maps a stripe-go invoice to the trust-boundary-edge Invoice
// projection the cycle consumer mirrors.
func projectInvoice(inv *stripego.Invoice) Invoice {
	return Invoice{
		ID:         inv.ID,
		Status:     string(inv.Status),
		AmountDue:  inv.AmountDue,
		AmountPaid: inv.AmountPaid,
		Currency:   string(inv.Currency),
	}
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
