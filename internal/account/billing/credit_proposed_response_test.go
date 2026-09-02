package billing

import (
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	billingstripe "github.com/mirrorstack-ai/billing-engine/internal/shared/stripe"
)

// 🔴 A PROPOSED PURCHASE MUST NOT CLAIM A RAIL BLOCK.
//
// The intent rail seals the purchase and creates no Stripe object, so there is
// no client secret and nothing to redirect to. Returning an empty
// StripePurchaseInit would be a lie in the shape of an answer — the field says
// "here is how to pay" and carries nothing that can pay — and a browser would
// redirect to "" or hand an empty secret to Stripe.js.
func TestAProposedPurchaseReturnsNoRailBlock(t *testing.T) {
	res := creditPurchaseStartResponse(CreditPurchaseRow{
		ID:     uuid.New(),
		Status: "proposed",
	}, billingstripe.Invoice{})

	require.Equal(t, PurchaseStartProposed, res.Status)
	require.Nil(t, res.Stripe,
		"a proposed purchase carried a Stripe block; there is no invoice behind it, so "+
			"every field in it would be empty and a client would act on nothing")
	require.Nil(t, res.NewebPay)
	require.NotEmpty(t, res.PurchaseID, "the client cannot poll a purchase it cannot name")
}

// The legacy path is unchanged, or the test above would pass against a
// response builder that had simply stopped working.
func TestAReadyPurchaseStillCarriesItsSecret(t *testing.T) {
	res := creditPurchaseStartResponse(CreditPurchaseRow{
		ID:     uuid.New(),
		Status: "pending",
	}, billingstripe.Invoice{
		ClientSecret:     "pi_secret_1",
		HostedInvoiceURL: "https://invoice.stripe.test/1",
	})

	require.Equal(t, PurchaseStartReady, res.Status)
	require.NotNil(t, res.Stripe)
	require.Equal(t, "pi_secret_1", res.Stripe.ClientSecret)
	require.Equal(t, "https://invoice.stripe.test/1", res.Stripe.HostedInvoiceURL)
}

// A client that has never heard of `status` must still fail safe: it looks for
// a hosted URL, finds none, and takes its existing no-checkout branch. That is
// what makes the engine side shippable before the browser side.
func TestAnOldClientFindsNothingToRedirectTo(t *testing.T) {
	res := creditPurchaseStartResponse(CreditPurchaseRow{
		ID:     uuid.New(),
		Status: "proposed",
	}, billingstripe.Invoice{})

	// This is exactly what CreditSection.tsx reads: res.stripe?.hosted_invoice_url.
	require.Nil(t, res.Stripe,
		"an old client would read res.stripe.hosted_invoice_url as an empty string and "+
			"redirect the customer to nowhere")
}
