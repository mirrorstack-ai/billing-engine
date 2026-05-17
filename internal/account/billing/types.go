// Package billing implements the v1 RPC surface for billing-engine's
// cmd/account-api: Ensure, PrepareAddPaymentMethod, GetPaymentMethods.
//
// The package contract maps one-for-one to the RPCs documented in
// mirrorstack-docs/api/billing/account-api.md. JSON tags match the
// wire format that lambda.Invoke callers + the HTTP local-dev path
// both serialize against.
package billing

import "github.com/google/uuid"

// EnsureRequest is the payload of the Ensure RPC.
//
// Require lists the capabilities the caller wants verified for the user.
// Defaults to [RequirePaymentMethod] when empty — preserves the PaaS/
// BaaS-first behavior. The handler checks each requested capability
// independently and returns the union of unmet capabilities in
// EnsureResponse.Missing.
//
// Valid Require values: RequirePaymentMethod, RequireSubscription.
// Unknown values produce an INVALID_INPUT error.
type EnsureRequest struct {
	UserID  uuid.UUID `json:"user_id"`
	Require []string  `json:"require,omitempty"`
}

// EnsureResponse is the body of the Ensure RPC's success response
// envelope (the outer envelope adds {"ok": true, "response": …}).
//
// Missing is empty when the user has every required capability. When
// non-empty, the entries are drawn from
// {"billing_account", "payment_method", "subscription"}.
// api-platform surfaces these to web-account as a 402 with
// code: "billing_not_ready" and a per-entry CTA.
type EnsureResponse struct {
	Missing []string `json:"missing"`
}

// Ready returns true when the user has every required capability.
func (r *EnsureResponse) Ready() bool { return len(r.Missing) == 0 }

// Request-side vocabulary: values valid in EnsureRequest.Require.
const (
	RequirePaymentMethod = "payment_method"
	RequireSubscription  = "subscription"
)

// Response-side vocabulary: values that may appear in EnsureResponse.Missing.
// billing_account is implicit (always checked first when any other
// capability is requested); payment_method / subscription mirror the
// Require constants 1:1.
const (
	MissingBillingAccount = "billing_account"
	MissingPaymentMethod  = "payment_method"
	MissingSubscription   = "subscription"
)

// PrepareAddPaymentMethodRequest is the payload of PrepareAddPaymentMethod.
type PrepareAddPaymentMethodRequest struct {
	UserID uuid.UUID `json:"user_id"`
}

// PrepareAddPaymentMethodResponse is the body of the success envelope.
//
// SetupIntentClientSecret is what web-account passes to Stripe Elements
// to drive the client-side card-attach flow. It expires per Stripe's
// SetupIntent lifecycle (usually 24 hours).
type PrepareAddPaymentMethodResponse struct {
	BillingAccountID        uuid.UUID `json:"billing_account_id"`
	SetupIntentClientSecret string    `json:"setup_intent_client_secret"`
}

// GetPaymentMethodsRequest is the payload of GetPaymentMethods.
type GetPaymentMethodsRequest struct {
	UserID uuid.UUID `json:"user_id"`
}

// GetPaymentMethodsResponse is the body of the success envelope.
type GetPaymentMethodsResponse struct {
	PaymentMethods []PaymentMethod `json:"payment_methods"`
}

// PaymentMethod is the projection of a payment_methods_mirror row
// returned to UI consumers. Card-only in v1.
type PaymentMethod struct {
	ID                    uuid.UUID `json:"id"`
	StripePaymentMethodID string    `json:"stripe_payment_method_id"`
	Brand                 string    `json:"brand"`
	Last4                 string    `json:"last4"`
	ExpMonth              int       `json:"exp_month"`
	ExpYear               int       `json:"exp_year"`
	IsDefault             bool      `json:"is_default"`
}
