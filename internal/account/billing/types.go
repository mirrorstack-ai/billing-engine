// Package billing implements the v1 RPC surface for billing-engine's
// cmd/account-api: Ensure, PrepareAddPaymentMethod, GetPaymentMethods.
//
// The package contract maps one-for-one to the RPCs documented in
// mirrorstack-docs/api/billing/account-api.md. JSON tags match the
// wire format that lambda.Invoke callers + the HTTP local-dev path
// both serialize against.
package billing

import "github.com/google/uuid"

// Capability is a typed string for the EnsureRequest.Require vocabulary.
// Typing the slice prevents callers from passing arbitrary strings;
// unknown values can't reach the handler in the first place.
type Capability string

const (
	RequirePaymentMethod Capability = "payment_method"
	RequireSubscription  Capability = "subscription"
)

// EnsureRequest is the payload of the Ensure RPC.
//
// Require lists the capabilities the caller wants verified. Defaults
// to [RequirePaymentMethod] when empty (PaaS/BaaS-first behavior).
// Each requested capability is checked independently; the union of
// unmet ones appears in EnsureResponse.Missing.
type EnsureRequest struct {
	UserID  uuid.UUID    `json:"user_id"`
	Require []Capability `json:"require,omitempty"`
}

// EnsureResponse is the body of the Ensure RPC's success response
// envelope (the outer envelope adds {"ok": true, "response": …}).
//
// Missing is empty when every required capability is met. Entries are
// drawn from {"billing_account", "payment_method", "subscription"}.
// api-platform surfaces these to web-account as 402 + per-entry CTA.
type EnsureResponse struct {
	Missing []string `json:"missing"`
}

// Ready returns true when the user has every required capability.
func (r *EnsureResponse) Ready() bool { return len(r.Missing) == 0 }

// Response-side vocabulary: values that may appear in EnsureResponse.Missing.
// Kept as plain strings because they're a wire-format projection; not all
// Missing values have Require counterparts (billing_account doesn't).
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
