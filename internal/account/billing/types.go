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
	// Email is the account email, set on the Stripe Customer so a
	// setup-mode Checkout Session can be confirmed (Stripe requires one)
	// and for receipts/dunning. api-platform supplies it from the
	// authenticated user; empty is tolerated but blocks confirm.
	Email string `json:"email,omitempty"`
}

// PrepareAddPaymentMethodResponse is the body of the success envelope.
//
// ClientSecret is the setup-mode Checkout Session client secret that
// web-account passes to Stripe's CheckoutElementsProvider to drive the
// client-side card-attach flow. It expires per Stripe's Checkout
// Session lifecycle (24 hours).
//
// Deprecated: use StartAddPaymentMethod + FinishAddPaymentMethod for
// new callers. The request-id flow eliminates the frontend's need to
// diff the card list to detect duplicates. Kept until web-account is
// fully migrated; removed in a follow-up cycle.
type PrepareAddPaymentMethodResponse struct {
	BillingAccountID uuid.UUID `json:"billing_account_id"`
	ClientSecret     string    `json:"client_secret"`
}

// DetachPaymentMethodRequest is the payload of DetachPaymentMethod.
type DetachPaymentMethodRequest struct {
	UserID          uuid.UUID `json:"user_id"`
	PaymentMethodID uuid.UUID `json:"payment_method_id"`
}

// DetachPaymentMethodResponse is the (empty) success body. The mirror row
// is soft-deleted asynchronously by the payment_method.detached webhook.
type DetachPaymentMethodResponse struct{}

// SetDefaultPaymentMethodRequest is the payload of SetDefaultPaymentMethod.
type SetDefaultPaymentMethodRequest struct {
	UserID          uuid.UUID `json:"user_id"`
	PaymentMethodID uuid.UUID `json:"payment_method_id"`
}

// SetDefaultPaymentMethodResponse is the (empty) success body. is_default
// is synced asynchronously by the customer.updated webhook.
type SetDefaultPaymentMethodResponse struct{}

// StartAddPaymentMethodRequest is the payload of StartAddPaymentMethod —
// the half of the add-card RPC that allocates a durable request_id the
// frontend can correlate against.
//
// Email mirrors PrepareAddPaymentMethodRequest.Email: api-platform
// supplies the authenticated user's email so the Stripe Customer can
// be confirmed against the setup-mode Checkout Session. Empty tolerated
// but blocks confirm.
type StartAddPaymentMethodRequest struct {
	UserID uuid.UUID `json:"user_id"`
	Email  string    `json:"email,omitempty"`
}

// StartAddPaymentMethodResponse is the body of the success envelope.
//
// RequestID is the row's primary key in ms_billing.add_card_requests;
// the frontend stashes it locally for the matching Finish call.
// ClientSecret is the setup-mode Checkout Session client secret used
// to drive Stripe's CheckoutElementsProvider; expires after 24h.
type StartAddPaymentMethodResponse struct {
	RequestID        uuid.UUID `json:"request_id"`
	BillingAccountID uuid.UUID `json:"billing_account_id"`
	ClientSecret     string    `json:"client_secret"`
}

// FinishAddPaymentMethodRequest is the payload of FinishAddPaymentMethod —
// the polling half of the add-card RPC. The frontend retries until
// status is no longer "pending".
type FinishAddPaymentMethodRequest struct {
	UserID    uuid.UUID `json:"user_id"`
	RequestID uuid.UUID `json:"request_id"`
}

// AddCardStatus is the wire vocabulary for FinishAddPaymentMethodResponse.Status.
// Mirrors ms_billing.add_card_request_status one-for-one.
type AddCardStatus string

const (
	AddCardStatusPending   AddCardStatus = "pending"
	AddCardStatusCompleted AddCardStatus = "completed"
	AddCardStatusDuplicate AddCardStatus = "duplicate"
	AddCardStatusFailed    AddCardStatus = "failed"
)

// FinishAddPaymentMethodResponse is the body of the success envelope.
//
// Status is one of pending / completed / duplicate / failed:
//   - pending:   webhook hasn't resolved yet; caller should retry.
//   - completed: card attached; PaymentMethod is populated.
//   - duplicate: card was already in payment_methods_mirror; PaymentMethod
//     points at the existing row (so the UI can highlight it).
//   - failed:    setup_intent reached a terminal failure state.
//
// PaymentMethod is populated only on completed / duplicate.
type FinishAddPaymentMethodResponse struct {
	Status        AddCardStatus  `json:"status"`
	PaymentMethod *PaymentMethod `json:"payment_method,omitempty"`
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
