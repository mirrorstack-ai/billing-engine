// Package billing implements the v1 RPC surface for billing-engine's
// cmd/account-api: Ensure, PrepareAddPaymentMethod, GetPaymentMethods.
//
// The package contract maps one-for-one to the RPCs documented in
// mirrorstack-docs/api/billing/account-api.md. JSON tags match the
// wire format that lambda.Invoke callers + the HTTP local-dev path
// both serialize against.
package billing

import (
	"time"

	"github.com/google/uuid"
)

// Capability is a typed string for the EnsureRequest.Require vocabulary.
// Typing the slice prevents callers from passing arbitrary strings;
// unknown values can't reach the handler in the first place.
type Capability string

const (
	RequirePaymentMethod Capability = "payment_method"
	RequireSubscription  Capability = "subscription"
	// RequireNotDelinquent asks Ensure to verify the account has no unpaid
	// (open/uncollectible) invoice. The delinquency is DERIVED from the
	// invoices mirror (reconciled by the invoice.* webhooks), not a stored
	// flag. This wires the SIGNAL only — the enforcement POLICY
	// (grace/suspend/prepaid, risk-graded collection) is PR #9.
	RequireNotDelinquent Capability = "not_delinquent"
)

// EnsureRequest is the payload of the Ensure RPC.
//
// Require lists the capabilities the caller wants verified. Defaults
// to [RequirePaymentMethod] when empty (PaaS/BaaS-first behavior).
// Each requested capability is checked independently; the union of
// unmet ones appears in EnsureResponse.Missing.
type EnsureRequest struct {
	// Exactly one of UserID / OrgID — the payer principal. An org resolves
	// through its funding designation (migration 041): not designated / not
	// activated → Missing: ["billing_account"].
	UserID  uuid.UUID    `json:"user_id,omitempty"`
	OrgID   uuid.UUID    `json:"org_id,omitempty"`
	Require []Capability `json:"require,omitempty"`
}

// EnsureResponse is the body of the Ensure RPC's success response
// envelope (the outer envelope adds {"ok": true, "response": …}).
//
// Missing is empty when every required capability is met. Entries are
// drawn from {"billing_account", "payment_method", "subscription",
// "delinquent"}. api-platform surfaces these to web-account as 402 +
// per-entry CTA.
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
	// MissingDelinquent appears when the account has an unpaid
	// (open/uncollectible) invoice. It is the past-due SIGNAL only; what to
	// DO about it (block, grace, suspend, force prepaid) is the collection
	// POLICY in PR #9.
	MissingDelinquent = "delinquent"
)

// PrepareAddPaymentMethodRequest is the payload of PrepareAddPaymentMethod.
type PrepareAddPaymentMethodRequest struct {
	// Exactly one of UserID / OrgID. The org leg get-or-creates the ORG
	// account row (regardless of designation state) so the card can bind
	// before a funding='org' designation completes.
	UserID uuid.UUID `json:"user_id,omitempty"`
	OrgID  uuid.UUID `json:"org_id,omitempty"`
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
	// Exactly one of UserID / OrgID — the PM must belong to that owner's account.
	UserID          uuid.UUID `json:"user_id,omitempty"`
	OrgID           uuid.UUID `json:"org_id,omitempty"`
	PaymentMethodID uuid.UUID `json:"payment_method_id"`
}

// DetachPaymentMethodResponse is the (empty) success body. The mirror row
// is soft-deleted asynchronously by the payment_method.detached webhook.
type DetachPaymentMethodResponse struct{}

// SetDefaultPaymentMethodRequest is the payload of SetDefaultPaymentMethod.
type SetDefaultPaymentMethodRequest struct {
	// Exactly one of UserID / OrgID — the PM must belong to that owner's account.
	UserID          uuid.UUID `json:"user_id,omitempty"`
	OrgID           uuid.UUID `json:"org_id,omitempty"`
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
	// Exactly one of UserID / OrgID (org leg: see PrepareAddPaymentMethodRequest).
	UserID uuid.UUID `json:"user_id,omitempty"`
	OrgID  uuid.UUID `json:"org_id,omitempty"`
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
	// Exactly one of UserID / OrgID — must match the Start call's owner.
	UserID    uuid.UUID `json:"user_id,omitempty"`
	OrgID     uuid.UUID `json:"org_id,omitempty"`
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
	// Exactly one of UserID / OrgID. A sponsor-funded org account owns no PM
	// rows (the sponsor's account does) — an empty list is its normal state.
	UserID uuid.UUID `json:"user_id,omitempty"`
	OrgID  uuid.UUID `json:"org_id,omitempty"`
}

// GetPaymentMethodsResponse is the body of the success envelope.
type GetPaymentMethodsResponse struct {
	PaymentMethods []PaymentMethod `json:"payment_methods"`
}

// ServiceSignals are the raw service-block gate inputs read for one account
// (db.ServiceBlockSignals). The service maps these into eligibility.Signals:
// FirstChargeStatus is the earliest real invoice's Stripe status ("" when the
// account has no charge yet), mapped to the FirstChargeState enum.
type ServiceSignals struct {
	UsableCardCount    int
	FailedChargeStreak int
	FirstChargeStatus  string
}

// UnpaidInvoiceRow is the store projection of one unpaid mirror invoice
// (open/uncollectible, amount_due > 0) — the read behind ListUnpaidInvoices.
// Money is int64 micro-dollars (the store converts the mirror's whole Stripe
// cents ×10_000, the same boundary usage's invoice reads use).
type UnpaidInvoiceRow struct {
	ID              uuid.UUID
	Number          string
	AmountDueMicros int64
	CreatedAt       time.Time
}

// InvoicePayTarget is the store projection PayInvoice resolves an owned
// mirror invoice to: the Stripe-side id to pay and the mirror's current
// status (for the paid short-circuit / non-payable rejection).
type InvoicePayTarget struct {
	StripeInvoiceID string
	Status          string
}

// GetServiceStatusRequest is the payload of GetServiceStatus — the account is
// addressed by owner, the same as Ensure / GetPaymentMethods (exactly one of
// UserID / OrgID). An org resolves through its funding designation
// (ResolveOrgFundedAccount); an org without a resolvable designation is
// BLOCKED — unfunded orgs have no serving standing (funding-gates design).
type GetServiceStatusRequest struct {
	UserID uuid.UUID `json:"user_id,omitempty"`
	OrgID  uuid.UUID `json:"org_id,omitempty"`
}

// GetServiceStatusResponse is the service-block verdict for the account. Blocked
// is the single field a caller must read to gate service; Reason is the primary
// machine-readable cause (eligibility.Reason, "ELIGIBLE" when not blocked);
// Reasons lists every failing gate (omitted when eligible).
type GetServiceStatusResponse struct {
	Blocked     bool     `json:"blocked"`
	Reason      string   `json:"reason"`
	Reasons     []string `json:"reasons,omitempty"`
	BlockReason string   `json:"block_reason,omitempty"`
}

// BillingMode is the universal-wallet policy stored on a billing account.
// It is deliberately separate from the arrears/prepaid collection-risk mode.
type BillingMode string

const (
	BillingModeStandard BillingMode = "standard"
	BillingModeCredits  BillingMode = "credits"
)

// AutoTopUpConfig is the public projection of one account's optional credit
// auto-top-up configuration.
type AutoTopUpConfig struct {
	Enabled         bool   `json:"enabled"`
	ThresholdMicros int64  `json:"threshold_micros"`
	AmountMicros    int64  `json:"amount_micros"`
	PaymentMethodID string `json:"payment_method_id,omitempty"`
}

// GetCreditStandingRequest addresses one wallet by its owning user or org.
type GetCreditStandingRequest struct {
	OwnerUserID uuid.UUID `json:"owner_user_id,omitempty"`
	OwnerOrgID  uuid.UUID `json:"owner_org_id,omitempty"`
}

// GetCreditStandingResponse combines the wallet snapshot with the existing
// payment-standing verdict so consumers can render one coherent state.
type GetCreditStandingResponse struct {
	BillingMode       BillingMode      `json:"billing_mode"`
	BalanceMicros     int64            `json:"balance_micros"`
	CreditLimitMicros int64            `json:"credit_limit_micros"`
	AutoTopUp         *AutoTopUpConfig `json:"auto_topup,omitempty"`
	Blocked           bool             `json:"blocked"`
	BlockReason       string           `json:"block_reason,omitempty"`
}

// ListCreditLedgerRequest asks for a newest-first keyset page of the wallet
// journal. Cursor is opaque to callers.
type ListCreditLedgerRequest struct {
	OwnerUserID uuid.UUID `json:"owner_user_id,omitempty"`
	OwnerOrgID  uuid.UUID `json:"owner_org_id,omitempty"`
	Limit       int       `json:"limit,omitempty"`
	Cursor      string    `json:"cursor,omitempty"`
}

// CreditLedgerEntry is one immutable money-journal row on the wire.
type CreditLedgerEntry struct {
	ID                 string     `json:"id"`
	AmountMicros       int64      `json:"amount_micros"`
	Type               string     `json:"type"`
	Status             string     `json:"status"`
	BalanceAfterMicros int64      `json:"balance_after_micros"`
	Actor              string     `json:"actor"`
	ReceiptURL         string     `json:"receipt_url,omitempty"`
	ExpiresAt          *time.Time `json:"expires_at,omitempty"`
	CreatedAt          time.Time  `json:"created_at"`
}

// ListCreditLedgerResponse is one journal page. An empty next_cursor means
// the page exhausted the account's history.
type ListCreditLedgerResponse struct {
	Entries    []CreditLedgerEntry `json:"entries"`
	NextCursor string              `json:"next_cursor"`
}

// StartCreditPurchaseRequest opens an idempotent Stripe-backed wallet top-up.
type StartCreditPurchaseRequest struct {
	OwnerUserID    uuid.UUID `json:"owner_user_id,omitempty"`
	OwnerOrgID     uuid.UUID `json:"owner_org_id,omitempty"`
	AmountMicros   int64     `json:"amount_micros"`
	IdempotencyKey string    `json:"idempotency_key"`
}

// StripePurchaseInit is the Stripe rail's client-confirmation payload.
type StripePurchaseInit struct {
	ClientSecret     string `json:"client_secret"`
	HostedInvoiceURL string `json:"hosted_invoice_url,omitempty"`
}

// NewebPayPurchaseInit reserves the alternate rail's form-post payload.
type NewebPayPurchaseInit struct {
	ActionURL string            `json:"action_url"`
	Fields    map[string]string `json:"fields"`
}

// StartCreditPurchaseResponse is rail-discriminated. PurchaseID is the
// durable pending-ledger handle used by FinishCreditPurchase.
type StartCreditPurchaseResponse struct {
	PurchaseID string                `json:"purchase_id"`
	Rail       string                `json:"rail"`
	Stripe     *StripePurchaseInit   `json:"stripe,omitempty"`
	NewebPay   *NewebPayPurchaseInit `json:"newebpay,omitempty"`
}

type FinishCreditPurchaseRequest struct {
	OwnerUserID uuid.UUID `json:"owner_user_id,omitempty"`
	OwnerOrgID  uuid.UUID `json:"owner_org_id,omitempty"`
	PurchaseID  string    `json:"purchase_id"`
}

type FinishCreditPurchaseResponse struct {
	Status        string `json:"status"`
	BalanceMicros int64  `json:"balance_micros"`
	ReceiptURL    string `json:"receipt_url,omitempty"`
}

type SetAutoTopUpRequest struct {
	OwnerUserID     uuid.UUID `json:"owner_user_id,omitempty"`
	OwnerOrgID      uuid.UUID `json:"owner_org_id,omitempty"`
	Enabled         bool      `json:"enabled"`
	ThresholdMicros int64     `json:"threshold_micros"`
	AmountMicros    int64     `json:"amount_micros"`
	PaymentMethodID string    `json:"payment_method_id,omitempty"`
}

type SetAutoTopUpResponse struct {
	AutoTopUp AutoTopUpConfig `json:"auto_topup"`
}

// SetCustomerBillingModeRequest supports the pinned owner/distributor fields.
// CreditLimitMicros is additive and optional: omitting it while enabling
// credits applies the platform's $5 default; a pointer preserves explicit 0.
type SetCustomerBillingModeRequest struct {
	OwnerUserID       uuid.UUID   `json:"owner_user_id,omitempty"`
	OwnerOrgID        uuid.UUID   `json:"owner_org_id,omitempty"`
	DistributorOrgID  uuid.UUID   `json:"distributor_org_id,omitempty"`
	BillingMode       BillingMode `json:"billing_mode"`
	CreditLimitMicros *int64      `json:"credit_limit_micros,omitempty"`
}

type SetCustomerBillingModeResponse struct {
	BillingMode BillingMode `json:"billing_mode"`
}

type ListDistributorCustomersRequest struct {
	DistributorOrgID uuid.UUID `json:"distributor_org_id"`
}

// DistributorCustomerRow mirrors the pinned core fields and includes the
// richer wallet snapshot required by the distributor billing surface.
type DistributorCustomerRow struct {
	CustomerOrgID     uuid.UUID   `json:"customer_org_id"`
	BillingMode       BillingMode `json:"billing_mode"`
	CreditLimitMicros int64       `json:"credit_limit_micros"`
	Standing          string      `json:"standing"`
	AutoTopUpEnabled  bool        `json:"auto_topup_enabled"`
	BalanceMicros     int64       `json:"balance_micros"`
}

type ListDistributorCustomersResponse struct {
	Customers []DistributorCustomerRow `json:"customers"`
}

// GrantCreditsRequest accepts the pinned distributor grant shape. ExpiresAt is
// an additive optional field supported by the ledger's expiring-grant column.
type GrantCreditsRequest struct {
	DistributorOrgID uuid.UUID  `json:"distributor_org_id"`
	CustomerOrgID    uuid.UUID  `json:"customer_org_id"`
	AmountMicros     int64      `json:"amount_micros"`
	Actor            string     `json:"actor"`
	IdempotencyKey   string     `json:"idempotency_key"`
	ExpiresAt        *time.Time `json:"expires_at,omitempty"`
}

type GrantCreditsResponse struct {
	LedgerID      string `json:"ledger_id"`
	BalanceMicros int64  `json:"balance_micros"`
}

// CreditStandingRow and CreditPurchaseRow are store projections used by the
// RPC service. They intentionally have no JSON tags: only the types above are
// part of the wire contract.
type CreditStandingRow struct {
	BillingMode       BillingMode
	BalanceMicros     int64
	CreditLimitMicros int64
	AutoTopUp         *AutoTopUpConfig
}

type CreditPurchaseRow struct {
	ID                 uuid.UUID
	AccountID          uuid.UUID
	AmountMicros       int64
	Type               string
	Status             string
	BalanceAfterMicros int64
	Actor              string
	IdempotencyKey     string
	StripeInvoiceID    string
	ReceiptURL         string
	CreatedAt          time.Time
}

type CreditLedgerRecord struct {
	ID                 uuid.UUID
	AccountID          uuid.UUID
	AmountMicros       int64
	Type               string
	Status             string
	BalanceAfterMicros int64
	Actor              string
	IdempotencyKey     string
	StripeInvoiceID    string
	ReceiptURL         string
	ExpiresAt          *time.Time
	CreatedAt          time.Time
}

type CreditLedgerCursor struct {
	CreatedAt time.Time
	ID        uuid.UUID
}

type DistributorCustomerState struct {
	CustomerOrgID            uuid.UUID
	BillingMode              BillingMode
	CreditLimitMicros        int64
	AutoTopUpEnabled         bool
	AutoTopUpThresholdMicros int64
	BalanceMicros            int64
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
