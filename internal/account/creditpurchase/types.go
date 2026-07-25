// Package creditpurchase owns the resource-authoritative manual wallet
// purchase state machine shared by the foreground RPC and both webhook roots.
package creditpurchase

import (
	"context"

	"github.com/google/uuid"

	"github.com/mirrorstack-ai/billing-engine/internal/account/creditledger"
	billingstripe "github.com/mirrorstack-ai/billing-engine/internal/shared/stripe"
)

const microsPerCent int64 = 10_000

// Attempt is the durable identity a Stripe resource must prove before it can
// advance wallet money. StripeCustomerID is required only while creating or
// recovering an unattached invoice. Once attached, the exact invoice customer
// is frozen by that resource and must also own the successful PaymentIntent.
type Attempt struct {
	ID               uuid.UUID
	AccountID        uuid.UUID
	AmountMicros     int64
	Status           string
	StripeInvoiceID  string
	ReceiptURL       string
	StripeCustomerID string
}

// Result is the latest independently-read Stripe truth. TerminalFailure is
// deliberately separate from settlement: only the billing service may apply
// the existing pending->failed transition, while every paid transition uses
// the shared creditledger settlement primitive.
type Result struct {
	Attempt         Attempt
	Invoice         billingstripe.Invoice
	Settlement      creditledger.Settlement
	TerminalFailure bool
	// FailureTransitioned is true only for the first exact void
	// pending->failed commit.
	FailureTransitioned bool
}

// Store is the small durable-attempt surface needed by the shared executor.
// Get stabilizes a foreground replay; FindByStripeInvoice is the webhook's
// event-payload-independent route to the attached purchase.
type Store interface {
	Get(ctx context.Context, accountID, attemptID uuid.UUID) (Attempt, error)
	FindByStripeInvoice(ctx context.Context, stripeInvoiceID string) (Attempt, bool, error)
	AttachInvoice(ctx context.Context, attempt Attempt, invoice billingstripe.Invoice) (Attempt, error)
	Fail(ctx context.Context, attempt Attempt, receiptURL string) (Attempt, bool, error)
}

// Settler is satisfied by creditledger.Store. Manual paid proof is completed
// before this method is called; the transaction then enforces amount/currency,
// purchase type, and pending|failed->settled exactly once.
type Settler interface {
	SettleManualStripeInvoice(
		ctx context.Context,
		stripeInvoiceID string,
		amountPaidCents int64,
		currency string,
		receiptURL string,
	) (creditledger.Settlement, error)
}

type StripeClient interface {
	billingstripe.CreditPurchaseClient
}
