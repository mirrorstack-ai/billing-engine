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
// advance wallet money. FundingAccountID, FundingGeneration, and
// StripeCustomerID are one immutable database claim armed before Stripe. The
// invoice and successful PaymentIntent must both prove that exact customer.
type Attempt struct {
	ID                uuid.UUID
	AccountID         uuid.UUID
	AmountMicros      int64
	Status            string
	StripeInvoiceID   string
	ReceiptURL        string
	FundingAccountID  uuid.UUID
	FundingGeneration uuid.UUID
	StripeCustomerID  string
	// ProposedReference is the intent this purchase was sealed as, prefixed
	// "intent:<digest>" per migration 057. Empty until the seal, and empty
	// forever on a pre-cutover row that was collected before this leg stopped
	// charging.
	ProposedReference string
}

// Placeholder policy revisions, identical to every other cut-over leg.
//
// 🔴 They are what makes proposing SAFE. predicate.ClausePolicyPublished
// refuses to collect an intent sealed under an unpublished revision, so a
// proposed purchase is a document and not yet a charge. Routing a leg and
// ENABLING it are separate steps, and these constants are the separation.
const (
	proposedTermsRevision     = "unpublished/pending-decision-12"
	proposedPriceBookRevision = "unpublished/pending-decision-12"
	proposedNoticePolicy      = "unpublished/pending-decision-12"
	proposedTaxRuleRevision   = "unpublished/pending-decision-12"
	proposedRail              = "stripe"
	proposedRoutingPolicy     = "unpublished/pending-decision-12"

	// purchaseCurrency is the only currency this leg has ever collected in;
	// §12 item 11 (currency) is unanswered, so it is not a parameter.
	purchaseCurrency = "usd"
)

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
	// MarkProposed records that this attempt's charge was sealed as an intent
	// instead of collected. Terminal for the legacy rail.
	//
	// Returns false rather than an error when no row moved: the update is
	// scoped to a PENDING purchase, so a row another worker already settled,
	// failed or proposed is a lost race, not a fault. The caller decides what
	// a lost race means; a store that raised here would make an ordinary
	// concurrency outcome look like a defect.
	MarkProposed(ctx context.Context, attempt Attempt, intentReference string) (bool, error)
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

// StripeClient is the surface this leg keeps after the cutover.
//
// 🔴 It deliberately no longer embeds billingstripe.CreditPurchaseClient.
// That interface carries CreateCreditPurchaseInvoice, CreateInvoiceItem and
// FinalizeInvoice — the three calls this leg used to collect with — and
// embedding it would leave the executor holding a write port it must never use
// again. Narrowing it here is what makes the deletion structural instead of
// merely a code path nobody happens to reach.
type StripeClient interface {
	GetInvoice(ctx context.Context, stripeInvoiceID string) (billingstripe.Invoice, error)
	ListInvoiceItems(ctx context.Context, invoiceID string) ([]billingstripe.InvoiceItem, error)
	ListInvoicePayments(ctx context.Context, invoiceID string) ([]billingstripe.InvoicePaymentProof, error)
	// VoidInvoice is the one mutation left, and it moves no money: it closes
	// an in-flight invoice the provider gave up collecting so the ledger row
	// can reach a terminal state. Reachable only for an attempt already
	// attached to that invoice.
	VoidInvoice(ctx context.Context, invoiceID, idemKey string) (billingstripe.Invoice, error)
}
