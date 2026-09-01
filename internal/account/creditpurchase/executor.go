package creditpurchase

import (
	"context"
	"fmt"
	"strings"
	"time"

	stripego "github.com/stripe/stripe-go/v85"

	"github.com/mirrorstack-ai/billing-engine/internal/account/creditledger"
	"github.com/mirrorstack-ai/billing-engine/internal/intent"
	"github.com/mirrorstack-ai/billing-engine/internal/intent/proposer"
	billingstripe "github.com/mirrorstack-ai/billing-engine/internal/shared/stripe"
)

type Executor struct {
	store    Store
	settler  Settler
	stripe   StripeClient
	proposer chargeProposer
	nowFn    func() time.Time
}

// now is the executor's clock, injectable so a test does not depend on the
// wall clock for an execution window it asserts on.
func (e *Executor) now() time.Time {
	if e.nowFn != nil {
		return e.nowFn().UTC()
	}
	return time.Now().UTC()
}

// WithNow replaces the clock. Test-only in practice; production takes the
// default.
func (e *Executor) WithNow(fn func() time.Time) *Executor {
	e.nowFn = fn
	return e
}

// chargeProposer is the narrow seam this leg proposes through.
//
// Declared here rather than imported so this package does not depend on the
// intent packages when no proposer is installed — the same shape the cycle
// legs use (cycle/service.go:197-205).
type chargeProposer interface {
	Propose(ctx context.Context, c proposer.Charge) (intent.ChargeIntent, error)
}

func NewExecutor(store Store, settler Settler, stripe StripeClient) *Executor {
	if store == nil || settler == nil || stripe == nil {
		panic("creditpurchase.NewExecutor: store, settler, and stripe must not be nil")
	}
	return &Executor{store: store, settler: settler, stripe: stripe}
}

// WithIntentProposer installs the seam this leg proposes through.
//
// 🔴 IT IS NO LONGER OPTIONAL. The legacy collector is deleted, so an executor
// without a proposer cannot start a purchase at all — Resume refuses a fresh
// one rather than charging it. The seam stays a setter only because the
// constructor is called from three binaries this change may not touch; those
// call sites are what must be wired next.
//
// 🔴 ARMING THIS CHANGES A CUSTOMER-FACING CONTRACT, which is why it is the
// last leg to be routed and why it took an owner decision rather than an
// engineering one.
//
// The other five legs run in background workers: a proposal replaces a charge
// nobody was waiting on. This one is synchronous. StartCreditPurchase returns
// a Stripe client_secret that exists only after the invoice is finalized
// (billing/credit.go:624-637), and a proposing version has no invoice and
// therefore no secret. The browser must poll the purchase instead.
//
// So an armed deployment must have a browser that can handle the async shape.
// Arming it against the old front end leaves a customer on a page waiting for
// a secret that will never arrive.
func (e *Executor) WithIntentProposer(p chargeProposer) *Executor {
	e.proposer = p
	return e
}

// IntentProposerArmed reports whether the seam is attached, so a deployment
// test can prove the wiring rather than a comment asserting it.
func (e *Executor) IntentProposerArmed() bool { return e.proposer != nil }

// Resume drives a fresh or already-authorized foreground purchase. The durable
// attempt is the sole payer authority; caller-supplied customer state is never
// allowed to redirect an already-created ledger row.
func (e *Executor) Resume(ctx context.Context, supplied Attempt) (Result, error) {
	if err := validateAttempt(supplied); err != nil {
		return Result{}, err
	}
	attempt, err := e.store.Get(ctx, supplied.AccountID, supplied.ID)
	if err != nil {
		return Result{}, fmt.Errorf("load manual credit purchase: %w", err)
	}
	if err := validateAttempt(attempt); err != nil {
		return Result{}, err
	}
	if attempt.Status == "settled" {
		return Result{Attempt: attempt}, nil
	}
	// Terminal for the legacy rail. The intent rail has taken this attempt and
	// holds a sealed obligation for it; resuming here would raise a second
	// invoice beside that document and collect twice.
	//
	// A soft return rather than an error, matching the auto-top-up leg
	// (autotopup/executor.go:417-418). An error here would 500 two reachable
	// callers — FinishCreditPurchase, and the idempotent StartCreditPurchase
	// replay — for a state that is correct.
	if attempt.Status == "proposed" {
		return Result{Attempt: attempt}, nil
	}
	if attempt.Status != "pending" && attempt.Status != "failed" {
		return Result{}, fmt.Errorf(
			"manual credit purchase %s has unsupported status %q",
			attempt.ID,
			attempt.Status,
		)
	}
	if attempt.StripeInvoiceID == "" && attempt.Status == "failed" {
		return Result{Attempt: attempt}, nil
	}

	// 🔴 THE CUTOVER, and there is no branch left: this leg PROPOSES.
	//
	// Unlike the cycle legs there is no durable arming claim to sit after:
	// this leg's claim IS the pending ledger row, taken when the purchase was
	// inserted, and MarkProposed moves that same row under a pending-only
	// predicate. So the proposal belongs here — after the recovery guard
	// above, which is what keeps an attempt that already reached Stripe out
	// of it.
	//
	// A purchase carrying a StripeInvoiceID has an invoice at the provider and
	// must be FINISHED there. Abandoning it would strand a charge the customer
	// can see and nobody can prove.
	if attempt.StripeInvoiceID != "" {
		return e.finishInFlightInvoice(ctx, attempt)
	}

	// 🔴 Fail closed — and deliberately BELOW the in-flight path.
	//
	// There is no collector to fall back to any more, so an unarmed executor
	// must say so rather than dereference a nil seam. It is checked here and
	// not at the top of Resume because an in-flight invoice has to stay
	// finishable on a deployment that was never armed; refusing it up there
	// would strand exactly the charge the guard above exists to protect.
	if e.proposer == nil {
		return Result{Attempt: attempt}, fmt.Errorf(
			"manual credit purchase %s cannot proceed: this leg no longer charges "+
				"and no intent proposer is installed",
			attempt.ID,
		)
	}
	return e.proposePurchase(ctx, attempt)
}

// finishInFlightInvoice completes a purchase whose invoice already exists at
// the provider.
//
// 🔴 THE ONLY PATH LEFT IN THIS LEG THAT TOUCHES STRIPE, and it ORIGINATES
// NOTHING: it re-reads the invoice this attempt is already attached to and
// reconciles the ledger against what it finds. The attachment is the proof
// that a charge left this process — abandoning it would leave a customer who
// can see that invoice, and may already have paid it, with a wallet that never
// moved and a row nobody can settle.
//
// A draft that was attached but never finalized reaches reconcileResource's
// default arm and errors by name. That is deliberate rather than an oversight:
// such a draft carries auto_advance=false so it can never collect on its own,
// nothing was taken from the customer, and finalizing it is precisely the code
// this cutover deleted.
func (e *Executor) finishInFlightInvoice(
	ctx context.Context,
	attempt Attempt,
) (Result, error) {
	invoice, err := e.stripe.GetInvoice(ctx, attempt.StripeInvoiceID)
	if err != nil {
		return Result{Attempt: attempt}, fmt.Errorf(
			"retrieve in-flight manual credit purchase invoice: %w",
			err,
		)
	}
	if err := validateInvoiceIdentity(attempt, invoice, true); err != nil {
		return Result{Attempt: attempt, Invoice: invoice}, err
	}
	return e.reconcileResource(ctx, attempt, invoice)
}

// ReconcileWebhookPaid treats the event as a notification only. The durable
// attempt and all money facts are independently loaded before settlement.
func (e *Executor) ReconcileWebhookPaid(
	ctx context.Context,
	stripeInvoiceID string,
) (creditledger.Settlement, error) {
	attempt, found, err := e.store.FindByStripeInvoice(ctx, stripeInvoiceID)
	if err != nil {
		return creditledger.Settlement{}, err
	}
	if !found {
		return creditledger.Settlement{}, nil
	}
	result := creditledger.Settlement{
		Found:     true,
		AccountID: attempt.AccountID,
		LedgerID:  attempt.ID,
		Type:      "purchase",
	}
	if attempt.Status == "settled" {
		return result, nil
	}
	if attempt.Status != "pending" && attempt.Status != "failed" {
		return result, fmt.Errorf(
			"manual credit purchase paid reconciliation found unsupported status %q",
			attempt.Status,
		)
	}
	invoice, err := e.stripe.GetInvoice(ctx, stripeInvoiceID)
	if err != nil {
		return result, fmt.Errorf("retrieve paid manual credit purchase invoice: %w", err)
	}
	items, err := e.stripe.ListInvoiceItems(ctx, invoice.ID)
	if err != nil {
		return result, fmt.Errorf("list paid manual credit purchase invoice items: %w", err)
	}
	payments, err := e.stripe.ListInvoicePayments(ctx, invoice.ID)
	if err != nil {
		return result, fmt.Errorf("list paid manual credit purchase invoice payments: %w", err)
	}
	if err := validatePaidInvoiceResource(attempt, invoice, items, payments); err != nil {
		return result, err
	}
	settlement, err := e.settle(ctx, attempt, invoice)
	if err != nil {
		return result, err
	}
	return settlement, nil
}

// ReconcileWebhookFailure independently re-reads a routed manual purchase.
// Open payment failures remain pending, uncollectible is explicitly voided and
// re-read because Stripe can still reverse it to paid, exact void fails once,
// and paid truth always wins over a prior failure.
func (e *Executor) ReconcileWebhookFailure(
	ctx context.Context,
	stripeInvoiceID string,
	failureCode string,
) (creditledger.FailureReconciliation, error) {
	attempt, found, err := e.store.FindByStripeInvoice(ctx, stripeInvoiceID)
	if err != nil {
		return creditledger.FailureReconciliation{}, err
	}
	if !found {
		return creditledger.FailureReconciliation{}, nil
	}
	result := creditledger.FailureReconciliation{
		Found:       true,
		AccountID:   attempt.AccountID,
		LedgerID:    attempt.ID,
		Status:      attempt.Status,
		FailureCode: failureCode,
	}
	if attempt.Status == "settled" {
		result.FailureCode = ""
		return result, nil
	}
	if attempt.Status != "pending" && attempt.Status != "failed" {
		return result, fmt.Errorf(
			"manual credit purchase failure reconciliation found unsupported status %q",
			attempt.Status,
		)
	}
	invoice, err := e.stripe.GetInvoice(ctx, stripeInvoiceID)
	if err != nil {
		return result, fmt.Errorf(
			"retrieve failed manual credit purchase invoice: %w",
			err,
		)
	}
	reconciled, err := e.reconcileResource(ctx, attempt, invoice)
	if err != nil {
		return result, err
	}
	if reconciled.Settlement.Found {
		result.Transitioned = reconciled.Settlement.Transitioned
		result.AccountID = reconciled.Settlement.AccountID
		result.LedgerID = reconciled.Settlement.LedgerID
		result.Status = "settled"
		result.FailureCode = ""
		return result, nil
	}
	if reconciled.TerminalFailure {
		result.Transitioned = reconciled.FailureTransitioned
		result.Status = "failed"
		return result, nil
	}
	result.Status = reconciled.Attempt.Status
	if result.Status != "failed" {
		result.FailureCode = ""
	}
	return result, nil
}

func (e *Executor) reconcileResource(
	ctx context.Context,
	attempt Attempt,
	invoice billingstripe.Invoice,
) (Result, error) {
	items, err := e.stripe.ListInvoiceItems(ctx, invoice.ID)
	if err != nil {
		return Result{Attempt: attempt, Invoice: invoice}, fmt.Errorf(
			"list manual credit purchase invoice items: %w",
			err,
		)
	}
	switch invoice.Status {
	case "paid":
		payments, err := e.stripe.ListInvoicePayments(ctx, invoice.ID)
		if err != nil {
			return Result{Attempt: attempt, Invoice: invoice}, fmt.Errorf(
				"list manual credit purchase invoice payments: %w",
				err,
			)
		}
		if err := validatePaidInvoiceResource(attempt, invoice, items, payments); err != nil {
			return Result{Attempt: attempt, Invoice: invoice}, err
		}
		settlement, err := e.settle(ctx, attempt, invoice)
		if err != nil {
			return Result{Attempt: attempt, Invoice: invoice}, err
		}
		attempt.Status = "settled"
		attempt.ReceiptURL = invoice.HostedInvoiceURL
		return Result{
			Attempt: attempt, Invoice: invoice, Settlement: settlement,
		}, nil
	case "uncollectible":
		return e.reconcileUncollectible(ctx, attempt, invoice, items)
	case "void":
		if err := validateTerminalUnpaidInvoiceResource(attempt, invoice, items); err != nil {
			return Result{Attempt: attempt, Invoice: invoice}, err
		}
		failed, transitioned, err := e.store.Fail(
			ctx,
			attempt,
			invoice.HostedInvoiceURL,
		)
		if err != nil {
			return Result{Attempt: attempt, Invoice: invoice}, fmt.Errorf(
				"fail void manual credit purchase: %w",
				err,
			)
		}
		if failed.Status == "settled" {
			return Result{Attempt: failed, Invoice: invoice}, nil
		}
		if failed.Status != "failed" {
			return Result{Attempt: failed, Invoice: invoice}, fmt.Errorf(
				"manual credit purchase %s remained %q after exact void",
				failed.ID,
				failed.Status,
			)
		}
		return Result{
			Attempt:             failed,
			Invoice:             invoice,
			TerminalFailure:     true,
			FailureTransitioned: transitioned,
		}, nil
	case "open":
		if err := validateOpenInvoiceResource(attempt, invoice, items); err != nil {
			return Result{Attempt: attempt, Invoice: invoice}, err
		}
		if attempt.Status == "pending" && invoice.HostedInvoiceURL != "" {
			updated, err := e.store.AttachInvoice(ctx, attempt, invoice)
			if err != nil {
				return Result{Attempt: attempt, Invoice: invoice}, fmt.Errorf(
					"enrich manual credit purchase invoice: %w",
					err,
				)
			}
			attempt = updated
		}
		return Result{Attempt: attempt, Invoice: invoice}, nil
	default:
		return Result{Attempt: attempt, Invoice: invoice}, fmt.Errorf(
			"manual credit purchase invoice %s has unsupported status %q",
			invoice.ID,
			invoice.Status,
		)
	}
}

func (e *Executor) reconcileUncollectible(
	ctx context.Context,
	attempt Attempt,
	invoice billingstripe.Invoice,
	items []billingstripe.InvoiceItem,
) (Result, error) {
	if err := validateUnpaidInvoiceResource(
		attempt,
		invoice,
		items,
		"uncollectible",
	); err != nil {
		return Result{Attempt: attempt, Invoice: invoice}, err
	}
	_, voidErr := e.stripe.VoidInvoice(
		ctx,
		invoice.ID,
		"credit-void:"+attempt.ID.String(),
	)
	// uncollectible is reversible in Stripe. Never fail the ledger from that
	// snapshot or the Void response: independently re-read so a paid race wins.
	latest, readErr := e.stripe.GetInvoice(ctx, invoice.ID)
	if readErr != nil {
		if voidErr != nil {
			return Result{Attempt: attempt, Invoice: invoice}, fmt.Errorf(
				"void uncollectible manual credit purchase invoice: %v (re-read also failed: %w)",
				voidErr,
				readErr,
			)
		}
		return Result{Attempt: attempt, Invoice: invoice}, fmt.Errorf(
			"verify voided manual credit purchase invoice: %w",
			readErr,
		)
	}
	latestItems, err := e.stripe.ListInvoiceItems(ctx, latest.ID)
	if err != nil {
		return Result{Attempt: attempt, Invoice: latest}, fmt.Errorf(
			"re-list manual credit purchase invoice after void: %w",
			err,
		)
	}
	switch latest.Status {
	case "paid":
		payments, err := e.stripe.ListInvoicePayments(ctx, latest.ID)
		if err != nil {
			return Result{Attempt: attempt, Invoice: latest}, fmt.Errorf(
				"list paid manual credit purchase invoice after void race: %w",
				err,
			)
		}
		if err := validatePaidInvoiceResource(
			attempt,
			latest,
			latestItems,
			payments,
		); err != nil {
			return Result{Attempt: attempt, Invoice: latest}, err
		}
		settlement, err := e.settle(ctx, attempt, latest)
		if err != nil {
			return Result{Attempt: attempt, Invoice: latest}, err
		}
		attempt.Status = "settled"
		attempt.ReceiptURL = latest.HostedInvoiceURL
		return Result{
			Attempt: attempt, Invoice: latest, Settlement: settlement,
		}, nil
	case "void":
		if err := validateTerminalUnpaidInvoiceResource(
			attempt,
			latest,
			latestItems,
		); err != nil {
			return Result{Attempt: attempt, Invoice: latest}, err
		}
		failed, transitioned, err := e.store.Fail(
			ctx,
			attempt,
			latest.HostedInvoiceURL,
		)
		if err != nil {
			return Result{Attempt: attempt, Invoice: latest}, fmt.Errorf(
				"fail voided manual credit purchase: %w",
				err,
			)
		}
		if failed.Status == "settled" {
			return Result{Attempt: failed, Invoice: latest}, nil
		}
		if failed.Status != "failed" {
			return Result{Attempt: failed, Invoice: latest}, fmt.Errorf(
				"manual credit purchase %s remained %q after verified void",
				failed.ID,
				failed.Status,
			)
		}
		return Result{
			Attempt:             failed,
			Invoice:             latest,
			TerminalFailure:     true,
			FailureTransitioned: transitioned,
		}, nil
	default:
		if voidErr != nil {
			return Result{Attempt: attempt, Invoice: latest}, fmt.Errorf(
				"void uncollectible manual credit purchase invoice failed and current status is %q: %w",
				latest.Status,
				voidErr,
			)
		}
		return Result{Attempt: attempt, Invoice: latest}, fmt.Errorf(
			"void manual credit purchase invoice returned status %q",
			latest.Status,
		)
	}
}

func (e *Executor) settle(
	ctx context.Context,
	attempt Attempt,
	invoice billingstripe.Invoice,
) (creditledger.Settlement, error) {
	settlement, err := e.settler.SettleManualStripeInvoice(
		ctx,
		invoice.ID,
		invoice.AmountPaid,
		string(invoice.Currency),
		invoice.HostedInvoiceURL,
	)
	if err != nil {
		return creditledger.Settlement{}, fmt.Errorf(
			"settle paid manual credit purchase invoice: %w",
			err,
		)
	}
	if !settlement.Found ||
		settlement.Type != "purchase" ||
		settlement.AccountID != attempt.AccountID ||
		settlement.LedgerID != attempt.ID {
		return creditledger.Settlement{}, fmt.Errorf(
			"paid manual credit purchase invoice %s did not resolve its exact durable attempt",
			invoice.ID,
		)
	}
	return settlement, nil
}

func validateAttempt(attempt Attempt) error {
	if attempt.ID == [16]byte{} || attempt.AccountID == [16]byte{} {
		return fmt.Errorf("manual credit purchase requires ledger and account ids")
	}
	if attempt.FundingAccountID == [16]byte{} ||
		attempt.FundingGeneration == [16]byte{} ||
		strings.TrimSpace(attempt.StripeCustomerID) == "" {
		return fmt.Errorf(
			"manual credit purchase %s requires a durable funding claim",
			attempt.ID,
		)
	}
	if attempt.AmountMicros <= 0 {
		return fmt.Errorf("manual credit purchase %s has non-positive amount", attempt.ID)
	}
	return nil
}

func validateInvoiceIdentity(
	attempt Attempt,
	invoice billingstripe.Invoice,
	requireAttached bool,
) error {
	if invoice.ID == "" {
		return fmt.Errorf("Stripe returned a manual credit purchase invoice without id")
	}
	if requireAttached && invoice.ID != attempt.StripeInvoiceID {
		return fmt.Errorf(
			"manual credit purchase invoice id is %q; expected attached invoice %q",
			invoice.ID,
			attempt.StripeInvoiceID,
		)
	}
	if invoice.CustomerID == "" {
		return fmt.Errorf("manual credit purchase invoice %s has no customer", invoice.ID)
	}
	if invoice.CustomerID != attempt.StripeCustomerID {
		return fmt.Errorf(
			"manual credit purchase invoice %s customer is %q; expected %q",
			invoice.ID,
			invoice.CustomerID,
			attempt.StripeCustomerID,
		)
	}
	if invoice.ChargeRef != "credit-purchase:"+attempt.ID.String() ||
		invoice.CreditOperation != "purchase" ||
		invoice.CreditAccountID != attempt.AccountID.String() ||
		invoice.CreditLedgerID != attempt.ID.String() {
		return fmt.Errorf(
			"manual credit purchase invoice %s metadata does not match account %s ledger %s",
			invoice.ID,
			attempt.AccountID,
			attempt.ID,
		)
	}
	return nil
}

func validateInvoiceIdentityAndLine(
	attempt Attempt,
	invoice billingstripe.Invoice,
	items []billingstripe.InvoiceItem,
) error {
	if err := validateInvoiceIdentity(attempt, invoice, true); err != nil {
		return err
	}
	expectedCents := microsToCentsRoundHalfUp(attempt.AmountMicros)
	if invoice.Total != expectedCents {
		return fmt.Errorf(
			"manual credit purchase invoice %s total is %d; expected %d",
			invoice.ID,
			invoice.Total,
			expectedCents,
		)
	}
	if !strings.EqualFold(string(invoice.Currency), "usd") {
		return fmt.Errorf(
			"manual credit purchase invoice %s currency is %q; expected usd",
			invoice.ID,
			invoice.Currency,
		)
	}
	if len(items) != 1 {
		return fmt.Errorf(
			"manual credit purchase invoice %s has %d attached items; expected exactly one",
			invoice.ID,
			len(items),
		)
	}
	return validateExactItem(attempt, invoice.ID, items[0])
}

func validateExactItem(
	attempt Attempt,
	invoiceID string,
	item billingstripe.InvoiceItem,
) error {
	expectedCents := microsToCentsRoundHalfUp(attempt.AmountMicros)
	if item.ID == "" ||
		item.AmountCents != expectedCents ||
		!strings.EqualFold(item.Currency, "usd") {
		return fmt.Errorf(
			"manual credit purchase invoice %s item is id=%q amount=%d currency=%q; expected one %d usd item",
			invoiceID,
			item.ID,
			item.AmountCents,
			item.Currency,
			expectedCents,
		)
	}
	return nil
}

func validateOpenInvoiceResource(
	attempt Attempt,
	invoice billingstripe.Invoice,
	items []billingstripe.InvoiceItem,
) error {
	if err := validateInvoiceIdentityAndLine(attempt, invoice, items); err != nil {
		return err
	}
	expectedCents := microsToCentsRoundHalfUp(attempt.AmountMicros)
	if invoice.CollectionMethod != string(stripego.InvoiceCollectionMethodChargeAutomatically) ||
		invoice.AmountDue != expectedCents ||
		invoice.AmountPaid != 0 ||
		invoice.AmountRemaining != expectedCents ||
		invoice.AmountPaidOffStripe != 0 {
		return fmt.Errorf(
			"open manual credit purchase invoice %s is not exact unpaid Stripe resource",
			invoice.ID,
		)
	}
	return nil
}

func validatePaidInvoiceResource(
	attempt Attempt,
	invoice billingstripe.Invoice,
	items []billingstripe.InvoiceItem,
	payments []billingstripe.InvoicePaymentProof,
) error {
	if err := validateInvoiceIdentityAndLine(attempt, invoice, items); err != nil {
		return err
	}
	expectedCents := microsToCentsRoundHalfUp(attempt.AmountMicros)
	if invoice.Status != "paid" ||
		invoice.CollectionMethod != string(stripego.InvoiceCollectionMethodChargeAutomatically) ||
		invoice.Total != expectedCents ||
		invoice.AmountDue != expectedCents ||
		invoice.AmountPaid != expectedCents ||
		invoice.AmountRemaining != 0 ||
		invoice.AmountPaidOffStripe != 0 {
		return fmt.Errorf(
			"paid manual credit purchase invoice %s lacks exact on-Stripe amount proof",
			invoice.ID,
		)
	}
	if len(payments) != 1 {
		return fmt.Errorf(
			"paid manual credit purchase invoice %s has %d paid allocations; expected exactly one",
			invoice.ID,
			len(payments),
		)
	}
	payment := payments[0]
	if payment.ID == "" ||
		payment.InvoiceID != invoice.ID ||
		payment.Status != "paid" ||
		!payment.IsDefault ||
		payment.AmountPaid != expectedCents ||
		payment.AmountRequested != expectedCents ||
		!strings.EqualFold(payment.Currency, "usd") ||
		payment.PaymentType != string(stripego.InvoicePaymentPaymentTypePaymentIntent) ||
		payment.PaymentIntentID == "" ||
		payment.PaymentIntentStatus != string(stripego.PaymentIntentStatusSucceeded) ||
		payment.PaymentIntentCustomer != invoice.CustomerID ||
		payment.PaymentMethodID == "" ||
		payment.PaymentIntentAmount != expectedCents ||
		payment.AmountReceived != expectedCents ||
		!strings.EqualFold(payment.PaymentIntentCurrency, "usd") {
		return fmt.Errorf(
			"paid manual credit purchase invoice %s does not have one exact successful PaymentIntent allocation",
			invoice.ID,
		)
	}
	return nil
}

func validateTerminalUnpaidInvoiceResource(
	attempt Attempt,
	invoice billingstripe.Invoice,
	items []billingstripe.InvoiceItem,
) error {
	return validateUnpaidInvoiceResource(attempt, invoice, items, "void")
}

func validateUnpaidInvoiceResource(
	attempt Attempt,
	invoice billingstripe.Invoice,
	items []billingstripe.InvoiceItem,
	requiredStatus string,
) error {
	if err := validateInvoiceIdentityAndLine(attempt, invoice, items); err != nil {
		return err
	}
	expectedCents := microsToCentsRoundHalfUp(attempt.AmountMicros)
	expectedRemaining := expectedCents
	if requiredStatus == "void" {
		expectedRemaining = 0
	}
	if invoice.Status != requiredStatus ||
		invoice.CollectionMethod != string(stripego.InvoiceCollectionMethodChargeAutomatically) ||
		invoice.AmountDue != expectedCents ||
		invoice.AmountPaid != 0 ||
		invoice.AmountRemaining != expectedRemaining ||
		invoice.AmountPaidOffStripe != 0 {
		return fmt.Errorf(
			"%s manual credit purchase invoice %s is not exact unpaid Stripe resource",
			requiredStatus,
			invoice.ID,
		)
	}
	return nil
}

func microsToCentsRoundHalfUp(micros int64) int64 {
	return (micros + microsPerCent/2) / microsPerCent
}

// proposePurchase seals this purchase as an intent instead of collecting it.
//
// 🔴 IT MOVES NO MONEY, and unlike every other leg the customer is WAITING.
// The caller's response carries no client secret because no invoice exists, so
// the browser has to poll the purchase until something settles it.
//
// The order is: seal first, then mark. A mark written before the seal would
// claim an intent that does not exist, and the paired CHECK on
// proposed_reference means such a row could not even be written — it would
// fail in the database rather than in code that can explain it.
func (e *Executor) proposePurchase(ctx context.Context, attempt Attempt) (Result, error) {
	sealed, err := e.proposer.Propose(ctx, proposer.Charge{
		// The proposer resolves this to the account's FUNDER's owner. A leg
		// that built an intent.Subject here is how the payer and the
		// executor's resolver came to disagree.
		AccountID: attempt.AccountID.String(),
		Kind:      intent.KindCreditPurchase,
		Currency:  purchaseCurrency,
		Lines: proposer.SingleLine(
			"MirrorStack credit purchase",
			"credit-purchase:"+attempt.ID.String(),
			attempt.AmountMicros,
		),

		// 🔴 walletFunding = 0. §6 is explicit that buying credit is not a
		// service you consumed, and a purchase funded from the wallet it is
		// topping up is circular: it would spend the balance to increase it,
		// and the taxable basis would move with a funding choice. The whole
		// obligation is the provider's to collect.
		WalletAllocationMicros: 0,

		AuthorizationID:   "credit-purchase:" + attempt.ID.String(),
		TermsRevision:     proposedTermsRevision,
		PriceBookRevision: proposedPriceBookRevision,
		NoticePolicy:      proposedNoticePolicy,
		SelectedRail:      proposedRail,

		RoutingPolicyRevision: proposedRoutingPolicy,
		// Zero tax, resolved — the same honest state the other legs record.
		// This leg has never applied tax; claiming an unresolved determination
		// would quarantine every purchase and claiming a computed one would
		// invent a figure.
		Tax: intent.TaxDetermination{
			Resolved:     true,
			Jurisdiction: "not-applicable",
			RuleRevision: proposedTaxRuleRevision,
			Verification: intent.TaxNotApplicable,
		},
		// The window a collection may happen in. A customer-present purchase
		// is expected to collect promptly, but the window is deliberately
		// generous: an intent that expires before anything can execute it is
		// dead on arrival, which is the defect two earlier legs shipped with.
		ExecuteNotBefore: e.now(),
		ExecuteNotAfter:  e.now().AddDate(0, 0, 7),
	})
	if err != nil {
		return Result{Attempt: attempt}, fmt.Errorf("propose credit purchase intent: %w", err)
	}

	// Prefixed per migration 057, "so nothing downstream can read a digest as
	// a provider object id".
	moved, err := e.store.MarkProposed(ctx, attempt, "intent:"+sealed.Digest())
	if err != nil {
		return Result{Attempt: attempt}, fmt.Errorf("mark credit purchase proposed: %w", err)
	}
	if !moved {
		// Another worker moved the row first. The intent is sealed and stored
		// either way — Propose is idempotent on the digest — so this is a lost
		// race rather than a fault, and the caller re-reads the winner.
		return Result{Attempt: attempt}, nil
	}

	attempt.Status = "proposed"
	attempt.ProposedReference = "intent:" + sealed.Digest()
	return Result{Attempt: attempt}, nil
}
