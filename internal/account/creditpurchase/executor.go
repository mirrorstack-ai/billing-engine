package creditpurchase

import (
	"context"
	"fmt"
	"strings"

	stripego "github.com/stripe/stripe-go/v85"

	"github.com/mirrorstack-ai/billing-engine/internal/account/creditledger"
	billingstripe "github.com/mirrorstack-ai/billing-engine/internal/shared/stripe"
)

type Executor struct {
	store   Store
	settler Settler
	stripe  StripeClient
}

func NewExecutor(store Store, settler Settler, stripe StripeClient) *Executor {
	if store == nil || settler == nil || stripe == nil {
		panic("creditpurchase.NewExecutor: store, settler, and stripe must not be nil")
	}
	return &Executor{store: store, settler: settler, stripe: stripe}
}

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

	invoice, attempt, err := e.recoverOrCreateInvoice(ctx, attempt)
	if err != nil {
		return Result{Attempt: attempt}, err
	}
	if invoice.Status == "draft" {
		invoice, err = e.finalizeDraft(ctx, attempt, invoice)
		if err != nil {
			return Result{Attempt: attempt, Invoice: invoice}, err
		}
	} else if invoice.Status == "" {
		return Result{Attempt: attempt, Invoice: invoice}, fmt.Errorf(
			"manual credit purchase invoice %s has empty status",
			invoice.ID,
		)
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

func (e *Executor) recoverOrCreateInvoice(
	ctx context.Context,
	attempt Attempt,
) (billingstripe.Invoice, Attempt, error) {
	ref := "credit-purchase:" + attempt.ID.String()
	var (
		invoice billingstripe.Invoice
		err     error
	)
	if attempt.StripeInvoiceID != "" {
		invoice, err = e.stripe.GetInvoice(ctx, attempt.StripeInvoiceID)
		if err != nil {
			return billingstripe.Invoice{}, attempt, fmt.Errorf(
				"retrieve manual credit purchase invoice: %w",
				err,
			)
		}
	} else {
		if strings.TrimSpace(attempt.StripeCustomerID) == "" {
			return billingstripe.Invoice{}, attempt, fmt.Errorf(
				"manual credit purchase %s has no expected Stripe customer",
				attempt.ID,
			)
		}
		var found bool
		invoice, found, err = e.stripe.FindInvoiceByRef(
			ctx,
			attempt.StripeCustomerID,
			ref,
		)
		if err != nil {
			return billingstripe.Invoice{}, attempt, fmt.Errorf(
				"recover manual credit purchase invoice: %w",
				err,
			)
		}
		if !found {
			invoice, err = e.stripe.CreateCreditPurchaseInvoice(
				ctx,
				attempt.StripeCustomerID,
				attempt.AccountID.String(),
				attempt.ID.String(),
				"credit-inv:"+attempt.ID.String(),
			)
			if err != nil {
				return billingstripe.Invoice{}, attempt, fmt.Errorf(
					"create manual credit purchase invoice: %w",
					err,
				)
			}
		}
		if invoice.CustomerID != attempt.StripeCustomerID {
			return billingstripe.Invoice{}, attempt, fmt.Errorf(
				"manual credit purchase invoice customer %q does not match expected customer %q",
				invoice.CustomerID,
				attempt.StripeCustomerID,
			)
		}
		if err := validateInvoiceIdentity(attempt, invoice, false); err != nil {
			return billingstripe.Invoice{}, attempt, err
		}
		attempt, err = e.store.AttachInvoice(ctx, attempt, invoice)
		if err != nil {
			return billingstripe.Invoice{}, attempt, fmt.Errorf(
				"attach manual credit purchase invoice: %w",
				err,
			)
		}
	}
	if err := validateInvoiceIdentity(attempt, invoice, true); err != nil {
		return billingstripe.Invoice{}, attempt, err
	}
	return invoice, attempt, nil
}

func (e *Executor) finalizeDraft(
	ctx context.Context,
	attempt Attempt,
	invoice billingstripe.Invoice,
) (billingstripe.Invoice, error) {
	verified, err := e.ensureDraftLine(ctx, attempt, invoice)
	if err != nil {
		return invoice, err
	}
	finalized, err := e.stripe.FinalizeInvoice(
		ctx,
		verified.ID,
		"credit-fin:"+attempt.ID.String(),
	)
	if err != nil {
		return invoice, fmt.Errorf("finalize manual credit purchase invoice: %w", err)
	}
	if finalized.ID != verified.ID {
		return finalized, fmt.Errorf(
			"Stripe finalized invoice %q instead of attached invoice %q",
			finalized.ID,
			verified.ID,
		)
	}
	// Never trust the finalize response as payment truth.
	latest, err := e.stripe.GetInvoice(ctx, verified.ID)
	if err != nil {
		return finalized, fmt.Errorf(
			"retrieve finalized manual credit purchase invoice: %w",
			err,
		)
	}
	return latest, nil
}

func (e *Executor) ensureDraftLine(
	ctx context.Context,
	attempt Attempt,
	invoice billingstripe.Invoice,
) (billingstripe.Invoice, error) {
	if err := validateInertDraft(attempt, invoice); err != nil {
		return invoice, err
	}
	items, err := e.stripe.ListInvoiceItems(ctx, invoice.ID)
	if err != nil {
		return invoice, fmt.Errorf("list manual credit purchase draft items: %w", err)
	}
	if len(items) > 1 {
		return invoice, fmt.Errorf(
			"manual credit purchase invoice %s has %d draft items; expected zero or one",
			invoice.ID,
			len(items),
		)
	}
	if len(items) == 1 {
		if err := validateExactItem(attempt, invoice.ID, items[0]); err != nil {
			return invoice, err
		}
	} else {
		if invoice.Total != 0 ||
			invoice.AmountDue != 0 ||
			invoice.AmountPaid != 0 ||
			invoice.AmountRemaining != 0 ||
			invoice.AmountPaidOffStripe != 0 {
			return invoice, fmt.Errorf(
				"empty manual credit purchase draft %s carries money total=%d due=%d paid=%d remaining=%d off_stripe=%d",
				invoice.ID,
				invoice.Total,
				invoice.AmountDue,
				invoice.AmountPaid,
				invoice.AmountRemaining,
				invoice.AmountPaidOffStripe,
			)
		}
		expectedCents := microsToCentsRoundHalfUp(attempt.AmountMicros)
		if _, err := e.stripe.CreateInvoiceItem(
			ctx,
			invoice.CustomerID,
			invoice.ID,
			expectedCents,
			"usd",
			"MirrorStack credit purchase",
			billingstripe.LinePeriod{},
			"credit-item:"+attempt.ID.String(),
		); err != nil {
			return invoice, fmt.Errorf("create manual credit purchase invoice item: %w", err)
		}
		invoice, err = e.stripe.GetInvoice(ctx, invoice.ID)
		if err != nil {
			return invoice, fmt.Errorf(
				"retrieve manual credit purchase draft after item: %w",
				err,
			)
		}
		items, err = e.stripe.ListInvoiceItems(ctx, invoice.ID)
		if err != nil {
			return invoice, fmt.Errorf(
				"re-list manual credit purchase draft items: %w",
				err,
			)
		}
	}
	if err := validateInertDraft(attempt, invoice); err != nil {
		return invoice, err
	}
	if err := validateInvoiceIdentityAndLine(attempt, invoice, items); err != nil {
		return invoice, err
	}
	expectedCents := microsToCentsRoundHalfUp(attempt.AmountMicros)
	if invoice.AmountDue != expectedCents ||
		invoice.AmountPaid != 0 ||
		invoice.AmountRemaining != expectedCents ||
		invoice.AmountPaidOffStripe != 0 {
		return invoice, fmt.Errorf(
			"manual credit purchase draft %s has due=%d paid=%d remaining=%d off_stripe=%d; expected %d/0/%d/0",
			invoice.ID,
			invoice.AmountDue,
			invoice.AmountPaid,
			invoice.AmountRemaining,
			invoice.AmountPaidOffStripe,
			expectedCents,
			expectedCents,
		)
	}
	return invoice, nil
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

func validateInertDraft(attempt Attempt, invoice billingstripe.Invoice) error {
	if err := validateInvoiceIdentity(attempt, invoice, true); err != nil {
		return err
	}
	if invoice.Status != "draft" {
		return fmt.Errorf(
			"manual credit purchase invoice %s status is %q; expected draft",
			invoice.ID,
			invoice.Status,
		)
	}
	if invoice.CollectionMethod != string(stripego.InvoiceCollectionMethodChargeAutomatically) {
		return fmt.Errorf(
			"manual credit purchase draft %s collection_method is %q; expected charge_automatically",
			invoice.ID,
			invoice.CollectionMethod,
		)
	}
	if invoice.AutoAdvance {
		return fmt.Errorf(
			"manual credit purchase draft %s has auto_advance enabled",
			invoice.ID,
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
