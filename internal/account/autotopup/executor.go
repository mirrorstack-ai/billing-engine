package autotopup

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/google/uuid"
	stripego "github.com/stripe/stripe-go/v85"

	"github.com/mirrorstack-ai/billing-engine/internal/account/creditledger"
	billingstripe "github.com/mirrorstack-ai/billing-engine/internal/shared/stripe"
)

const microsPerCent int64 = 10_000

type Executor struct {
	store    Store
	settler  Settler
	stripe   StripeClient
	observer SettlementObserver
	nowFn    func() time.Time
}

func NewExecutor(store Store, settler Settler, stripe StripeClient) *Executor {
	if store == nil || settler == nil || stripe == nil {
		panic("autotopup.NewExecutor: store, settler, and stripe must not be nil")
	}
	return &Executor{
		store: store, settler: settler, stripe: stripe, nowFn: time.Now,
	}
}

func (e *Executor) WithSettlementObserver(observer SettlementObserver) *Executor {
	e.observer = observer
	return e
}

func (e *Executor) WithNow(nowFn func() time.Time) *Executor {
	if nowFn == nil {
		panic("autotopup.Executor.WithNow: nowFn must not be nil")
	}
	e.nowFn = nowFn
	return e
}

// Trigger is called by the runtime credit coordinator before it applies a
// credits-only shortfall block. Store.Acquire re-checks mode, limit, threshold,
// and card ownership under the account lock; this method then resumes the one
// durable attempt through Stripe.
func (e *Executor) Trigger(
	ctx context.Context,
	accountID uuid.UUID,
	projectedChargeMicros int64,
) (Result, error) {
	attempt, kind, err := e.store.Acquire(ctx, accountID, projectedChargeMicros, e.nowFn())
	if err != nil {
		return Result{}, err
	}
	if kind == AcquireNone {
		return Result{}, nil
	}

	if kind == AcquireExisting && attempt.Expired(e.nowFn()) {
		result, err := e.reconcileExpired(ctx, attempt)
		if err != nil || result.Status != "failed" {
			return result, err
		}

		// The old row is terminal now. Re-enter the serialized threshold check
		// before creating a replacement; paid recovery never reaches this branch.
		replacement, replacementKind, err := e.store.Acquire(
			ctx,
			accountID,
			projectedChargeMicros,
			e.nowFn(),
		)
		if err != nil {
			return result, err
		}
		if replacementKind == AcquireNone || replacement.ID == attempt.ID {
			return result, nil
		}
		return e.resume(ctx, replacement, replacementKind == AcquireNew)
	}
	return e.resume(ctx, attempt, kind == AcquireNew)
}

// ReconcileWebhookPaid handles invoice.paid without trusting the event payload
// as money truth. Auto-top-up invoices are re-read from Stripe and checked
// against the durable frozen customer, card, amount, currency, and single-line
// resource before the shared settlement transaction may advance wallet credit.
// found=false distinguishes ordinary/manual invoices for the webhook router.
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
		Type:      "auto_topup",
	}
	if attempt.Status == "settled" {
		return result, nil
	}
	if attempt.Status != "pending" && attempt.Status != "failed" {
		return result, fmt.Errorf(
			"auto-top-up paid reconciliation found unsupported ledger status %q",
			attempt.Status,
		)
	}

	invoice, err := e.stripe.GetInvoice(ctx, stripeInvoiceID)
	if err != nil {
		return result, fmt.Errorf("retrieve paid webhook auto-top-up invoice: %w", err)
	}
	items, err := e.stripe.ListInvoiceItems(ctx, stripeInvoiceID)
	if err != nil {
		return result, fmt.Errorf("list paid webhook auto-top-up invoice items: %w", err)
	}
	payments, err := e.stripe.ListInvoicePayments(ctx, stripeInvoiceID)
	if err != nil {
		return result, fmt.Errorf("list paid webhook auto-top-up invoice payments: %w", err)
	}
	if err := validatePaidInvoiceResource(attempt, invoice, items, payments); err != nil {
		return result, err
	}
	settlement, err := e.settler.SettleStripeInvoice(
		ctx,
		invoice.ID,
		invoice.AmountPaid,
		string(invoice.Currency),
		invoice.HostedInvoiceURL,
	)
	if err != nil {
		return result, fmt.Errorf("settle paid webhook auto-top-up invoice: %w", err)
	}
	if !settlement.Found {
		return result, fmt.Errorf(
			"paid auto-top-up invoice %s has no durable ledger attempt",
			invoice.ID,
		)
	}
	return settlement, nil
}

// ReconcileWebhookFailure handles non-paid invoice terminal/failure webhooks
// without trusting the event payload as money truth. It first resolves the
// linked durable attempt, then re-reads Stripe and proves the frozen
// invoice/customer/amount/currency/single-line invariants. An exact unpaid open
// invoice is deterministically voided and re-read before the ledger may fail.
// A paid re-read wins over every earlier failure and settles exactly once.
//
// Invariant mismatches, foreign resources, partial payments, and non-terminal
// states deliberately remain pending. Stripe read/write ambiguity returns an
// error so the webhook transport retries while the 10-minute durable expiry
// remains the final recovery bound.
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
		FailureCode: attempt.FailureCode,
	}
	if attempt.Status == "settled" {
		return result, nil
	}
	if attempt.Status != "pending" && attempt.Status != "failed" {
		return result, fmt.Errorf(
			"auto-top-up webhook reconciliation found unsupported ledger status %q",
			attempt.Status,
		)
	}
	if failureCode == "" {
		failureCode = "payment_failed"
	}

	invoice, err := e.stripe.GetInvoice(ctx, stripeInvoiceID)
	if err != nil {
		return result, fmt.Errorf("retrieve webhook auto-top-up invoice: %w", err)
	}

	switch invoice.Status {
	case "paid":
		items, err := e.stripe.ListInvoiceItems(ctx, stripeInvoiceID)
		if err != nil {
			return result, fmt.Errorf("list webhook auto-top-up invoice items: %w", err)
		}
		return e.settleWebhookPaid(ctx, attempt, invoice, items, result)
	case "void", "uncollectible":
		if validateTerminalUnpaidInvoiceResource(attempt, invoice) != nil {
			return result, nil
		}
		return e.failWebhookAttempt(ctx, attempt, invoice, failureCode, result)
	case "open":
		items, err := e.stripe.ListInvoiceItems(ctx, stripeInvoiceID)
		if err != nil {
			return result, fmt.Errorf("list webhook auto-top-up invoice items: %w", err)
		}
		if validateInvoiceResource(attempt, invoice, items, "open") != nil {
			return result, nil
		}
	default:
		return result, nil
	}

	// The exact open invoice is unpaid and owned by this attempt. Close the
	// Stripe resource first, using the same deterministic idempotency key as
	// foreground recovery, then independently re-read before touching ledger
	// state.
	_, voidErr := e.stripe.VoidInvoice(
		ctx,
		invoice.ID,
		"credit-auto-topup-void:"+attempt.ID.String(),
	)
	latest, readErr := e.stripe.GetInvoice(ctx, invoice.ID)
	if readErr != nil {
		if voidErr != nil {
			return result, fmt.Errorf(
				"void webhook auto-top-up invoice: %v (re-read also failed: %w)",
				voidErr,
				readErr,
			)
		}
		return result, fmt.Errorf("verify voided webhook auto-top-up invoice: %w", readErr)
	}
	if latest.Status == "paid" {
		latestItems, err := e.stripe.ListInvoiceItems(ctx, invoice.ID)
		if err != nil {
			return result, fmt.Errorf("re-list webhook auto-top-up invoice items: %w", err)
		}
		return e.settleWebhookPaid(ctx, attempt, latest, latestItems, result)
	}
	if latest.Status != "void" && latest.Status != "uncollectible" {
		if voidErr != nil {
			return result, fmt.Errorf(
				"void webhook auto-top-up invoice failed and current status is %q: %w",
				latest.Status,
				voidErr,
			)
		}
		return result, fmt.Errorf(
			"void webhook auto-top-up invoice returned non-terminal status %q",
			latest.Status,
		)
	}
	if validateTerminalUnpaidInvoiceResource(attempt, latest) != nil {
		return result, nil
	}
	return e.failWebhookAttempt(ctx, attempt, latest, failureCode, result)
}

func (e *Executor) settleWebhookPaid(
	ctx context.Context,
	attempt Attempt,
	invoice billingstripe.Invoice,
	items []billingstripe.InvoiceItem,
	result creditledger.FailureReconciliation,
) (creditledger.FailureReconciliation, error) {
	payments, err := e.stripe.ListInvoicePayments(ctx, invoice.ID)
	if err != nil {
		return result, fmt.Errorf("list paid webhook auto-top-up invoice payments: %w", err)
	}
	if err := validatePaidInvoiceResource(attempt, invoice, items, payments); err != nil {
		return result, nil
	}
	settlement, err := e.settler.SettleStripeInvoice(
		ctx,
		invoice.ID,
		invoice.AmountPaid,
		string(invoice.Currency),
		invoice.HostedInvoiceURL,
	)
	if err != nil {
		return result, err
	}
	if !settlement.Found {
		return result, fmt.Errorf(
			"paid auto-top-up invoice %s has no durable ledger attempt",
			invoice.ID,
		)
	}
	result.Transitioned = settlement.Transitioned
	result.AccountID = settlement.AccountID
	result.LedgerID = settlement.LedgerID
	result.Status = "settled"
	result.FailureCode = ""
	return result, nil
}

func (e *Executor) failWebhookAttempt(
	ctx context.Context,
	attempt Attempt,
	invoice billingstripe.Invoice,
	failureCode string,
	result creditledger.FailureReconciliation,
) (creditledger.FailureReconciliation, error) {
	failed, transitioned, err := e.store.Fail(
		ctx,
		attempt,
		failureCode,
		invoice.HostedInvoiceURL,
	)
	if err != nil {
		return result, err
	}
	result.Transitioned = transitioned
	result.Status = failed.Status
	result.FailureCode = failed.FailureCode
	return result, nil
}

func (e *Executor) resume(ctx context.Context, attempt Attempt, isNew bool) (Result, error) {
	current, err := e.store.Get(ctx, attempt.AccountID, attempt.ID)
	if err != nil {
		return Result{}, err
	}
	if current.Status != "pending" {
		return resultFor(current, isNew), nil
	}
	attempt = current

	invoice, attempt, err := e.recoverOrCreateInvoice(ctx, attempt)
	if err != nil {
		return resultFor(attempt, isNew), err
	}
	if handled, result, err := e.handleTerminalInvoice(ctx, attempt, invoice, isNew); handled {
		return result, err
	}

	if invoice.Status == "" || invoice.Status == "draft" {
		invoice, err = e.finalizeDraft(ctx, attempt, invoice)
		if err != nil {
			return resultFor(attempt, isNew), err
		}
		if handled, result, err := e.handleTerminalInvoice(ctx, attempt, invoice, isNew); handled {
			return result, err
		}
	}

	if invoice.Status != "open" {
		return resultFor(attempt, isNew), fmt.Errorf(
			"auto-top-up invoice %s is not payable in status %q",
			invoice.ID,
			invoice.Status,
		)
	}
	if invariantErr := e.validatePayableInvoice(ctx, attempt, invoice); invariantErr != nil {
		// An open invoice with no money collected is safe to close. Void it
		// before releasing the durable pending guard, exactly like a proven
		// card decline. A partially paid or unreadable resource stays pending.
		if invoice.AmountPaid == 0 &&
			invoice.ID == attempt.StripeInvoiceID &&
			invoice.CustomerID == attempt.StripeCustomerID {
			result, closeErr := e.voidAndFail(
				ctx,
				attempt,
				invoice,
				"invoice_invariant_mismatch",
				isNew,
			)
			if closeErr != nil {
				return result, fmt.Errorf(
					"auto-top-up invoice invariant failed: %v; safe close failed: %w",
					invariantErr,
					closeErr,
				)
			}
			return result, nil
		}
		return resultFor(attempt, isNew), fmt.Errorf(
			"auto-top-up invoice invariant failed with nonzero amount_paid; leaving pending: %w",
			invariantErr,
		)
	}

	paid, payErr := e.stripe.PayInvoiceWithMethod(
		ctx,
		invoice.ID,
		attempt.StripePaymentMethodID,
		"credit-auto-topup-pay:"+attempt.ID.String(),
	)
	if payErr != nil {
		return e.resolvePayError(ctx, attempt, invoice, payErr, isNew)
	}
	if handled, result, err := e.handleTerminalInvoice(ctx, attempt, paid, isNew); handled {
		return result, err
	}
	return resultFor(attempt, isNew), nil
}

// reconcileExpired establishes terminal Stripe truth before releasing the
// partial unique pending guard. A recovered draft is permanently deleted and
// independently verified missing; a finalized invoice is voided or settled
// from exact payment truth. Both paths establish a resource-level barrier
// against a concurrent stale worker paying after the ledger is failed.
func (e *Executor) reconcileExpired(ctx context.Context, attempt Attempt) (Result, error) {
	invoice, attempt, err := e.recoverOrCreateInvoice(ctx, attempt)
	if err != nil {
		return resultFor(attempt, false), err
	}
	if handled, result, err := e.handleTerminalInvoice(ctx, attempt, invoice, false); handled {
		return result, err
	}
	return e.voidAndFail(ctx, attempt, invoice, "attempt_expired", false)
}

func (e *Executor) recoverOrCreateInvoice(
	ctx context.Context,
	attempt Attempt,
) (billingstripe.Invoice, Attempt, error) {
	var (
		invoice billingstripe.Invoice
		err     error
	)
	ref := "credit-auto-topup:" + attempt.ID.String()
	if attempt.StripeInvoiceID != "" {
		invoice, err = e.stripe.GetInvoice(ctx, attempt.StripeInvoiceID)
		if err != nil {
			return billingstripe.Invoice{}, attempt, fmt.Errorf("retrieve auto-top-up invoice: %w", err)
		}
	} else {
		var found bool
		invoice, found, err = e.stripe.FindInvoiceByRef(ctx, attempt.StripeCustomerID, ref)
		if err != nil {
			return billingstripe.Invoice{}, attempt, fmt.Errorf("recover auto-top-up invoice: %w", err)
		}
		if !found {
			invoice, err = e.stripe.CreateAutoTopUpInvoice(
				ctx,
				attempt.StripeCustomerID,
				attempt.StripePaymentMethodID,
				ref,
				"credit-auto-topup-invoice:"+attempt.ID.String(),
			)
			if err != nil {
				return billingstripe.Invoice{}, attempt, fmt.Errorf("create auto-top-up invoice: %w", err)
			}
		}
		attempt, err = e.store.AttachInvoice(ctx, attempt, invoice)
		if err != nil {
			return billingstripe.Invoice{}, attempt, fmt.Errorf("attach auto-top-up invoice: %w", err)
		}
	}
	if invoice.ID == "" {
		return billingstripe.Invoice{}, attempt, fmt.Errorf("Stripe returned an invoice without id")
	}
	if invoice.CustomerID != attempt.StripeCustomerID {
		return billingstripe.Invoice{}, attempt, fmt.Errorf(
			"auto-top-up invoice customer %q does not match frozen customer %q",
			invoice.CustomerID,
			attempt.StripeCustomerID,
		)
	}
	return invoice, attempt, nil
}

// finalizeDraft always pins the exact credit line before opening an invoice.
// This helper is shared by the ordinary charge path and expired-attempt
// reconciliation: a process may crash immediately after the durable attempt
// insert, leaving no Stripe resource/line to recover. Finalizing that empty
// draft could otherwise produce a paid $0 invoice and strand the pending
// guard. Both calls are deterministically idempotent.
func (e *Executor) finalizeDraft(
	ctx context.Context,
	attempt Attempt,
	invoice billingstripe.Invoice,
) (billingstripe.Invoice, error) {
	verifiedDraft, err := e.ensureDraftLine(ctx, attempt, invoice)
	if err != nil {
		return invoice, err
	}
	finalized, err := e.stripe.FinalizeInvoiceWithoutAutoAdvance(
		ctx,
		verifiedDraft.ID,
		"credit-auto-topup-finalize:"+attempt.ID.String(),
	)
	if err != nil {
		return invoice, fmt.Errorf("finalize auto-top-up invoice: %w", err)
	}
	if finalized.ID == "" {
		return invoice, fmt.Errorf("Stripe finalized an invoice without id")
	}
	if finalized.ID != verifiedDraft.ID {
		return invoice, fmt.Errorf(
			"Stripe finalized invoice %q instead of attached invoice %q",
			finalized.ID,
			verifiedDraft.ID,
		)
	}
	// Never pay based only on the finalize response. Re-read the resource so
	// post-finalization amount/currency/customer truth is independently
	// established before the explicit pay call.
	latest, err := e.stripe.GetInvoice(ctx, finalized.ID)
	if err != nil {
		return finalized, fmt.Errorf("retrieve finalized auto-top-up invoice: %w", err)
	}
	return latest, nil
}

// ensureDraftLine establishes Stripe resource truth before relying on an
// idempotency key. Stripe may prune keys after roughly 24 hours, while a
// durable attempt can be recovered later. An already-priced draft is reused
// only when its total and currency exactly match this frozen attempt; a
// mismatched or duplicate full line fails closed before finalize/pay.
func (e *Executor) ensureDraftLine(
	ctx context.Context,
	attempt Attempt,
	invoice billingstripe.Invoice,
) (billingstripe.Invoice, error) {
	expectedCents := microsToCentsRoundHalfUp(attempt.AmountMicros)
	items, err := e.stripe.ListInvoiceItems(ctx, invoice.ID)
	if err != nil {
		return invoice, fmt.Errorf("list auto-top-up draft items: %w", err)
	}
	if len(items) == 0 {
		if invoice.AmountDue != 0 || invoice.AmountPaid != 0 {
			return invoice, fmt.Errorf(
				"empty auto-top-up draft %s has amount_due=%d amount_paid=%d",
				invoice.ID,
				invoice.AmountDue,
				invoice.AmountPaid,
			)
		}
		if _, err := e.stripe.CreateInvoiceItem(
			ctx,
			attempt.StripeCustomerID,
			invoice.ID,
			expectedCents,
			"usd",
			"MirrorStack automatic credit top-up",
			billingstripe.LinePeriod{},
			"credit-auto-topup-item:"+attempt.ID.String(),
		); err != nil {
			return invoice, fmt.Errorf("create auto-top-up invoice item: %w", err)
		}
		// Re-read after the write; this proves a newly-created (or idempotently
		// replayed) item became exactly one attached line.
		invoice, err = e.stripe.GetInvoice(ctx, invoice.ID)
		if err != nil {
			return invoice, fmt.Errorf("retrieve auto-top-up draft after item: %w", err)
		}
		items, err = e.stripe.ListInvoiceItems(ctx, invoice.ID)
		if err != nil {
			return invoice, fmt.Errorf("re-list auto-top-up draft items: %w", err)
		}
	}
	if err := validateInvoiceResource(attempt, invoice, items, "draft"); err != nil {
		return invoice, err
	}
	return invoice, nil
}

func (e *Executor) validatePayableInvoice(
	ctx context.Context,
	attempt Attempt,
	invoice billingstripe.Invoice,
) error {
	items, err := e.stripe.ListInvoiceItems(ctx, invoice.ID)
	if err != nil {
		return fmt.Errorf("list finalized auto-top-up invoice items: %w", err)
	}
	return validateInvoiceResource(attempt, invoice, items, "open")
}

func validateInvoiceResource(
	attempt Attempt,
	invoice billingstripe.Invoice,
	items []billingstripe.InvoiceItem,
	requiredStatus string,
) error {
	expectedCents := microsToCentsRoundHalfUp(attempt.AmountMicros)
	if err := validateInvoiceIdentityAndLine(attempt, invoice, items); err != nil {
		return err
	}
	if invoice.Status != requiredStatus {
		return fmt.Errorf(
			"auto-top-up invoice %s status is %q; expected %q",
			invoice.ID,
			invoice.Status,
			requiredStatus,
		)
	}
	if invoice.CollectionMethod != string(stripego.InvoiceCollectionMethodChargeAutomatically) {
		return fmt.Errorf(
			"auto-top-up invoice %s collection_method is %q; expected charge_automatically",
			invoice.ID,
			invoice.CollectionMethod,
		)
	}
	if invoice.AutoAdvance {
		return fmt.Errorf(
			"auto-top-up invoice %s has auto_advance enabled; expected disabled",
			invoice.ID,
		)
	}
	if invoice.DefaultPaymentMethodID != attempt.StripePaymentMethodID {
		return fmt.Errorf(
			"auto-top-up invoice %s default payment method is %q; expected frozen method %q",
			invoice.ID,
			invoice.DefaultPaymentMethodID,
			attempt.StripePaymentMethodID,
		)
	}
	if invoice.AmountDue != expectedCents || invoice.AmountPaid != 0 {
		return fmt.Errorf(
			"auto-top-up invoice %s has amount_due=%d amount_paid=%d; expected %d/0",
			invoice.ID,
			invoice.AmountDue,
			invoice.AmountPaid,
			expectedCents,
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
	expectedCents := microsToCentsRoundHalfUp(attempt.AmountMicros)
	if err := validateInvoiceIdentityAndLine(attempt, invoice, items); err != nil {
		return err
	}
	if invoice.Status != "paid" ||
		invoice.AmountPaid != expectedCents ||
		invoice.AmountDue != 0 ||
		invoice.AmountPaidOffStripe != 0 {
		return fmt.Errorf(
			"paid auto-top-up invoice %s has status=%q amount_paid=%d amount_due=%d amount_paid_off_stripe=%d; expected paid/%d/0/0",
			invoice.ID,
			invoice.Status,
			invoice.AmountPaid,
			invoice.AmountDue,
			invoice.AmountPaidOffStripe,
			expectedCents,
		)
	}
	if len(payments) != 1 {
		return fmt.Errorf(
			"paid auto-top-up invoice %s has %d paid allocations; expected exactly one",
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
		payment.PaymentIntentCustomer != attempt.StripeCustomerID ||
		payment.PaymentMethodID != attempt.StripePaymentMethodID ||
		payment.PaymentIntentAmount != expectedCents ||
		payment.AmountReceived != expectedCents ||
		!strings.EqualFold(payment.PaymentIntentCurrency, "usd") {
		return fmt.Errorf(
			"paid auto-top-up invoice %s does not have one exact frozen-card PaymentIntent allocation",
			invoice.ID,
		)
	}
	return nil
}

func validateTerminalUnpaidInvoiceResource(
	attempt Attempt,
	invoice billingstripe.Invoice,
) error {
	if invoice.ID != attempt.StripeInvoiceID {
		return fmt.Errorf(
			"terminal auto-top-up invoice id is %q; expected attached invoice %q",
			invoice.ID,
			attempt.StripeInvoiceID,
		)
	}
	if invoice.CustomerID != attempt.StripeCustomerID {
		return fmt.Errorf(
			"terminal auto-top-up invoice %s customer is %q; expected %q",
			invoice.ID,
			invoice.CustomerID,
			attempt.StripeCustomerID,
		)
	}
	if (invoice.Status != "void" && invoice.Status != "uncollectible") ||
		invoice.AmountPaid != 0 ||
		invoice.AmountPaidOffStripe != 0 {
		return fmt.Errorf(
			"terminal auto-top-up invoice %s has status=%q amount_paid=%d amount_paid_off_stripe=%d; expected void|uncollectible/0/0",
			invoice.ID,
			invoice.Status,
			invoice.AmountPaid,
			invoice.AmountPaidOffStripe,
		)
	}
	return nil
}

func validateInvoiceIdentityAndLine(
	attempt Attempt,
	invoice billingstripe.Invoice,
	items []billingstripe.InvoiceItem,
) error {
	expectedCents := microsToCentsRoundHalfUp(attempt.AmountMicros)
	if invoice.ID != attempt.StripeInvoiceID {
		return fmt.Errorf(
			"auto-top-up invoice id is %q; expected attached invoice %q",
			invoice.ID,
			attempt.StripeInvoiceID,
		)
	}
	if invoice.CustomerID != attempt.StripeCustomerID {
		return fmt.Errorf(
			"auto-top-up invoice %s customer is %q; expected %q",
			invoice.ID,
			invoice.CustomerID,
			attempt.StripeCustomerID,
		)
	}
	if invoice.Total != expectedCents {
		return fmt.Errorf(
			"auto-top-up invoice %s total is %d; expected %d",
			invoice.ID,
			invoice.Total,
			expectedCents,
		)
	}
	if !strings.EqualFold(string(invoice.Currency), "usd") {
		return fmt.Errorf(
			"auto-top-up invoice %s currency is %q; expected usd",
			invoice.ID,
			invoice.Currency,
		)
	}
	if len(items) != 1 {
		return fmt.Errorf(
			"auto-top-up invoice %s has %d attached items; expected exactly one",
			invoice.ID,
			len(items),
		)
	}
	item := items[0]
	if item.ID == "" ||
		item.AmountCents != expectedCents ||
		!strings.EqualFold(item.Currency, "usd") {
		return fmt.Errorf(
			"auto-top-up invoice %s item is id=%q amount=%d currency=%q; expected one %d usd item",
			invoice.ID,
			item.ID,
			item.AmountCents,
			item.Currency,
			expectedCents,
		)
	}
	return nil
}

func (e *Executor) resolvePayError(
	ctx context.Context,
	attempt Attempt,
	invoice billingstripe.Invoice,
	payErr error,
	isNew bool,
) (Result, error) {
	latest, readErr := e.stripe.GetInvoice(ctx, invoice.ID)
	if readErr == nil {
		if handled, result, err := e.handleTerminalInvoice(ctx, attempt, latest, isNew); handled {
			return result, err
		}
	}

	failureCode, deterministic := deterministicPaymentFailure(payErr)
	if !deterministic {
		if readErr != nil {
			return resultFor(attempt, isNew), fmt.Errorf(
				"pay auto-top-up invoice: %w (re-read also failed: %v)",
				payErr,
				readErr,
			)
		}
		// Network/API ambiguity remains pending and recoverable.
		return resultFor(attempt, isNew), fmt.Errorf("pay auto-top-up invoice: %w", payErr)
	}
	if readErr != nil {
		// Never fail the ledger from an error alone. We must prove paid or close
		// the unpaid Stripe resource first.
		return resultFor(attempt, isNew), fmt.Errorf(
			"deterministic payment failure %q could not be reconciled: %w",
			failureCode,
			readErr,
		)
	}
	return e.voidAndFail(ctx, attempt, latest, failureCode, isNew)
}

func (e *Executor) voidAndFail(
	ctx context.Context,
	attempt Attempt,
	invoice billingstripe.Invoice,
	failureCode string,
	isNew bool,
) (Result, error) {
	if invoice.Status == "" {
		if invoice.ID != attempt.StripeInvoiceID ||
			invoice.CustomerID != attempt.StripeCustomerID ||
			invoice.AmountPaid != 0 {
			return resultFor(attempt, isNew), fmt.Errorf(
				"cannot re-read ambiguous auto-top-up invoice %q: attached=%q customer=%q expected_customer=%q amount_paid=%d",
				invoice.ID,
				attempt.StripeInvoiceID,
				invoice.CustomerID,
				attempt.StripeCustomerID,
				invoice.AmountPaid,
			)
		}
		latest, err := e.stripe.GetInvoice(ctx, invoice.ID)
		if err != nil {
			return resultFor(attempt, isNew), fmt.Errorf(
				"retrieve auto-top-up invoice before safe close: %w",
				err,
			)
		}
		invoice = latest
	}
	if invoice.Status == "draft" {
		return e.deleteDraftAndFail(ctx, attempt, invoice, failureCode, isNew)
	}
	if handled, result, err := e.handleTerminalInvoice(ctx, attempt, invoice, isNew); handled {
		return result, err
	}
	if invoice.Status != "open" {
		return resultFor(attempt, isNew), fmt.Errorf(
			"cannot close unpaid auto-top-up invoice %s in status %q",
			invoice.ID,
			invoice.Status,
		)
	}
	if invoice.ID != attempt.StripeInvoiceID ||
		invoice.CustomerID != attempt.StripeCustomerID ||
		invoice.AmountPaid != 0 {
		return resultFor(attempt, isNew), fmt.Errorf(
			"cannot safely close auto-top-up invoice %q: attached=%q customer=%q expected_customer=%q amount_paid=%d",
			invoice.ID,
			attempt.StripeInvoiceID,
			invoice.CustomerID,
			attempt.StripeCustomerID,
			invoice.AmountPaid,
		)
	}

	_, voidErr := e.stripe.VoidInvoice(
		ctx,
		invoice.ID,
		"credit-auto-topup-void:"+attempt.ID.String(),
	)
	// Never release the guard from the Void response alone. Re-read the exact
	// resource so a malformed response, a pay/void race, or an ambiguous write
	// resolves from current Stripe truth.
	latest, readErr := e.stripe.GetInvoice(ctx, invoice.ID)
	if readErr != nil {
		if voidErr != nil {
			return resultFor(attempt, isNew), fmt.Errorf(
				"void auto-top-up invoice: %w (re-read also failed: %v)",
				voidErr,
				readErr,
			)
		}
		return resultFor(attempt, isNew), fmt.Errorf(
			"verify voided auto-top-up invoice: %w",
			readErr,
		)
	}
	if latest.Status == "paid" {
		if handled, result, err := e.handleTerminalInvoice(ctx, attempt, latest, isNew); handled {
			return result, err
		}
	}
	if err := validateTerminalUnpaidInvoiceResource(attempt, latest); err != nil {
		if voidErr != nil {
			return resultFor(attempt, isNew), fmt.Errorf(
				"void auto-top-up invoice failed and current resource is unsafe: %v: %w",
				err,
				voidErr,
			)
		}
		return resultFor(attempt, isNew), err
	}
	failed, transitioned, err := e.store.Fail(ctx, attempt, failureCode, latest.HostedInvoiceURL)
	if err != nil {
		return resultFor(attempt, isNew), fmt.Errorf("fail auto-top-up ledger: %w", err)
	}
	if transitioned {
		e.observe(ctx, failed.AccountID)
	}
	return resultFor(failed, isNew), nil
}

func (e *Executor) deleteDraftAndFail(
	ctx context.Context,
	attempt Attempt,
	invoice billingstripe.Invoice,
	failureCode string,
	isNew bool,
) (Result, error) {
	if invoice.ID != attempt.StripeInvoiceID ||
		invoice.CustomerID != attempt.StripeCustomerID ||
		invoice.Status != "draft" ||
		invoice.AmountPaid != 0 {
		return resultFor(attempt, isNew), fmt.Errorf(
			"cannot safely delete auto-top-up draft %q: attached=%q customer=%q expected_customer=%q status=%q amount_paid=%d",
			invoice.ID,
			attempt.StripeInvoiceID,
			invoice.CustomerID,
			attempt.StripeCustomerID,
			invoice.Status,
			invoice.AmountPaid,
		)
	}

	deleted, deleteErr := e.stripe.DeleteDraftInvoice(ctx, invoice.ID)
	invalidDeleteResponse := deleteErr == nil &&
		(deleted.ID != invoice.ID || !deleted.Deleted)

	// A DELETE response alone is not enough to release a money guard. Always
	// re-read the exact attached resource; only Stripe's resource_missing truth
	// proves a stale worker can no longer finalize or pay this draft.
	latest, readErr := e.stripe.GetInvoice(ctx, invoice.ID)
	if isStripeResourceMissing(readErr) {
		if invalidDeleteResponse {
			return resultFor(attempt, isNew), fmt.Errorf(
				"delete auto-top-up draft returned invalid response id=%q deleted=%t",
				deleted.ID,
				deleted.Deleted,
			)
		}
		return e.failWithoutStripeResource(ctx, attempt, failureCode, isNew)
	}
	if readErr != nil {
		if deleteErr != nil {
			return resultFor(attempt, isNew), fmt.Errorf(
				"delete auto-top-up draft: %v (verification read also failed: %w)",
				deleteErr,
				readErr,
			)
		}
		return resultFor(attempt, isNew), fmt.Errorf(
			"verify deleted auto-top-up draft: %w",
			readErr,
		)
	}

	// Delete may lose a race to another worker's finalize/pay. Route the
	// authoritative post-delete read through the normal terminal/open logic.
	if handled, result, err := e.handleTerminalInvoice(ctx, attempt, latest, isNew); handled {
		return result, err
	}
	if latest.Status == "open" {
		return e.voidAndFail(ctx, attempt, latest, failureCode, isNew)
	}
	if deleteErr != nil {
		return resultFor(attempt, isNew), fmt.Errorf(
			"delete auto-top-up draft failed and invoice remains %q: %w",
			latest.Status,
			deleteErr,
		)
	}
	return resultFor(attempt, isNew), fmt.Errorf(
		"delete auto-top-up draft returned but invoice remains %q",
		latest.Status,
	)
}

func (e *Executor) failWithoutStripeResource(
	ctx context.Context,
	attempt Attempt,
	failureCode string,
	isNew bool,
) (Result, error) {
	failed, transitioned, err := e.store.Fail(ctx, attempt, failureCode, attempt.ReceiptURL)
	if err != nil {
		return resultFor(attempt, isNew), fmt.Errorf(
			"fail auto-top-up attempt without Stripe resource: %w",
			err,
		)
	}
	if transitioned {
		e.observe(ctx, failed.AccountID)
	}
	return resultFor(failed, isNew), nil
}

func isStripeResourceMissing(err error) bool {
	var stripeErr *stripego.Error
	return errors.As(err, &stripeErr) &&
		(stripeErr.Code == stripego.ErrorCodeResourceMissing ||
			stripeErr.HTTPStatusCode == 404)
}

func (e *Executor) handleTerminalInvoice(
	ctx context.Context,
	attempt Attempt,
	invoice billingstripe.Invoice,
	isNew bool,
) (bool, Result, error) {
	switch invoice.Status {
	case "paid":
		items, err := e.stripe.ListInvoiceItems(ctx, invoice.ID)
		if err != nil {
			return true, resultFor(attempt, isNew), fmt.Errorf(
				"list paid auto-top-up invoice items: %w",
				err,
			)
		}
		payments, err := e.stripe.ListInvoicePayments(ctx, invoice.ID)
		if err != nil {
			return true, resultFor(attempt, isNew), fmt.Errorf(
				"list paid auto-top-up invoice payments: %w",
				err,
			)
		}
		if err := validatePaidInvoiceResource(attempt, invoice, items, payments); err != nil {
			return true, resultFor(attempt, isNew), err
		}
		settlement, err := e.settler.SettleStripeInvoice(
			ctx,
			invoice.ID,
			invoice.AmountPaid,
			string(invoice.Currency),
			invoice.HostedInvoiceURL,
		)
		if err != nil {
			return true, resultFor(attempt, isNew), fmt.Errorf("settle auto-top-up invoice: %w", err)
		}
		if !settlement.Found {
			return true, resultFor(attempt, isNew), fmt.Errorf(
				"paid auto-top-up invoice %s has no durable ledger attempt",
				invoice.ID,
			)
		}
		if settlement.Transitioned {
			e.observe(ctx, settlement.AccountID)
		}
		current, err := e.store.Get(ctx, attempt.AccountID, attempt.ID)
		if err != nil {
			return true, resultFor(attempt, isNew), err
		}
		return true, resultFor(current, isNew), nil
	case "void", "uncollectible":
		if err := validateTerminalUnpaidInvoiceResource(attempt, invoice); err != nil {
			return true, resultFor(attempt, isNew), err
		}
		failureCode := "invoice_" + invoice.Status
		failed, transitioned, err := e.store.Fail(
			ctx,
			attempt,
			failureCode,
			invoice.HostedInvoiceURL,
		)
		if err != nil {
			return true, resultFor(attempt, isNew), err
		}
		if transitioned {
			e.observe(ctx, failed.AccountID)
		}
		return true, resultFor(failed, isNew), nil
	default:
		return false, Result{}, nil
	}
}

func (e *Executor) observe(ctx context.Context, accountID uuid.UUID) {
	if e.observer == nil {
		return
	}
	if err := e.observer.ObserveAccount(creditledger.WithSettlementObservation(ctx), accountID); err != nil {
		slog.WarnContext(ctx, "auto-top-up settlement observer failed; durable truth committed",
			"account_id", accountID, "error", err)
	}
}

func deterministicPaymentFailure(err error) (string, bool) {
	var stripeErr *stripego.Error
	if !errors.As(err, &stripeErr) {
		return "", false
	}
	if stripeErr.Code == stripego.ErrorCodeInvoicePaymentIntentRequiresAction {
		return "authentication_required", true
	}
	if stripeErr.Type != stripego.ErrorTypeCard && stripeErr.DeclineCode == "" {
		return "", false
	}
	code := string(stripeErr.DeclineCode)
	if code == "" {
		code = string(stripeErr.Code)
	}
	if code == "" {
		code = "card_declined"
	}
	return strings.ToLower(code), true
}

func resultFor(attempt Attempt, isNew bool) Result {
	return Result{
		Triggered:   attempt.ID != uuid.Nil,
		NewAttempt:  isNew,
		AttemptID:   attempt.ID,
		Status:      attempt.Status,
		FailureCode: attempt.FailureCode,
	}
}

func microsToCentsRoundHalfUp(micros int64) int64 {
	return (micros + microsPerCent/2) / microsPerCent
}

var _ Settler = (*creditledger.Store)(nil)
