package autotopup

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	stripego "github.com/stripe/stripe-go/v85"

	"github.com/mirrorstack-ai/billing-engine/internal/account/creditledger"
	"github.com/mirrorstack-ai/billing-engine/internal/intent"
	"github.com/mirrorstack-ai/billing-engine/internal/intent/proposer"
	billingstripe "github.com/mirrorstack-ai/billing-engine/internal/shared/stripe"
)

const microsPerCent int64 = 10_000

type Executor struct {
	store    Store
	settler  Settler
	stripe   StripeClient
	observer SettlementObserver
	proposer chargeProposer
	nowFn    func() time.Time
}

// chargeProposer is the intent seam, declared locally so this package owns the
// shape it seals through rather than inheriting it.
type chargeProposer interface {
	Propose(ctx context.Context, c proposer.Charge) (intent.ChargeIntent, error)
}

// WithIntentProposer installs the proposer this executor seals through.
//
// It is no longer a cutover switch. The legacy collector is deleted, so an
// executor without a real proposer has no other path — it refuses (see
// unarmedProposer) rather than falling back to charging a card, because there
// is nothing left to fall back to.
//
// NewStandardExecutor is what makes this ONE hang point rather than six —
// SECURITY.md records four ordinary read and ingest paths reaching this
// executor, so a proposer installed in five of six binaries would leave a
// path that seals nothing and silently stops refilling wallets.
//
// 🔴 Installing it is still as deliberate as cmd/billing-cycle's flag, and
// for a harder reason: a proposing auto-top-up collects nothing, so wallets
// never refill and blocked accounts stay blocked until the intent executor
// runs. That is worse than the cycle legs' revenue stop.
func (e *Executor) WithIntentProposer(p chargeProposer) *Executor {
	if p == nil {
		panic("autotopup.Executor.WithIntentProposer: proposer must not be nil")
	}
	e.proposer = p
	return e
}

// ErrProposerUnarmed is what this leg does instead of collecting when no
// proposer has been installed.
//
// It is not a degraded mode and it is not "evidence off". The collector this
// executor used to fall back to no longer exists, so a deployment that has
// not installed a proposer cannot fund a wallet by any route. Saying so at the
// first trigger is the only honest option: the alternatives are a nil panic in
// a webhook handler, or a silent no-op that looks exactly like an account that
// did not need topping up.
var ErrProposerUnarmed = errors.New(
	"autotopup: no intent proposer installed and the legacy collector is deleted",
)

// unarmedProposer is the proposer every executor starts with, so the leg has
// exactly one path and no nil check standing between it and a provider.
type unarmedProposer struct{}

func (unarmedProposer) Propose(context.Context, proposer.Charge) (intent.ChargeIntent, error) {
	return intent.ChargeIntent{}, ErrProposerUnarmed
}

// NewStandardExecutor builds the executor the way every binary that has one
// builds it: this package's store, the credit ledger as settler, and the
// auto-top-up Stripe client.
//
// The same five lines were repeated in SIX binaries — account-api,
// account-webhook, account-webhook-eventbridge, billing-cycle,
// infra-egress-sync and infra-ssr-compute-sync. That is the shape of the
// problem docs/DESIGN.md §6 leaves for this leg: unlike cycle.Service, which
// has one constructor to hang a seam on, auto-top-up has six installation
// sites, and a cutover would have to find all of them.
//
// Consolidating first means the intent seam gets ONE hang point rather than
// six chances to miss one. SECURITY.md records that four ordinary read and
// ingest paths can reach this executor.
//
// 🔴 This constructor installs NO proposer, and since the legacy collector
// was deleted that is a leg which refuses every trigger (ErrProposerUnarmed)
// rather than one that quietly charges a card. Wiring a real proposer is the
// deployment's job; cmd/billing-cycle's withIntentCutover is the pattern,
// including its refusal to start without evidence signing material.
func NewStandardExecutor(pool *pgxpool.Pool, stripeKey string) *Executor {
	return NewExecutor(
		NewStore(pool),
		creditledger.NewStore(pool),
		billingstripe.NewAutoTopUpClient(stripeKey),
	)
}

func NewExecutor(store Store, settler Settler, stripe StripeClient) *Executor {
	if store == nil || settler == nil || stripe == nil {
		panic("autotopup.NewExecutor: store, settler, and stripe must not be nil")
	}
	return &Executor{
		store: store, settler: settler, stripe: stripe,
		proposer: unarmedProposer{}, nowFn: time.Now,
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

// Recover resumes only the already-authorized pending attempt for accountID.
// Unlike Trigger it never evaluates current mutable policy, never creates a
// replacement, and accepts no amount, invoice, customer, or card identity from
// the caller. This is the explicit liveness path after a rollout rollback or a
// crash with no webhook-producing Stripe resource.
func (e *Executor) Recover(ctx context.Context, accountID uuid.UUID) (Result, error) {
	if accountID == uuid.Nil {
		return Result{}, fmt.Errorf("account id required")
	}
	attempt, found, err := e.store.Pending(ctx, accountID)
	if err != nil {
		return Result{}, err
	}
	if !found {
		return Result{}, nil
	}
	if attempt.Expired(e.nowFn()) {
		return e.reconcileExpired(ctx, attempt)
	}
	return e.resume(ctx, attempt, false)
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
// or uncollectible invoice is deterministically voided and re-read before the
// ledger may fail. Uncollectible is reversible in Stripe, so it is never itself
// terminal proof. A paid re-read wins over every earlier failure and settles
// exactly once.
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
	case "void":
		if validateVoidedInvoiceResource(attempt, invoice) != nil {
			return result, nil
		}
		return e.failWebhookAttempt(ctx, attempt, invoice, failureCode, result)
	case "open", "uncollectible":
		var invariantErr error
		if invoice.Status == "open" {
			invariantErr = validateOpenUnpaidInvoiceResource(attempt, invoice)
		} else {
			invariantErr = validateUncollectibleInvoiceResource(attempt, invoice)
		}
		if invariantErr != nil {
			return result, nil
		}
	default:
		return result, nil
	}

	// The exact open or uncollectible invoice is unpaid and owned by this
	// attempt. Close the Stripe resource first, using the same deterministic
	// idempotency key as foreground recovery, then independently re-read before
	// touching ledger state.
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
	if latest.Status != "void" {
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
	if validateVoidedInvoiceResource(attempt, latest) != nil {
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

	// 🔴 THE LEGACY COLLECTOR IS GONE. This leg proposes, and the
	// proposal is not conditional on anything — there is no second branch
	// to fall through to, and no arming flag that turns collecting back on.
	//
	// Still AFTER the durable claim: Acquire created this attempt pending
	// before anything could reach a provider, so the crash-recovery read
	// below is looking at a row that already exists.
	//
	// StripeInvoiceID is the direct refutation of "nothing has been
	// collected for this attempt", and it is the ONE thing here that may
	// still touch Stripe. A row carrying one was armed by a pre-cutover run
	// that left a draft or a finalized invoice at the provider. Proposing
	// over it would seal a SECOND obligation for the same money — the bug
	// the boundary leg shipped. Resolving it is not collecting; see
	// resolveLegacyInvoice.
	if attempt.StripeInvoiceID == "" {
		return e.proposeAutoTopUp(ctx, attempt, isNew)
	}
	return e.resolveLegacyInvoice(ctx, attempt, "legacy_collector_removed", isNew)
}

// resolveLegacyInvoice finishes an attempt that a pre-cutover run already
// left at the provider. It is all that remains of the legacy rail, and every
// branch of it either records money Stripe ALREADY took or destroys an
// obligation that was never collected:
//
//   - paid → settle, because the customer was charged and the wallet must
//     show it;
//   - draft → delete, because an unfinalized draft has taken nothing;
//   - open or uncollectible → void, because an open invoice is one the
//     customer can still pay through its hosted URL. Abandoning it would
//     leave a live charge against a ledger row that is already terminal —
//     money arriving with nothing to credit it to, which is exactly the
//     charge nobody can prove.
//
// It never creates, finalizes, or pays. A top-up not collected here is
// re-proposed by the next Trigger through the intent rail.
func (e *Executor) resolveLegacyInvoice(
	ctx context.Context,
	attempt Attempt,
	failureCode string,
	isNew bool,
) (Result, error) {
	invoice, err := e.stripe.GetInvoice(ctx, attempt.StripeInvoiceID)
	if err != nil {
		return resultFor(attempt, isNew), fmt.Errorf("retrieve auto-top-up invoice: %w", err)
	}
	if err := validateInvoiceOwnership(attempt, invoice); err != nil {
		return resultFor(attempt, isNew), err
	}
	if handled, result, err := e.handleTerminalInvoice(ctx, attempt, invoice, isNew); handled {
		return result, err
	}
	return e.voidAndFail(ctx, attempt, invoice, failureCode, isNew)
}

// reconcileExpired establishes terminal truth before releasing the partial
// unique pending guard.
//
// An attempt with no attached invoice has no provider truth to establish. The
// intent path reaches nothing before MarkProposed, and the deleted legacy path
// only ever added the priced line AFTER AttachInvoice — so a run that crashed
// between creating the invoice and attaching it left at most an EMPTY draft,
// with auto_advance disabled and no amount. That cannot collect, so the guard
// is released from durable truth alone, with no provider call at all.
//
// An attempt that does carry an invoice is resolved exactly like a foreground
// in-flight row: settled if Stripe already took the money, closed if it did
// not. Both establish a resource-level barrier against a stale worker.
func (e *Executor) reconcileExpired(ctx context.Context, attempt Attempt) (Result, error) {
	if attempt.StripeInvoiceID == "" {
		return e.failWithoutStripeResource(ctx, attempt, "attempt_expired", false)
	}
	return e.resolveLegacyInvoice(ctx, attempt, "attempt_expired", false)
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
		invoice.AmountDue != expectedCents ||
		invoice.AmountRemaining != 0 ||
		invoice.AmountPaidOffStripe != 0 {
		return fmt.Errorf(
			"paid auto-top-up invoice %s has status=%q amount_paid=%d amount_due=%d amount_remaining=%d amount_paid_off_stripe=%d; expected paid/%d/%d/0/0",
			invoice.ID,
			invoice.Status,
			invoice.AmountPaid,
			invoice.AmountDue,
			invoice.AmountRemaining,
			invoice.AmountPaidOffStripe,
			expectedCents,
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

func validateVoidedInvoiceResource(
	attempt Attempt,
	invoice billingstripe.Invoice,
) error {
	return validateClosedUnpaidInvoiceResource(attempt, invoice, "void")
}

func validateUncollectibleInvoiceResource(
	attempt Attempt,
	invoice billingstripe.Invoice,
) error {
	return validateClosedUnpaidInvoiceResource(attempt, invoice, "uncollectible")
}

func validateOpenUnpaidInvoiceResource(
	attempt Attempt,
	invoice billingstripe.Invoice,
) error {
	return validateClosedUnpaidInvoiceResource(attempt, invoice, "open")
}

// validateClosedUnpaidInvoiceResource is intentionally narrower than payable
// validation. Once an exactly owned invoice is irreversibly void, line/total,
// collection-mode, or frozen-card mismatches cannot make it collectible again
// and must not strand the durable pending row. The same ownership + unpaid
// proof authorizes attempting to void an open or reversible-uncollectible
// resource; an independent exact void reread is still required before failure.
func validateClosedUnpaidInvoiceResource(
	attempt Attempt,
	invoice billingstripe.Invoice,
	expectedStatus string,
) error {
	if err := validateInvoiceOwnership(attempt, invoice); err != nil {
		return err
	}
	if invoice.Status != expectedStatus ||
		invoice.AmountPaid != 0 ||
		invoice.AmountPaidOffStripe != 0 {
		return fmt.Errorf(
			"%s auto-top-up invoice %s has status=%q amount_paid=%d amount_paid_off_stripe=%d; expected %s/0/0",
			expectedStatus,
			invoice.ID,
			invoice.Status,
			invoice.AmountPaid,
			invoice.AmountPaidOffStripe,
			expectedStatus,
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
	if err := validateInvoiceOwnership(attempt, invoice); err != nil {
		return err
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

// validateInvoiceOwnership proves the immutable Stripe resource anchors without
// relying on amount, line, or status truth. It runs before a recovered resource
// is attached and again at every destructive close boundary. This permits a
// malformed owned invoice to be closed safely while a same-customer foreign or
// partially tagged resource remains unattached and untouched.
func validateInvoiceOwnership(attempt Attempt, invoice billingstripe.Invoice) error {
	if invoice.ID == "" {
		return fmt.Errorf("Stripe returned an invoice without id")
	}
	if attempt.StripeInvoiceID != "" && invoice.ID != attempt.StripeInvoiceID {
		return fmt.Errorf(
			"auto-top-up invoice id is %q; expected attached invoice %q",
			invoice.ID,
			attempt.StripeInvoiceID,
		)
	}
	if invoice.CustomerID != attempt.StripeCustomerID {
		return fmt.Errorf(
			"auto-top-up invoice customer %q does not match frozen customer %q",
			invoice.CustomerID,
			attempt.StripeCustomerID,
		)
	}
	if invoice.ChargeRef != "credit-auto-topup:"+attempt.ID.String() ||
		invoice.CreditOperation != "auto_topup" ||
		invoice.CreditAccountID != attempt.AccountID.String() ||
		invoice.CreditLedgerID != attempt.ID.String() {
		return fmt.Errorf(
			"auto-top-up invoice %s credit metadata does not match account %s attempt %s",
			invoice.ID,
			attempt.AccountID,
			attempt.ID,
		)
	}
	return nil
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
	if err := validateInvoiceOwnership(attempt, invoice); err != nil {
		return resultFor(attempt, isNew), err
	}
	switch invoice.Status {
	case "paid", "void":
		if handled, result, err := e.handleTerminalInvoice(ctx, attempt, invoice, isNew); handled {
			return result, err
		}
	case "uncollectible":
		if err := validateUncollectibleInvoiceResource(attempt, invoice); err != nil {
			return resultFor(attempt, isNew), err
		}
	}
	if invoice.Status != "open" && invoice.Status != "uncollectible" {
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
	if err := validateVoidedInvoiceResource(attempt, latest); err != nil {
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
	if err := validateInvoiceOwnership(attempt, invoice); err != nil {
		return resultFor(attempt, isNew), err
	}
	if invoice.Status != "draft" ||
		invoice.AmountPaid != 0 {
		return resultFor(attempt, isNew), fmt.Errorf(
			"cannot safely delete auto-top-up draft %q: status=%q amount_paid=%d",
			invoice.ID,
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
		// Settlement is already committed and paid is the highest money truth.
		// Preserve that terminal fact in the returned Result even if the
		// convenience refresh below fails; the outer coordinator uses Status to
		// own the one unblock notification after a deferred nested observer.
		attempt.Status = "settled"
		attempt.FailureCode = ""
		attempt.ReceiptURL = invoice.HostedInvoiceURL
		current, err := e.store.Get(ctx, attempt.AccountID, attempt.ID)
		if err != nil {
			return true, resultFor(attempt, isNew), err
		}
		return true, resultFor(current, isNew), nil
	case "void":
		if err := validateVoidedInvoiceResource(attempt, invoice); err != nil {
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
	case "uncollectible":
		result, err := e.voidAndFail(
			ctx,
			attempt,
			invoice,
			"invoice_uncollectible",
			isNew,
		)
		return true, result, err
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

// proposeAutoTopUp seals the top-up as an intent and stops. It reaches no
// provider.
//
// docs/DESIGN.md §6 gives this leg its own kind and funding rule —
// "walletFunding = 0; providerRemainder = grossObligation", because a purchase
// of credit cannot be paid for with credit.
func (e *Executor) proposeAutoTopUp(ctx context.Context, attempt Attempt, isNew bool) (Result, error) {
	at := e.nowFn().UTC()

	// Seal what a collection would actually take, not the raw derived
	// micros. Sealing the unrounded figure attests to an amount the
	// customer was never charged — a repricing that shadow reconciliation
	// cannot see, because it compares the new rater against history rather
	// than the sealed intent against the leg it replaced.
	sealMicros := microsToCentsRoundHalfUp(attempt.AmountMicros) * microsPerCent

	sealed, err := e.proposer.Propose(ctx, proposer.Charge{
		// The proposer resolves this to the account OWNER. A leg that built
		// an intent.Subject here is how the payer and the executor's
		// resolver came to disagree; see proposer.Charge.AccountID.
		AccountID: attempt.AccountID.String(),
		Kind:      intent.KindAutoTopUp,
		Currency:  "usd",
		Lines: proposer.SingleLine(
			"MirrorStack credit auto top-up",
			"autotopup:"+attempt.ID.String(),
			sealMicros,
		),

		AuthorizationID:   "autotopup:" + attempt.AccountID.String(),
		TermsRevision:     proposedTermsRevision,
		PriceBookRevision: proposedPriceBookRevision,
		NoticePolicy:      proposedNoticePolicy,
		// The only rail this engine has an adapter for. The routing
		// policy that is supposed to CHOOSE it does not exist yet, so
		// the revision is a placeholder like the other four and
		// ClausePolicyPublished refuses on it — which is the honest
		// state, not a gap being hidden.
		SelectedRail:          proposedRail,
		RoutingPolicyRevision: proposedRoutingPolicy,
		Tax: intent.TaxDetermination{
			Resolved:     true,
			Jurisdiction: "not-applicable",
			RuleRevision: proposedTaxRuleRevision,
			// Not "reproducible": nothing recomputed this. The engine
			// determined no tax arises, which is a real determination
			// and is exactly what this class names.
			Verification: intent.TaxNotApplicable,
		},
		// The window must CONTAIN the seal instant or the document is dead
		// on arrival — ClauseWithinExecutionWindow refuses it forever, the
		// provider leg has already stopped, and the top-up evaporates. The
		// boundary leg shipped exactly that bug by reusing a coverage
		// period that had already closed.
		ExecuteNotBefore: at,
		ExecuteNotAfter:  at.AddDate(0, 1, 0),
	})
	if err != nil {
		return resultFor(attempt, isNew), fmt.Errorf("propose auto top-up intent: %w", err)
	}

	// The reference is prefixed so nothing downstream reads a digest as a
	// provider object id, and the migration-057 CHECK refuses a proposed
	// row without one.
	marked, err := e.store.MarkProposed(ctx, attempt, "intent:"+sealed.Digest())
	if err != nil {
		return resultFor(attempt, isNew), err
	}
	if !marked {
		// Lost a race: the attempt is no longer pending. The intent is
		// saved and inert — nothing executes it — which is the safe side
		// of this failure.
		return resultFor(attempt, isNew), nil
	}

	attempt.Status = "proposed"
	return resultFor(attempt, isNew), nil
}

// Policy identifiers a proposed charge is sealed under.
//
// 🔴 Placeholders, and named so. DESIGN §12 leaves these undecided;
// predicate.ClausePolicyPublished refuses to COLLECT under them, which is what
// makes proposing under them safe.
const (
	proposedTermsRevision     = "unpublished/pending-decision-12"
	proposedPriceBookRevision = "unpublished/pending-decision-12"
	proposedNoticePolicy      = "unpublished/pending-decision-12"
	proposedTaxRuleRevision   = "unpublished/pending-decision-12"
	proposedRail              = "stripe"
	proposedRoutingPolicy     = "unpublished/pending-decision-12"
)
