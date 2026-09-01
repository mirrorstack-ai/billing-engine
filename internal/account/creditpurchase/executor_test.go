package creditpurchase

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	stripego "github.com/stripe/stripe-go/v85"

	"github.com/mirrorstack-ai/billing-engine/internal/account/creditledger"
	billingstripe "github.com/mirrorstack-ai/billing-engine/internal/shared/stripe"
)

type fakeStore struct {
	proposedRefs []string
	attempt      Attempt
	failCalls    int
}

func (s *fakeStore) Get(
	_ context.Context,
	accountID, attemptID uuid.UUID,
) (Attempt, error) {
	if s.attempt.AccountID != accountID || s.attempt.ID != attemptID {
		return Attempt{}, errors.New("attempt not found")
	}
	return s.attempt, nil
}

func (s *fakeStore) FindByStripeInvoice(
	_ context.Context,
	stripeInvoiceID string,
) (Attempt, bool, error) {
	if stripeInvoiceID == "" || s.attempt.StripeInvoiceID != stripeInvoiceID {
		return Attempt{}, false, nil
	}
	return s.attempt, true, nil
}

func (s *fakeStore) AttachInvoice(
	_ context.Context,
	attempt Attempt,
	invoice billingstripe.Invoice,
) (Attempt, error) {
	if s.attempt.ID != attempt.ID || s.attempt.AccountID != attempt.AccountID {
		return Attempt{}, errors.New("attempt not found")
	}
	if s.attempt.StripeInvoiceID != "" &&
		s.attempt.StripeInvoiceID != invoice.ID {
		return Attempt{}, errors.New("different invoice already attached")
	}
	if s.attempt.StripeCustomerID != invoice.CustomerID {
		return Attempt{}, errors.New("invoice customer mismatch")
	}
	s.attempt.StripeInvoiceID = invoice.ID
	if invoice.HostedInvoiceURL != "" {
		s.attempt.ReceiptURL = invoice.HostedInvoiceURL
	}
	return s.attempt, nil
}

func (s *fakeStore) Fail(
	_ context.Context,
	attempt Attempt,
	receiptURL string,
) (Attempt, bool, error) {
	s.failCalls++
	if s.attempt.ID != attempt.ID || s.attempt.AccountID != attempt.AccountID {
		return Attempt{}, false, errors.New("attempt not found")
	}
	if s.attempt.Status != "pending" {
		return s.attempt, false, nil
	}
	s.attempt.Status = "failed"
	s.attempt.ReceiptURL = receiptURL
	return s.attempt, true, nil
}

// MarkProposed records the intent reference the way the real store does.
//
// Pending-only, matching the SQL predicate. A row already moved is a lost race,
// not a fault — a fake that ignored this would make the executor's race
// handling untestable.
func (s *fakeStore) MarkProposed(_ context.Context, _ Attempt, ref string) (bool, error) {
	if ref == "" {
		return false, errors.New("fake store: a proposed attempt needs its intent reference")
	}
	if s.attempt.Status != "pending" {
		return false, nil
	}
	s.attempt.Status = "proposed"
	s.attempt.ProposedReference = ref
	s.proposedRefs = append(s.proposedRefs, ref)
	return true, nil
}

type fakeSettler struct {
	store *fakeStore
	calls int
}

func (s *fakeSettler) SettleManualStripeInvoice(
	_ context.Context,
	stripeInvoiceID string,
	_ int64,
	_ string,
	receiptURL string,
) (creditledger.Settlement, error) {
	s.calls++
	s.store.attempt.Status = "settled"
	s.store.attempt.ReceiptURL = receiptURL
	return creditledger.Settlement{
		Found:        true,
		Transitioned: true,
		AccountID:    s.store.attempt.AccountID,
		LedgerID:     s.store.attempt.ID,
		Type:         "purchase",
	}, nil
}

type fakeStripe struct {
	invoice            billingstripe.Invoice
	items              []billingstripe.InvoiceItem
	payments           []billingstripe.InvoicePaymentProof
	afterVoid          *billingstripe.Invoice
	afterVoidPayments  []billingstripe.InvoicePaymentProof
	createInvoiceCalls int
	createItemCalls    int
	finalizeCalls      int
	getCalls           int
	listItemCalls      int
	listPaymentCalls   int
	voidCalls          int
}

func (s *fakeStripe) CreateCreditPurchaseInvoice(
	_ context.Context,
	customerID, accountID, ledgerID, _ string,
) (billingstripe.Invoice, error) {
	s.createInvoiceCalls++
	s.invoice = billingstripe.Invoice{
		ID:               "in_manual",
		CustomerID:       customerID,
		Status:           "draft",
		CollectionMethod: "charge_automatically",
		ChargeRef:        "credit-purchase:" + ledgerID,
		CreditOperation:  "purchase",
		CreditAccountID:  accountID,
		CreditLedgerID:   ledgerID,
		Currency:         "usd",
	}
	return s.invoice, nil
}

func (s *fakeStripe) CreateInvoiceItem(
	_ context.Context,
	_, _ string,
	amountCents int64,
	currency, _ string,
	_ billingstripe.LinePeriod,
	_ string,
) (billingstripe.InvoiceItem, error) {
	s.createItemCalls++
	item := billingstripe.InvoiceItem{
		ID: "ii_manual", AmountCents: amountCents, Currency: currency,
	}
	s.items = append(s.items, item)
	s.invoice.Total = amountCents
	s.invoice.AmountDue = amountCents
	s.invoice.AmountRemaining = amountCents
	s.invoice.Currency = currency
	return item, nil
}

func (s *fakeStripe) ListInvoiceItems(
	_ context.Context,
	_ string,
) ([]billingstripe.InvoiceItem, error) {
	s.listItemCalls++
	return append([]billingstripe.InvoiceItem(nil), s.items...), nil
}

func (s *fakeStripe) ListInvoicePayments(
	_ context.Context,
	_ string,
) ([]billingstripe.InvoicePaymentProof, error) {
	s.listPaymentCalls++
	return append([]billingstripe.InvoicePaymentProof(nil), s.payments...), nil
}

func (s *fakeStripe) FinalizeInvoice(
	_ context.Context,
	_ string,
	_ string,
) (billingstripe.Invoice, error) {
	s.finalizeCalls++
	s.invoice.Status = "open"
	s.invoice.AutoAdvance = true
	s.invoice.HostedInvoiceURL = "https://stripe.test/in_manual"
	return s.invoice, nil
}

func (s *fakeStripe) GetInvoice(
	_ context.Context,
	_ string,
) (billingstripe.Invoice, error) {
	s.getCalls++
	return s.invoice, nil
}

func (s *fakeStripe) FindInvoiceByRef(
	_ context.Context,
	_, _ string,
) (billingstripe.Invoice, bool, error) {
	return billingstripe.Invoice{}, false, nil
}

func (s *fakeStripe) VoidInvoice(
	_ context.Context,
	_ string,
	_ string,
) (billingstripe.Invoice, error) {
	s.voidCalls++
	if s.afterVoid != nil {
		s.invoice = *s.afterVoid
		s.payments = append(
			[]billingstripe.InvoicePaymentProof(nil),
			s.afterVoidPayments...,
		)
	} else {
		s.invoice.Status = "void"
		s.invoice.AmountRemaining = 0
	}
	return s.invoice, nil
}

func testAttempt(status string) Attempt {
	return Attempt{
		ID:                uuid.New(),
		AccountID:         uuid.New(),
		AmountMicros:      5_000_000,
		Status:            status,
		StripeInvoiceID:   "in_manual",
		FundingAccountID:  uuid.New(),
		FundingGeneration: uuid.New(),
		StripeCustomerID:  "cus_original",
	}
}

func exactInvoice(attempt Attempt, status string) billingstripe.Invoice {
	expectedCents := microsToCentsRoundHalfUp(attempt.AmountMicros)
	invoice := billingstripe.Invoice{
		ID:                  attempt.StripeInvoiceID,
		CustomerID:          "cus_original",
		Status:              status,
		CollectionMethod:    "charge_automatically",
		ChargeRef:           "credit-purchase:" + attempt.ID.String(),
		CreditOperation:     "purchase",
		CreditAccountID:     attempt.AccountID.String(),
		CreditLedgerID:      attempt.ID.String(),
		Total:               expectedCents,
		AmountDue:           expectedCents,
		AmountRemaining:     expectedCents,
		AmountPaidOffStripe: 0,
		Currency:            "usd",
		HostedInvoiceURL:    "https://stripe.test/" + attempt.StripeInvoiceID,
	}
	if status == "paid" {
		invoice.AmountPaid = expectedCents
		invoice.AmountRemaining = 0
	}
	if status == "void" {
		invoice.AmountRemaining = 0
	}
	return invoice
}

func exactItem(attempt Attempt) billingstripe.InvoiceItem {
	return billingstripe.InvoiceItem{
		ID:          "ii_manual",
		AmountCents: microsToCentsRoundHalfUp(attempt.AmountMicros),
		Currency:    "usd",
	}
}

func exactPayment(attempt Attempt, invoice billingstripe.Invoice) billingstripe.InvoicePaymentProof {
	expectedCents := microsToCentsRoundHalfUp(attempt.AmountMicros)
	return billingstripe.InvoicePaymentProof{
		ID:                    "inpay_manual",
		InvoiceID:             invoice.ID,
		Status:                "paid",
		IsDefault:             true,
		AmountPaid:            expectedCents,
		AmountRequested:       expectedCents,
		Currency:              "usd",
		PaymentType:           string(stripego.InvoicePaymentPaymentTypePaymentIntent),
		PaymentIntentID:       "pi_manual",
		PaymentIntentStatus:   string(stripego.PaymentIntentStatusSucceeded),
		PaymentIntentCustomer: invoice.CustomerID,
		PaymentMethodID:       "pm_manual",
		PaymentIntentAmount:   expectedCents,
		AmountReceived:        expectedCents,
		PaymentIntentCurrency: "usd",
	}
}

func TestReconcileWebhookPaidRejectsEveryUnprovedMoneyShape(t *testing.T) {
	baseAttempt := testAttempt("pending")
	baseInvoice := exactInvoice(baseAttempt, "paid")
	baseItem := exactItem(baseAttempt)
	basePayment := exactPayment(baseAttempt, baseInvoice)

	tests := []struct {
		name   string
		mutate func(*Attempt, *billingstripe.Invoice, *[]billingstripe.InvoiceItem, *[]billingstripe.InvoicePaymentProof)
	}{
		{
			name: "foreign account metadata",
			mutate: func(_ *Attempt, invoice *billingstripe.Invoice, _ *[]billingstripe.InvoiceItem, _ *[]billingstripe.InvoicePaymentProof) {
				invoice.CreditAccountID = uuid.NewString()
			},
		},
		{
			name: "wrong operation",
			mutate: func(_ *Attempt, invoice *billingstripe.Invoice, _ *[]billingstripe.InvoiceItem, _ *[]billingstripe.InvoicePaymentProof) {
				invoice.CreditOperation = "auto_topup"
			},
		},
		{
			name: "duplicate line",
			mutate: func(_ *Attempt, _ *billingstripe.Invoice, items *[]billingstripe.InvoiceItem, _ *[]billingstripe.InvoicePaymentProof) {
				*items = append(*items, (*items)[0])
			},
		},
		{
			name: "wrong total",
			mutate: func(_ *Attempt, invoice *billingstripe.Invoice, _ *[]billingstripe.InvoiceItem, _ *[]billingstripe.InvoicePaymentProof) {
				invoice.Total++
			},
		},
		{
			name: "remaining money",
			mutate: func(_ *Attempt, invoice *billingstripe.Invoice, _ *[]billingstripe.InvoiceItem, _ *[]billingstripe.InvoicePaymentProof) {
				invoice.AmountRemaining = 1
			},
		},
		{
			name: "paid outside Stripe",
			mutate: func(_ *Attempt, invoice *billingstripe.Invoice, _ *[]billingstripe.InvoiceItem, _ *[]billingstripe.InvoicePaymentProof) {
				invoice.AmountPaidOffStripe = invoice.AmountPaid
			},
		},
		{
			name: "no paid allocation",
			mutate: func(_ *Attempt, _ *billingstripe.Invoice, _ *[]billingstripe.InvoiceItem, payments *[]billingstripe.InvoicePaymentProof) {
				*payments = nil
			},
		},
		{
			name: "multiple paid allocations",
			mutate: func(_ *Attempt, _ *billingstripe.Invoice, _ *[]billingstripe.InvoiceItem, payments *[]billingstripe.InvoicePaymentProof) {
				*payments = append(*payments, (*payments)[0])
			},
		},
		{
			name: "foreign payment intent customer",
			mutate: func(_ *Attempt, _ *billingstripe.Invoice, _ *[]billingstripe.InvoiceItem, payments *[]billingstripe.InvoicePaymentProof) {
				(*payments)[0].PaymentIntentCustomer = "cus_foreign"
			},
		},
		{
			name: "missing payment method",
			mutate: func(_ *Attempt, _ *billingstripe.Invoice, _ *[]billingstripe.InvoiceItem, payments *[]billingstripe.InvoicePaymentProof) {
				(*payments)[0].PaymentMethodID = ""
			},
		},
		{
			name: "unsuccessful payment intent",
			mutate: func(_ *Attempt, _ *billingstripe.Invoice, _ *[]billingstripe.InvoiceItem, payments *[]billingstripe.InvoicePaymentProof) {
				(*payments)[0].PaymentIntentStatus = "requires_action"
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			attempt := baseAttempt
			invoice := baseInvoice
			items := []billingstripe.InvoiceItem{baseItem}
			payments := []billingstripe.InvoicePaymentProof{basePayment}
			test.mutate(&attempt, &invoice, &items, &payments)
			store := &fakeStore{attempt: attempt}
			stripe := &fakeStripe{
				invoice: invoice, items: items, payments: payments,
			}
			settler := &fakeSettler{store: store}
			executor := NewExecutor(store, settler, stripe)

			settlement, err := executor.ReconcileWebhookPaid(
				context.Background(),
				attempt.StripeInvoiceID,
			)

			require.Error(t, err)
			require.True(t, settlement.Found)
			require.Zero(t, settler.calls)
		})
	}
}

func TestReconcileWebhookPaidSettlesPendingAndFailedPaidHighest(t *testing.T) {
	for _, status := range []string{"pending", "failed"} {
		t.Run(status, func(t *testing.T) {
			attempt := testAttempt(status)
			invoice := exactInvoice(attempt, "paid")
			store := &fakeStore{attempt: attempt}
			stripe := &fakeStripe{
				invoice:  invoice,
				items:    []billingstripe.InvoiceItem{exactItem(attempt)},
				payments: []billingstripe.InvoicePaymentProof{exactPayment(attempt, invoice)},
			}
			settler := &fakeSettler{store: store}
			executor := NewExecutor(store, settler, stripe)

			settlement, err := executor.ReconcileWebhookPaid(
				context.Background(),
				attempt.StripeInvoiceID,
			)

			require.NoError(t, err)
			require.True(t, settlement.Found)
			require.True(t, settlement.Transitioned)
			require.Equal(t, 1, settler.calls)
		})
	}
}

func TestResumeUncollectibleVoidRacePaidWins(t *testing.T) {
	attempt := testAttempt("failed")
	uncollectible := exactInvoice(attempt, "uncollectible")
	paid := exactInvoice(attempt, "paid")
	store := &fakeStore{attempt: attempt}
	stripe := &fakeStripe{
		invoice:   uncollectible,
		items:     []billingstripe.InvoiceItem{exactItem(attempt)},
		afterVoid: &paid,
		afterVoidPayments: []billingstripe.InvoicePaymentProof{
			exactPayment(attempt, paid),
		},
	}
	settler := &fakeSettler{store: store}
	executor := NewExecutor(store, settler, stripe)

	result, err := executor.Resume(context.Background(), attempt)

	require.NoError(t, err)
	require.False(t, result.TerminalFailure)
	require.True(t, result.Settlement.Transitioned)
	require.Equal(t, 1, stripe.voidCalls)
	require.Equal(t, 1, settler.calls)
}

func TestResumeUncollectibleFailsOnlyAfterIndependentVoidRead(t *testing.T) {
	attempt := testAttempt("pending")
	uncollectible := exactInvoice(attempt, "uncollectible")
	voided := exactInvoice(attempt, "void")
	store := &fakeStore{attempt: attempt}
	stripe := &fakeStripe{
		invoice:   uncollectible,
		items:     []billingstripe.InvoiceItem{exactItem(attempt)},
		afterVoid: &voided,
	}
	settler := &fakeSettler{store: store}
	executor := NewExecutor(store, settler, stripe)

	result, err := executor.Resume(context.Background(), attempt)

	require.NoError(t, err)
	require.True(t, result.TerminalFailure)
	require.Equal(t, "void", result.Invoice.Status)
	require.Equal(t, 1, stripe.voidCalls)
	require.GreaterOrEqual(t, stripe.getCalls, 2)
	require.Zero(t, settler.calls)
}

func TestResumeAttachedInvoiceIgnoresCurrentSponsorCustomerDrift(t *testing.T) {
	attempt := testAttempt("failed")
	invoice := exactInvoice(attempt, "paid")
	// The caller's current funding resolution has drifted, but the attached
	// invoice and its PaymentIntent still bind the original customer.
	supplied := attempt
	supplied.StripeCustomerID = "cus_new_sponsor"
	store := &fakeStore{attempt: attempt}
	stripe := &fakeStripe{
		invoice:  invoice,
		items:    []billingstripe.InvoiceItem{exactItem(attempt)},
		payments: []billingstripe.InvoicePaymentProof{exactPayment(attempt, invoice)},
	}
	settler := &fakeSettler{store: store}
	executor := NewExecutor(store, settler, stripe)

	result, err := executor.Resume(context.Background(), supplied)

	require.NoError(t, err)
	require.True(t, result.Settlement.Transitioned)
	require.Equal(t, 1, settler.calls)
}

func TestReconcileWebhookFailureOpenStaysPending(t *testing.T) {
	attempt := testAttempt("pending")
	invoice := exactInvoice(attempt, "open")
	store := &fakeStore{attempt: attempt}
	stripe := &fakeStripe{
		invoice: invoice,
		items:   []billingstripe.InvoiceItem{exactItem(attempt)},
	}
	settler := &fakeSettler{store: store}
	executor := NewExecutor(store, settler, stripe)

	result, err := executor.ReconcileWebhookFailure(
		context.Background(),
		attempt.StripeInvoiceID,
		"payment_failed",
	)

	require.NoError(t, err)
	require.True(t, result.Found)
	require.False(t, result.Transitioned)
	require.Equal(t, "pending", result.Status)
	require.Zero(t, store.failCalls)
	require.Zero(t, settler.calls)
}

func TestReconcileWebhookFailureExactVoidFailsOnceAcrossReplay(t *testing.T) {
	attempt := testAttempt("pending")
	invoice := exactInvoice(attempt, "void")
	store := &fakeStore{attempt: attempt}
	stripe := &fakeStripe{
		invoice: invoice,
		items:   []billingstripe.InvoiceItem{exactItem(attempt)},
	}
	settler := &fakeSettler{store: store}
	executor := NewExecutor(store, settler, stripe)

	first, err := executor.ReconcileWebhookFailure(
		context.Background(),
		attempt.StripeInvoiceID,
		"invoice_void",
	)
	require.NoError(t, err)
	second, err := executor.ReconcileWebhookFailure(
		context.Background(),
		attempt.StripeInvoiceID,
		"invoice_void",
	)

	require.NoError(t, err)
	require.True(t, first.Transitioned)
	require.Equal(t, "failed", first.Status)
	require.False(t, second.Transitioned)
	require.Equal(t, "failed", second.Status)
	require.Equal(t, 2, store.failCalls)
	require.Zero(t, settler.calls)
}

func TestReconcileWebhookFailureUncollectiblePaidRaceSettles(t *testing.T) {
	attempt := testAttempt("pending")
	uncollectible := exactInvoice(attempt, "uncollectible")
	paid := exactInvoice(attempt, "paid")
	store := &fakeStore{attempt: attempt}
	stripe := &fakeStripe{
		invoice:   uncollectible,
		items:     []billingstripe.InvoiceItem{exactItem(attempt)},
		afterVoid: &paid,
		afterVoidPayments: []billingstripe.InvoicePaymentProof{
			exactPayment(attempt, paid),
		},
	}
	settler := &fakeSettler{store: store}
	executor := NewExecutor(store, settler, stripe)

	result, err := executor.ReconcileWebhookFailure(
		context.Background(),
		attempt.StripeInvoiceID,
		"invoice_uncollectible",
	)

	require.NoError(t, err)
	require.True(t, result.Transitioned)
	require.Equal(t, "settled", result.Status)
	require.Empty(t, result.FailureCode)
	require.Equal(t, 1, stripe.voidCalls)
	require.Zero(t, store.failCalls)
	require.Equal(t, 1, settler.calls)
}
