package autotopup

import (
	"context"
	"errors"
	"math"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	stripego "github.com/stripe/stripe-go/v85"

	"github.com/mirrorstack-ai/billing-engine/internal/account/creditledger"
	billingstripe "github.com/mirrorstack-ai/billing-engine/internal/shared/stripe"
)

func TestExecutorTrigger_HappyPathFreezesSelectedCardAndSettlesOnce(t *testing.T) {
	now := time.Date(2026, time.July, 25, 1, 2, 3, 0, time.UTC)
	attempt := testAttempt(now, 5_005_000)
	store := newMemoryStore(attempt, AcquireNew)
	stripe := &scriptedStripe{
		createResult: controlledInvoice(attempt, billingstripe.Invoice{ID: "in_topup", CustomerID: attempt.StripeCustomerID, Status: "draft"}),
		finalizeResult: controlledInvoice(attempt, billingstripe.Invoice{
			ID: "in_topup", CustomerID: attempt.StripeCustomerID, Status: "open",
			AmountDue: 501, Total: 501, Currency: "usd",
		}),
		payResult: controlledInvoice(attempt, billingstripe.Invoice{
			ID: "in_topup", CustomerID: attempt.StripeCustomerID, Status: "paid",
			AmountDue: 501, AmountPaid: 501, Total: 501, Currency: "usd",
			HostedInvoiceURL: "https://stripe.test/in_topup",
		}),
		listPayments: map[string][]billingstripe.InvoicePaymentProof{
			"in_topup": {exactInvoicePayment(attempt, "in_topup")},
		},
		voidResult: billingstripe.Invoice{ID: "in_topup", Status: "void"},
	}
	settler := &memorySettler{store: store, transitioned: true}
	observer := &recordingObserver{}
	executor := NewExecutor(store, settler, stripe).
		WithNow(func() time.Time { return now }).
		WithSettlementObserver(observer)

	result, err := executor.Trigger(context.Background(), attempt.AccountID, 1_000_000)

	require.NoError(t, err)
	require.Equal(t, Result{
		Triggered: true, NewAttempt: true, AttemptID: attempt.ID, Status: "settled",
	}, result)
	require.Equal(t, []createCall{{
		customerID:      attempt.StripeCustomerID,
		paymentMethodID: attempt.StripePaymentMethodID,
		accountID:       attempt.AccountID.String(),
		ledgerID:        attempt.ID.String(),
		idemKey:         "credit-auto-topup-invoice:" + attempt.ID.String(),
	}}, stripe.createCalls)
	require.Equal(t, []itemCall{{
		customerID:  attempt.StripeCustomerID,
		invoiceID:   "in_topup",
		amountCents: 501,
		currency:    "usd",
		description: "MirrorStack automatic credit top-up",
		idemKey:     "credit-auto-topup-item:" + attempt.ID.String(),
	}}, stripe.itemCalls)
	require.Equal(t, []finalizeCall{{
		invoiceID: "in_topup",
		idemKey:   "credit-auto-topup-finalize:" + attempt.ID.String(),
	}}, stripe.finalizeCalls)
	require.Equal(t, []payCall{{
		invoiceID:       "in_topup",
		paymentMethodID: attempt.StripePaymentMethodID,
		idemKey:         "credit-auto-topup-pay:" + attempt.ID.String(),
	}}, stripe.payCalls)
	require.Empty(t, stripe.voidCalls)
	require.Equal(t, []settleCall{{
		invoiceID:       "in_topup",
		amountPaidCents: 501,
		currency:        "usd",
		receiptURL:      "https://stripe.test/in_topup",
	}}, settler.calls)
	require.Equal(t, []uuid.UUID{attempt.AccountID}, observer.accounts)
	require.Equal(t, []bool{true}, observer.settlementObservations)

	// A recovered paid invoice remains found but does not re-run the observer
	// once the shared settler reports that no first transition occurred.
	settler.transitioned = false
	store.acquire = []acquireResult{{attempt: store.mustGet(attempt.ID), kind: AcquireExisting}}
	stripe.getResults = map[string]billingstripe.Invoice{
		"in_topup": stripe.payResult,
	}
	result, err = executor.Trigger(context.Background(), attempt.AccountID, 1_000_000)
	require.NoError(t, err)
	require.Equal(t, "settled", result.Status)
	require.Equal(t, []uuid.UUID{attempt.AccountID}, observer.accounts, "settled replay must not observe twice")
}

func TestExecutorTrigger_PostSettlementRefreshFailurePreservesTerminalResult(t *testing.T) {
	now := time.Date(2026, time.July, 25, 1, 30, 0, 0, time.UTC)
	attempt := testAttempt(now, 5_000_000)
	store := newMemoryStore(attempt, AcquireNew)
	store.getErrors = []error{nil, errors.New("post-settlement read unavailable")}
	paid := controlledInvoice(attempt, billingstripe.Invoice{
		ID: "in_settled_read_error", CustomerID: attempt.StripeCustomerID, Status: "paid",
		AmountDue: 500, AmountPaid: 500, Total: 500, Currency: "usd",
		HostedInvoiceURL: "https://stripe.test/in_settled_read_error",
	})
	stripe := &scriptedStripe{
		createResult: controlledInvoice(attempt, billingstripe.Invoice{
			ID: "in_settled_read_error", CustomerID: attempt.StripeCustomerID, Status: "draft",
		}),
		finalizeResult: controlledInvoice(attempt, billingstripe.Invoice{
			ID: "in_settled_read_error", CustomerID: attempt.StripeCustomerID, Status: "open",
			AmountDue: 500, Total: 500, Currency: "usd",
		}),
		payResult: paid,
		listPayments: map[string][]billingstripe.InvoicePaymentProof{
			paid.ID: {exactInvoicePayment(attempt, paid.ID)},
		},
	}
	settler := &memorySettler{store: store, transitioned: true}
	observer := &recordingObserver{}
	executor := NewExecutor(store, settler, stripe).
		WithNow(func() time.Time { return now }).
		WithSettlementObserver(observer)

	result, err := executor.Trigger(context.Background(), attempt.AccountID, 1)

	require.ErrorContains(t, err, "post-settlement read unavailable")
	require.True(t, result.Triggered)
	require.True(t, result.NewAttempt)
	require.Equal(t, "settled", result.Status,
		"committed paid truth must survive a fallible convenience refresh")
	require.Empty(t, result.FailureCode)
	require.Equal(t, "settled", store.mustGet(attempt.ID).Status)
	require.Equal(t, []uuid.UUID{attempt.AccountID}, observer.accounts)
}

func TestExecutorTrigger_DeterministicDeclineAndSCAAreVoidedBeforeFailure(t *testing.T) {
	tests := []struct {
		name string
		err  error
		code string
	}{
		{
			name: "card decline",
			err: &stripego.Error{
				Type: stripego.ErrorTypeCard, DeclineCode: stripego.DeclineCode("insufficient_funds"),
			},
			code: "insufficient_funds",
		},
		{
			name: "sca required",
			err:  &stripego.Error{Code: stripego.ErrorCodeInvoicePaymentIntentRequiresAction},
			code: "authentication_required",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			now := time.Date(2026, time.July, 25, 2, 0, 0, 0, time.UTC)
			attempt := testAttempt(now, 5_000_000)
			store := newMemoryStore(attempt, AcquireNew)
			stripe := &scriptedStripe{
				createResult: controlledInvoice(attempt, billingstripe.Invoice{ID: "in_decline", CustomerID: attempt.StripeCustomerID, Status: "draft"}),
				finalizeResult: controlledInvoice(attempt, billingstripe.Invoice{
					ID: "in_decline", CustomerID: attempt.StripeCustomerID, Status: "open",
					AmountDue: 500, Total: 500, Currency: "usd",
				}),
				payErr: tt.err,
				getResults: map[string]billingstripe.Invoice{
					"in_decline": controlledInvoice(attempt, billingstripe.Invoice{
						ID: "in_decline", CustomerID: attempt.StripeCustomerID, Status: "open",
						AmountDue: 500, Total: 500, Currency: "usd",
					}),
				},
				voidResult: func() billingstripe.Invoice {
					invoice := exactTerminalInvoice(attempt, "void")
					invoice.ID = "in_decline"
					invoice.HostedInvoiceURL = "https://stripe.test/in_decline"
					return invoice
				}(),
			}
			settler := &memorySettler{store: store}
			observer := &recordingObserver{}
			executor := NewExecutor(store, settler, stripe).
				WithNow(func() time.Time { return now }).
				WithSettlementObserver(observer)

			result, err := executor.Trigger(context.Background(), attempt.AccountID, 1)

			require.NoError(t, err)
			require.Equal(t, "failed", result.Status)
			require.Equal(t, tt.code, result.FailureCode)
			require.Equal(t, []voidCall{{
				invoiceID: "in_decline",
				idemKey:   "credit-auto-topup-void:" + attempt.ID.String(),
			}}, stripe.voidCalls)
			require.Equal(t, []failCall{{
				attemptID:   attempt.ID,
				failureCode: tt.code,
				receiptURL:  "https://stripe.test/in_decline",
			}}, store.failCalls)
			require.Empty(t, settler.calls, "an unpaid invoice must never increase balance")
			require.Equal(t, []uuid.UUID{attempt.AccountID}, observer.accounts)
		})
	}
}

func TestExecutorTrigger_AmbiguousPayFailureStaysPending(t *testing.T) {
	now := time.Date(2026, time.July, 25, 3, 0, 0, 0, time.UTC)
	attempt := testAttempt(now, 5_000_000)
	store := newMemoryStore(attempt, AcquireNew)
	stripe := &scriptedStripe{
		createResult: controlledInvoice(attempt, billingstripe.Invoice{ID: "in_ambiguous", CustomerID: attempt.StripeCustomerID, Status: "draft"}),
		finalizeResult: controlledInvoice(attempt, billingstripe.Invoice{
			ID: "in_ambiguous", CustomerID: attempt.StripeCustomerID, Status: "open",
			AmountDue: 500, Total: 500, Currency: "usd",
		}),
		payErr: errors.New("connection reset after write"),
		getResults: map[string]billingstripe.Invoice{
			"in_ambiguous": {ID: "in_ambiguous", CustomerID: attempt.StripeCustomerID, Status: "open"},
		},
	}
	settler := &memorySettler{store: store}
	observer := &recordingObserver{}
	executor := NewExecutor(store, settler, stripe).
		WithNow(func() time.Time { return now }).
		WithSettlementObserver(observer)

	result, err := executor.Trigger(context.Background(), attempt.AccountID, 1)

	require.ErrorContains(t, err, "connection reset after write")
	require.Equal(t, "pending", result.Status)
	require.Empty(t, stripe.voidCalls, "network ambiguity must retain recoverable Stripe state")
	require.Empty(t, store.failCalls, "network ambiguity must retain the durable pending guard")
	require.Empty(t, settler.calls)
	require.Empty(t, observer.accounts)
}

func TestExecutorTrigger_PaidRereadWinsOverPayError(t *testing.T) {
	now := time.Date(2026, time.July, 25, 4, 0, 0, 0, time.UTC)
	attempt := testAttempt(now, 5_000_000)
	store := newMemoryStore(attempt, AcquireNew)
	stripe := &scriptedStripe{
		createResult: controlledInvoice(attempt, billingstripe.Invoice{ID: "in_paid", CustomerID: attempt.StripeCustomerID, Status: "draft"}),
		finalizeResult: controlledInvoice(attempt, billingstripe.Invoice{
			ID: "in_paid", CustomerID: attempt.StripeCustomerID, Status: "open",
			AmountDue: 500, Total: 500, Currency: "usd",
		}),
		payErr: errors.New("timeout"),
		getResults: map[string]billingstripe.Invoice{
			"in_paid": controlledInvoice(attempt, billingstripe.Invoice{
				ID: "in_paid", CustomerID: attempt.StripeCustomerID, Status: "paid",
				AmountDue: 500, AmountPaid: 500, Total: 500, Currency: "usd",
			}),
		},
		listPayments: map[string][]billingstripe.InvoicePaymentProof{
			"in_paid": {exactInvoicePayment(attempt, "in_paid")},
		},
	}
	settler := &memorySettler{store: store, transitioned: true}
	observer := &recordingObserver{}
	executor := NewExecutor(store, settler, stripe).
		WithNow(func() time.Time { return now }).
		WithSettlementObserver(observer)

	result, err := executor.Trigger(context.Background(), attempt.AccountID, 1)

	require.NoError(t, err)
	require.Equal(t, "settled", result.Status)
	require.Empty(t, stripe.voidCalls)
	require.Empty(t, store.failCalls)
	require.Len(t, settler.calls, 1)
	require.Equal(t, []uuid.UUID{attempt.AccountID}, observer.accounts)
}

func TestExecutorTrigger_ExpiredAttemptClosesBeforeReplacement(t *testing.T) {
	now := time.Date(2026, time.July, 25, 5, 0, 0, 0, time.UTC)
	expired := testAttempt(now.Add(-PendingGrace), 5_000_000)
	expired.ExpiresAt = now
	expired.StripeInvoiceID = "in_old"
	replacement := testAttempt(now, 5_000_000)
	store := newMemoryStore(expired, AcquireExisting)
	store.attempts[replacement.ID] = replacement
	store.acquire = append(store.acquire, acquireResult{attempt: replacement, kind: AcquireNew})
	stripe := &scriptedStripe{
		getResults: map[string]billingstripe.Invoice{
			"in_old": exactOpenInvoice(expired),
		},
		createResult: controlledInvoice(replacement, billingstripe.Invoice{ID: "in_new", CustomerID: replacement.StripeCustomerID, Status: "draft"}),
		finalizeResult: controlledInvoice(replacement, billingstripe.Invoice{
			ID: "in_new", CustomerID: replacement.StripeCustomerID, Status: "open",
			AmountDue: 500, Total: 500, Currency: "usd",
		}),
		payResult: controlledInvoice(replacement, billingstripe.Invoice{
			ID: "in_new", CustomerID: replacement.StripeCustomerID, Status: "paid",
			AmountDue: 500, AmountPaid: 500, Total: 500, Currency: "usd",
		}),
		listPayments: map[string][]billingstripe.InvoicePaymentProof{
			"in_new": {exactInvoicePayment(replacement, "in_new")},
		},
		listItems: map[string][]billingstripe.InvoiceItem{
			"in_old": {exactInvoiceItem(expired)},
		},
		voidResult: exactTerminalInvoice(expired, "void"),
	}
	settler := &memorySettler{store: store, transitioned: true}
	observer := &recordingObserver{}
	executor := NewExecutor(store, settler, stripe).
		WithNow(func() time.Time { return now }).
		WithSettlementObserver(observer)

	result, err := executor.Trigger(context.Background(), expired.AccountID, 1)

	require.NoError(t, err)
	require.Equal(t, replacement.ID, result.AttemptID)
	require.Equal(t, "settled", result.Status)
	require.Equal(t, []voidCall{{
		invoiceID: "in_old",
		idemKey:   "credit-auto-topup-void:" + expired.ID.String(),
	}}, stripe.voidCalls)
	require.Equal(t, "failed", store.mustGet(expired.ID).Status)
	require.Equal(t, "attempt_expired", store.mustGet(expired.ID).FailureCode)
	require.Equal(t, "settled", store.mustGet(replacement.ID).Status)
	require.Equal(t, []uuid.UUID{expired.AccountID, replacement.AccountID}, observer.accounts)
}

func TestExecutorTrigger_ExpiredPartiallyPaidTerminalInvoiceStaysPending(t *testing.T) {
	now := time.Date(2026, time.July, 25, 5, 15, 0, 0, time.UTC)
	attempt := testAttempt(now.Add(-PendingGrace), 5_000_000)
	attempt.ExpiresAt = now
	attempt.StripeInvoiceID = "in_partial_terminal"
	store := newMemoryStore(attempt, AcquireExisting)
	terminal := exactTerminalInvoice(attempt, "uncollectible")
	terminal.AmountPaid = 1
	stripe := &scriptedStripe{
		getResults: map[string]billingstripe.Invoice{
			attempt.StripeInvoiceID: terminal,
		},
		listItems: map[string][]billingstripe.InvoiceItem{
			attempt.StripeInvoiceID: {exactInvoiceItem(attempt)},
		},
	}
	settler := &memorySettler{store: store}
	executor := NewExecutor(store, settler, stripe).WithNow(func() time.Time { return now })

	result, err := executor.Trigger(context.Background(), attempt.AccountID, 1)

	require.ErrorContains(t, err, "amount_paid=1")
	require.Equal(t, "pending", result.Status)
	require.Empty(t, stripe.payCalls)
	require.Empty(t, stripe.voidCalls)
	require.Empty(t, store.failCalls,
		"partially collected money must remain pending for manual reconciliation")
	require.Empty(t, settler.calls)
}

func TestExecutorTrigger_ExpiredImmediatelyAfterLedgerInsertDeletesInertDraft(t *testing.T) {
	now := time.Date(2026, time.July, 25, 5, 30, 0, 0, time.UTC)
	expired := testAttempt(now.Add(-PendingGrace), 5_005_000)
	expired.ExpiresAt = now
	// StripeInvoiceID deliberately empty: this models a crash immediately
	// after the durable ledger insert, before any Stripe resource was attached.
	store := newMemoryStore(expired, AcquireExisting)
	stripe := &scriptedStripe{
		createResult: controlledInvoice(expired, billingstripe.Invoice{
			ID: "in_expired_empty", CustomerID: expired.StripeCustomerID, Status: "draft",
		}),
		deleteResult: billingstripe.Invoice{
			ID: "in_expired_empty", Deleted: true,
		},
	}
	settler := &memorySettler{store: store}
	executor := NewExecutor(store, settler, stripe).WithNow(func() time.Time { return now })

	result, err := executor.Trigger(context.Background(), expired.AccountID, 1)

	require.NoError(t, err)
	require.Equal(t, "failed", result.Status)
	require.Equal(t, "attempt_expired", result.FailureCode)
	require.Equal(t, []deleteCall{{invoiceID: "in_expired_empty"}}, stripe.deleteCalls)
	require.Equal(t, []string{
		"find", "create", "delete", "get",
	}, stripe.sequence)
	require.Empty(t, stripe.itemCalls)
	require.Empty(t, stripe.finalizeCalls,
		"expired drafts are deleted, never finalized into payable resources")
	require.Empty(t, stripe.payCalls, "expiry closes the resource; it never pays")
	require.Empty(t, stripe.voidCalls)
	require.Empty(t, settler.calls, "an expired unpaid attempt never increases balance")
	require.Equal(t, "failed", store.mustGet(expired.ID).Status)
}

func TestExecutorTrigger_ExpiredMalformedDraftDeletesAndReleasesPendingGuard(t *testing.T) {
	now := time.Date(2026, time.July, 25, 5, 45, 0, 0, time.UTC)
	expired := testAttempt(now.Add(-PendingGrace), 5_000_000)
	expired.ExpiresAt = now
	expired.StripeInvoiceID = "in_expired_malformed"
	store := newMemoryStore(expired, AcquireExisting)
	malformed := controlledInvoice(expired, billingstripe.Invoice{
		ID:         expired.StripeInvoiceID,
		CustomerID: expired.StripeCustomerID,
		Status:     "draft",
		AmountDue:  777,
		Total:      777,
		Currency:   "eur",
	})
	malformed.CollectionMethod = "send_invoice"
	malformed.AutoAdvance = true
	malformed.DefaultPaymentMethodID = "pm_foreign"
	stripe := &scriptedStripe{
		getResults: map[string]billingstripe.Invoice{
			expired.StripeInvoiceID: malformed,
		},
		deleteResult: billingstripe.Invoice{
			ID: expired.StripeInvoiceID, Deleted: true,
		},
	}
	executor := NewExecutor(
		store,
		&memorySettler{store: store},
		stripe,
	).WithNow(func() time.Time { return now })

	result, err := executor.Trigger(context.Background(), expired.AccountID, 1)

	require.NoError(t, err)
	require.Equal(t, "failed", result.Status)
	require.Equal(t, "attempt_expired", result.FailureCode)
	require.Equal(t, []deleteCall{{invoiceID: expired.StripeInvoiceID}}, stripe.deleteCalls)
	require.Empty(t, stripe.itemCalls)
	require.Empty(t, stripe.finalizeCalls)
	require.Empty(t, stripe.payCalls)
	require.Empty(t, stripe.voidCalls)
	require.Len(t, store.failCalls, 1,
		"verified deletion must release the partial unique pending guard")
}

func TestExecutorRecover_ResumesOnlyExistingPendingAttempt(t *testing.T) {
	now := time.Date(2026, time.July, 25, 5, 46, 0, 0, time.UTC)
	attempt := testAttempt(now, 5_000_000)
	attempt.StripeInvoiceID = "in_explicit_recovery"
	store := newMemoryStore(attempt, AcquireNone)
	stripe := &scriptedStripe{
		getResults: map[string]billingstripe.Invoice{
			attempt.StripeInvoiceID: exactOpenInvoice(attempt),
		},
		listItems: map[string][]billingstripe.InvoiceItem{
			attempt.StripeInvoiceID: {exactInvoiceItem(attempt)},
		},
		payResult: exactPaidInvoice(attempt),
		listPayments: map[string][]billingstripe.InvoicePaymentProof{
			attempt.StripeInvoiceID: {
				exactInvoicePayment(attempt, attempt.StripeInvoiceID),
			},
		},
	}
	settler := &memorySettler{store: store, transitioned: true}
	executor := NewExecutor(store, settler, stripe).WithNow(func() time.Time { return now })

	result, err := executor.Recover(context.Background(), attempt.AccountID)

	require.NoError(t, err)
	require.True(t, result.Triggered)
	require.False(t, result.NewAttempt)
	require.Equal(t, attempt.ID, result.AttemptID)
	require.Equal(t, "settled", result.Status)
	require.Empty(t, stripe.createCalls, "explicit recovery must never create an attempt invoice")
	require.Empty(t, stripe.finalizeCalls)
	require.Len(t, stripe.payCalls, 1)
	require.Len(t, store.acquire, 1, "explicit recovery must never enter Acquire")
}

func TestExecutorRecover_ExpiredCrashClosesWithoutReplacement(t *testing.T) {
	now := time.Date(2026, time.July, 25, 5, 46, 30, 0, time.UTC)
	attempt := testAttempt(now.Add(-PendingGrace), 5_000_000)
	attempt.ExpiresAt = now
	store := newMemoryStore(attempt, AcquireNone)
	stripe := &scriptedStripe{
		createResult: controlledInvoice(attempt, billingstripe.Invoice{
			ID: "in_explicit_expired", CustomerID: attempt.StripeCustomerID, Status: "draft",
		}),
		deleteResult: billingstripe.Invoice{
			ID: "in_explicit_expired", Deleted: true,
		},
	}
	executor := NewExecutor(store, &memorySettler{store: store}, stripe).
		WithNow(func() time.Time { return now })

	result, err := executor.Recover(context.Background(), attempt.AccountID)

	require.NoError(t, err)
	require.Equal(t, attempt.ID, result.AttemptID)
	require.Equal(t, "failed", result.Status)
	require.Equal(t, "attempt_expired", result.FailureCode)
	require.Empty(t, stripe.payCalls)
	require.Len(t, stripe.deleteCalls, 1)
	require.Len(t, store.acquire, 1,
		"expired explicit recovery must not create a replacement attempt")
}

func TestExecutorRecover_NoPendingIsNoOp(t *testing.T) {
	now := time.Date(2026, time.July, 25, 5, 46, 45, 0, time.UTC)
	attempt := testAttempt(now, 5_000_000)
	attempt.Status = "failed"
	store := newMemoryStore(attempt, AcquireNone)
	stripe := &scriptedStripe{}
	executor := NewExecutor(store, &memorySettler{store: store}, stripe).
		WithNow(func() time.Time { return now })

	result, err := executor.Recover(context.Background(), attempt.AccountID)

	require.NoError(t, err)
	require.Equal(t, Result{}, result)
	require.Empty(t, stripe.sequence)
	require.Len(t, store.acquire, 1, "no-op recovery must not evaluate policy")
}

func TestExecutorTrigger_DeleteErrorWithVerifiedMissingDraftReleasesGuard(t *testing.T) {
	now := time.Date(2026, time.July, 25, 5, 47, 0, 0, time.UTC)
	expired := testAttempt(now.Add(-PendingGrace), 5_000_000)
	expired.ExpiresAt = now
	expired.StripeInvoiceID = "in_delete_ambiguous_but_gone"
	store := newMemoryStore(expired, AcquireExisting)
	stripe := &scriptedStripe{
		getQueues: map[string][]billingstripe.Invoice{
			expired.StripeInvoiceID: {
				controlledInvoice(expired, billingstripe.Invoice{
					ID: expired.StripeInvoiceID, CustomerID: expired.StripeCustomerID, Status: "draft",
				}),
			},
		},
		getErrQueues: map[string][]error{
			expired.StripeInvoiceID: {nil, resourceMissingError()},
		},
		deleteErr: errors.New("connection reset after DELETE write"),
	}
	executor := NewExecutor(
		store,
		&memorySettler{store: store},
		stripe,
	).WithNow(func() time.Time { return now })

	result, err := executor.Trigger(context.Background(), expired.AccountID, 1)

	require.NoError(t, err)
	require.Equal(t, "failed", result.Status)
	require.Equal(t, "attempt_expired", result.FailureCode)
	require.Len(t, stripe.deleteCalls, 1)
	require.Len(t, store.failCalls, 1,
		"same-client post-delete resource_missing is authoritative despite response loss")
}

func TestExecutorTrigger_DeletePostReadAmbiguityStaysPending(t *testing.T) {
	tests := []struct {
		name         string
		deleteResult billingstripe.Invoice
		deleteErr    error
	}{
		{
			name:         "delete returned success",
			deleteResult: billingstripe.Invoice{ID: "in_delete_read_timeout", Deleted: true},
		},
		{
			name:      "delete response was ambiguous",
			deleteErr: errors.New("connection reset after DELETE write"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			now := time.Date(2026, time.July, 25, 5, 48, 0, 0, time.UTC)
			expired := testAttempt(now.Add(-PendingGrace), 5_000_000)
			expired.ExpiresAt = now
			expired.StripeInvoiceID = "in_delete_read_timeout"
			store := newMemoryStore(expired, AcquireExisting)
			stripe := &scriptedStripe{
				getQueues: map[string][]billingstripe.Invoice{
					expired.StripeInvoiceID: {
						controlledInvoice(expired, billingstripe.Invoice{
							ID: expired.StripeInvoiceID, CustomerID: expired.StripeCustomerID, Status: "draft",
						}),
					},
				},
				getErrQueues: map[string][]error{
					expired.StripeInvoiceID: {nil, errors.New("verification read timeout")},
				},
				deleteResult: tt.deleteResult,
				deleteErr:    tt.deleteErr,
			}
			executor := NewExecutor(
				store,
				&memorySettler{store: store},
				stripe,
			).WithNow(func() time.Time { return now })

			result, err := executor.Trigger(context.Background(), expired.AccountID, 1)

			require.ErrorContains(t, err, "verification read")
			require.Equal(t, "pending", result.Status)
			require.Empty(t, store.failCalls,
				"ambiguous post-delete truth must retain the durable guard")
		})
	}
}

func TestExecutorTrigger_DeleteLosesFinalizeRaceVoidsVerifiedOpenInvoice(t *testing.T) {
	now := time.Date(2026, time.July, 25, 5, 49, 0, 0, time.UTC)
	expired := testAttempt(now.Add(-PendingGrace), 5_000_000)
	expired.ExpiresAt = now
	expired.StripeInvoiceID = "in_delete_finalize_race"
	store := newMemoryStore(expired, AcquireExisting)
	draft := exactOpenInvoice(expired)
	draft.Status = "draft"
	open := exactOpenInvoice(expired)
	stripe := &scriptedStripe{
		getQueues: map[string][]billingstripe.Invoice{
			expired.StripeInvoiceID: {draft, open},
		},
		deleteErr:  errors.New("invoice is no longer a draft"),
		voidResult: exactTerminalInvoice(expired, "void"),
		listItems: map[string][]billingstripe.InvoiceItem{
			expired.StripeInvoiceID: {exactInvoiceItem(expired)},
		},
	}
	executor := NewExecutor(
		store,
		&memorySettler{store: store},
		stripe,
	).WithNow(func() time.Time { return now })

	result, err := executor.Trigger(context.Background(), expired.AccountID, 1)

	require.NoError(t, err)
	require.Equal(t, "failed", result.Status)
	require.Equal(t, "attempt_expired", result.FailureCode)
	require.Len(t, stripe.deleteCalls, 1)
	require.Len(t, stripe.voidCalls, 1)
	require.Len(t, store.failCalls, 1)
	require.Empty(t, stripe.payCalls)
}

func TestExecutorTrigger_DeleteLosesPayRaceSettlesExactFrozenPayment(t *testing.T) {
	now := time.Date(2026, time.July, 25, 5, 51, 0, 0, time.UTC)
	expired := testAttempt(now.Add(-PendingGrace), 5_000_000)
	expired.ExpiresAt = now
	expired.StripeInvoiceID = "in_delete_pay_race"
	store := newMemoryStore(expired, AcquireExisting)
	draft := exactOpenInvoice(expired)
	draft.Status = "draft"
	paid := exactPaidInvoice(expired)
	stripe := &scriptedStripe{
		getQueues: map[string][]billingstripe.Invoice{
			expired.StripeInvoiceID: {draft, paid},
		},
		deleteErr: errors.New("invoice is no longer a draft"),
		listItems: map[string][]billingstripe.InvoiceItem{
			expired.StripeInvoiceID: {exactInvoiceItem(expired)},
		},
		listPayments: map[string][]billingstripe.InvoicePaymentProof{
			expired.StripeInvoiceID: {exactInvoicePayment(expired, expired.StripeInvoiceID)},
		},
	}
	settler := &memorySettler{store: store, transitioned: true}
	executor := NewExecutor(store, settler, stripe).WithNow(func() time.Time { return now })

	result, err := executor.Trigger(context.Background(), expired.AccountID, 1)

	require.NoError(t, err)
	require.Equal(t, "settled", result.Status)
	require.Len(t, stripe.deleteCalls, 1)
	require.Empty(t, stripe.voidCalls)
	require.Empty(t, store.failCalls)
	require.Len(t, settler.calls, 1)
}

func TestExecutorTrigger_AttachedInvoiceResourceMissingStaysPending(t *testing.T) {
	now := time.Date(2026, time.July, 25, 5, 50, 0, 0, time.UTC)
	attempt := testAttempt(now, 5_000_000)
	attempt.StripeInvoiceID = "in_already_deleted"
	store := newMemoryStore(attempt, AcquireExisting)
	stripe := &scriptedStripe{getErr: resourceMissingError()}
	executor := NewExecutor(
		store,
		&memorySettler{store: store},
		stripe,
	).WithNow(func() time.Time { return now })

	result, err := executor.Trigger(context.Background(), attempt.AccountID, 1)

	require.ErrorContains(t, err, "resource_missing")
	require.Equal(t, "pending", result.Status)
	require.Empty(t, result.FailureCode)
	require.Empty(t, store.failCalls,
		"a wrong Stripe mode/account can also return 404; it is not deletion proof")
	require.Empty(t, stripe.deleteCalls)
	require.Empty(t, stripe.finalizeCalls)
	require.Empty(t, stripe.payCalls)
	require.Empty(t, stripe.voidCalls)
}

func TestExecutorTrigger_DelayedDraftRecoveryReusesExactExistingLine(t *testing.T) {
	now := time.Date(2026, time.July, 25, 6, 0, 0, 0, time.UTC)
	attempt := testAttempt(now, 5_005_000)
	store := newMemoryStore(attempt, AcquireExisting)
	stripe := &scriptedStripe{
		findFound: true,
		findResult: controlledInvoice(attempt, billingstripe.Invoice{
			ID: "in_delayed", CustomerID: attempt.StripeCustomerID, Status: "draft",
			AmountDue: 501, Total: 501, Currency: "usd",
		}),
		finalizeResult: controlledInvoice(attempt, billingstripe.Invoice{
			ID: "in_delayed", CustomerID: attempt.StripeCustomerID, Status: "open",
			AmountDue: 501, Total: 501, Currency: "usd",
		}),
		listItems: map[string][]billingstripe.InvoiceItem{
			"in_delayed": {{ID: "ii_existing", AmountCents: 501, Currency: "usd"}},
		},
		payResult: controlledInvoice(attempt, billingstripe.Invoice{
			ID: "in_delayed", CustomerID: attempt.StripeCustomerID, Status: "paid",
			AmountPaid: 501, AmountDue: 501, Total: 501, Currency: "usd",
		}),
		listPayments: map[string][]billingstripe.InvoicePaymentProof{
			"in_delayed": {exactInvoicePayment(attempt, "in_delayed")},
		},
	}
	settler := &memorySettler{store: store, transitioned: true}
	executor := NewExecutor(store, settler, stripe).WithNow(func() time.Time { return now })

	result, err := executor.Trigger(context.Background(), attempt.AccountID, 1)

	require.NoError(t, err)
	require.Equal(t, "settled", result.Status)
	require.Empty(t, stripe.itemCalls, "resource truth replaces an expired Stripe idempotency key")
	require.Equal(t, []string{
		"find", "list", "finalize", "get", "list", "pay", "list", "payments",
	}, stripe.sequence)
	require.Len(t, settler.calls, 1)
}

func TestExecutorTrigger_DelayedDraftRecoveryRejectsMismatchedOrDuplicateLine(t *testing.T) {
	tests := []struct {
		name      string
		amountDue int64
		currency  string
		items     []billingstripe.InvoiceItem
	}{
		{
			name: "mismatched amount", amountDue: 499, currency: "usd",
			items: []billingstripe.InvoiceItem{{ID: "ii_wrong", AmountCents: 499, Currency: "usd"}},
		},
		{
			name: "duplicate full line", amountDue: 1_000, currency: "usd",
			items: []billingstripe.InvoiceItem{
				{ID: "ii_one", AmountCents: 500, Currency: "usd"},
				{ID: "ii_two", AmountCents: 500, Currency: "usd"},
			},
		},
		{
			name: "wrong currency", amountDue: 500, currency: "eur",
			items: []billingstripe.InvoiceItem{{ID: "ii_eur", AmountCents: 500, Currency: "eur"}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			now := time.Date(2026, time.July, 25, 6, 30, 0, 0, time.UTC)
			attempt := testAttempt(now, 5_000_000)
			store := newMemoryStore(attempt, AcquireExisting)
			stripe := &scriptedStripe{
				findFound: true,
				findResult: controlledInvoice(attempt, billingstripe.Invoice{
					ID: "in_bad_draft", CustomerID: attempt.StripeCustomerID, Status: "draft",
					AmountDue: tt.amountDue, Total: tt.amountDue, Currency: tt.currency,
				}),
				listItems: map[string][]billingstripe.InvoiceItem{
					"in_bad_draft": tt.items,
				},
			}
			executor := NewExecutor(store, &memorySettler{store: store}, stripe).
				WithNow(func() time.Time { return now })

			result, err := executor.Trigger(context.Background(), attempt.AccountID, 1)

			require.Error(t, err)
			require.Equal(t, "pending", result.Status)
			require.Empty(t, stripe.itemCalls)
			require.Empty(t, stripe.finalizeCalls, "bad resource truth must stop before finalization")
			require.Empty(t, stripe.payCalls, "bad resource truth must stop before payment")
		})
	}
}

func TestExecutorTrigger_RecoveredOpenInvariantMismatchVoidsAndFailsOwnedInvoice(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*billingstripe.Invoice, *[]billingstripe.InvoiceItem)
	}{
		{
			name: "wrong amount",
			mutate: func(invoice *billingstripe.Invoice, items *[]billingstripe.InvoiceItem) {
				invoice.AmountDue--
				invoice.Total--
				(*items)[0].AmountCents--
			},
		},
		{
			name: "wrong currency",
			mutate: func(invoice *billingstripe.Invoice, items *[]billingstripe.InvoiceItem) {
				invoice.Currency = "eur"
				(*items)[0].Currency = "eur"
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			now := time.Date(2026, time.July, 25, 6, 40, 0, 0, time.UTC)
			attempt := testAttempt(now, 5_000_000)
			attempt.StripeInvoiceID = "in_open_bad"
			store := newMemoryStore(attempt, AcquireExisting)
			invoice := exactOpenInvoice(attempt)
			items := []billingstripe.InvoiceItem{exactInvoiceItem(attempt)}
			tt.mutate(&invoice, &items)
			stripe := &scriptedStripe{
				getResults: map[string]billingstripe.Invoice{
					attempt.StripeInvoiceID: invoice,
				},
				listItems: map[string][]billingstripe.InvoiceItem{
					attempt.StripeInvoiceID: items,
				},
				voidResult: exactTerminalInvoice(attempt, "void"),
			}
			settler := &memorySettler{store: store}
			executor := NewExecutor(store, settler, stripe).WithNow(func() time.Time { return now })

			result, err := executor.Trigger(context.Background(), attempt.AccountID, 1)

			require.NoError(t, err)
			require.Equal(t, "failed", result.Status)
			require.Equal(t, "invoice_invariant_mismatch", result.FailureCode)
			require.Empty(t, stripe.payCalls,
				"resource mismatch must be closed before any payment call")
			require.Len(t, stripe.voidCalls, 1)
			require.Len(t, store.failCalls, 1,
				"exact irreversible void proof must release the durable guard")
			require.Empty(t, settler.calls)
		})
	}
}

func TestExecutorTrigger_VoidResponseRequiresExactTerminalReread(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*billingstripe.Invoice)
	}{
		{
			name: "foreign invoice",
			mutate: func(invoice *billingstripe.Invoice) {
				invoice.CustomerID = "cus_foreign"
			},
		},
		{
			name: "partial payment",
			mutate: func(invoice *billingstripe.Invoice) {
				invoice.AmountPaid = 1
			},
		},
		{
			name: "invoice remains open",
			mutate: func(invoice *billingstripe.Invoice) {
				invoice.Status = "open"
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			now := time.Date(2026, time.July, 25, 6, 42, 0, 0, time.UTC)
			attempt := testAttempt(now, 5_000_000)
			attempt.StripeInvoiceID = "in_void_bad_reread"
			store := newMemoryStore(attempt, AcquireExisting)
			open := exactOpenInvoice(attempt)
			open.AmountDue--
			open.Total--
			items := []billingstripe.InvoiceItem{exactInvoiceItem(attempt)}
			items[0].AmountCents--
			unsafeTerminal := exactTerminalInvoice(attempt, "void")
			tt.mutate(&unsafeTerminal)
			stripe := &scriptedStripe{
				getResults: map[string]billingstripe.Invoice{
					attempt.StripeInvoiceID: open,
				},
				listItems: map[string][]billingstripe.InvoiceItem{
					attempt.StripeInvoiceID: items,
				},
				voidResult: unsafeTerminal,
			}
			settler := &memorySettler{store: store}
			executor := NewExecutor(store, settler, stripe).WithNow(func() time.Time { return now })

			result, err := executor.Trigger(context.Background(), attempt.AccountID, 1)

			require.Error(t, err)
			require.Equal(t, "pending", result.Status)
			require.Len(t, stripe.voidCalls, 1)
			require.Empty(t, store.failCalls)
			require.Empty(t, settler.calls)
		})
	}
}

func TestExecutorTrigger_VoidLosesPayRaceSettlesExactFrozenPayment(t *testing.T) {
	now := time.Date(2026, time.July, 25, 6, 44, 0, 0, time.UTC)
	attempt := testAttempt(now, 5_000_000)
	attempt.StripeInvoiceID = "in_foreground_void_pay_race"
	store := newMemoryStore(attempt, AcquireExisting)
	open := exactOpenInvoice(attempt)
	open.AutoAdvance = true
	paid := exactPaidInvoice(attempt)
	stripe := &scriptedStripe{
		getQueues: map[string][]billingstripe.Invoice{
			attempt.StripeInvoiceID: {open, paid},
		},
		listItems: map[string][]billingstripe.InvoiceItem{
			attempt.StripeInvoiceID: {exactInvoiceItem(attempt)},
		},
		listPayments: map[string][]billingstripe.InvoicePaymentProof{
			attempt.StripeInvoiceID: {exactInvoicePayment(attempt, attempt.StripeInvoiceID)},
		},
		voidResult:         exactTerminalInvoice(attempt, "void"),
		voidDoesNotPersist: true,
	}
	settler := &memorySettler{store: store, transitioned: true}
	executor := NewExecutor(store, settler, stripe).WithNow(func() time.Time { return now })

	result, err := executor.Trigger(context.Background(), attempt.AccountID, 1)

	require.NoError(t, err)
	require.Equal(t, "settled", result.Status)
	require.Len(t, stripe.voidCalls, 1)
	require.Empty(t, store.failCalls)
	require.Len(t, settler.calls, 1)
}

func TestExecutorTrigger_UncollectibleRequiresExactVoidProofBeforeFailure(t *testing.T) {
	now := time.Date(2026, time.July, 25, 6, 44, 10, 0, time.UTC)
	attempt := testAttempt(now, 5_000_000)
	attempt.StripeInvoiceID = "in_foreground_uncollectible_void"
	store := newMemoryStore(attempt, AcquireExisting)
	uncollectible := exactTerminalInvoice(attempt, "uncollectible")
	voided := exactTerminalInvoice(attempt, "void")
	voided.HostedInvoiceURL = "https://stripe.test/in_foreground_uncollectible_void"
	stripe := &scriptedStripe{
		getQueues: map[string][]billingstripe.Invoice{
			attempt.StripeInvoiceID: {uncollectible, voided},
		},
		listItems: map[string][]billingstripe.InvoiceItem{
			attempt.StripeInvoiceID: {exactInvoiceItem(attempt)},
		},
		voidResult:         voided,
		voidDoesNotPersist: true,
	}
	settler := &memorySettler{store: store}
	executor := NewExecutor(store, settler, stripe).WithNow(func() time.Time { return now })

	result, err := executor.Trigger(context.Background(), attempt.AccountID, 1)

	require.NoError(t, err)
	require.Equal(t, "failed", result.Status)
	require.Equal(t, "invoice_uncollectible", result.FailureCode)
	require.Equal(t, []string{"get", "void", "get"}, stripe.sequence,
		"the reversible state must be voided and independently re-read before failure")
	require.Equal(t, []voidCall{{
		invoiceID: attempt.StripeInvoiceID,
		idemKey:   "credit-auto-topup-void:" + attempt.ID.String(),
	}}, stripe.voidCalls)
	require.Len(t, store.failCalls, 1)
	require.Empty(t, stripe.payCalls)
	require.Empty(t, settler.calls)
}

func TestExecutorTrigger_UncollectibleVoidLosesPayRaceSettlesExactFrozenPayment(t *testing.T) {
	now := time.Date(2026, time.July, 25, 6, 44, 20, 0, time.UTC)
	attempt := testAttempt(now, 5_000_000)
	attempt.StripeInvoiceID = "in_foreground_uncollectible_paid_race"
	store := newMemoryStore(attempt, AcquireExisting)
	uncollectible := exactTerminalInvoice(attempt, "uncollectible")
	paid := exactPaidInvoice(attempt)
	stripe := &scriptedStripe{
		getQueues: map[string][]billingstripe.Invoice{
			attempt.StripeInvoiceID: {uncollectible, paid},
		},
		listItems: map[string][]billingstripe.InvoiceItem{
			attempt.StripeInvoiceID: {exactInvoiceItem(attempt)},
		},
		listPayments: map[string][]billingstripe.InvoicePaymentProof{
			attempt.StripeInvoiceID: {
				exactInvoicePayment(attempt, attempt.StripeInvoiceID),
			},
		},
		voidResult:         exactTerminalInvoice(attempt, "void"),
		voidDoesNotPersist: true,
	}
	settler := &memorySettler{store: store, transitioned: true}
	executor := NewExecutor(store, settler, stripe).WithNow(func() time.Time { return now })

	result, err := executor.Trigger(context.Background(), attempt.AccountID, 1)

	require.NoError(t, err)
	require.Equal(t, "settled", result.Status)
	require.Equal(t, []string{"get", "void", "get", "list", "payments"}, stripe.sequence)
	require.Len(t, stripe.voidCalls, 1)
	require.Empty(t, store.failCalls,
		"authoritative paid truth must win over the earlier reversible state")
	require.Len(t, settler.calls, 1)
}

func TestExecutorTrigger_UncollectibleRereadRetainsPendingGuard(t *testing.T) {
	now := time.Date(2026, time.July, 25, 6, 44, 30, 0, time.UTC)
	attempt := testAttempt(now, 5_000_000)
	attempt.StripeInvoiceID = "in_foreground_still_uncollectible"
	store := newMemoryStore(attempt, AcquireExisting)
	uncollectible := exactTerminalInvoice(attempt, "uncollectible")
	stripe := &scriptedStripe{
		getQueues: map[string][]billingstripe.Invoice{
			attempt.StripeInvoiceID: {uncollectible, uncollectible},
		},
		listItems: map[string][]billingstripe.InvoiceItem{
			attempt.StripeInvoiceID: {exactInvoiceItem(attempt)},
		},
		voidResult:         uncollectible,
		voidDoesNotPersist: true,
	}
	executor := NewExecutor(
		store,
		&memorySettler{store: store},
		stripe,
	).WithNow(func() time.Time { return now })

	result, err := executor.Trigger(context.Background(), attempt.AccountID, 1)

	require.ErrorContains(t, err, "expected void/0/0")
	require.Equal(t, "pending", result.Status)
	require.Len(t, stripe.voidCalls, 1)
	require.Empty(t, store.failCalls,
		"an independently re-read reversible state must retain the durable guard")
}

func TestExecutorRecover_ExpiredUncollectibleUsesVoidBarrierWithoutReplacement(t *testing.T) {
	now := time.Date(2026, time.July, 25, 6, 44, 40, 0, time.UTC)
	attempt := testAttempt(now.Add(-PendingGrace), 5_000_000)
	attempt.ExpiresAt = now
	attempt.StripeInvoiceID = "in_recover_expired_uncollectible"
	store := newMemoryStore(attempt, AcquireNone)
	uncollectible := exactTerminalInvoice(attempt, "uncollectible")
	voided := exactTerminalInvoice(attempt, "void")
	stripe := &scriptedStripe{
		getQueues: map[string][]billingstripe.Invoice{
			attempt.StripeInvoiceID: {uncollectible, voided},
		},
		listItems: map[string][]billingstripe.InvoiceItem{
			attempt.StripeInvoiceID: {exactInvoiceItem(attempt)},
		},
		voidResult:         voided,
		voidDoesNotPersist: true,
	}
	executor := NewExecutor(
		store,
		&memorySettler{store: store},
		stripe,
	).WithNow(func() time.Time { return now })

	result, err := executor.Recover(context.Background(), attempt.AccountID)

	require.NoError(t, err)
	require.Equal(t, "failed", result.Status)
	require.Equal(t, "invoice_uncollectible", result.FailureCode)
	require.Equal(t, []string{"get", "void", "get"}, stripe.sequence)
	require.Len(t, stripe.voidCalls, 1)
	require.Len(t, store.failCalls, 1)
	require.Len(t, store.acquire, 1,
		"expired explicit recovery must not create a replacement attempt")
}

func TestExecutorTrigger_RecoveredInvoicePartialMetadataIsNeverAttachedOrClosed(t *testing.T) {
	tests := []struct {
		name     string
		status   string
		attached bool
		mutate   func(*billingstripe.Invoice)
	}{
		{
			name:   "unattached draft missing ledger anchor",
			status: "draft",
			mutate: func(invoice *billingstripe.Invoice) {
				invoice.CreditLedgerID = ""
			},
		},
		{
			name:   "unattached open missing operation",
			status: "open",
			mutate: func(invoice *billingstripe.Invoice) {
				invoice.CreditOperation = ""
			},
		},
		{
			name:     "attached draft wrong account anchor",
			status:   "draft",
			attached: true,
			mutate: func(invoice *billingstripe.Invoice) {
				invoice.CreditAccountID = uuid.NewString()
			},
		},
		{
			name:     "attached open partial reference",
			status:   "open",
			attached: true,
			mutate: func(invoice *billingstripe.Invoice) {
				invoice.ChargeRef = "credit-auto-topup:"
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			now := time.Date(2026, time.July, 25, 6, 44, 50, 0, time.UTC)
			attempt := testAttempt(now.Add(-PendingGrace), 5_000_000)
			attempt.ExpiresAt = now
			invoiceID := "in_partial_metadata_" + tt.status
			if tt.attached {
				attempt.StripeInvoiceID = invoiceID
			}
			store := newMemoryStore(attempt, AcquireExisting)
			invoice := controlledInvoice(attempt, billingstripe.Invoice{
				ID:         invoiceID,
				CustomerID: attempt.StripeCustomerID,
				Status:     tt.status,
				AmountDue:  microsToCentsRoundHalfUp(attempt.AmountMicros),
				Total:      microsToCentsRoundHalfUp(attempt.AmountMicros),
				Currency:   "usd",
			})
			tt.mutate(&invoice)
			stripe := &scriptedStripe{
				findFound:  !tt.attached,
				findResult: invoice,
				getResults: map[string]billingstripe.Invoice{
					invoiceID: invoice,
				},
				deleteResult: billingstripe.Invoice{ID: invoiceID, Deleted: true},
				voidResult:   exactTerminalInvoice(attempt, "void"),
			}
			executor := NewExecutor(
				store,
				&memorySettler{store: store},
				stripe,
			).WithNow(func() time.Time { return now })

			result, err := executor.Trigger(context.Background(), attempt.AccountID, 1)

			require.ErrorContains(t, err, "credit metadata does not match")
			require.Equal(t, "pending", result.Status)
			require.Equal(t, attempt.StripeInvoiceID, store.mustGet(attempt.ID).StripeInvoiceID,
				"a recovered foreign resource must not be attached")
			require.Empty(t, stripe.deleteCalls,
				"partial ownership metadata must never authorize draft deletion")
			require.Empty(t, stripe.voidCalls,
				"partial ownership metadata must never authorize invoice voiding")
			require.Empty(t, stripe.finalizeCalls)
			require.Empty(t, stripe.payCalls)
			require.Empty(t, store.failCalls)
		})
	}
}

func TestExecutorTrigger_RecoveredForeignCustomerStaysPendingWithoutStripeWrite(t *testing.T) {
	now := time.Date(2026, time.July, 25, 6, 45, 0, 0, time.UTC)
	attempt := testAttempt(now, 5_000_000)
	attempt.StripeInvoiceID = "in_foreign"
	store := newMemoryStore(attempt, AcquireExisting)
	invoice := exactOpenInvoice(attempt)
	invoice.CustomerID = "cus_foreign"
	stripe := &scriptedStripe{
		getResults: map[string]billingstripe.Invoice{
			attempt.StripeInvoiceID: invoice,
		},
	}
	settler := &memorySettler{store: store}
	executor := NewExecutor(store, settler, stripe).WithNow(func() time.Time { return now })

	result, err := executor.Trigger(context.Background(), attempt.AccountID, 1)

	require.ErrorContains(t, err, "does not match frozen customer")
	require.Equal(t, "pending", result.Status)
	require.Empty(t, stripe.itemCalls)
	require.Empty(t, stripe.finalizeCalls)
	require.Empty(t, stripe.payCalls)
	require.Empty(t, stripe.voidCalls,
		"a foreign-customer resource must never be mutated")
	require.Empty(t, store.failCalls)
	require.Empty(t, settler.calls)
}

func TestExecutorTrigger_PostFinalizeRereadMismatchVoidsAndNeverPays(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*billingstripe.Invoice)
	}{
		{
			name: "wrong amount",
			mutate: func(invoice *billingstripe.Invoice) {
				invoice.AmountDue++
				invoice.Total++
			},
		},
		{
			name: "wrong currency",
			mutate: func(invoice *billingstripe.Invoice) {
				invoice.Currency = "eur"
			},
		},
		{
			name: "automatic collection re-enabled",
			mutate: func(invoice *billingstripe.Invoice) {
				invoice.AutoAdvance = true
			},
		},
		{
			name: "collection method changed",
			mutate: func(invoice *billingstripe.Invoice) {
				invoice.CollectionMethod = string(stripego.InvoiceCollectionMethodSendInvoice)
			},
		},
		{
			name: "frozen payment method changed",
			mutate: func(invoice *billingstripe.Invoice) {
				invoice.DefaultPaymentMethodID = "pm_other"
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			now := time.Date(2026, time.July, 25, 6, 50, 0, 0, time.UTC)
			attempt := testAttempt(now, 5_000_000)
			store := newMemoryStore(attempt, AcquireNew)
			attached := attempt
			attached.StripeInvoiceID = "in_finalized_bad"
			draft := exactOpenInvoice(attached)
			draft.Status = "draft"
			finalizedResponse := exactOpenInvoice(attached)
			latest := exactOpenInvoice(attached)
			tt.mutate(&latest)
			stripe := &scriptedStripe{
				createResult: controlledInvoice(attempt, billingstripe.Invoice{
					ID: "in_finalized_bad", CustomerID: attempt.StripeCustomerID, Status: "draft",
				}),
				finalizeResult: finalizedResponse,
				getQueues: map[string][]billingstripe.Invoice{
					"in_finalized_bad": {draft, latest},
				},
				voidResult: exactTerminalInvoice(attached, "void"),
			}
			settler := &memorySettler{store: store}
			executor := NewExecutor(store, settler, stripe).WithNow(func() time.Time { return now })

			result, err := executor.Trigger(context.Background(), attempt.AccountID, 1)

			require.NoError(t, err)
			require.Equal(t, "failed", result.Status)
			require.Equal(t, "invoice_invariant_mismatch", result.FailureCode)
			require.Empty(t, stripe.payCalls)
			require.Len(t, stripe.voidCalls, 1)
			require.Empty(t, settler.calls)
		})
	}
}

func TestExecutorReconcileWebhookPaid_ReReadsExactResourceBeforeSettlement(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(
			*billingstripe.Invoice,
			*[]billingstripe.InvoiceItem,
			*[]billingstripe.InvoicePaymentProof,
		)
	}{
		{
			name: "wrong customer",
			mutate: func(
				invoice *billingstripe.Invoice,
				_ *[]billingstripe.InvoiceItem,
				_ *[]billingstripe.InvoicePaymentProof,
			) {
				invoice.CustomerID = "cus_foreign"
			},
		},
		{
			name: "wrong line",
			mutate: func(
				_ *billingstripe.Invoice,
				items *[]billingstripe.InvoiceItem,
				_ *[]billingstripe.InvoicePaymentProof,
			) {
				(*items)[0].AmountCents--
			},
		},
		{
			name: "paid out of band",
			mutate: func(
				invoice *billingstripe.Invoice,
				_ *[]billingstripe.InvoiceItem,
				_ *[]billingstripe.InvoicePaymentProof,
			) {
				invoice.AmountPaidOffStripe = invoice.AmountPaid
			},
		},
		{
			name: "wrong actual payment method",
			mutate: func(
				_ *billingstripe.Invoice,
				_ *[]billingstripe.InvoiceItem,
				payments *[]billingstripe.InvoicePaymentProof,
			) {
				(*payments)[0].PaymentMethodID = "pm_other"
			},
		},
		{
			name: "multiple paid allocations",
			mutate: func(
				_ *billingstripe.Invoice,
				_ *[]billingstripe.InvoiceItem,
				payments *[]billingstripe.InvoicePaymentProof,
			) {
				*payments = append(*payments, (*payments)[0])
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			now := time.Date(2026, time.July, 25, 6, 55, 0, 0, time.UTC)
			attempt := testAttempt(now, 5_000_000)
			attempt.StripeInvoiceID = "in_paid_webhook_bad"
			store := newMemoryStore(attempt, AcquireExisting)
			invoice := exactPaidInvoice(attempt)
			items := []billingstripe.InvoiceItem{exactInvoiceItem(attempt)}
			payments := []billingstripe.InvoicePaymentProof{
				exactInvoicePayment(attempt, attempt.StripeInvoiceID),
			}
			tt.mutate(&invoice, &items, &payments)
			stripe := &scriptedStripe{
				getResults: map[string]billingstripe.Invoice{
					attempt.StripeInvoiceID: invoice,
				},
				listItems: map[string][]billingstripe.InvoiceItem{
					attempt.StripeInvoiceID: items,
				},
				listPayments: map[string][]billingstripe.InvoicePaymentProof{
					attempt.StripeInvoiceID: payments,
				},
			}
			settler := &memorySettler{store: store, transitioned: true}
			executor := NewExecutor(store, settler, stripe)

			result, err := executor.ReconcileWebhookPaid(
				context.Background(),
				attempt.StripeInvoiceID,
			)

			require.Error(t, err)
			require.True(t, result.Found)
			require.False(t, result.Transitioned)
			require.Empty(t, settler.calls,
				"unverified paid event must never advance wallet credit")
			require.Equal(t, "pending", store.mustGet(attempt.ID).Status)
		})
	}
}

func TestExecutorReconcileWebhookPaid_ValidResourceSettlesAndUnknownFallsThrough(t *testing.T) {
	now := time.Date(2026, time.July, 25, 6, 57, 0, 0, time.UTC)
	attempt := testAttempt(now, 5_000_000)
	attempt.StripeInvoiceID = "in_paid_webhook_exact"
	store := newMemoryStore(attempt, AcquireExisting)
	stripe := &scriptedStripe{
		getResults: map[string]billingstripe.Invoice{
			attempt.StripeInvoiceID: exactPaidInvoice(attempt),
		},
		listItems: map[string][]billingstripe.InvoiceItem{
			attempt.StripeInvoiceID: {exactInvoiceItem(attempt)},
		},
		listPayments: map[string][]billingstripe.InvoicePaymentProof{
			attempt.StripeInvoiceID: {exactInvoicePayment(attempt, attempt.StripeInvoiceID)},
		},
	}
	settler := &memorySettler{store: store, transitioned: true}
	executor := NewExecutor(store, settler, stripe)

	result, err := executor.ReconcileWebhookPaid(
		context.Background(),
		attempt.StripeInvoiceID,
	)

	require.NoError(t, err)
	require.True(t, result.Found)
	require.True(t, result.Transitioned)
	require.Equal(t, "settled", store.mustGet(attempt.ID).Status)
	require.Len(t, settler.calls, 1)

	unknown, err := executor.ReconcileWebhookPaid(context.Background(), "in_ordinary")
	require.NoError(t, err)
	require.Equal(t, creditledger.Settlement{}, unknown)
}

func TestExecutorReconcileWebhookFailure_VoidsVerifiesAndFailsExactlyOnce(t *testing.T) {
	now := time.Date(2026, time.July, 25, 7, 0, 0, 0, time.UTC)
	attempt := testAttempt(now, 5_005_000)
	attempt.StripeInvoiceID = "in_webhook_failed"
	store := newMemoryStore(attempt, AcquireExisting)
	open := exactOpenInvoice(attempt)
	voided := exactTerminalInvoice(attempt, "void")
	voided.HostedInvoiceURL = "https://stripe.test/in_webhook_failed"
	stripe := &scriptedStripe{
		getQueues: map[string][]billingstripe.Invoice{
			attempt.StripeInvoiceID: {open, voided},
		},
		getResults: map[string]billingstripe.Invoice{
			attempt.StripeInvoiceID: voided,
		},
		listItems: map[string][]billingstripe.InvoiceItem{
			attempt.StripeInvoiceID: {exactInvoiceItem(attempt)},
		},
		voidResult: voided,
	}
	settler := &memorySettler{store: store}
	executor := NewExecutor(store, settler, stripe)

	first, err := executor.ReconcileWebhookFailure(
		context.Background(),
		attempt.StripeInvoiceID,
		"payment_failed",
	)

	require.NoError(t, err)
	require.True(t, first.Found)
	require.True(t, first.Transitioned)
	require.Equal(t, "failed", first.Status)
	require.Equal(t, "payment_failed", first.FailureCode)
	require.Equal(t, []voidCall{{
		invoiceID: attempt.StripeInvoiceID,
		idemKey:   "credit-auto-topup-void:" + attempt.ID.String(),
	}}, stripe.voidCalls)
	require.Len(t, store.failCalls, 1)
	require.Empty(t, settler.calls, "an unpaid failure must never create wallet credit")

	second, err := executor.ReconcileWebhookFailure(
		context.Background(),
		attempt.StripeInvoiceID,
		"payment_failed",
	)

	require.NoError(t, err)
	require.True(t, second.Found)
	require.False(t, second.Transitioned)
	require.Equal(t, "failed", second.Status)
	require.Len(t, store.failCalls, 1, "replay must not commit a second transition")
	require.Len(t, stripe.voidCalls, 1, "a verified terminal replay needs no second void")
	require.Empty(t, settler.calls)
}

func TestExecutorReconcileWebhookFailure_VoidResponseCannotOverrideUnsafeReread(t *testing.T) {
	tests := []struct {
		name      string
		mutate    func(*billingstripe.Invoice)
		wantError bool
	}{
		{
			name: "invoice remains open",
			mutate: func(invoice *billingstripe.Invoice) {
				invoice.Status = "open"
			},
			wantError: true,
		},
		{
			name: "foreign terminal invoice",
			mutate: func(invoice *billingstripe.Invoice) {
				invoice.CustomerID = "cus_foreign"
			},
		},
		{
			name: "partially paid terminal invoice",
			mutate: func(invoice *billingstripe.Invoice) {
				invoice.AmountPaid = 1
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			now := time.Date(2026, time.July, 25, 7, 10, 0, 0, time.UTC)
			attempt := testAttempt(now, 5_000_000)
			attempt.StripeInvoiceID = "in_void_reread_unsafe"
			store := newMemoryStore(attempt, AcquireExisting)
			open := exactOpenInvoice(attempt)
			unsafe := exactTerminalInvoice(attempt, "void")
			tt.mutate(&unsafe)
			stripe := &scriptedStripe{
				getQueues: map[string][]billingstripe.Invoice{
					attempt.StripeInvoiceID: {open, unsafe},
				},
				listItems: map[string][]billingstripe.InvoiceItem{
					attempt.StripeInvoiceID: {exactInvoiceItem(attempt)},
				},
				voidResult:         exactTerminalInvoice(attempt, "void"),
				voidDoesNotPersist: true,
			}
			settler := &memorySettler{store: store}
			executor := NewExecutor(store, settler, stripe)

			result, err := executor.ReconcileWebhookFailure(
				context.Background(),
				attempt.StripeInvoiceID,
				"payment_failed",
			)

			if tt.wantError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			require.True(t, result.Found)
			require.False(t, result.Transitioned)
			require.Equal(t, "pending", store.mustGet(attempt.ID).Status)
			require.Empty(t, store.failCalls)
			require.Empty(t, settler.calls)
		})
	}
}

func TestExecutorReconcileWebhookFailure_VoidLosesPayRaceSettlesExactPayment(t *testing.T) {
	now := time.Date(2026, time.July, 25, 7, 20, 0, 0, time.UTC)
	attempt := testAttempt(now, 5_000_000)
	attempt.StripeInvoiceID = "in_void_pay_race"
	store := newMemoryStore(attempt, AcquireExisting)
	open := exactOpenInvoice(attempt)
	paid := exactPaidInvoice(attempt)
	stripe := &scriptedStripe{
		getQueues: map[string][]billingstripe.Invoice{
			attempt.StripeInvoiceID: {open, paid},
		},
		listItems: map[string][]billingstripe.InvoiceItem{
			attempt.StripeInvoiceID: {exactInvoiceItem(attempt)},
		},
		listPayments: map[string][]billingstripe.InvoicePaymentProof{
			attempt.StripeInvoiceID: {exactInvoicePayment(attempt, attempt.StripeInvoiceID)},
		},
		voidResult:         exactTerminalInvoice(attempt, "void"),
		voidDoesNotPersist: true,
	}
	settler := &memorySettler{store: store, transitioned: true}
	executor := NewExecutor(store, settler, stripe)

	result, err := executor.ReconcileWebhookFailure(
		context.Background(),
		attempt.StripeInvoiceID,
		"payment_failed",
	)

	require.NoError(t, err)
	require.True(t, result.Found)
	require.True(t, result.Transitioned)
	require.Equal(t, "settled", result.Status)
	require.Empty(t, store.failCalls)
	require.Len(t, settler.calls, 1)
}

func TestExecutorReconcileWebhookFailure_UncollectibleRequiresExactVoidProof(t *testing.T) {
	now := time.Date(2026, time.July, 25, 7, 21, 0, 0, time.UTC)
	attempt := testAttempt(now, 5_000_000)
	attempt.StripeInvoiceID = "in_webhook_uncollectible_void"
	store := newMemoryStore(attempt, AcquireExisting)
	uncollectible := exactTerminalInvoice(attempt, "uncollectible")
	voided := exactTerminalInvoice(attempt, "void")
	voided.HostedInvoiceURL = "https://stripe.test/in_webhook_uncollectible_void"
	stripe := &scriptedStripe{
		getQueues: map[string][]billingstripe.Invoice{
			attempt.StripeInvoiceID: {uncollectible, voided},
		},
		listItems: map[string][]billingstripe.InvoiceItem{
			attempt.StripeInvoiceID: {exactInvoiceItem(attempt)},
		},
		voidResult:         voided,
		voidDoesNotPersist: true,
	}
	settler := &memorySettler{store: store}
	executor := NewExecutor(store, settler, stripe)

	result, err := executor.ReconcileWebhookFailure(
		context.Background(),
		attempt.StripeInvoiceID,
		"payment_failed",
	)

	require.NoError(t, err)
	require.True(t, result.Found)
	require.True(t, result.Transitioned)
	require.Equal(t, "failed", result.Status)
	require.Equal(t, "payment_failed", result.FailureCode)
	require.Equal(t, []string{"get", "void", "get"}, stripe.sequence)
	require.Equal(t, []voidCall{{
		invoiceID: attempt.StripeInvoiceID,
		idemKey:   "credit-auto-topup-void:" + attempt.ID.String(),
	}}, stripe.voidCalls)
	require.Len(t, store.failCalls, 1)
	require.Empty(t, settler.calls)
}

func TestExecutorReconcileWebhookFailure_UncollectibleVoidLosesPayRace(t *testing.T) {
	now := time.Date(2026, time.July, 25, 7, 22, 0, 0, time.UTC)
	attempt := testAttempt(now, 5_000_000)
	attempt.StripeInvoiceID = "in_webhook_uncollectible_paid_race"
	store := newMemoryStore(attempt, AcquireExisting)
	uncollectible := exactTerminalInvoice(attempt, "uncollectible")
	paid := exactPaidInvoice(attempt)
	stripe := &scriptedStripe{
		getQueues: map[string][]billingstripe.Invoice{
			attempt.StripeInvoiceID: {uncollectible, paid},
		},
		listItems: map[string][]billingstripe.InvoiceItem{
			attempt.StripeInvoiceID: {exactInvoiceItem(attempt)},
		},
		listPayments: map[string][]billingstripe.InvoicePaymentProof{
			attempt.StripeInvoiceID: {
				exactInvoicePayment(attempt, attempt.StripeInvoiceID),
			},
		},
		voidResult:         exactTerminalInvoice(attempt, "void"),
		voidDoesNotPersist: true,
	}
	settler := &memorySettler{store: store, transitioned: true}
	executor := NewExecutor(store, settler, stripe)

	result, err := executor.ReconcileWebhookFailure(
		context.Background(),
		attempt.StripeInvoiceID,
		"payment_failed",
	)

	require.NoError(t, err)
	require.True(t, result.Found)
	require.True(t, result.Transitioned)
	require.Equal(t, "settled", result.Status)
	require.Empty(t, result.FailureCode)
	require.Equal(t, []string{"get", "void", "get", "list", "payments"}, stripe.sequence)
	require.Len(t, stripe.voidCalls, 1)
	require.Empty(t, store.failCalls)
	require.Len(t, settler.calls, 1)
}

func TestExecutorReconcileWebhookFailure_OwnedUnpaidMismatchClosesButForeignOrPartialStaysPending(t *testing.T) {
	tests := []struct {
		name     string
		closable bool
		mutate   func(*billingstripe.Invoice, *[]billingstripe.InvoiceItem)
	}{
		{
			name: "wrong customer",
			mutate: func(invoice *billingstripe.Invoice, _ *[]billingstripe.InvoiceItem) {
				invoice.CustomerID = "cus_foreign"
			},
		},
		{
			name:     "wrong total",
			closable: true,
			mutate: func(invoice *billingstripe.Invoice, _ *[]billingstripe.InvoiceItem) {
				invoice.Total--
				invoice.AmountDue--
			},
		},
		{
			name:     "wrong currency",
			closable: true,
			mutate: func(invoice *billingstripe.Invoice, items *[]billingstripe.InvoiceItem) {
				invoice.Currency = "eur"
				(*items)[0].Currency = "eur"
			},
		},
		{
			name:     "duplicate line",
			closable: true,
			mutate: func(_ *billingstripe.Invoice, items *[]billingstripe.InvoiceItem) {
				*items = append(*items, (*items)[0])
			},
		},
		{
			name: "partially paid",
			mutate: func(invoice *billingstripe.Invoice, _ *[]billingstripe.InvoiceItem) {
				invoice.AmountPaid = 1
				invoice.AmountDue--
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			now := time.Date(2026, time.July, 25, 7, 30, 0, 0, time.UTC)
			attempt := testAttempt(now, 5_000_000)
			attempt.StripeInvoiceID = "in_invariant"
			store := newMemoryStore(attempt, AcquireExisting)
			invoice := exactOpenInvoice(attempt)
			items := []billingstripe.InvoiceItem{exactInvoiceItem(attempt)}
			tt.mutate(&invoice, &items)
			stripe := &scriptedStripe{
				getResults: map[string]billingstripe.Invoice{
					attempt.StripeInvoiceID: invoice,
				},
				listItems: map[string][]billingstripe.InvoiceItem{
					attempt.StripeInvoiceID: items,
				},
				voidResult: exactTerminalInvoice(attempt, "void"),
			}
			settler := &memorySettler{store: store}
			executor := NewExecutor(store, settler, stripe)

			result, err := executor.ReconcileWebhookFailure(
				context.Background(),
				attempt.StripeInvoiceID,
				"payment_failed",
			)

			require.NoError(t, err)
			require.True(t, result.Found)
			if tt.closable {
				require.True(t, result.Transitioned)
				require.Equal(t, "failed", result.Status)
				require.Len(t, stripe.voidCalls, 1)
				require.Len(t, store.failCalls, 1,
					"owned unpaid line/presentment mismatch must not strand an irreversible void")
			} else {
				require.False(t, result.Transitioned)
				require.Equal(t, "pending", result.Status)
				require.Empty(t, stripe.voidCalls)
				require.Empty(t, store.failCalls)
			}
			require.Empty(t, settler.calls)
		})
	}
}

func TestExecutorReconcileWebhookFailure_OnlyTerminalOwnershipMismatchRemainsPending(t *testing.T) {
	tests := []struct {
		name              string
		ownershipMismatch bool
		mutate            func(*billingstripe.Invoice, *[]billingstripe.InvoiceItem)
	}{
		{
			name:              "missing operation",
			ownershipMismatch: true,
			mutate: func(invoice *billingstripe.Invoice, _ *[]billingstripe.InvoiceItem) {
				invoice.CreditOperation = ""
			},
		},
		{
			name:              "wrong reference",
			ownershipMismatch: true,
			mutate: func(invoice *billingstripe.Invoice, _ *[]billingstripe.InvoiceItem) {
				invoice.ChargeRef = "credit-auto-topup:" + uuid.NewString()
			},
		},
		{
			name:              "wrong account anchor",
			ownershipMismatch: true,
			mutate: func(invoice *billingstripe.Invoice, _ *[]billingstripe.InvoiceItem) {
				invoice.CreditAccountID = uuid.NewString()
			},
		},
		{
			name:              "wrong ledger anchor",
			ownershipMismatch: true,
			mutate: func(invoice *billingstripe.Invoice, _ *[]billingstripe.InvoiceItem) {
				invoice.CreditLedgerID = uuid.NewString()
			},
		},
		{
			name: "wrong amount",
			mutate: func(invoice *billingstripe.Invoice, _ *[]billingstripe.InvoiceItem) {
				invoice.Total--
			},
		},
		{
			name: "wrong currency",
			mutate: func(invoice *billingstripe.Invoice, _ *[]billingstripe.InvoiceItem) {
				invoice.Currency = "eur"
			},
		},
		{
			name: "duplicate line",
			mutate: func(_ *billingstripe.Invoice, items *[]billingstripe.InvoiceItem) {
				*items = append(*items, (*items)[0])
			},
		},
		{
			name: "wrong collection mode",
			mutate: func(invoice *billingstripe.Invoice, _ *[]billingstripe.InvoiceItem) {
				invoice.CollectionMethod = string(stripego.InvoiceCollectionMethodSendInvoice)
			},
		},
		{
			name: "wrong frozen card",
			mutate: func(invoice *billingstripe.Invoice, _ *[]billingstripe.InvoiceItem) {
				invoice.DefaultPaymentMethodID = "pm_foreign"
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			now := time.Date(2026, time.July, 25, 7, 45, 0, 0, time.UTC)
			attempt := testAttempt(now, 5_000_000)
			attempt.StripeInvoiceID = "in_terminal_invariant"
			store := newMemoryStore(attempt, AcquireExisting)
			invoice := exactTerminalInvoice(attempt, "void")
			items := []billingstripe.InvoiceItem{exactInvoiceItem(attempt)}
			tt.mutate(&invoice, &items)
			stripe := &scriptedStripe{
				getResults: map[string]billingstripe.Invoice{
					attempt.StripeInvoiceID: invoice,
				},
				listItems: map[string][]billingstripe.InvoiceItem{
					attempt.StripeInvoiceID: items,
				},
			}
			settler := &memorySettler{store: store}
			executor := NewExecutor(store, settler, stripe)

			result, err := executor.ReconcileWebhookFailure(
				context.Background(),
				attempt.StripeInvoiceID,
				"payment_failed",
			)

			require.NoError(t, err)
			require.True(t, result.Found)
			if tt.ownershipMismatch {
				require.False(t, result.Transitioned)
				require.Equal(t, "pending", store.mustGet(attempt.ID).Status)
				require.Empty(t, store.failCalls)
			} else {
				require.True(t, result.Transitioned)
				require.Equal(t, "failed", store.mustGet(attempt.ID).Status)
				require.Len(t, store.failCalls, 1,
					"irreversibly void owned invoice must release the pending guard")
			}
			require.Empty(t, settler.calls)
		})
	}
}

func TestExecutorReconcileWebhookFailure_PaidRereadIsHighestTruth(t *testing.T) {
	now := time.Date(2026, time.July, 25, 8, 0, 0, 0, time.UTC)
	attempt := testAttempt(now, 5_000_000)
	attempt.StripeInvoiceID = "in_webhook_paid"
	attempt.Status = "failed"
	attempt.FailureCode = "payment_failed"
	store := newMemoryStore(attempt, AcquireExisting)
	paid := exactPaidInvoice(attempt)
	stripe := &scriptedStripe{
		getResults: map[string]billingstripe.Invoice{
			attempt.StripeInvoiceID: paid,
		},
		listItems: map[string][]billingstripe.InvoiceItem{
			attempt.StripeInvoiceID: {exactInvoiceItem(attempt)},
		},
		listPayments: map[string][]billingstripe.InvoicePaymentProof{
			attempt.StripeInvoiceID: {exactInvoicePayment(attempt, attempt.StripeInvoiceID)},
		},
	}
	settler := &memorySettler{store: store, transitioned: true}
	executor := NewExecutor(store, settler, stripe)

	result, err := executor.ReconcileWebhookFailure(
		context.Background(),
		attempt.StripeInvoiceID,
		"payment_failed",
	)

	require.NoError(t, err)
	require.True(t, result.Found)
	require.True(t, result.Transitioned)
	require.Equal(t, "settled", result.Status)
	require.Empty(t, result.FailureCode)
	require.Empty(t, stripe.voidCalls)
	require.Empty(t, store.failCalls)
	require.Len(t, settler.calls, 1)
	require.Equal(t, "settled", store.mustGet(attempt.ID).Status)
}

func TestExecutorReconcileWebhookFailure_ConcurrentEventsCommitOneZeroCreditFailure(t *testing.T) {
	now := time.Date(2026, time.July, 25, 8, 30, 0, 0, time.UTC)
	attempt := testAttempt(now, 5_000_000)
	attempt.StripeInvoiceID = "in_concurrent_failure"
	store := newMemoryStore(attempt, AcquireExisting)
	stripe := newConcurrentFailureStripe(attempt)
	settler := &memorySettler{store: store}
	executor := NewExecutor(store, settler, stripe)

	const workers = 12
	start := make(chan struct{})
	results := make(chan creditledger.FailureReconciliation, workers)
	errs := make(chan error, workers)
	var wg sync.WaitGroup
	wg.Add(workers)
	for range workers {
		go func() {
			defer wg.Done()
			<-start
			result, err := executor.ReconcileWebhookFailure(
				context.Background(),
				attempt.StripeInvoiceID,
				"payment_failed",
			)
			results <- result
			errs <- err
		}()
	}
	close(start)
	wg.Wait()
	close(results)
	close(errs)

	transitioned := 0
	for err := range errs {
		require.NoError(t, err)
	}
	for result := range results {
		require.True(t, result.Found)
		require.Equal(t, "failed", result.Status)
		if result.Transitioned {
			transitioned++
		}
	}
	require.Equal(t, 1, transitioned)
	require.Len(t, store.failCalls, 1)
	require.Empty(t, settler.calls, "concurrent unpaid terminal events add zero credit")
	require.Equal(t, "failed", store.mustGet(attempt.ID).Status)
}

func TestDeterministicPaymentFailure(t *testing.T) {
	tests := []struct {
		name          string
		err           error
		code          string
		deterministic bool
	}{
		{name: "non stripe", err: errors.New("timeout")},
		{
			name: "stripe api error remains ambiguous",
			err:  &stripego.Error{Type: stripego.ErrorTypeAPI, Code: stripego.ErrorCodeRateLimit},
		},
		{
			name: "card code fallback",
			err:  &stripego.Error{Type: stripego.ErrorTypeCard, Code: stripego.ErrorCodeCardDeclined},
			code: "card_declined", deterministic: true,
		},
		{
			name: "requires action",
			err:  &stripego.Error{Type: stripego.ErrorTypeInvalidRequest, Code: stripego.ErrorCodeInvoicePaymentIntentRequiresAction},
			code: "authentication_required", deterministic: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			code, deterministic := deterministicPaymentFailure(tt.err)
			require.Equal(t, tt.code, code)
			require.Equal(t, tt.deterministic, deterministic)
		})
	}
}

func TestCheckedSub(t *testing.T) {
	tests := []struct {
		name    string
		a       int64
		b       int64
		want    int64
		wantErr bool
	}{
		{name: "ordinary shortfall", a: 10, b: 15, want: -5},
		{name: "largest projected charge from zero remains representable", a: 0, b: math.MaxInt64, want: -math.MaxInt64},
		{name: "positive operand underflow", a: math.MinInt64, b: 1, wantErr: true},
		{name: "negative operand overflow", a: math.MaxInt64, b: -1, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := checkedSub(tt.a, tt.b)
			if tt.wantErr {
				require.Error(t, err)
				require.Zero(t, got)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}

type acquireResult struct {
	attempt Attempt
	kind    AcquireKind
	err     error
}

type memoryStore struct {
	mu        sync.Mutex
	acquire   []acquireResult
	attempts  map[uuid.UUID]Attempt
	getErrors []error
	failCalls []failCall
}

type failCall struct {
	attemptID   uuid.UUID
	failureCode string
	receiptURL  string
}

func newMemoryStore(attempt Attempt, kind AcquireKind) *memoryStore {
	return &memoryStore{
		acquire:  []acquireResult{{attempt: attempt, kind: kind}},
		attempts: map[uuid.UUID]Attempt{attempt.ID: attempt},
	}
}

func (s *memoryStore) Acquire(_ context.Context, _ uuid.UUID, _ int64, _ time.Time) (Attempt, AcquireKind, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.acquire) == 0 {
		return Attempt{}, AcquireNone, nil
	}
	next := s.acquire[0]
	s.acquire = s.acquire[1:]
	return next.attempt, next.kind, next.err
}

func (s *memoryStore) Pending(_ context.Context, accountID uuid.UUID) (Attempt, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, attempt := range s.attempts {
		if attempt.AccountID == accountID && attempt.Status == "pending" {
			return attempt, true, nil
		}
	}
	return Attempt{}, false, nil
}

func (s *memoryStore) Get(_ context.Context, _, attemptID uuid.UUID) (Attempt, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.getErrors) > 0 {
		err := s.getErrors[0]
		s.getErrors = s.getErrors[1:]
		if err != nil {
			return Attempt{}, err
		}
	}
	attempt, ok := s.attempts[attemptID]
	if !ok {
		return Attempt{}, errors.New("attempt not found")
	}
	return attempt, nil
}

func (s *memoryStore) FindByStripeInvoice(_ context.Context, stripeInvoiceID string) (Attempt, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, attempt := range s.attempts {
		if attempt.StripeInvoiceID == stripeInvoiceID {
			return attempt, true, nil
		}
	}
	return Attempt{}, false, nil
}

func (s *memoryStore) AttachInvoice(_ context.Context, attempt Attempt, invoice billingstripe.Invoice) (Attempt, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	current := s.attempts[attempt.ID]
	if current.StripeInvoiceID != "" && current.StripeInvoiceID != invoice.ID {
		return Attempt{}, errors.New("different invoice already attached")
	}
	current.StripeInvoiceID = invoice.ID
	current.ReceiptURL = invoice.HostedInvoiceURL
	s.attempts[attempt.ID] = current
	return current, nil
}

// MarkProposed records the intent-path terminal marker. The fake mirrors the
// real store's guard: only a pending attempt may be proposed, so a lost race
// returns false rather than overwriting a terminal state.
func (s *memoryStore) MarkProposed(_ context.Context, attempt Attempt, ref string) (bool, error) {
	cur, ok := s.attempts[attempt.ID]
	if !ok || cur.Status != "pending" {
		return false, nil
	}
	cur.Status = "proposed"
	s.attempts[attempt.ID] = cur
	return true, nil
}

func (s *memoryStore) Fail(_ context.Context, attempt Attempt, failureCode, receiptURL string) (Attempt, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	current := s.attempts[attempt.ID]
	if current.Status != "pending" {
		return current, false, nil
	}
	current.Status = "failed"
	current.FailureCode = failureCode
	current.ReceiptURL = receiptURL
	s.attempts[attempt.ID] = current
	s.failCalls = append(s.failCalls, failCall{
		attemptID: attempt.ID, failureCode: failureCode, receiptURL: receiptURL,
	})
	return current, true, nil
}

func (s *memoryStore) settle(attemptID uuid.UUID) {
	s.mu.Lock()
	defer s.mu.Unlock()
	current := s.attempts[attemptID]
	current.Status = "settled"
	current.FailureCode = ""
	s.attempts[attemptID] = current
}

func (s *memoryStore) mustGet(attemptID uuid.UUID) Attempt {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.attempts[attemptID]
}

type settleCall struct {
	invoiceID       string
	amountPaidCents int64
	currency        string
	receiptURL      string
}

type memorySettler struct {
	store        *memoryStore
	transitioned bool
	calls        []settleCall
}

func (s *memorySettler) SettleStripeInvoice(_ context.Context, invoiceID string, amountPaidCents int64, currency, receiptURL string) (creditledger.Settlement, error) {
	s.calls = append(s.calls, settleCall{
		invoiceID: invoiceID, amountPaidCents: amountPaidCents,
		currency: currency, receiptURL: receiptURL,
	})
	var attempt Attempt
	for _, candidate := range s.store.attempts {
		if candidate.StripeInvoiceID == invoiceID {
			attempt = candidate
			break
		}
	}
	if attempt.ID == uuid.Nil {
		return creditledger.Settlement{}, nil
	}
	if s.transitioned {
		s.store.settle(attempt.ID)
	}
	return creditledger.Settlement{
		Found: true, Transitioned: s.transitioned,
		AccountID: attempt.AccountID, LedgerID: attempt.ID, Type: "auto_topup",
	}, nil
}

type recordingObserver struct {
	accounts               []uuid.UUID
	settlementObservations []bool
	err                    error
}

func (o *recordingObserver) ObserveAccount(ctx context.Context, accountID uuid.UUID) error {
	o.accounts = append(o.accounts, accountID)
	o.settlementObservations = append(
		o.settlementObservations,
		creditledger.IsSettlementObservation(ctx),
	)
	return o.err
}

type createCall struct {
	customerID      string
	paymentMethodID string
	accountID       string
	ledgerID        string
	idemKey         string
}

type itemCall struct {
	customerID  string
	invoiceID   string
	amountCents int64
	currency    string
	description string
	idemKey     string
}

type finalizeCall struct {
	invoiceID string
	idemKey   string
}

type payCall struct {
	invoiceID       string
	paymentMethodID string
	idemKey         string
}

type voidCall struct {
	invoiceID string
	idemKey   string
}

type deleteCall struct {
	invoiceID string
}

type scriptedStripe struct {
	sequence []string

	createResult billingstripe.Invoice
	createErr    error
	createCalls  []createCall

	findResult billingstripe.Invoice
	findFound  bool
	findErr    error

	itemErr   error
	itemCalls []itemCall
	listItems map[string][]billingstripe.InvoiceItem
	listErr   error

	listPayments map[string][]billingstripe.InvoicePaymentProof
	paymentsErr  error

	finalizeResult billingstripe.Invoice
	finalizeErr    error
	finalizeCalls  []finalizeCall

	payResult billingstripe.Invoice
	payErr    error
	payCalls  []payCall

	getResults   map[string]billingstripe.Invoice
	getQueues    map[string][]billingstripe.Invoice
	getErrQueues map[string][]error
	getErr       error

	voidResult         billingstripe.Invoice
	voidErr            error
	voidCalls          []voidCall
	voided             map[string]billingstripe.Invoice
	voidDoesNotPersist bool

	deleteResult       billingstripe.Invoice
	deleteErr          error
	deleteMakesMissing bool
	deleteCalls        []deleteCall
	deletedInvoices    map[string]bool
}

func (s *scriptedStripe) CreateAutoTopUpInvoice(
	_ context.Context,
	customerID string,
	paymentMethodID string,
	accountID string,
	ledgerID string,
	idemKey string,
) (billingstripe.Invoice, error) {
	s.sequence = append(s.sequence, "create")
	s.createCalls = append(s.createCalls, createCall{
		customerID: customerID, paymentMethodID: paymentMethodID,
		accountID: accountID, ledgerID: ledgerID, idemKey: idemKey,
	})
	return s.createResult, s.createErr
}

func (s *scriptedStripe) CreateInvoiceItem(_ context.Context, customerID, invoiceID string, amountCents int64, currency, description string, _ billingstripe.LinePeriod, idemKey string) (billingstripe.InvoiceItem, error) {
	s.sequence = append(s.sequence, "item")
	s.itemCalls = append(s.itemCalls, itemCall{
		customerID: customerID, invoiceID: invoiceID, amountCents: amountCents,
		currency: currency, description: description, idemKey: idemKey,
	})
	return billingstripe.InvoiceItem{
		ID: "ii_topup", AmountCents: amountCents, Currency: currency,
	}, s.itemErr
}

func (s *scriptedStripe) ListInvoiceItems(_ context.Context, invoiceID string) ([]billingstripe.InvoiceItem, error) {
	s.sequence = append(s.sequence, "list")
	if s.listErr != nil {
		return nil, s.listErr
	}
	if items, ok := s.listItems[invoiceID]; ok {
		return append([]billingstripe.InvoiceItem(nil), items...), nil
	}
	for i := len(s.itemCalls) - 1; i >= 0; i-- {
		call := s.itemCalls[i]
		if call.invoiceID == invoiceID {
			return []billingstripe.InvoiceItem{{
				ID: "ii_topup", AmountCents: call.amountCents, Currency: call.currency,
			}}, nil
		}
	}
	return nil, nil
}

func (s *scriptedStripe) ListInvoicePayments(_ context.Context, invoiceID string) ([]billingstripe.InvoicePaymentProof, error) {
	s.sequence = append(s.sequence, "payments")
	if s.paymentsErr != nil {
		return nil, s.paymentsErr
	}
	return append(
		[]billingstripe.InvoicePaymentProof(nil),
		s.listPayments[invoiceID]...,
	), nil
}

func (s *scriptedStripe) FinalizeInvoiceWithoutAutoAdvance(_ context.Context, invoiceID, idemKey string) (billingstripe.Invoice, error) {
	s.sequence = append(s.sequence, "finalize")
	s.finalizeCalls = append(s.finalizeCalls, finalizeCall{invoiceID: invoiceID, idemKey: idemKey})
	return s.finalizeResult, s.finalizeErr
}

func (s *scriptedStripe) PayInvoiceWithMethod(_ context.Context, invoiceID, paymentMethodID, idemKey string) (billingstripe.Invoice, error) {
	s.sequence = append(s.sequence, "pay")
	s.payCalls = append(s.payCalls, payCall{
		invoiceID: invoiceID, paymentMethodID: paymentMethodID, idemKey: idemKey,
	})
	return s.payResult, s.payErr
}

func (s *scriptedStripe) GetInvoice(_ context.Context, invoiceID string) (billingstripe.Invoice, error) {
	s.sequence = append(s.sequence, "get")
	if s.getErr != nil {
		return billingstripe.Invoice{}, s.getErr
	}
	if queue := s.getErrQueues[invoiceID]; len(queue) > 0 {
		err := queue[0]
		s.getErrQueues[invoiceID] = queue[1:]
		if err != nil {
			return billingstripe.Invoice{}, err
		}
	}
	if queue := s.getQueues[invoiceID]; len(queue) > 0 {
		invoice := queue[0]
		s.getQueues[invoiceID] = queue[1:]
		return invoice, nil
	}
	if s.deletedInvoices[invoiceID] {
		return billingstripe.Invoice{}, resourceMissingError()
	}
	if invoice, ok := s.voided[invoiceID]; ok {
		return invoice, nil
	}
	// Once Pay has been attempted, explicit getResults model the
	// reconciliation read (paid/open/etc.) after a response error.
	if len(s.payCalls) > 0 {
		if invoice, ok := s.getResults[invoiceID]; ok {
			return invoice, nil
		}
	}
	if s.finalizeResult.ID == invoiceID && len(s.finalizeCalls) > 0 {
		return s.finalizeResult, nil
	}
	if s.createResult.ID == invoiceID {
		invoice := s.createResult
		for i := len(s.itemCalls) - 1; i >= 0; i-- {
			call := s.itemCalls[i]
			if call.invoiceID == invoiceID {
				invoice.AmountDue = call.amountCents
				invoice.Total = call.amountCents
				invoice.Currency = call.currency
				break
			}
		}
		return invoice, nil
	}
	if invoice, ok := s.getResults[invoiceID]; ok {
		return invoice, nil
	}
	return billingstripe.Invoice{}, nil
}

func (s *scriptedStripe) FindInvoiceByRef(_ context.Context, _, _ string) (billingstripe.Invoice, bool, error) {
	s.sequence = append(s.sequence, "find")
	return s.findResult, s.findFound, s.findErr
}

func (s *scriptedStripe) VoidInvoice(_ context.Context, invoiceID, idemKey string) (billingstripe.Invoice, error) {
	s.sequence = append(s.sequence, "void")
	s.voidCalls = append(s.voidCalls, voidCall{invoiceID: invoiceID, idemKey: idemKey})
	if !s.voidDoesNotPersist &&
		s.voidErr == nil &&
		s.voidResult.Status == "void" {
		if s.voided == nil {
			s.voided = map[string]billingstripe.Invoice{}
		}
		s.voided[invoiceID] = s.voidResult
	}
	return s.voidResult, s.voidErr
}

func (s *scriptedStripe) DeleteDraftInvoice(_ context.Context, invoiceID string) (billingstripe.Invoice, error) {
	s.sequence = append(s.sequence, "delete")
	s.deleteCalls = append(s.deleteCalls, deleteCall{invoiceID: invoiceID})
	if s.deleteMakesMissing || (s.deleteErr == nil && s.deleteResult.Deleted) {
		if s.deletedInvoices == nil {
			s.deletedInvoices = map[string]bool{}
		}
		s.deletedInvoices[invoiceID] = true
	}
	return s.deleteResult, s.deleteErr
}

func resourceMissingError() error {
	return &stripego.Error{
		Code:           stripego.ErrorCodeResourceMissing,
		HTTPStatusCode: 404,
		Type:           stripego.ErrorTypeInvalidRequest,
	}
}

func testAttempt(now time.Time, amountMicros int64) Attempt {
	return Attempt{
		ID: uuid.New(), AccountID: uuid.New(),
		AmountMicros: amountMicros, Status: "pending", BalanceAfterMicros: amountMicros,
		IdempotencyKey:  "attempt-idem",
		PaymentMethodID: uuid.New(), StripePaymentMethodID: "pm_frozen",
		StripeCustomerID: "cus_frozen",
		ExpiresAt:        now.Add(PendingGrace), CreatedAt: now,
	}
}

func exactInvoiceItem(attempt Attempt) billingstripe.InvoiceItem {
	return billingstripe.InvoiceItem{
		ID:          "ii_" + attempt.ID.String(),
		AmountCents: microsToCentsRoundHalfUp(attempt.AmountMicros),
		Currency:    "usd",
	}
}

func exactInvoicePayment(attempt Attempt, invoiceID string) billingstripe.InvoicePaymentProof {
	amountCents := microsToCentsRoundHalfUp(attempt.AmountMicros)
	return billingstripe.InvoicePaymentProof{
		ID:                    "inpay_" + attempt.ID.String(),
		InvoiceID:             invoiceID,
		Status:                "paid",
		IsDefault:             true,
		AmountPaid:            amountCents,
		AmountRequested:       amountCents,
		Currency:              "usd",
		PaymentType:           string(stripego.InvoicePaymentPaymentTypePaymentIntent),
		PaymentIntentID:       "pi_" + attempt.ID.String(),
		PaymentIntentStatus:   string(stripego.PaymentIntentStatusSucceeded),
		PaymentIntentCustomer: attempt.StripeCustomerID,
		PaymentMethodID:       attempt.StripePaymentMethodID,
		PaymentIntentAmount:   amountCents,
		AmountReceived:        amountCents,
		PaymentIntentCurrency: "usd",
	}
}

func controlledInvoice(attempt Attempt, invoice billingstripe.Invoice) billingstripe.Invoice {
	invoice.CollectionMethod = string(stripego.InvoiceCollectionMethodChargeAutomatically)
	invoice.AutoAdvance = false
	invoice.DefaultPaymentMethodID = attempt.StripePaymentMethodID
	invoice.ChargeRef = "credit-auto-topup:" + attempt.ID.String()
	invoice.CreditOperation = "auto_topup"
	invoice.CreditAccountID = attempt.AccountID.String()
	invoice.CreditLedgerID = attempt.ID.String()
	return invoice
}

func exactOpenInvoice(attempt Attempt) billingstripe.Invoice {
	amountCents := microsToCentsRoundHalfUp(attempt.AmountMicros)
	return controlledInvoice(attempt, billingstripe.Invoice{
		ID:         attempt.StripeInvoiceID,
		CustomerID: attempt.StripeCustomerID,
		Status:     "open",
		AmountDue:  amountCents,
		Total:      amountCents,
		Currency:   "usd",
	})
}

func exactPaidInvoice(attempt Attempt) billingstripe.Invoice {
	amountCents := microsToCentsRoundHalfUp(attempt.AmountMicros)
	return controlledInvoice(attempt, billingstripe.Invoice{
		ID:         attempt.StripeInvoiceID,
		CustomerID: attempt.StripeCustomerID,
		Status:     "paid",
		AmountDue:  amountCents,
		AmountPaid: amountCents,
		Total:      amountCents,
		Currency:   "usd",
	})
}

func exactTerminalInvoice(attempt Attempt, status string) billingstripe.Invoice {
	amountCents := microsToCentsRoundHalfUp(attempt.AmountMicros)
	return controlledInvoice(attempt, billingstripe.Invoice{
		ID:         attempt.StripeInvoiceID,
		CustomerID: attempt.StripeCustomerID,
		Status:     status,
		Total:      amountCents,
		Currency:   "usd",
	})
}

type concurrentFailureStripe struct {
	*scriptedStripe
	mu      sync.Mutex
	attempt Attempt
	voided  bool
}

func newConcurrentFailureStripe(attempt Attempt) *concurrentFailureStripe {
	return &concurrentFailureStripe{
		scriptedStripe: &scriptedStripe{},
		attempt:        attempt,
	}
}

func (s *concurrentFailureStripe) GetInvoice(
	_ context.Context,
	_ string,
) (billingstripe.Invoice, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.voided {
		return exactTerminalInvoice(s.attempt, "void"), nil
	}
	return exactOpenInvoice(s.attempt), nil
}

func (s *concurrentFailureStripe) ListInvoiceItems(
	_ context.Context,
	_ string,
) ([]billingstripe.InvoiceItem, error) {
	return []billingstripe.InvoiceItem{exactInvoiceItem(s.attempt)}, nil
}

func (s *concurrentFailureStripe) VoidInvoice(
	_ context.Context,
	_, _ string,
) (billingstripe.Invoice, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.voided = true
	return exactTerminalInvoice(s.attempt, "void"), nil
}
