package webhook_test

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	stripego "github.com/stripe/stripe-go/v85"

	"github.com/mirrorstack-ai/billing-engine/internal/account/creditledger"
	"github.com/mirrorstack-ai/billing-engine/internal/account/creditrecovery"
	"github.com/mirrorstack-ai/billing-engine/internal/account/webhook"
	"github.com/mirrorstack-ai/billing-engine/internal/account/webhook/webhooktest"
)

type countingCreditRecovery struct {
	paidCalls    atomic.Int32
	failureCalls atomic.Int32
}

func (r *countingCreditRecovery) ReconcileWebhookPaid(
	context.Context,
	string,
) (creditledger.Settlement, error) {
	r.paidCalls.Add(1)
	return creditledger.Settlement{Found: true}, nil
}

func (r *countingCreditRecovery) ReconcileWebhookFailure(
	context.Context,
	string,
	string,
) (creditledger.FailureReconciliation, error) {
	r.failureCalls.Add(1)
	return creditledger.FailureReconciliation{Found: true}, nil
}

func TestCreditRecoveryCapabilityOrdinaryInvoicesNeverProbe(t *testing.T) {
	for _, event := range []struct {
		name string
		make func() stripego.Event
	}{
		{
			name: "ordinary paid",
			make: func() stripego.Event {
				return invoiceEvent(
					"evt_ordinary_paid_capability",
					"invoice.paid",
					"in_ordinary",
					"paid",
					500,
					500,
				)
			},
		},
		{
			name: "ordinary payment failure",
			make: func() stripego.Event {
				return invoiceEvent(
					"evt_ordinary_failed_capability",
					"invoice.payment_failed",
					"in_ordinary",
					"open",
					0,
					500,
				)
			},
		},
	} {
		t.Run(event.name, func(t *testing.T) {
			var probeCalls atomic.Int32
			capability := creditrecovery.NewRuntimeCapability(
				func(context.Context) (bool, error) {
					probeCalls.Add(1)
					return false, nil
				},
			)
			executor := &countingCreditRecovery{}
			guarded := creditrecovery.GuardWebhookReconciler(
				capability,
				executor,
			)
			fixture := event.make()
			verifier := &webhooktest.FakeVerifier{Event: fixture}
			router := newRouter(verifier, webhooktest.NewFakeStore()).
				WithCreditPaidReconciler(guarded).
				WithManualCreditPaidReconciler(guarded).
				WithCreditFailureReconciler(guarded).
				WithManualCreditFailureReconciler(guarded)

			result := router.Process(context.Background(), []byte(`{}`), "sig")

			require.Equal(t, 200, result.HTTPStatus)
			require.Zero(t, probeCalls.Load())
			require.Zero(t, executor.paidCalls.Load())
			require.Zero(t, executor.failureCalls.Load())
		})
	}
}

func TestCanonicalCreditRecoveryCapabilityFailureRetriesBeforeExecutor(t *testing.T) {
	accountID, ledgerID := uuid.New(), uuid.New()
	tests := []struct {
		name      string
		eventType string
		status    string
		paid      int64
		due       int64
	}{
		{
			name:      "paid",
			eventType: "invoice.paid",
			status:    "paid",
			paid:      500,
			due:       500,
		},
		{
			name:      "failure",
			eventType: "invoice.payment_failed",
			status:    "open",
			due:       500,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var probeCalls atomic.Int32
			capability := creditrecovery.NewRuntimeCapability(
				func(context.Context) (bool, error) {
					probeCalls.Add(1)
					return false, nil
				},
			)
			executor := &countingCreditRecovery{}
			guarded := creditrecovery.GuardWebhookReconciler(
				capability,
				executor,
			)
			event := creditInvoiceEvent(
				"evt_canonical_capability_"+tc.name,
				tc.eventType,
				"in_credit_capability",
				tc.status,
				tc.paid,
				tc.due,
				"purchase",
				accountID,
				ledgerID,
			)
			store := webhooktest.NewFakeStore()
			router := newRouter(
				&webhooktest.FakeVerifier{Event: event},
				store,
			).
				WithManualCreditPaidReconciler(guarded).
				WithManualCreditFailureReconciler(guarded)

			result := router.Process(context.Background(), []byte(`{}`), "sig")

			require.Equal(t, 500, result.HTTPStatus)
			require.Equal(t, webhook.StatusInternal, result.Status)
			require.EqualValues(t, 1, probeCalls.Load())
			require.Zero(t, executor.paidCalls.Load())
			require.Zero(t, executor.failureCalls.Load())
			require.False(t, store.Processed[event.ID],
				"5xx must unmark the event so the transport retries")
		})
	}
}
