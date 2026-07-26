package main

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/aws/aws-lambda-go/events"
	"github.com/stretchr/testify/require"
	stripego "github.com/stripe/stripe-go/v85"

	"github.com/mirrorstack-ai/billing-engine/internal/account/webhook"
	"github.com/mirrorstack-ai/billing-engine/internal/account/webhook/webhooktest"
)

// makeRouter mirrors cmd/account-webhook/main_test.go's helper: the
// verifier is intentionally a FakeVerifier that always errors, since a
// correctly-wired eventBridgeHandler must never call it (ProcessTrusted
// skips verification entirely — see router.go).
func makeRouter(t *testing.T, store *webhooktest.FakeStore, charges *webhooktest.FakeChargeRetriever) *webhook.Router {
	t.Helper()
	v := &webhooktest.FakeVerifier{Err: errors.New("eventbridge handler must never call the verifier")}
	if charges == nil {
		charges = &webhooktest.FakeChargeRetriever{}
	}
	return webhook.NewRouter(v, store, charges, charges, webhooktest.SilentLogger())
}

func customerCreatedDetail(t *testing.T, eventID, customerID string) []byte {
	t.Helper()
	event := stripego.Event{
		ID:   eventID,
		Type: stripego.EventTypeCustomerCreated,
		Data: &stripego.EventData{Raw: json.RawMessage(`{"id":"` + customerID + `"}`)},
	}
	raw, err := json.Marshal(event)
	require.NoError(t, err)
	return raw
}

func TestEventBridgeHandler_WellFormedEvent_Dispatches(t *testing.T) {
	store := webhooktest.NewFakeStore()
	router := makeRouter(t, store, nil)
	handler := eventBridgeHandler(router)

	evt := events.EventBridgeEvent{
		ID:         "eb-evt-1",
		DetailType: "customer.created",
		Source:     "aws.partner/stripe.com/ed_123",
		Detail:     customerCreatedDetail(t, "evt_ok_1", "cus_x"),
	}

	err := handler(context.Background(), evt)

	require.NoError(t, err)
	require.True(t, store.Processed["evt_ok_1"], "a well-formed EventBridge event must reach dispatch")
}

func TestEventBridgeHandler_MalformedDetail_ReturnsNilNoRetry(t *testing.T) {
	store := webhooktest.NewFakeStore()
	router := makeRouter(t, store, nil)
	handler := eventBridgeHandler(router)

	evt := events.EventBridgeEvent{
		ID:     "eb-evt-bad",
		Detail: []byte(`{not-json`),
	}

	err := handler(context.Background(), evt)

	require.NoError(t, err, "an undecodable Detail must be logged and acked, not retried forever")
	require.Empty(t, store.Processed, "a malformed payload must never reach dispatch")
}

func TestEventBridgeHandler_5xxResult_SurfacesError(t *testing.T) {
	store := webhooktest.NewFakeStore()
	store.ErrMark = errors.New("db down") // forces processVerifiedEvent to return 500/Internal
	router := makeRouter(t, store, nil)
	handler := eventBridgeHandler(router)

	evt := events.EventBridgeEvent{
		ID:     "eb-evt-500",
		Detail: customerCreatedDetail(t, "evt_500_1", "cus_x"),
	}

	err := handler(context.Background(), evt)

	require.Error(t, err, "a genuine 5xx must surface as a non-nil error so EventBridge retries/DLQs")
}

func TestEventBridgeWebhookDelivery_SuppressesAutoTopUpCardCharge(t *testing.T) {
	probe := webhooktest.NewAutoTopUpChargeProbe()
	// Prove the fixture reaches the card-charge seam when its context is not
	// suppressed, then exercise the real EventBridge transport boundary.
	probe.NotifyStripeInvoice(context.Background(), "in_control")
	require.Equal(t, 1, probe.PayInvoiceCalls())
	probe.Reset()

	event := stripego.Event{
		ID:   "evt_no_topup",
		Type: stripego.EventTypeInvoicePaid,
		Data: &stripego.EventData{Raw: json.RawMessage(`{
			"id":"in_no_topup",
			"status":"paid",
			"amount_paid":500,
			"amount_due":500
		}`)},
	}
	detail, err := json.Marshal(event)
	require.NoError(t, err)

	router := makeRouter(t, webhooktest.NewFakeStore(), nil).
		WithServingBlockNotifier(probe)
	err = eventBridgeHandler(router)(context.Background(), events.EventBridgeEvent{
		ID:     "eb-no-topup",
		Detail: detail,
	})

	require.NoError(t, err)
	require.Equal(t, 1, probe.EvaluationCalls(), "webhook should still evaluate standing")
	require.Empty(t, probe.Errors())
	require.Zero(t, probe.PayInvoiceCalls(), "webhook must never reach PayInvoiceWithMethod")
}
