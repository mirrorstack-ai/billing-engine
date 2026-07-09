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
func makeRouter(t *testing.T, store *webhooktest.FakeStore, charges webhook.ChargeRetriever) *webhook.Router {
	t.Helper()
	v := &webhooktest.FakeVerifier{Err: errors.New("eventbridge handler must never call the verifier")}
	if charges == nil {
		charges = &webhooktest.FakeChargeRetriever{}
	}
	return webhook.NewRouter(v, store, charges, webhooktest.SilentLogger())
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
