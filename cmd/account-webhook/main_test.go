package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/aws/aws-lambda-go/events"
	stripego "github.com/stripe/stripe-go/v85"

	"github.com/mirrorstack-ai/billing-engine/internal/account/webhook"
)

// stubVerifier implements billingstripe.Verifier for transport-layer
// tests. Tests configure event (returned when err is nil) and err
// (returned otherwise).
type stubVerifier struct {
	event stripego.Event
	err   error
}

func (s *stubVerifier) Verify(_ []byte, _ string) (stripego.Event, error) {
	if s.err != nil {
		return stripego.Event{}, s.err
	}
	return s.event, nil
}

// stubStore implements webhook.Store with the minimum needed to drive
// transport-layer tests through router.Process. firstTime controls the
// idempotency-record outcome; the per-event handlers (TouchAccount, ...)
// return successful zero values so unhandled-event tests don't trip.
type stubStore struct {
	firstTime bool
}

func (s *stubStore) MarkEventProcessed(_ context.Context, _, _ string) (bool, error) {
	return s.firstTime, nil
}

func (s *stubStore) TouchAccountByStripeCustomer(_ context.Context, _ string) (bool, error) {
	return true, nil
}

func (s *stubStore) SetDefaultPaymentMethod(_ context.Context, _, _ string) error {
	return nil
}

func (s *stubStore) InsertPaymentMethod(_ context.Context, _ string, _ webhook.InsertPaymentMethodParams) (bool, error) {
	return true, nil
}

func (s *stubStore) SoftDeletePaymentMethod(_ context.Context, _ string) (bool, error) {
	return true, nil
}

func makeRouter(t *testing.T, verifier *stubVerifier, store *stubStore) *webhook.Router {
	t.Helper()
	return webhook.NewRouter(verifier, store, slog.New(slog.NewTextHandler(io.Discard, nil)))
}

// --- httpHandler ----------------------------------------------------------

func TestHTTPHandler_BadSignature(t *testing.T) {
	router := makeRouter(t,
		&stubVerifier{err: errors.New("signature mismatch")},
		&stubStore{firstTime: true},
	)
	handler := httpHandler(router)

	req := httptest.NewRequest(http.MethodPost, webhookPath,
		strings.NewReader(`{"id":"evt_1","type":"customer.created"}`))
	req.Header.Set(stripeSigHeader, "t=0,v1=garbage")
	rr := httptest.NewRecorder()

	handler.ServeHTTP(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Errorf("status: got %d, want 400", rr.Code)
	}
	if got := decodeStatus(t, rr.Body); got != webhook.StatusBadSignature {
		t.Errorf("status body: got %q, want %q", got, webhook.StatusBadSignature)
	}
}

func TestHTTPHandler_UnhandledEvent(t *testing.T) {
	router := makeRouter(t,
		&stubVerifier{event: stripego.Event{ID: "evt_unhandled", Type: "ping"}},
		&stubStore{firstTime: true},
	)
	handler := httpHandler(router)

	req := httptest.NewRequest(http.MethodPost, webhookPath,
		strings.NewReader(`{"id":"evt_unhandled","type":"ping"}`))
	req.Header.Set(stripeSigHeader, "t=0,v1=stub")
	rr := httptest.NewRecorder()

	handler.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Errorf("status: got %d, want 200", rr.Code)
	}
	if got := decodeStatus(t, rr.Body); got != webhook.StatusUnhandled {
		t.Errorf("status body: got %q, want %q", got, webhook.StatusUnhandled)
	}
}

func TestHTTPHandler_Duplicate(t *testing.T) {
	router := makeRouter(t,
		&stubVerifier{event: stripego.Event{ID: "evt_dup", Type: stripego.EventTypeCustomerCreated}},
		&stubStore{firstTime: false},
	)
	handler := httpHandler(router)

	req := httptest.NewRequest(http.MethodPost, webhookPath,
		strings.NewReader(`{"id":"evt_dup"}`))
	req.Header.Set(stripeSigHeader, "t=0,v1=stub")
	rr := httptest.NewRecorder()

	handler.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Errorf("status: got %d, want 200", rr.Code)
	}
	if got := decodeStatus(t, rr.Body); got != webhook.StatusDuplicate {
		t.Errorf("status body: got %q, want %q", got, webhook.StatusDuplicate)
	}
}

// --- proxyHandler (Lambda transport — same router contract) ---------------

func TestProxyHandler_BadSignature(t *testing.T) {
	router := makeRouter(t,
		&stubVerifier{err: errors.New("signature mismatch")},
		&stubStore{firstTime: true},
	)
	handler := proxyHandler(router)

	req := events.APIGatewayProxyRequest{
		Headers: map[string]string{stripeSigHeader: "t=0,v1=garbage"},
		Body:    `{"id":"evt_1"}`,
	}
	res, err := handler(context.Background(), req)
	if err != nil {
		t.Fatalf("handler returned err: %v", err)
	}
	if res.StatusCode != http.StatusBadRequest {
		t.Errorf("status: got %d, want 400", res.StatusCode)
	}
	if got := decodeStatus(t, strings.NewReader(res.Body)); got != webhook.StatusBadSignature {
		t.Errorf("status body: got %q, want %q", got, webhook.StatusBadSignature)
	}
}

func TestProxyHandler_LowercaseHeaderFallback(t *testing.T) {
	// API Gateway REST APIs sometimes deliver headers lowercased; the
	// handler probes both forms.
	router := makeRouter(t,
		&stubVerifier{event: stripego.Event{ID: "evt_lc", Type: "ping"}},
		&stubStore{firstTime: true},
	)
	handler := proxyHandler(router)

	req := events.APIGatewayProxyRequest{
		Headers: map[string]string{"stripe-signature": "t=0,v1=stub"},
		Body:    `{"id":"evt_lc"}`,
	}
	res, err := handler(context.Background(), req)
	if err != nil {
		t.Fatalf("handler returned err: %v", err)
	}
	if res.StatusCode != http.StatusOK {
		t.Errorf("status: got %d, want 200", res.StatusCode)
	}
}

// --- helpers --------------------------------------------------------------

func decodeStatus(t *testing.T, r io.Reader) webhook.Status {
	t.Helper()
	body, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	var out struct {
		Status webhook.Status `json:"status"`
	}
	if err := json.Unmarshal(bytes.TrimSpace(body), &out); err != nil {
		t.Fatalf("unmarshal body %q: %v", body, err)
	}
	return out.Status
}
