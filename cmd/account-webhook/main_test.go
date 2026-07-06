package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/aws/aws-lambda-go/events"
	stripego "github.com/stripe/stripe-go/v85"

	"github.com/mirrorstack-ai/billing-engine/internal/account/webhook"
	"github.com/mirrorstack-ai/billing-engine/internal/account/webhook/webhooktest"
)

func makeRouter(t *testing.T, verifier *webhooktest.FakeVerifier, store *webhooktest.FakeStore) *webhook.Router {
	t.Helper()
	return webhook.NewRouter(verifier, store, &webhooktest.FakeChargeRetriever{}, webhooktest.SilentLogger())
}

// --- httpHandler ----------------------------------------------------------

func TestHTTPHandler_BadSignature(t *testing.T) {
	router := makeRouter(t,
		&webhooktest.FakeVerifier{Err: errors.New("signature mismatch")},
		webhooktest.NewFakeStore(),
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
		&webhooktest.FakeVerifier{Event: stripego.Event{ID: "evt_unhandled", Type: "ping"}},
		webhooktest.NewFakeStore(),
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
	store := webhooktest.NewFakeStore()
	store.Processed["evt_dup"] = true // pre-populated → MarkEventProcessed reports duplicate

	router := makeRouter(t,
		&webhooktest.FakeVerifier{Event: stripego.Event{ID: "evt_dup", Type: stripego.EventTypeCustomerCreated}},
		store,
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
		&webhooktest.FakeVerifier{Err: errors.New("signature mismatch")},
		webhooktest.NewFakeStore(),
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
		&webhooktest.FakeVerifier{Event: stripego.Event{ID: "evt_lc", Type: "ping"}},
		webhooktest.NewFakeStore(),
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
