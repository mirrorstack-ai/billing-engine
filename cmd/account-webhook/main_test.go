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
	stripe := &webhooktest.FakeChargeRetriever{}
	return webhook.NewRouter(verifier, store, stripe, stripe, webhooktest.SilentLogger())
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

func TestHTTPWebhookTransports_SuppressAutoTopUpCardCharge(t *testing.T) {
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

	tests := []struct {
		name    string
		deliver func(*webhook.Router) webhook.Status
	}{
		{
			name: "API Gateway proxy",
			deliver: func(router *webhook.Router) webhook.Status {
				res, err := proxyHandler(router)(context.Background(), events.APIGatewayProxyRequest{
					HTTPMethod: http.MethodPost,
					Headers:    map[string]string{stripeSigHeader: "t=0,v1=stub"},
					Body:       `{}`,
				})
				if err != nil {
					t.Fatalf("proxy handler returned err: %v", err)
				}
				return decodeStatus(t, strings.NewReader(res.Body))
			},
		},
		{
			name: "local HTTP",
			deliver: func(router *webhook.Router) webhook.Status {
				req := httptest.NewRequest(http.MethodPost, webhookPath, strings.NewReader(`{}`))
				req.Header.Set(stripeSigHeader, "t=0,v1=stub")
				rr := httptest.NewRecorder()
				httpHandler(router).ServeHTTP(rr, req)
				return decodeStatus(t, rr.Body)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			probe := webhooktest.NewAutoTopUpChargeProbe()
			// Prove the fixture reaches the card-charge seam when its context is
			// not suppressed, then exercise the real transport boundary.
			probe.NotifyStripeInvoice(context.Background(), "in_control")
			if got := probe.PayInvoiceCalls(); got != 1 {
				t.Fatalf("control PayInvoiceWithMethod calls: got %d, want 1", got)
			}
			probe.Reset()

			router := makeRouter(
				t,
				&webhooktest.FakeVerifier{Event: event},
				webhooktest.NewFakeStore(),
			).WithServingBlockNotifier(probe)

			if got := tt.deliver(router); got != webhook.StatusOK {
				t.Fatalf("status: got %q, want %q", got, webhook.StatusOK)
			}
			if got := probe.EvaluationCalls(); got != 1 {
				t.Fatalf("standing evaluations: got %d, want 1", got)
			}
			if errs := probe.Errors(); len(errs) != 0 {
				t.Fatalf("standing evaluation errors: %v", errs)
			}
			if got := probe.PayInvoiceCalls(); got != 0 {
				t.Fatalf("webhook reached PayInvoiceWithMethod %d times, want 0", got)
			}
		})
	}
}

// --- proxyHandler (Lambda transport — same router contract) ---------------

// A GET must return 200 without ever calling router.Process - the
// verifier here always errors, so a 400/BadSignature result would prove
// the health-check branch didn't short-circuit before it.
func TestProxyHandler_HealthCheck_GetBypassesRouter(t *testing.T) {
	router := makeRouter(t,
		&webhooktest.FakeVerifier{Err: errors.New("signature mismatch")},
		webhooktest.NewFakeStore(),
	)
	handler := proxyHandler(router)

	req := events.APIGatewayProxyRequest{HTTPMethod: http.MethodGet}
	res, err := handler(context.Background(), req)
	if err != nil {
		t.Fatalf("handler returned err: %v", err)
	}
	if res.StatusCode != http.StatusOK {
		t.Errorf("status: got %d, want 200", res.StatusCode)
	}
	if got := decodeStatus(t, strings.NewReader(res.Body)); got != webhook.StatusOK {
		t.Errorf("status body: got %q, want %q", got, webhook.StatusOK)
	}
}

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
