package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/aws/aws-lambda-go/events"
)

// 🔴 THE POINT OF THESE TESTS IS THAT NOTHING IS DISPATCHED. Stripe moved to
// the EventBridge partner bus and this binary kept only its transports, held
// open for a PSP that cannot publish to that bus (NewebPay in Taiwan). If a
// future change wires a provider in, these tests must be REPLACED with that
// provider's own verification coverage — not deleted for being in the way.

func TestHTTPHandler_AnswersNotImplemented(t *testing.T) {
	rr := httptest.NewRecorder()
	httpHandler().ServeHTTP(
		rr,
		httptest.NewRequest(http.MethodPost, webhookPath, strings.NewReader(`{"any":"payload"}`)),
	)

	if rr.Code != http.StatusNotImplemented {
		t.Fatalf("status: got %d, want %d", rr.Code, http.StatusNotImplemented)
	}
	var body map[string]string
	if err := json.Unmarshal(rr.Body.Bytes(), &body); err != nil {
		t.Fatalf("body is not JSON: %v", err)
	}
	if body["error"] != noProviderMessage {
		t.Errorf("error: got %q, want %q", body["error"], noProviderMessage)
	}
}

func TestProxyHandler_AnswersNotImplemented(t *testing.T) {
	resp, err := proxyHandler()(context.Background(), events.APIGatewayProxyRequest{
		Path:       webhookPath,
		HTTPMethod: http.MethodPost,
		Body:       `{"any":"payload"}`,
	})
	if err != nil {
		t.Fatalf("proxyHandler returned an error: %v", err)
	}
	if resp.StatusCode != http.StatusNotImplemented {
		t.Fatalf("status: got %d, want %d", resp.StatusCode, http.StatusNotImplemented)
	}
	if got := resp.Headers["Content-Type"]; got != "application/json" {
		t.Errorf("content-type: got %q, want application/json", got)
	}
	if !strings.Contains(resp.Body, noProviderMessage) {
		t.Errorf("body %q does not explain why", resp.Body)
	}
}

// 🔴 A SIGNED STRIPE DELIVERY MUST NOT BE HONOURED HERE EITHER. The leaked
// whsec_ in web-ui-kit's history was inert only because the deployed secret
// slot was empty — a fail-closed default, not a decision. This asserts the
// decision: even a request carrying a Stripe-Signature gets 501, because there
// is no verifier and no dispatch left to reach.
func TestStripeSignedDeliveryIsNotHonoured(t *testing.T) {
	req := httptest.NewRequest(http.MethodPost, webhookPath,
		strings.NewReader(`{"id":"evt_1","type":"invoice.paid"}`))
	req.Header.Set("Stripe-Signature", "t=1,v1=whatever")
	rr := httptest.NewRecorder()

	httpHandler().ServeHTTP(rr, req)

	if rr.Code != http.StatusNotImplemented {
		t.Fatalf("status: got %d, want %d — a signed Stripe delivery reached logic", rr.Code, http.StatusNotImplemented)
	}
}
