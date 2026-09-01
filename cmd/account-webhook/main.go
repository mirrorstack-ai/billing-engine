// Command account-webhook is the HTTP ingress for payment-provider webhooks
// that CANNOT deliver over EventBridge.
//
// 🔴 STRIPE NO LONGER ARRIVES HERE, AND THIS BINARY NO LONGER VERIFIES IT.
// Stripe delivers onto the EventBridge partner bus, consumed by
// cmd/account-webhook-eventbridge: only Stripe's AWS account may PutEvents to
// that bus and only that Lambda's Rule may consume it, so trust is structural
// and no HMAC is checked anywhere. The Stripe verifier, the Stripe-Signature
// header and STRIPE_WEBHOOK_SECRET are gone with the dual-run. This completes
// the cleanup docs-temp/stripe-eventbridge-migration/plan.md deferred.
//
// 🔴 THE ENDPOINT IS KEPT DELIBERATELY, AND DELIBERATELY EMPTY. A distributor
// outside Stripe's supported countries settles through a local PSP — NewebPay
// (藍新) in Taiwan is the first — and no such provider can publish to an AWS
// partner event bus. An HTTP POST is the only ingress they have. Deleting this
// Lambda would also retire its API Gateway id and URL, and that URL is exactly
// what a PSP registration is pinned to, so the surface is preserved and the
// dispatch table is simply empty until the first provider lands.
//
// 🔴 THE FIRST PROVIDER MUST BRING ITS OWN VERIFICATION — THE STRIPE SEAM DOES
// NOT GENERALIZE. webhook.Verifier is Stripe-shaped at both ends: it takes a
// body plus a Stripe-Signature header and returns a stripego.Event, and every
// handler behind it consumes stripego types. NewebPay posts an AES-256-CBC
// TradeInfo blob authenticated by a SHA-256 TradeSha over a SHARED
// HashKey/HashIV — symmetric, no signature header, and the decrypted payload is
// not a stripego.Event. Reaching for webhook.Router here would mean inventing a
// fake Stripe event, which is how a provider's semantics get silently mapped
// onto another provider's assumptions. It needs its own verifier, its own event
// type and its own handlers.
//
// internal/account/webhook is UNTOUCHED by this change: the EventBridge binary
// is its production caller, and Router.Process (the HMAC path) remains the seam
// its ~30 behavioural tests drive the handlers through.
//
// Transport selection:
//   - AWS_LAMBDA_FUNCTION_NAME set → lambda.Start(proxyHandler)
//   - Otherwise → http.ListenAndServe on ACCOUNT_WEBHOOK_PORT (default 8092)
//
// Spec: mirrorstack-docs/api/billing/account-webhook.md.
package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"

	"github.com/mirrorstack-ai/billing-engine/internal/shared/config"
	"github.com/mirrorstack-ai/billing-engine/internal/shared/httputil"
)

const (
	defaultLocalHTTPPort = "8092"
	webhookPath          = "/webhook"
)

// noProviderMessage is the body every request gets while the dispatch table is
// empty. It names the reason rather than a bare status so a PSP engineer
// registering the URL sees why their delivery bounced.
const noProviderMessage = "no payment provider is wired to this endpoint"

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stderr, nil)))

	if config.IsLambda() {
		lambda.Start(proxyHandler())
		return
	}

	port := config.Port("ACCOUNT_WEBHOOK_PORT", defaultLocalHTTPPort)
	mux := http.NewServeMux()
	mux.Handle(webhookPath, httpHandler())
	slog.Info("local HTTP mode", "port", port, "path", webhookPath)
	if err := http.ListenAndServe(":"+port, mux); err != nil {
		slog.Error("listener failed", "error", err)
		os.Exit(1)
	}
}

// proxyHandler is the Lambda entrypoint. Uses APIGatewayProxyRequest (REST API
// / v1 proxy shape) because the HTTP API in front of this function is pinned to
// payload format 1.0 — see mirrorstack-infra stacks/billing.go.
//
// 🔴 501, NOT 404. A 404 says the address is wrong and invites a PSP engineer
// to go hunting for the right path; 501 says the address is right and the
// feature is not built yet, which is the true statement. It also keeps the URL
// answering, so an endpoint registration made ahead of the integration does not
// have to be re-created later.
func proxyHandler() func(context.Context, events.APIGatewayProxyRequest) (events.APIGatewayProxyResponse, error) {
	return func(ctx context.Context, req events.APIGatewayProxyRequest) (events.APIGatewayProxyResponse, error) {
		slog.InfoContext(ctx, "provider webhook received with no provider wired",
			"path", req.Path, "method", req.HTTPMethod)
		return events.APIGatewayProxyResponse{
			StatusCode: http.StatusNotImplemented,
			Headers:    map[string]string{"Content-Type": "application/json"},
			Body:       `{"error":"` + noProviderMessage + `"}`,
		}, nil
	}
}

func httpHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		slog.InfoContext(r.Context(), "provider webhook received with no provider wired",
			"path", r.URL.Path, "method", r.Method)
		httputil.JSON(w, http.StatusNotImplemented, map[string]string{"error": noProviderMessage})
	})
}
