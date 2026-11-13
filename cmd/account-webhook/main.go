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
// 🔴 GET/HEAD AGAINST THIS PATH ARE HEALTH PROBES, NOT DELIVERIES, AND ANSWER
// 200. #180 left every request — probe and delivery alike — answering 501,
// which API Gateway and the Cloudflare health check in front of it both count
// as a 5xx failure. No PSP delivers a webhook over GET or HEAD, so isProbe
// claims both methods for health without any risk of mistaking a real
// delivery for one. See proxyHandler and httpHandler. #189.
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
	"time"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"

	"github.com/mirrorstack-ai/billing-engine/internal/shared/config"
	"github.com/mirrorstack-ai/billing-engine/internal/shared/httputil"
)

const (
	defaultLocalHTTPPort = "8092"
	webhookPath          = "/webhook"
)

// noProviderMessage is the body every delivery gets while the dispatch table
// is empty. It names the reason rather than a bare status so a PSP engineer
// registering the URL sees why their delivery bounced.
const noProviderMessage = "no payment provider is wired to this endpoint"

// healthMessage is the body a GET/HEAD probe gets. 9x/minute something polls
// this exact URL expecting 2xx (mirrorstack-infra cloudflare/adopted.tf:583),
// and API Gateway counts the 501 above as a 5xx — so a probe cannot share the
// delivery branch's status code even though it shares the path. #189.
const healthMessage = "account-webhook endpoint reachable; no payment provider is wired yet"

// isProbe reports whether a request is a health check rather than a provider
// delivery. PSPs deliver webhooks with POST (NewebPay included, once wired);
// GET and HEAD are not a delivery shape any provider uses against this path,
// so both are free to claim for health without risking a real delivery being
// mistaken for one.
func isProbe(method string) bool {
	return method == http.MethodGet || method == http.MethodHead
}

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
	// An explicit Server rather than ListenAndServe: the package-level helper
	// sets no timeouts at all, so a client that opens a connection and never
	// finishes its headers holds a goroutine indefinitely. This path is
	// local-development only — production runs on Lambda — but a dev server
	// with no read deadline is still the wrong default to leave lying around.
	srv := &http.Server{
		Addr:              ":" + port,
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second,
	}
	if err := srv.ListenAndServe(); err != nil {
		slog.Error("listener failed", "error", err)
		os.Exit(1)
	}
}

// proxyHandler is the Lambda entrypoint. Uses APIGatewayProxyRequest (REST API
// / v1 proxy shape) because the HTTP API in front of this function is pinned to
// payload format 1.0 — see mirrorstack-infra stacks/billing.go.
//
// 🔴 501, NOT 404, FOR A DELIVERY. A 404 says the address is wrong and invites
// a PSP engineer to go hunting for the right path; 501 says the address is
// right and the feature is not built yet, which is the true statement. It
// also keeps the URL answering, so an endpoint registration made ahead of the
// integration does not have to be re-created later.
//
// 🔴 BUT NOT FOR A PROBE. A GET/HEAD is never a provider delivery — see
// isProbe — and API Gateway counts 501 as a 5xx, which trips the Cloudflare
// health check and the billing CodeDeploy gate alike. #189 is that bug; the
// fix is a branch on method, not a change to what a delivery gets back.
func proxyHandler() func(context.Context, events.APIGatewayProxyRequest) (events.APIGatewayProxyResponse, error) {
	return func(ctx context.Context, req events.APIGatewayProxyRequest) (events.APIGatewayProxyResponse, error) {
		if isProbe(req.HTTPMethod) {
			slog.InfoContext(ctx, "health probe", "path", req.Path, "method", req.HTTPMethod)
			body := `{"status":"ok","message":"` + healthMessage + `"}`
			if req.HTTPMethod == http.MethodHead {
				body = ""
			}
			return events.APIGatewayProxyResponse{
				StatusCode: http.StatusOK,
				Headers:    map[string]string{"Content-Type": "application/json"},
				Body:       body,
			}, nil
		}
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
		if isProbe(r.Method) {
			slog.InfoContext(r.Context(), "health probe", "path", r.URL.Path, "method", r.Method)
			httputil.JSON(w, http.StatusOK, map[string]string{"status": "ok", "message": healthMessage})
			return
		}
		slog.InfoContext(r.Context(), "provider webhook received with no provider wired",
			"path", r.URL.Path, "method", r.Method)
		httputil.JSON(w, http.StatusNotImplemented, map[string]string{"error": noProviderMessage})
	})
}
