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
// 🔴 NEWEBPAY'S FIRST ROUTE IS SANDBOX-ONLY, AND IT DOES NOT VERIFY EITHER.
// Owner decision D515 (docs/newebpay-sandbox-topology.md) puts the first
// NewebPay notice on POST /newebpay/sandbox/notify, matched through the
// provider-generic route table in notify.go (method+path -> provider+mode,
// convention /{provider}/{mode}/notify, D549) rather than one hard-coded
// path — a route a request matches, never anything in its body. This handler
// does not decrypt TradeInfo, does not check TradeSha, does not hold the
// HashKey/HashIV, and does not look anything up: it validates only that the
// required fields are present and hands the raw notice to a NoticeSink
// (sink.go), which for the real deployment is one SQS SendMessage. If
// NEWEBPAY_NOTICE_QUEUE_URL is unset under Lambda — the infra half,
// mirrorstack-infra#435, not deployed yet — newNoticeSink falls back to an
// unconfiguredSink instead of exiting, and the route answers the same 501
// every unwired path gets. Verification and settlement belong to the notice
// worker downstream (N10, billing-engine#296), not to this ingress. The live
// path, /newebpay/notify, is not in the route table and stays unrouted until
// core-v2#1849 — every request to it still gets the 501 below.
//
// Transport selection:
//   - AWS_LAMBDA_FUNCTION_NAME set → lambda.Start(proxyHandler)
//   - Otherwise → http.ListenAndServe on ACCOUNT_WEBHOOK_PORT (default 8092)
//
// Spec: mirrorstack-docs/api/billing/account-webhook.md.
package main

import (
	"context"
	"encoding/base64"
	"io"
	"log/slog"
	"net/http"
	"os"
	"time"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/google/uuid"

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
		lambda.Start(proxyHandler(newNoticeSink()))
		return
	}

	port := config.Port("ACCOUNT_WEBHOOK_PORT", defaultLocalHTTPPort)
	mux := http.NewServeMux()
	sink := newNoticeSink()
	mux.Handle(webhookPath, httpHandler(sink))
	mux.Handle(sandboxNotifyPath, httpHandler(sink))
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

// newNoticeSink builds the NoticeSink this process uses: the real SQS sink
// under Lambda, once NEWEBPAY_NOTICE_QUEUE_URL names a queue (provisioned by
// the infra half of this tracker, mirrorstack-infra#435); an in-memory sink
// for local runs and tests, which never touch AWS.
//
// 🔴 NEVER os.Exit ON A MISSING QUEUE URL. mirrorstack-infra#435 and this PR
// can land in either order (D549 change request on billing-engine#295): if
// the queue variable is still unset when this Lambda cold-starts, that is an
// ordinary, expected, reversible state — not a reason to crash-loop the
// whole billing-webhook Lambda, which answers every other provider's route
// too. One WARN, and the sandbox notify route degrades to unconfiguredSink,
// which makes it answer 501 like every other not-yet-wired path.
func newNoticeSink() NoticeSink {
	if !config.IsLambda() {
		return &InMemorySink{}
	}
	queueURL := os.Getenv("NEWEBPAY_NOTICE_QUEUE_URL")
	if queueURL == "" {
		slog.Warn("NEWEBPAY_NOTICE_QUEUE_URL not set; sandbox notify route answers 501 until mirrorstack-infra#435 deploys the queue")
		return unconfiguredSink{}
	}
	awsCfg, err := awsconfig.LoadDefaultConfig(context.Background())
	if err != nil {
		slog.Error("account-webhook: load aws config failed", "error", err)
		os.Exit(1)
	}
	return NewSQSSink(awsCfg, queueURL)
}

// requestID returns req's API Gateway request id, falling back to a
// generated one on the (untrusted-input, should-not-happen) chance it is
// empty — every notice needs a request id to correlate send failures against.
func requestID(req events.APIGatewayProxyRequest) string {
	if id := req.RequestContext.RequestID; id != "" {
		return id
	}
	return uuid.NewString()
}

// proxyHandler is the Lambda entrypoint. Uses APIGatewayProxyRequest (REST API
// / v1 proxy shape) because the HTTP API in front of this function is pinned to
// payload format 1.0 — see mirrorstack-infra stacks/billing.go.
//
// 🔴 501, NOT 404, FOR EVERY PATH BUT ONE. A 404 says the address is wrong and
// invites a PSP engineer to go hunting for the right path; 501 says the
// address is right and the feature is not built yet, which is the true
// statement for every path except the sandbox NewebPay notify route added by
// billing-engine#295. It also keeps the URL answering, so an endpoint
// registration made ahead of the integration does not have to be re-created
// later.
func proxyHandler(sink NoticeSink) func(context.Context, events.APIGatewayProxyRequest) (events.APIGatewayProxyResponse, error) {
	return func(ctx context.Context, req events.APIGatewayProxyRequest) (events.APIGatewayProxyResponse, error) {
		if route, ok := matchNoticeRoute(req.HTTPMethod, req.Path); ok {
			body := []byte(req.Body)
			if req.IsBase64Encoded {
				decoded, err := base64.StdEncoding.DecodeString(req.Body)
				if err != nil {
					resp := jsonRouteResponse(http.StatusBadRequest, map[string]string{"error": "malformed body"})
					return events.APIGatewayProxyResponse{
						StatusCode: resp.status,
						Headers:    map[string]string{"Content-Type": "application/json"},
						Body:       string(resp.body),
					}, nil
				}
				body = decoded
			}
			resp := handleNotify(ctx, sink, route, req.Path, body, requestID(req))
			return events.APIGatewayProxyResponse{
				StatusCode: resp.status,
				Headers:    map[string]string{"Content-Type": "application/json"},
				Body:       string(resp.body),
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

func httpHandler(sink NoticeSink) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if route, ok := matchNoticeRoute(r.Method, r.URL.Path); ok {
			// io.LimitReader caps what is actually read off the wire at
			// maxNoticeBodyBytes+1 — an oversize body never gets buffered in
			// full before computeNoticeResponse's own length check turns it
			// into a 413.
			body, _ := io.ReadAll(io.LimitReader(r.Body, maxNoticeBodyBytes+1))
			resp := handleNotify(r.Context(), sink, route, r.URL.Path, body, uuid.NewString())
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(resp.status)
			_, _ = w.Write(resp.body)
			return
		}

		slog.InfoContext(r.Context(), "provider webhook received with no provider wired",
			"path", r.URL.Path, "method", r.Method)
		httputil.JSON(w, http.StatusNotImplemented, map[string]string{"error": noProviderMessage})
	})
}
