// Command account-webhook is the Stripe webhook receiver. It accepts
// Lambda invocations (API Gateway REST proxy shape) in production and
// plain HTTP in local development. Both transports decode the request
// body + Stripe-Signature header and feed them to the same
// router.Process — there is one body of logic, two thin transports.
//
// Transport selection:
//   - AWS_LAMBDA_FUNCTION_NAME set → lambda.Start(proxyHandler)
//   - Otherwise → http.ListenAndServe on ACCOUNT_WEBHOOK_PORT (default 8092)
//
// Local iteration with real Stripe events:
//
//	stripe listen --forward-to localhost:8092/webhook
//
// All real logic lives in internal/account/webhook so it can be
// unit-tested without either harness.
//
// Spec: mirrorstack-docs/api/billing/account-webhook.md.
package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"os"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/mirrorstack-ai/billing-engine/internal/account/webhook"
	billingstripe "github.com/mirrorstack-ai/billing-engine/internal/shared/stripe"
)

const (
	stripeSigHeader      = "Stripe-Signature"
	defaultLocalHTTPPort = "8092"
	webhookPath          = "/webhook"

	// Stripe caps webhook payloads at ~256 KB; double it for headroom
	// on the local HTTP path. Defends against pathological dev requests.
	maxWebhookBodyBytes = 512 << 10
)

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stderr, nil)))
	router := buildRouter()

	if isLambda() {
		lambda.Start(proxyHandler(router))
		return
	}

	port := os.Getenv("ACCOUNT_WEBHOOK_PORT")
	if port == "" {
		port = defaultLocalHTTPPort
	}
	mux := http.NewServeMux()
	mux.Handle(webhookPath, httpHandler(router))
	slog.Info("local HTTP mode", "port", port, "path", webhookPath)
	if err := http.ListenAndServe(":"+port, mux); err != nil {
		slog.Error("listener failed", "error", err)
		os.Exit(1)
	}
}

// buildRouter reads env vars and wires the pgxpool + verifier + store
// + router.
func buildRouter() *webhook.Router {
	webhookSecret := os.Getenv("STRIPE_WEBHOOK_SECRET")
	if webhookSecret == "" {
		slog.Error("STRIPE_WEBHOOK_SECRET not set")
		os.Exit(1)
	}
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		slog.Error("DATABASE_URL not set")
		os.Exit(1)
	}

	pool, err := pgxpool.New(context.Background(), dsn)
	if err != nil {
		slog.Error("pgxpool init failed", "error", err)
		os.Exit(1)
	}

	verifier := billingstripe.NewVerifier(webhookSecret)
	store := webhook.NewStore(pool)
	return webhook.NewRouter(verifier, store, slog.Default())
}

// proxyHandler is the Lambda entrypoint. Uses APIGatewayProxyRequest
// (REST API / v1 proxy shape) because api-platform's existing API
// Gateway already deploys behind it.
func proxyHandler(router *webhook.Router) func(context.Context, events.APIGatewayProxyRequest) (events.APIGatewayProxyResponse, error) {
	return func(ctx context.Context, req events.APIGatewayProxyRequest) (events.APIGatewayProxyResponse, error) {
		sig := req.Headers[stripeSigHeader]
		if sig == "" {
			// API Gateway REST APIs lowercase header keys; check both.
			sig = req.Headers["stripe-signature"]
		}

		body, err := decodeBody(req)
		if err != nil {
			slog.WarnContext(ctx, "failed to decode webhook body", "error", err)
			return proxyResponse(http.StatusBadRequest, webhook.StatusInvalidBody), nil
		}

		res := router.Process(ctx, body, sig)
		return proxyResponse(res.HTTPStatus, res.Status), nil
	}
}

// httpHandler is the local HTTP entrypoint. Same shape as proxyHandler
// — read body + signature, call router.Process, write the Result back
// as JSON. net/http canonicalizes header keys, so unlike the proxy
// path no lowercase fallback is needed.
func httpHandler(router *webhook.Router) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxWebhookBodyBytes))
		if err != nil {
			slog.WarnContext(r.Context(), "failed to read webhook body", "error", err)
			writeJSONResponse(w, http.StatusBadRequest, webhook.StatusInvalidBody)
			return
		}

		sig := r.Header.Get(stripeSigHeader)
		res := router.Process(r.Context(), body, sig)
		writeJSONResponse(w, res.HTTPStatus, res.Status)
	}
}

func decodeBody(req events.APIGatewayProxyRequest) ([]byte, error) {
	if req.IsBase64Encoded {
		// Stripe's signature is computed over the raw bytes; base64-
		// encoded proxy events must be decoded before verification.
		return base64.StdEncoding.DecodeString(req.Body)
	}
	return []byte(req.Body), nil
}

func proxyResponse(status int, statusBody webhook.Status) events.APIGatewayProxyResponse {
	body, _ := json.Marshal(map[string]string{"status": string(statusBody)})
	return events.APIGatewayProxyResponse{
		StatusCode: status,
		Headers:    map[string]string{"Content-Type": "application/json"},
		Body:       string(body),
	}
}

func writeJSONResponse(w http.ResponseWriter, status int, statusBody webhook.Status) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]string{"status": string(statusBody)})
}

func isLambda() bool {
	return os.Getenv("AWS_LAMBDA_FUNCTION_NAME") != ""
}
