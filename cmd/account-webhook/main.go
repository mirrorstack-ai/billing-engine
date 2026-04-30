// Command account-webhook is the Stripe webhook receiver Lambda for
// the billing-engine account surface (subscriptions and invoices).
//
// It is a thin adapter: HTTP body + Stripe-Signature header in,
// HTTP status out. All real logic lives in
// internal/account/webhook so it is unit-testable without a Lambda or
// API Gateway harness.
package main

import (
	"context"
	"encoding/base64"
	"log/slog"
	"os"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/mirrorstack-ai/billing-engine/internal/account/webhook"
)

// stripeSigHeader is the canonical name. API Gateway lower-cases
// header names in the proxy event, so we look up both forms.
const stripeSigHeader = "Stripe-Signature"

var handler *webhook.Handler

func init() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stderr, nil)))

	secret := os.Getenv("STRIPE_WEBHOOK_SECRET")
	if secret == "" {
		// Fail fast at cold-start rather than silently accepting every
		// request as bad-signature. Catches misconfigured deploys.
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
		slog.Error("failed to create pgx pool", "error", err)
		os.Exit(1)
	}

	handler = webhook.NewHandler(secret, pool)
}

// proxyHandler is the Lambda entrypoint. We use APIGatewayProxyRequest
// (REST API / v1 proxy shape) because that is what api-platform
// already deploys behind; switching to v2 (HTTP API) would be a
// platform-level migration, not a billing-engine choice.
func proxyHandler(ctx context.Context, req events.APIGatewayProxyRequest) (events.APIGatewayProxyResponse, error) {
	sig := req.Headers[stripeSigHeader]
	if sig == "" {
		// API Gateway REST APIs lowercase header keys; check both.
		sig = req.Headers["stripe-signature"]
	}

	body, err := decodeBody(req)
	if err != nil {
		slog.WarnContext(ctx, "failed to decode webhook body", "error", err)
		return jsonResponse(400, "invalid body"), nil
	}

	res := handler.Process(ctx, body, sig)
	return jsonResponse(res.StatusCode, res.Body), nil
}

func decodeBody(req events.APIGatewayProxyRequest) ([]byte, error) {
	if req.IsBase64Encoded {
		// Stripe's signature is over the raw bytes; base64-encoded
		// proxy events must be decoded before verification.
		return base64.StdEncoding.DecodeString(req.Body)
	}
	return []byte(req.Body), nil
}

func jsonResponse(status int, msg string) events.APIGatewayProxyResponse {
	return events.APIGatewayProxyResponse{
		StatusCode: status,
		Headers:    map[string]string{"Content-Type": "application/json"},
		Body:       `{"status":"` + msg + `"}`,
	}
}

func main() {
	if !isLambda() {
		// Local dev path: refuse to start without a runtime so
		// developers don't accidentally think a stub server is live.
		// Real local testing should drive Process() via tests or
		// an explicit `stripe trigger` against a deployed staging.
		slog.Error("account-webhook is only runnable inside AWS Lambda; use unit tests for local iteration")
		os.Exit(1)
	}
	lambda.Start(proxyHandler)
}

func isLambda() bool {
	return os.Getenv("AWS_LAMBDA_RUNTIME_API") != "" || os.Getenv("AWS_EXECUTION_ENV") != ""
}
