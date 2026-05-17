// Command account-webhook is the Stripe webhook receiver Lambda.
//
// Thin adapter: HTTP body + Stripe-Signature header in, HTTP status
// out. All real logic lives in internal/account/webhook so it can be
// unit-tested without a Lambda or API Gateway harness.
//
// Lambda-only. There is no local HTTP path — local iteration uses
// unit tests; end-to-end testing uses `stripe listen --forward-to`
// against deployed staging.
//
// Spec: mirrorstack-docs/api/billing/account-webhook.md.
package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"log/slog"
	"os"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/mirrorstack-ai/billing-engine/internal/account/webhook"
	billingstripe "github.com/mirrorstack-ai/billing-engine/internal/shared/stripe"
)

const stripeSigHeader = "Stripe-Signature"

var router *webhook.Router

func init() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stderr, nil)))

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
	router = webhook.NewRouter(verifier, store, slog.Default())
}

// proxyHandler is the Lambda entrypoint. Uses APIGatewayProxyRequest
// (REST API / v1 proxy shape) because api-platform's existing API
// Gateway already deploys behind it.
func proxyHandler(ctx context.Context, req events.APIGatewayProxyRequest) (events.APIGatewayProxyResponse, error) {
	sig := req.Headers[stripeSigHeader]
	if sig == "" {
		// API Gateway REST APIs lowercase header keys; check both.
		sig = req.Headers["stripe-signature"]
	}

	body, err := decodeBody(req)
	if err != nil {
		slog.WarnContext(ctx, "failed to decode webhook body", "error", err)
		return jsonResponse(400, webhook.StatusInvalidBody), nil
	}

	res := router.Process(ctx, body, sig)
	return jsonResponse(res.HTTPStatus, res.Status), nil
}

func decodeBody(req events.APIGatewayProxyRequest) ([]byte, error) {
	if req.IsBase64Encoded {
		// Stripe's signature is computed over the raw bytes; base64-
		// encoded proxy events must be decoded before verification.
		return base64.StdEncoding.DecodeString(req.Body)
	}
	return []byte(req.Body), nil
}

func jsonResponse(status int, statusBody webhook.Status) events.APIGatewayProxyResponse {
	body, _ := json.Marshal(map[string]string{"status": string(statusBody)})
	return events.APIGatewayProxyResponse{
		StatusCode: status,
		Headers:    map[string]string{"Content-Type": "application/json"},
		Body:       string(body),
	}
}

func main() {
	if !isLambda() {
		// account-webhook is Lambda-only in v1. Refuse to start outside
		// the Lambda runtime so developers don't accidentally think a
		// stub server is live. Use unit tests for local iteration;
		// `stripe trigger` against deployed staging for end-to-end.
		slog.Error("account-webhook is only runnable inside AWS Lambda; use unit tests for local iteration")
		os.Exit(1)
	}
	lambda.Start(proxyHandler)
}

func isLambda() bool {
	return os.Getenv("AWS_LAMBDA_RUNTIME_API") != "" || os.Getenv("AWS_EXECUTION_ENV") != ""
}
