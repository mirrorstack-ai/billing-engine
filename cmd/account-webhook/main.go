// Command account-webhook is the Stripe webhook receiver Lambda.
//
// v1 bootstrap: signature-verifies the incoming webhook against
// STRIPE_WEBHOOK_SECRET and returns 200. The real event-routing
// logic (subscription updates, invoice paid/failed, etc.) lands in
// subsequent PRs once the matching handlers under
// internal/account/webhook/ are wired.
//
// Lambda-only. There is no local dev path — use unit tests for
// signature-verify iteration; use `stripe trigger` against a
// deployed staging for end-to-end runs.
package main

import (
	"context"
	"encoding/base64"
	"log/slog"
	"os"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/stripe/stripe-go/v85/webhook"
)

// stripeSigHeader is the canonical name. API Gateway lower-cases
// header names in the proxy event, so we look up both forms.
const stripeSigHeader = "Stripe-Signature"

var webhookSecret string

func init() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stderr, nil)))

	webhookSecret = os.Getenv("STRIPE_WEBHOOK_SECRET")
	if webhookSecret == "" {
		// Fail fast at cold-start rather than silently accepting every
		// request as bad-signature. Catches misconfigured deploys.
		slog.Error("STRIPE_WEBHOOK_SECRET not set")
		os.Exit(1)
	}
}

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

	event, err := webhook.ConstructEvent(body, sig, webhookSecret)
	if err != nil {
		slog.WarnContext(ctx, "signature verify failed", "error", err)
		return jsonResponse(400, "bad signature"), nil
	}

	// Real event routing lands in a subsequent PR. For now we
	// acknowledge receipt so Stripe's webhook view shows green.
	slog.InfoContext(ctx, "webhook received", "event_id", event.ID, "type", event.Type)
	return jsonResponse(200, "ok"), nil
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
		slog.Error("account-webhook is only runnable inside AWS Lambda; use unit tests for local iteration")
		os.Exit(1)
	}
	lambda.Start(proxyHandler)
}

func isLambda() bool {
	return os.Getenv("AWS_LAMBDA_RUNTIME_API") != "" || os.Getenv("AWS_EXECUTION_ENV") != ""
}
