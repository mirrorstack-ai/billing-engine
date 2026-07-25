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
	"github.com/google/uuid"

	"github.com/mirrorstack-ai/billing-engine/internal/account/autotopup"
	"github.com/mirrorstack-ai/billing-engine/internal/account/billing"
	"github.com/mirrorstack-ai/billing-engine/internal/account/credit"
	"github.com/mirrorstack-ai/billing-engine/internal/account/credit/rollout"
	"github.com/mirrorstack-ai/billing-engine/internal/account/creditledger"
	"github.com/mirrorstack-ai/billing-engine/internal/account/creditpurchase"
	"github.com/mirrorstack-ai/billing-engine/internal/account/creditrecovery"
	"github.com/mirrorstack-ai/billing-engine/internal/account/standing"
	"github.com/mirrorstack-ai/billing-engine/internal/account/usage"
	"github.com/mirrorstack-ai/billing-engine/internal/account/webhook"
	"github.com/mirrorstack-ai/billing-engine/internal/shared/config"
	"github.com/mirrorstack-ai/billing-engine/internal/shared/httputil"
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

	if config.IsLambda() {
		lambda.Start(proxyHandler(router))
		return
	}

	port := config.Port("ACCOUNT_WEBHOOK_PORT", defaultLocalHTTPPort)
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
	// STRIPE_WEBHOOK_SECRET is OPTIONAL: EventBridge is the trusted
	// delivery path, so an unfilled secret slot must not crash-loop this
	// binary (which feeds the deploy canary's error alarm). Empty →
	// stripe.NewVerifier returns the fail-closed reject-all verifier and
	// every signed HTTP delivery is rejected until the secret is set.
	webhookSecret := os.Getenv("STRIPE_WEBHOOK_SECRET")
	if webhookSecret == "" {
		slog.Warn("STRIPE_WEBHOOK_SECRET not set — HTTP webhook path is fail-closed (EventBridge is the trusted delivery path)")
	}
	// The fraud handlers (charge.dispute.created / radar.early_fraud_warning.created)
	// carry only a charge id, so the webhook must retrieve the charge to resolve
	// the disputed card — this binary now also loads the Stripe API key (a
	// restricted rk_* needs charges:read and customers:write). Still inside
	// the billing-engine trust boundary (CLAUDE.md).
	stripeKey := config.MustEnv("STRIPE_SECRET_KEY")
	pool := config.MustPgxPool()

	verifier := billingstripe.NewVerifier(webhookSecret)
	store := webhook.NewStore(pool)
	charges := billingstripe.NewClient(stripeKey)
	autoTopUpExecutor := autotopup.NewExecutor(
		autotopup.NewStore(pool),
		creditledger.NewStore(pool),
		billingstripe.NewAutoTopUpClient(stripeKey),
	)
	manualPurchaseExecutor := creditpurchase.NewExecutor(
		creditpurchase.NewStore(pool),
		creditledger.NewStore(pool),
		billingstripe.NewCreditPurchaseClient(stripeKey),
	)
	recoveryCapability := creditrecovery.NewRuntimeCapability(
		func(ctx context.Context) (bool, error) {
			return config.CreditRecoverySchemaReady(ctx, pool)
		},
	)
	guardedAutoTopUpRecovery := creditrecovery.GuardWebhookReconciler(
		recoveryCapability,
		autoTopUpExecutor,
	)
	guardedManualPurchaseRecovery := creditrecovery.GuardWebhookReconciler(
		recoveryCapability,
		manualPurchaseExecutor,
	)
	candidate := rollout.FromEnv(rollout.ComponentAPI, true)
	schemaReady := false
	if candidate.Active() {
		ready, err := config.CreditRuntimeSchemaReady(context.Background(), pool)
		if err != nil {
			slog.Error("credit runtime schema probe failed", "error", err)
			os.Exit(1)
		}
		schemaReady = ready
	}
	policy := rollout.FromEnv(rollout.ComponentAPI, schemaReady)
	controller := rollout.NewController(policy, rollout.NewReporter(os.Stdout))
	walletEnabled := policy.Active()
	creditAccess := func(accountID uuid.UUID) bool {
		return controller.Decide(accountID).Enforced()
	}

	standingStore := billing.NewStore(pool)
	status := billing.NewService(standingStore, nil, "").
		WithCreditWallet(walletEnabled).
		WithCreditAccess(creditAccess)

	var (
		shadow      *rollout.CreditShadowEvaluator
		coordinator *credit.Coordinator
	)
	if walletEnabled {
		projection := usage.NewService(usage.NewStoreWithCreditAccess(
			pool,
			creditAccess,
		))
		shadowProjection := usage.NewService(usage.NewStoreWithCreditAccess(
			pool,
			rollout.ReadOnlySelectedAccess(controller),
		))
		shadow = rollout.NewCreditShadowEvaluator(
			rollout.SnapshotProviderFunc(func(ctx context.Context, accountID uuid.UUID) (rollout.CreditSnapshot, error) {
				snapshot, err := standingStore.CreditGateSnapshot(ctx, accountID)
				if err != nil {
					return rollout.CreditSnapshot{}, err
				}
				return rollout.CreditSnapshot{
					OwnerUserID:            snapshot.OwnerUserID,
					OwnerOrgID:             snapshot.OwnerOrgID,
					BillingMode:            snapshot.BillingMode,
					SettledBalanceMicros:   snapshot.SettledBalanceMicros,
					SpendableBalanceMicros: snapshot.SpendableBalanceMicros,
					CreditLimitMicros:      snapshot.CreditLimitMicros,
					PendingAutoTopUp:       snapshot.PendingAutoTopUp,
				}, nil
			}),
			shadowProjection,
		)
		if controller.Mode() == rollout.ModeEnforce {
			counter, err := credit.NewCounter(os.Getenv("REDIS_URL"))
			if err != nil {
				slog.Error("credit estimate cache unavailable; live projection fallback remains active", "error", err)
			}
			coordinator = credit.NewCoordinator(counter, standingStore, projection, nil)
			autoTopUpExecutor.WithSettlementObserver(coordinator)
			coordinator.WithAutoTopUpTrigger(credit.AutoTopUpTriggerFunc(
				func(ctx context.Context, accountID uuid.UUID, projectedChargeMicros int64) (credit.AutoTopUpTriggerResult, error) {
					result, err := autoTopUpExecutor.Trigger(ctx, accountID, projectedChargeMicros)
					return credit.AutoTopUpTriggerResult{
						Attempted:  result.Triggered,
						NewAttempt: result.NewAttempt,
						Terminal:   result.Status == "settled" || result.Status == "failed",
					}, err
				},
			))
		}
		status.WithCreditCoordinator(rollout.NewGate(controller, shadow, coordinator), coordinator)
	}
	// Serving-block notifier (funding-gates C6): pushes standing verdicts to
	// api-platform after standing-relevant events. Disabled (log-and-skip)
	// when APPLICATIONS_INTERNAL_URL / INTERNAL_SECRET are unset.
	notifier := standing.NewNotifierFromEnvWithStatus(pool, status, slog.Default())
	router := webhook.NewRouter(verifier, store, charges, charges, slog.Default()).
		WithServingBlockNotifier(notifier).
		WithCreditPaidReconciler(guardedAutoTopUpRecovery).
		WithManualCreditPaidReconciler(guardedManualPurchaseRecovery).
		WithCreditFailureReconciler(guardedAutoTopUpRecovery).
		WithManualCreditFailureReconciler(guardedManualPurchaseRecovery)
	if coordinator != nil {
		router.WithCreditSettlementObserver(
			rollout.NewSettlementObserver(controller, coordinator),
		)
	}
	return router
}

// proxyHandler is the Lambda entrypoint. Uses APIGatewayProxyRequest
// (REST API / v1 proxy shape) because api-platform's existing API
// Gateway already deploys behind it.
func proxyHandler(router *webhook.Router) func(context.Context, events.APIGatewayProxyRequest) (events.APIGatewayProxyResponse, error) {
	return func(ctx context.Context, req events.APIGatewayProxyRequest) (events.APIGatewayProxyResponse, error) {
		// Health-check branch: Stripe always sends POST with a signature,
		// so a GET here is unambiguously a health probe (Cloudflare or
		// otherwise), never a real webhook delivery. Returns before ever
		// calling router.Process - a GET/no-signature request there
		// resolves to a 500, which CodeDeploy's canary error-rate alarm
		// counts as a genuine Lambda error and can block deploys of this
		// whole stack (reproduced live: a health check polling this path
		// every 60s tripped WebhookCanaryErrorRate to 100%, blocking an
		// unrelated deploy). This branch changes nothing about real
		// webhook processing.
		if req.HTTPMethod == http.MethodGet {
			return proxyResponse(http.StatusOK, webhook.StatusOK), nil
		}

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
	body, _ := json.Marshal(webhook.StatusEnvelope{Status: statusBody})
	return events.APIGatewayProxyResponse{
		StatusCode: status,
		Headers:    map[string]string{"Content-Type": "application/json"},
		Body:       string(body),
	}
}

func writeJSONResponse(w http.ResponseWriter, status int, statusBody webhook.Status) {
	httputil.JSON(w, status, webhook.StatusEnvelope{Status: statusBody})
}
