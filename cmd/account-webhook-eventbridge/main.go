// Command account-webhook-eventbridge is the EventBridge-delivered Stripe
// webhook receiver — the second (dual-run) delivery path alongside
// cmd/account-webhook's HTTPS endpoint. Stripe's EventBridge integration
// publishes onto a partner event bus that only Stripe's AWS account can
// PutEvents to, and only this Lambda's EventBridge Rule can invoke; trust is
// therefore structural, so this binary never verifies an HMAC signature —
// it calls router.ProcessTrusted instead of router.Process.
//
// This binary is EventBridge-only: always lambda.Start, no local-HTTP-server
// branch (there is nothing to replicate locally the way `stripe listen` does
// for the HTTPS path).
//
// dispatch() and every handler in internal/account/webhook/handlers.go are
// unchanged — Stripe's EventBridge `detail` field is the same JSON
// webhook.ConstructEvent already parses, so json.Unmarshal(evt.Detail, &event)
// produces an identical stripego.Event.
//
// See docs-temp/stripe-eventbridge-migration/plan.md for the full migration
// plan (dual-run now; HTTPS path + STRIPE_WEBHOOK_SECRET removed only in a
// later, separate cleanup PR once EventBridge has burned in).
//
// Spec: mirrorstack-docs/api/billing/account-webhook.md.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/google/uuid"
	stripego "github.com/stripe/stripe-go/v85"

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
	billingstripe "github.com/mirrorstack-ai/billing-engine/internal/shared/stripe"
)

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stderr, nil)))
	router := buildRouter()
	lambda.Start(eventBridgeHandler(router))
}

// buildRouter reads env vars and wires the pgxpool + verifier + store +
// router. Mirrors cmd/account-webhook's buildRouter() exactly.
// STRIPE_WEBHOOK_SECRET is OPTIONAL here by construction: ProcessTrusted
// never calls the verifier (EventBridge partner events arrive pre-trusted),
// but Router's constructor requires a non-nil one — an empty secret wires
// stripe.NewVerifier's fail-closed reject-all verifier, which this binary
// never reaches. The HTTPS binary stays deployed for dual-run; removing it
// entirely remains the later cleanup (per the migration plan).
func buildRouter() *webhook.Router {
	webhookSecret := os.Getenv("STRIPE_WEBHOOK_SECRET")
	// The fraud handlers (charge.dispute.created / radar.early_fraud_warning.created)
	// carry only a charge id, so the webhook must retrieve the charge to resolve
	// the disputed card — this binary also loads the Stripe API key (a
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

// eventBridgeHandler is the Lambda entrypoint. It unmarshals the Stripe
// event straight out of evt.Detail (Stripe's EventBridge `detail` envelope
// is the same JSON shape webhook.ConstructEvent already parses in the HTTPS
// path) and calls router.ProcessTrusted — no signature verification, trust
// comes from the transport (see package doc).
//
// A non-nil error is returned ONLY when the result is a genuine 5xx, so
// EventBridge's own per-target retry policy + DLQ kicks in. A malformed /
// undecodable Detail is logged and acked with a nil error: there is no
// reason to let EventBridge retry forever on a payload that will never
// parse.
func eventBridgeHandler(router *webhook.Router) func(context.Context, events.EventBridgeEvent) error {
	return func(ctx context.Context, evt events.EventBridgeEvent) error {
		var event stripego.Event
		if err := json.Unmarshal(evt.Detail, &event); err != nil {
			slog.ErrorContext(ctx, "failed to decode EventBridge detail", "error", err, "eventbridge_id", evt.ID)
			return nil
		}

		res := router.ProcessTrusted(ctx, event)
		if res.HTTPStatus >= 500 {
			slog.ErrorContext(ctx, "webhook processing failed", "event_id", event.ID, "type", event.Type, "http_status", res.HTTPStatus, "status", res.Status)
			return fmt.Errorf("webhook: dispatch failed for event %s (%s)", event.ID, res.Status)
		}
		return nil
	}
}
