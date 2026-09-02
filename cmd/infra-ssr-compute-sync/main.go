// Command infra-ssr-compute-sync is the scheduled AWS Lambda/CloudWatch
// PULLER for app-hosting SSR compute metering — the platform-infra (Plane 1)
// chokepoint for `infra.compute.ssr.gb_seconds` / `infra.compute.ssr.request.count`.
//
// Each SSR app runs its own dedicated Lambda ("ms-apphost-<app_id>-<env>"),
// invoked directly by cdn-worker via IAM SigV4 lambda:InvokeFunction — no API
// Gateway, no dispatch layer, no existing metering chokepoint in the path at
// all. This binary closes that gap by PULLING usage back out of AWS itself:
// it enumerates the ms-apphost-* fleet via lambda:ListFunctions, queries
// cloudwatch:GetMetricData for each function's Duration/Invocations sums over
// the last few FULLY-CLOSED hour windows, and for each (app, env, window)
// calls billing-engine's RecordInfraUsage. Direction is billing-engine → AWS
// (an outbound pull, read-only) — mirrors cmd/infra-egress-sync's
// Cloudflare-pull shape exactly, substituting AWS CloudWatch/Lambda for
// Cloudflare's GraphQL Analytics API.
//
// Idempotency by construction: the event_id is a DETERMINISTIC hash of
// (metric, app_id, env, window_start) — see ssrEventID — so re-querying an
// already-ingested window produces the SAME ids and RecordInfraUsage's ON
// CONFLICT(event_id) DO NOTHING dedupes the re-run. Only CLOSED windows,
// aged past a CloudWatch propagation-lag margin, are ever queried (never the
// current partial hour, and never a window that closed less than
// propagationLag ago) — see windows.go.
//
// Dual-transport (same logic, two harnesses) — mirrors cmd/infra-egress-sync
// and cmd/billing-cycle:
//   - AWS_LAMBDA_FUNCTION_NAME set → lambda.Start(handler), driven by an
//     EventBridge Scheduler in production (a CloudWatchEvent). The schedule
//     is created State=DISABLED initially (see design doc §3 Decision B,
//     §7 Open Question 1b — an explicit product-owner decision on the
//     shared NewBillingStack's nightly teardown is still pending).
//   - Otherwise → a one-shot local run (make dev-ssr-compute-sync / go run
//     .), so dev never needs Lambda or a scheduler.
//
// AWS auth: region + credentials resolve through the SDK's default chain
// (the Lambda execution role in production) — unlike the Cloudflare puller,
// there is no separate AWS API token/secret to hold. The legacy path requires
// DATABASE_URL (+ optional DB_AUTH); selected credit-wallet enforcement also
// requires REDIS_URL and STRIPE_SECRET_KEY for authoritative projection and
// automatic top-up execution.
//
// Spec: docs-temp/app-hosting/ssr-metering-design.md.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/google/uuid"

	"github.com/mirrorstack-ai/billing-engine/internal/account/autotopup"
	"github.com/mirrorstack-ai/billing-engine/internal/account/billing"
	"github.com/mirrorstack-ai/billing-engine/internal/account/credit"
	"github.com/mirrorstack-ai/billing-engine/internal/account/credit/rollout"
	"github.com/mirrorstack-ai/billing-engine/internal/account/creditledger"
	"github.com/mirrorstack-ai/billing-engine/internal/account/standing"
	"github.com/mirrorstack-ai/billing-engine/internal/account/usage"
	"github.com/mirrorstack-ai/billing-engine/internal/shared/awslambdainv"
	"github.com/mirrorstack-ai/billing-engine/internal/shared/config"
	billingstripe "github.com/mirrorstack-ai/billing-engine/internal/shared/stripe"
)

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stderr, nil)))
	svc, lister, querier, idle := buildDeps()

	if config.IsLambda() {
		lambda.Start(handler(svc, lister, querier, idle))
		return
	}

	// Local one-shot run: sweep the closed windows ending at the current
	// hour (minus the propagation-lag margin), then exit.
	res := syncSSR(context.Background(), svc, lister, querier, idle, time.Now().UTC())
	logResult(context.Background(), "infra-ssr-compute-sync local run complete", res)
	if res.Failed {
		os.Exit(1)
	}
}

// buildDeps wires the pgxpool-backed usage.Service, the AWS Lambda/CloudWatch
// client (internal/shared/awslambdainv), and the idle-prefilter's direct
// pgxpool lookup, from config/the ambient AWS SDK credential chain.
func buildDeps() (*usage.Service, lambdaLister, metricsQuerier, idleChecker) {
	pool := config.MustPgxPool()
	candidate := rollout.FromEnv(rollout.ComponentWorker, true)
	schemaReady := false
	if candidate.Active() {
		ready, err := config.CreditRuntimeSchemaReady(context.Background(), pool)
		if err != nil {
			slog.Error("credit runtime schema probe failed", "error", err)
			os.Exit(1)
		}
		schemaReady = ready
	}
	policy := rollout.FromEnv(rollout.ComponentWorker, schemaReady)
	controller := rollout.NewController(policy, rollout.NewReporter(os.Stdout))
	walletEnabled := policy.Active()
	creditAccess := func(accountID uuid.UUID) bool {
		return controller.Decide(accountID).Enforced()
	}

	usageStore := usage.NewStore(pool)
	if walletEnabled {
		usageStore = usage.NewStoreWithCreditAccess(pool, creditAccess)
	}
	svc := usage.NewService(usageStore)
	if walletEnabled {
		standingStore := billing.NewStore(pool)
		shadowProjection := usage.NewService(usage.NewStoreWithCreditAccess(
			pool,
			rollout.ReadOnlySelectedAccess(controller),
		))
		shadow := rollout.NewCreditShadowEvaluator(
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

		var coordinator *credit.Coordinator
		if controller.Mode() == rollout.ModeEnforce {
			counter, err := credit.NewCounter(os.Getenv("REDIS_URL"))
			if err != nil {
				slog.Error("credit estimate cache unavailable; live projection fallback remains active", "error", err)
			}
			coordinator = credit.NewCoordinator(counter, standingStore, svc, nil)
			stripeKey := config.MustEnv("STRIPE_SECRET_KEY")
			autoTopUpExecutor := autotopup.NewExecutor(
				autotopup.NewStore(pool),
				creditledger.NewStore(pool),
				billingstripe.NewAutoTopUpClient(stripeKey),
			).WithSettlementObserver(coordinator)
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

			status := billing.NewService(standingStore, nil, "").
				WithCreditWallet(true).
				WithCreditAccess(creditAccess).
				WithCreditCoordinator(rollout.NewGate(controller, shadow, coordinator), coordinator)
			notifier := standing.NewNotifierFromEnvWithStatus(pool, status, slog.Default())
			if notifier.Enabled() {
				coordinator.WithNotifier(notifier)
			}
		}
		svc.WithCreditEvaluator(rollout.NewUsageEvaluator(
			controller,
			rollout.ReadOnlyUsageEvaluatorFunc(func(ctx context.Context, event credit.UsageEvent) error {
				_, err := shadow.EvaluateCreditReadOnly(ctx, event.AccountID)
				return err
			}),
			coordinator,
		))
	}

	awsCfg, err := awsconfig.LoadDefaultConfig(context.Background())
	if err != nil {
		slog.Error("ssr-compute-sync: load aws config failed", "error", err)
		os.Exit(1)
	}
	client := awslambdainv.NewClient(awsCfg)

	return svc, client, client, newPgxIdleChecker(pool)
}

// handler is the Lambda entrypoint for an EventBridge-scheduled invocation.
// The CloudWatchEvent carries no window (the scheduler fires on a cron), so
// the handler derives the closed-hour lookback from the event time.
func handler(svc *usage.Service, lister lambdaLister, querier metricsQuerier, idle idleChecker) func(context.Context, events.CloudWatchEvent) error {
	return func(ctx context.Context, ev events.CloudWatchEvent) error {
		at := ev.Time
		if at.IsZero() {
			at = time.Now().UTC()
		}
		res := syncSSR(ctx, svc, lister, querier, idle, at.UTC())
		logResult(ctx, "infra-ssr-compute-sync lambda run complete", res)
		// Enumeration failure fails the run (surfaces for EventBridge
		// retry/alerting); per-row errors are logged + counted but never abort
		// the sweep or fail the invocation (a single bad row is not worth
		// redoing the whole run for).
		if res.Failed {
			return res.Err
		}
		// A GetMetricData batch failure is NON-FATAL to the sweep itself
		// (syncSSR isolates it and keeps processing remaining batches), but it
		// DOES leave that batch's windows unrecorded this run. Returning a
		// retryable error here — even though res.Failed is false — lets
		// EventBridge Scheduler's own retry policy (short in-hour backoff)
		// give the failed batch more chances before its data ages out of the
		// lookback window, rather than waiting a full hour for the next
		// scheduled run. Safe to retry the whole invocation: every recorded
		// event is idempotent (ON CONFLICT DO NOTHING on the deterministic
		// event_id), so redundantly reprocessing already-succeeded batches on
		// retry is a no-op cost, not a correctness risk.
		if res.BatchErrors > 0 {
			return fmt.Errorf("infra-ssr-compute-sync: %d GetMetricData batch(es) failed this run (retryable)", res.BatchErrors)
		}
		return nil
	}
}

// logResult emits a single structured summary line for the sweep.
func logResult(ctx context.Context, msg string, res ssrSyncResult) {
	slog.InfoContext(ctx, msg,
		"functions", res.Functions, "skipped_idle", res.SkippedIdle,
		"batches", res.Batches, "batch_errors", res.BatchErrors,
		"not_ready", res.NotReady, "windows", res.Windows,
		"recorded", res.Recorded, "deduped", res.Deduped,
		"row_errors", res.RowErrors, "failed", res.Failed)
}
