// Command infra-db-sync is the scheduled DATABASE-SIZE sampler: the platform-infra
// (Plane 1) meter for Postgres storage, the sibling of infra-storage-sync (S3).
//
// 🔴 WHY IT EXISTS. Every module absorbs platform infra at price 0
// (ms.AbsorbInfra()), and until this binary nothing metered database bytes at
// all: the tables that only grow (credit_ledger, watch_events, quiz_attempts,
// ad_clicks, audit and session tables) were paid by the platform and appeared on
// no bill (core-v2#1757). infra.db.gib_hours is the backstop for any history
// table that escapes both a per-entry fee and retention.
//
// ATTRIBUTION. An app's database is ONE schema, app_<app_id>, holding every
// installed module's tables, each named "<prefix><table>" with the
// "<username>_<slug>_" prefix recorded in app_<app_id>.module_install. Summing
// pg_total_relation_size per table and assigning it to the install whose prefix
// is the longest match yields per-(app, module) bytes with no SDK change and no
// cooperation from the module. Tables no install owns (members, module_install,
// the leftovers of an uninstalled module) are the platform's, never a customer's.
// Module-scope mod_<id> schemas are shared by every app and have no app to
// bill: out of scope here (see the PR).
//
// READ-ONLY, FIXED QUERIES, NO SECRETS. The sampler connects as a SELECT-only
// role (DBSIZE_DATABASE_URL, a different identity from the service role that
// writes usage events) and runs exactly two statements (pgreader.go): the
// catalog size query and one SELECT module_id, prefix FROM app_<id>.module_install
// per app. Logs carry counts and ids, never table names or prefixes.
//
// IT EMITS A LEVEL, NOT A TOTAL. infra.db.gib_hours is time_weighted: Value is
// the GiB standing at the observation instant and the rollup integrates it, so
// emitting GiB-hours here would integrate twice (same contract as
// infra-storage-sync). Samples sit at CLOSED hour boundaries, the event_id is a
// deterministic UUIDv5 over (metric, app, module, instant), and a repeat sample
// dedupes rather than overwrites.
//
// Dual-transport, mirroring its siblings:
//   - AWS_LAMBDA_FUNCTION_NAME set → lambda.Start(handler), driven by an
//     EventBridge Scheduler in production.
//   - Otherwise → a one-shot local run.
package main

import (
	"context"
	"log/slog"
	"os"
	"time"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/google/uuid"

	"github.com/mirrorstack-ai/billing-engine/internal/account/autotopup"
	"github.com/mirrorstack-ai/billing-engine/internal/account/billing"
	"github.com/mirrorstack-ai/billing-engine/internal/account/credit"
	"github.com/mirrorstack-ai/billing-engine/internal/account/credit/rollout"
	"github.com/mirrorstack-ai/billing-engine/internal/account/standing"
	"github.com/mirrorstack-ai/billing-engine/internal/account/usage"
	"github.com/mirrorstack-ai/billing-engine/internal/shared/config"
)

// dbMetric is the reserved platform-infra metric this sampler records under.
// RecordInfraUsage resolves its kind (time_weighted) from the registry in
// internal/account/usage/infra.go and its price from the catalog (migration 089).
const dbMetric = "infra.db.gib_hours"

// bytesPerGiB converts bytes to GiB (2^30), the basis every platform-infra size
// metric is priced in.
const bytesPerGiB = 1024 * 1024 * 1024

// lookbackHours is how many CLOSED hour boundaries each run samples, ending at
// the top of the trigger hour. Greater than the schedule interval so a missed
// run catches up; the deterministic event_id makes the overlap a no-op. A
// resample is not a re-measure: it stamps the CURRENT level at an older instant,
// the accepted approximation for a gauge with no history, and the reason the
// lookback is small.
const lookbackHours = 3

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stderr, nil)))
	svc, rd := buildDeps()

	if config.IsLambda() {
		lambda.Start(handler(svc, rd))
		return
	}

	res := syncDB(context.Background(), svc, rd, time.Now().UTC())
	logResult(context.Background(), "infra-db-sync local run complete", res)
	if res.Failed {
		os.Exit(1)
	}
}

// buildDeps wires the usage service (the write path, identical to its sibling
// collectors) and the read-only reader. DBSIZE_DATABASE_URL is required: a
// missing one exits at startup, never mid-run, so a misconfiguration can never
// look like "no databases this hour".
func buildDeps() (*usage.Service, dbReader) {
	svc := buildUsageService()
	return svc, pgReader{pool: config.MustPgxPoolFromEnv("DBSIZE_DATABASE_URL")}
}

func buildUsageService() *usage.Service {
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
			autoTopUpExecutor := autotopup.NewStandardExecutor(pool, stripeKey).WithSettlementObserver(coordinator)
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
	return svc
}

// handler is the Lambda entrypoint for an EventBridge-scheduled invocation. The
// CloudWatchEvent carries no window, so the handler derives the closed-hour
// lookback from the event time.
func handler(svc *usage.Service, rd dbReader) func(context.Context, events.CloudWatchEvent) error {
	return func(ctx context.Context, ev events.CloudWatchEvent) error {
		at := ev.Time
		if at.IsZero() {
			at = time.Now().UTC()
		}
		res := syncDB(ctx, svc, rd, at.UTC())
		logResult(ctx, "infra-db-sync lambda run complete", res)
		// A read failure fails the run so EventBridge retries and the alarm sees
		// it; per-row record errors are logged and counted but never abort the
		// sweep: one bad module must not drop every other module's size.
		if res.Failed {
			return res.Err
		}
		return nil
	}
}

func logResult(ctx context.Context, msg string, res syncResult) {
	slog.InfoContext(ctx, msg,
		"samples", res.Samples, "apps", res.Apps, "app_errors", res.AppErrors,
		"modules", res.Modules, "recorded", res.Recorded, "deduped", res.Deduped,
		"skipped", res.Skipped, "row_errors", res.RowErrors,
		"unattributed_bytes", res.UnattributedBytes, "failed", res.Failed)
}
