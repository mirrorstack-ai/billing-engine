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
// installed module's tables, each physically named m<32 hex of the module
// uuid>_<table> (api-platform modulePhysicalPrefix; app-module-sdk
// ids.NormalizeModuleID). The prefix is derived from module_install.module_id,
// never read from module_install.prefix: that column is a "<username>_<slug>_"
// display value no deployed table carries. Summing pg_total_relation_size per
// table and assigning it to the install whose id the name carries yields
// per-(app, module) bytes with no SDK change and no cooperation from the module.
// Tables no install owns (members, module_install, the leftovers of an
// uninstalled module) are the platform's, never a customer's. Module-scope
// mod_<id> schemas are shared by every app and have no app to bill: out of scope
// here (see the PR).
//
// A MODULE THAT VANISHES IS ZEROED. The rollup carries the last level to the
// period end, so a pair sampled last hour and absent now (uninstalled module,
// dropped app) gets an explicit 0, found from its own previous samples in
// ms_billing.usage_events (levelHistory).
//
// A GREEN RUN MEANS IT MEASURED. It fails on: no app schema at all, no
// readable install, installs but no attributed byte, every row failing, any real
// row error, an unreadable history, any app whose installs could not be read, an
// app whose tables carry no installed module's m<id>_ prefix, and a read that
// would zero the fleet (installs empty fleet-wide, or half of 10+ prior pairs
// gone at once; refused before anything is recorded). A high unattributed share
// logs an alarm.
//
// READ-ONLY, FIXED QUERIES, NO SECRETS. The sampler connects as a SELECT-only
// role (DBSIZE_DATABASE_URL, a different identity from the service role that
// writes usage events) and runs exactly two statements (pgreader.go): the
// catalog size query and one SELECT module_id FROM app_<id>.module_install per
// app, each under statement_timeout and lock_timeout. Logs carry counts and ids,
// never table names.
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
	"github.com/jackc/pgx/v5/pgxpool"

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
	svc, rd, hist := buildDeps()

	if config.IsLambda() {
		lambda.Start(handler(svc, rd, hist))
		return
	}

	res := syncDB(context.Background(), svc, rd, hist, time.Now().UTC())
	logResult(context.Background(), "infra-db-sync local run complete", res)
	if res.Failed {
		os.Exit(1)
	}
}

// buildDeps wires the usage service (the write path, identical to its sibling
// collectors), the read-only reader, and the billing-side level history that
// lets the sampler zero a vanished module. DBSIZE_DATABASE_URL is required: a
// missing one exits at startup, never mid-run, so a misconfiguration can never
// look like "no databases this hour".
func buildDeps() (*usage.Service, dbReader, levelHistory) {
	svc, billingPool := buildUsageService()
	return svc, pgReader{pool: config.MustPgxPoolFromEnv("DBSIZE_DATABASE_URL")}, billingHistory{pool: billingPool}
}

func buildUsageService() (*usage.Service, *pgxpool.Pool) {
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
	return wireUsageService(pool, policy), pool
}

// wireUsageService builds the write path for a policy. Split from
// buildUsageService so a test can drive the enforce wiring without a database.
//
// 🔴 A USAGE-ONLY SAMPLER NEVER BUILDS A PAYMENT EXECUTOR. In enforce mode the
// coordinator keeps the wallet estimate and the out-of-credits gate, but no
// auto-top-up trigger is attached: Coordinator.maybeTriggerAutoTopUp returns at
// once without one. The trigger needed autotopup.NewStandardExecutor and
// config.MustEnv("STRIPE_SECRET_KEY"), a key infra#377 removed from this
// binary's environment, so on an enforce stage the Lambda would exit at startup.
// Settling a top-up belongs to the services that hold the Stripe credential.
func wireUsageService(pool *pgxpool.Pool, policy rollout.Policy) *usage.Service {
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
func handler(svc *usage.Service, rd dbReader, hist levelHistory) func(context.Context, events.CloudWatchEvent) error {
	return func(ctx context.Context, ev events.CloudWatchEvent) error {
		at := ev.Time
		if at.IsZero() {
			at = time.Now().UTC()
		}
		res := syncDB(ctx, svc, rd, hist, at.UTC())
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
		"installs", res.Installs, "modules", res.Modules, "zeroed", res.Zeroed, "recorded", res.Recorded, "deduped", res.Deduped,
		"skipped", res.Skipped, "row_errors", res.RowErrors,
		"attributed_bytes", res.AttributedBytes, "unattributed_bytes", res.UnattributedBytes, "failed", res.Failed)
}
