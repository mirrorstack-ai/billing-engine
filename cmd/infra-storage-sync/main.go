// Command infra-storage-sync is the scheduled OBJECT-STORAGE sampler — the
// platform-infra (Plane 1) storage metering chokepoint, and the missing half of
// the pair infra-egress-sync already fills for CDN egress.
//
// 🔴 WHY THIS BINARY HAS TO EXIST. infra.storage.gib_hours has been seeded,
// active and PRICED since migration 020 (31.5 µ$/GiB-hour), and NOTHING has
// ever emitted it. A price on a quantity nobody measures is zero: a module that
// stores a terabyte of video and one that stores nothing produce byte-identical
// bills. Measured on twkpa-edu 2026-09-10, video-core's card carried 運算時間
// and nothing else even under 顯示全部 — no storage line, because there is no
// storage event, because there is no collector.
//
// ATTRIBUTION IS FREE, WHICH IS WHY THIS IS A SAMPLER AND NOT A PROJECT. Every
// module's runtime objects live under the canonical prefix
// "apps/<app_id>/<module_id>/" minted by api-platform's storagecreds.Prefix —
// the same fence its IAM session policy enforces, so a key that exists outside
// its module's prefix could not have been written by that module. Summing
// object sizes per prefix therefore yields per-(app, module) bytes with no new
// contract, no SDK change and no cooperation from the module.
//
// 🔴 IT EMITS A LEVEL, NOT A TOTAL. infra.storage.gib_hours is kind
// time_weighted, and the rollup integrates it in SQL:
//
//	value * EXTRACT(EPOCH FROM (segment_end - observation_at)) / 3600.0
//	                                        (queries/rollup.sql:323)
//
// So Value is the GiB STANDING at the observation instant, and billable_quantity
// becomes GiB-hours downstream. Emitting GiB-hours here would integrate an
// already-integrated quantity and overcharge by a factor of the window length.
// Migration 020's "value = GiB-hours" note describes the rolled quantity, not
// the event.
//
// Idempotency by construction, exactly as infra-egress-sync: the event_id is a
// deterministic UUIDv5 over (metric, app_id, module_id, observation_instant), so
// re-sampling an already-recorded instant produces the SAME id and
// RecordInfraUsage's ON CONFLICT(event_id) DO NOTHING dedupes it. That is what
// makes the lookback overlap free.
//
// A sample is taken at each CLOSED hour boundary in the lookback, not at
// time.Now(): a gauge integrated from irregular instants gives a different
// answer depending on when the scheduler happened to fire, and two runs of the
// same period must agree.
//
// SCALE. This lists the bucket under "apps/" and sums sizes. That is O(objects)
// per run, which is right at MirrorStack's current scale and will not stay
// right: the replacement is an S3 Inventory manifest (daily, per-prefix,
// delivered as Parquet), which this binary's shape already anticipates — only
// prefixSizes changes, the emit path does not.
//
// Dual-transport, mirroring cmd/billing-cycle and its two sibling syncs:
//   - AWS_LAMBDA_FUNCTION_NAME set → lambda.Start(handler), driven by an
//     EventBridge Scheduler in production.
//   - Otherwise → a one-shot local run, so dev needs neither Lambda nor a
//     scheduler.
package main

import (
	"context"
	"log/slog"
	"os"
	"time"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/google/uuid"

	"github.com/mirrorstack-ai/billing-engine/internal/account/autotopup"
	"github.com/mirrorstack-ai/billing-engine/internal/account/billing"
	"github.com/mirrorstack-ai/billing-engine/internal/account/credit"
	"github.com/mirrorstack-ai/billing-engine/internal/account/credit/rollout"
	"github.com/mirrorstack-ai/billing-engine/internal/account/standing"
	"github.com/mirrorstack-ai/billing-engine/internal/account/usage"
	"github.com/mirrorstack-ai/billing-engine/internal/shared/config"
)

// storageMetric is the reserved platform-infra metric this sampler records
// under. RecordInfraUsage resolves its kind (time_weighted) and per-unit COGS
// from the platform-owned catalog (migration 020: 31.5 µ$/GiB-hour, seeded as
// 32 after rounding).
const storageMetric = "infra.storage.gib_hours"

// bytesPerGiB converts raw object bytes to GiB (2^30) — the basis every
// platform-infra size metric is priced in (migration 020).
const bytesPerGiB = 1024 * 1024 * 1024

// modulePrefixRoot is the root under which api-platform mints every module's
// runtime storage prefix ("apps/<app_id>/<module_id>/", storagecreds.Prefix).
// Listing from here rather than the bucket root keeps platform-owned objects
// (deploy artifacts, avatars) out of a customer's storage bill.
const modulePrefixRoot = "apps/"

// lookbackHours is how many CLOSED hour boundaries each run samples, ending at
// the top of the trigger hour. > the schedule interval so a missed run catches
// up; the deterministic event_id makes the overlap a no-op.
//
// 🔴 A RESAMPLE IS NOT A RE-MEASURE. Re-running samples the CURRENT bucket and
// stamps it at an OLDER instant, so a catch-up run attributes today's bytes to
// an earlier hour. That is the accepted approximation for a gauge with no
// history (S3 cannot answer "how big was this prefix at 03:00"), and it is why
// the lookback is small: the error is bounded by how much a prefix can change
// within lookbackHours. It is also why an already-recorded instant DEDUPES
// rather than overwrites — the first sample of an instant is the closest one to
// the truth, and a later run must never replace it.
const lookbackHours = 3

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stderr, nil)))
	svc, lister, bucket := buildDeps()

	if config.IsLambda() {
		lambda.Start(handler(svc, lister, bucket))
		return
	}

	res := syncStorage(context.Background(), svc, lister, bucket, time.Now().UTC())
	logResult(context.Background(), "infra-storage-sync local run complete", res)
	if res.Failed {
		os.Exit(1)
	}
}

// buildDeps wires the pgxpool-backed usage.Service (identical to its sibling
// collectors — the credit/rollout wiring is copied rather than re-decided) plus
// an S3 client and the module-storage bucket name. MODULE_STORAGE_BUCKET is
// required: a missing one exits at startup, never mid-run, so a
// misconfiguration can never look like "no storage this hour".
func buildDeps() (*usage.Service, objectLister, string) {
	svc := buildUsageService()
	awsCfg, err := awsconfig.LoadDefaultConfig(context.Background())
	if err != nil {
		slog.Error("infra-storage-sync: load aws config failed", "error", err)
		os.Exit(1)
	}
	return svc, s3.NewFromConfig(awsCfg), config.MustEnv("MODULE_STORAGE_BUCKET")
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
// CloudWatchEvent carries no window (the scheduler fires on a cron), so the
// handler derives the closed-hour lookback from the event time.
func handler(svc *usage.Service, lister objectLister, bucket string) func(context.Context, events.CloudWatchEvent) error {
	return func(ctx context.Context, ev events.CloudWatchEvent) error {
		at := ev.Time
		if at.IsZero() {
			at = time.Now().UTC()
		}
		res := syncStorage(ctx, svc, lister, bucket, at.UTC())
		logResult(ctx, "infra-storage-sync lambda run complete", res)
		// A listing error fails the run so EventBridge retries and alarms see
		// it; per-row record errors are logged and counted but never abort the
		// sweep — one bad prefix must not drop every other module's storage.
		if res.Failed {
			return res.Err
		}
		return nil
	}
}

func logResult(ctx context.Context, msg string, res syncResult) {
	slog.InfoContext(ctx, msg,
		"samples", res.Samples, "prefixes", res.Prefixes, "recorded", res.Recorded,
		"deduped", res.Deduped, "skipped", res.Skipped, "row_errors", res.RowErrors,
		"failed", res.Failed)
}

var _ = uuid.Nil // owner principal is deliberately omitted; see syncStorage
