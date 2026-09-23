// Command infra-egress-sync is the scheduled CDN-egress PULLER for Milestone D
// PR #10c — the platform-infra (Plane 1) egress metering chokepoint.
//
// Egress is extreme-volume and the CDN edge runs OUTSIDE the VPC holding no
// platform secret, so the cdn-worker does NOT call billing per request. Instead
// the Rust worker accumulates bytes into a Cloudflare Analytics Engine dataset
// ("cdn_egress_prod", see egressDataset below) via writeDataPoint, and THIS binary periodically PULLS the
// aggregated totals back: it holds a READ-ONLY Cloudflare API token, queries the
// CF GraphQL Analytics API for FULLY-CLOSED hour windows, and for each
// (app_id, module_id, window) calls billing-engine's RecordInfraUsage.
// Direction is billing-engine → Cloudflare (outbound pull); Cloudflare never
// calls back (design §3a / §5 PR #10c).
//
// SSR-origin vs static-file egress (migration 046): the dataset carries BOTH
// static-file egress (cdn-worker's static paths, blob2 = "" or a real
// module_id) and SSR-origin egress (cdn-worker's meter_ssr_egress, cdn-worker
// PR #16, blob2 = the literal string "ssr") — the two are otherwise
// shape-identical rows in the same dataset. A row's blob2 value alone
// distinguishes them: blob2=="ssr" records under the NEW
// infra.compute.ssr.egress.bytes metric (a real, non-trivial COGS — the SSR
// Lambda's response leaves AWS's network to reach cdn-worker on Cloudflare's
// network); every other row — static-file egress — records under
// infra.egress.cdn.bytes, priced per GiB by migration 078 (owner ruling
// 2026-09-13: CDN egress is charged at the catalog rate). Before 078 those
// rows went to the retired, price-0 infra.egress.bytes as raw bytes. The
// GROUPING/QUERY logic is unchanged; only the metric name branches on blob2,
// and BOTH branches now convert bytes to GiB (see egressMetricAndValue).
//
// Idempotency by construction: the event_id is a DETERMINISTIC hash of
// (metric, app_id, module_id, window_start), so re-querying an already-ingested
// window produces the SAME ids and RecordInfraUsage's ON CONFLICT(event_id) DO
// NOTHING dedupes the re-run — no double-count. Only CLOSED windows are queried
// (never the current partial hour, whose SUM would still be growing).
//
// Dual-transport (same logic, two harnesses) — mirrors cmd/billing-cycle:
//   - AWS_LAMBDA_FUNCTION_NAME set → lambda.Start(handler), driven by an
//     EventBridge Scheduler in production (a CloudWatchEvent).
//   - Otherwise → a one-shot local run (make dev-egress-sync / go run .), so dev
//     never needs Lambda or a scheduler.
//
// Lookback: each run sweeps the last `lookbackHours` CLOSED hour buckets ending
// at the top of the hour containing the trigger time. The lookback (> the
// schedule interval) bounds catch-up after a missed run; the deterministic
// event_id makes the overlap free (already-recorded windows dedupe).
//
// Spec: docs-temp/milestone-d-meter/design.md §3a / §5 (cdn-worker + module
// runtime) / §6 build order PR #10c.
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
	"github.com/mirrorstack-ai/billing-engine/internal/shared/cloudflare"
	"github.com/mirrorstack-ai/billing-engine/internal/shared/config"
)

// egressDataset is the Cloudflare Analytics Engine dataset name the cdn-worker
// writes to and this puller reads from. It MUST match the worker's
// wrangler.toml [[analytics_engine_datasets]] dataset binding exactly — it did
// NOT (cdn-worker PR #8 stage-suffixed the dataset to "cdn_egress_prod", this
// constant was never updated to follow), so every run of this puller since
// that rename queried a dataset with zero rows: CF's GraphQL API returns an
// empty result set for a dataset-name miss with HTTP 200, no error, so this
// was never surfaced. Fixed here — verify against the live wrangler.toml
// before ever renaming either side again.
const egressDataset = "cdn_egress_prod"

// egressMetric is the RETIRED reserved metric static-file egress recorded
// under before migration 078 (raw bytes, priced 0 since 019). Nothing emits it
// any more; it is kept so the historical event-id scheme stays documented and
// so a reader of the catalog knows why the row still exists.
const egressMetric = "infra.egress.bytes"

// cdnEgressMetric is the reserved platform-infra metric the puller records
// static-file egress under (blob2 empty or a real module_id) since migration
// 078: priced per GiB at the catalog rate, so the producer converts bytes to
// GiB exactly as the SSR branch does. RecordInfraUsage resolves its kind (sum)
// from the platform-owned registry.
const cdnEgressMetric = "infra.egress.cdn.bytes"

// ssrEgressMetric is the reserved platform-infra metric the puller records
// SSR-origin egress under — rows whose blob2 (module_id) dimension is exactly
// the literal string "ssr" (cdn-worker's meter_ssr_egress, cdn-worker PR #16).
// Priced per migration 046, distinct from and NOT co-mingled with the static
// egressMetric above.
const ssrEgressMetric = "infra.compute.ssr.egress.bytes"

// ssrModuleIDSentinel is the exact blob2 value cdn-worker's meter_ssr_egress
// writes for an SSR-origin egress datapoint. It is the ONLY signal that
// distinguishes an SSR row from a static-file row in the shape-identical
// cdn_egress dataset — the shared contract with cdn-worker PR #16.
const ssrModuleIDSentinel = "ssr"

// cdnRequestMetric and cdnR2ReadMetric are the two counts the second pull
// per window records (migration 080, billing-engine#212) from cdn-worker#58's
// double2 (one request) and blob4 (tier): every metered request for a deploy,
// and the subset the R2 tier served (one class-B read each). Both are priced
// per 1k, so the producer value is count / requestsPerUnit (rule 5).
const cdnRequestMetric = "infra.cdn.request.count"
const cdnR2ReadMetric = "infra.cdn.r2.read.count"

// requestsPerUnit is the billing unit of both request counts: 1,000.
const requestsPerUnit = 1000

// r2HitTier is cdn-worker's blob4 value for a request served from R2
// (EgressTier::R2Hit.as_str()) — the shared contract with cdn-worker#58.
const r2HitTier = "r2-hit"

// billedStage is the only blob5 value whose requests are billed. Dev and prod
// share one dataset; cdn-worker's wrangler.dev.toml stamps "dev", and rows from
// before cdn-worker#58 carry "" — neither is production traffic.
const billedStage = "prod"

// bytesPerGiB converts raw bytes to GiB (2^30 bytes) — the unit
// infra.compute.ssr.egress.bytes is priced in (migration 046), matching every
// other platform-infra `.bytes` metric's GiB-basis convention
// (infra.egress.api.bytes / infra.event.bytes, migration 020).
const bytesPerGiB = 1024 * 1024 * 1024

// lookbackHours is how many CLOSED hour buckets each run sweeps, ending at the
// top of the trigger hour. Chosen > the schedule interval so a missed/late run
// catches up; the deterministic event_id makes the overlap a no-op (re-ingested
// windows dedupe via ON CONFLICT DO NOTHING).
//
// SAFE SCHEDULE INTERVAL: this value only bounds catch-up if it covers at least
// one missed run, so the EventBridge schedule MUST be <= lookbackHours-1 hours
// (i.e. with lookbackHours=3 the schedule must be hourly or 2-hourly; a 3h+
// cadence leaves a permanent gap on any single missed invocation). The schedule
// interval is committed alongside this binary in the IaC — keep the two in sync.
const lookbackHours = 3

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stderr, nil)))
	svc, cf := buildDeps()

	if config.IsLambda() {
		lambda.Start(handler(svc, cf))
		return
	}

	// Local one-shot run: sweep the closed windows ending at the current hour,
	// then exit. No HTTP listener — the sync is a single batch invocation.
	res := syncEgress(context.Background(), svc, cf, time.Now().UTC())
	logResult(context.Background(), "infra-egress-sync local run complete", res)
	if res.Failed {
		os.Exit(1)
	}
}

// buildDeps wires the pgxpool-backed usage.Service + the Cloudflare analytics
// client from config. CF_ANALYTICS_API_TOKEN is the READ-ONLY CF token (held
// only here, never at the edge); CF_ACCOUNT_ID scopes the dataset. Both are
// required — a missing one exits at startup (config.MustEnv), never mid-run.
func buildDeps() (*usage.Service, cloudflare.AnalyticsQuerier) {
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
	cf := cloudflare.NewClient(
		config.MustEnv("CF_ANALYTICS_API_TOKEN"),
		config.MustEnv("CF_ACCOUNT_ID"),
	)
	return svc, cf
}

// handler is the Lambda entrypoint for an EventBridge-scheduled invocation. The
// CloudWatchEvent carries no window (the scheduler fires on a cron), so the
// handler derives the closed-hour lookback from the event time.
func handler(svc *usage.Service, cf cloudflare.AnalyticsQuerier) func(context.Context, events.CloudWatchEvent) error {
	return func(ctx context.Context, ev events.CloudWatchEvent) error {
		at := ev.Time
		if at.IsZero() {
			at = time.Now().UTC()
		}
		res := syncEgress(ctx, svc, cf, at.UTC())
		logResult(ctx, "infra-egress-sync lambda run complete", res)
		// A CF query error fails the run (so the exit code / Lambda error
		// surfaces it for retry); per-row record errors are logged + counted
		// but never abort the sweep (one bad row mustn't drop the rest).
		if res.Failed {
			return res.Err
		}
		return nil
	}
}

// syncResult tallies one sweep for logging / exit code.
type syncResult struct {
	Windows   int // closed hour windows queried
	Rows      int // total (app, module) rows returned across windows
	Recorded  int // events newly inserted
	Deduped   int // events that hit ON CONFLICT (already recorded)
	Skipped   int // rows skipped for an empty / unparseable app_id (org rows excluded)
	RowErrors int // per-row RecordInfraUsage errors (logged, non-fatal)
	// Org CDN egress (cdn-worker blob3, no app_id): counted and summed, never
	// recorded. Org egress is not a per-usage line and the usage ledger is
	// app-keyed, so there is nothing to record it against; the org plan leg
	// that consumes it is a separate change.
	OrgRows  int
	OrgBytes float64
	// The request pass (billing-engine#212): rows of the second query, the
	// (app, module) request events recorded, and the rows dropped for a
	// non-prod stage (or an empty one — pre-cdn-worker#58 rows).
	RequestRows     int
	RequestRecorded int
	RequestSkipped  int
	Failed          bool  // a CF query error aborted a window (run exits non-zero)
	Err             error // the first fatal (CF query) error, for the Lambda return
}

// syncEgress sweeps the last lookbackHours CLOSED hour windows ending at the top
// of the hour containing `at`, queries Cloudflare per window, and records each
// (app, module) egress total via RecordInfraUsage with a deterministic event_id.
//
// A CF query error for a window is FATAL (returns immediately with Failed) so a
// partial sweep never half-records — the next run re-sweeps the same windows and
// the deterministic event_id dedupes whatever did land. A per-row record error
// is logged + counted but never aborts the window (one bad row mustn't drop the
// rest of the batch).
func syncEgress(ctx context.Context, svc *usage.Service, cf cloudflare.AnalyticsQuerier, at time.Time) syncResult {
	var res syncResult

	for _, w := range closedHourWindows(at, lookbackHours) {
		rows, err := cf.QueryEgressWindow(ctx, egressDataset, w.start, w.end)
		if err != nil {
			// Fatal: abort the sweep cleanly. No partial double-write — the
			// deterministic event_id makes the next run's re-query idempotent.
			slog.ErrorContext(ctx, "cloudflare egress query failed",
				"window_start", w.start, "window_end", w.end, "error", err)
			res.Failed = true
			res.Err = err
			return res
		}
		res.Windows++

		for _, row := range rows {
			res.Rows++
			appID, err := uuid.Parse(row.AppID)
			if err != nil || appID == uuid.Nil {
				if orgID, oerr := uuid.Parse(row.OrgID); row.AppID == "" && oerr == nil && orgID != uuid.Nil {
					// An org CDN host's egress: attributed, just not metered.
					res.OrgRows++
					res.OrgBytes += row.Bytes
					slog.DebugContext(ctx, "org egress row counted, not recorded",
						"org_id", orgID, "window_start", w.start, "bytes", row.Bytes)
					continue
				}
				// Skip a row with an empty / garbage app_id — it can't be
				// attributed to a billing account. Logged at debug volume so a
				// flood of unattributable rows doesn't drown the log.
				res.Skipped++
				slog.DebugContext(ctx, "skipping egress row with unparseable app_id",
					"app_id", row.AppID, "module_id", row.ModuleID, "window_start", w.start)
				continue
			}

			// Branch the METRIC (and, for the ssr branch only, the emitted
			// VALUE's unit) on blob2 alone — the grouping/query above is
			// untouched, every row still yields exactly one RecordInfraUsage
			// call. Static-file rows (blob2 empty or a real module_id) are
			// byte-for-byte unchanged from before this PR.
			metric, value := egressMetricAndValue(row.ModuleID, row.Bytes)

			// No owner: egress rows carry no principal; the event records as a
			// lazy NULL-account event backfilled on conversion (design §8). The
			// omitted OwnerUserID/OwnerOrgID (uuid.Nil) is deliberate, not missing.
			resp, err := svc.RecordInfraUsage(ctx, usage.RecordInfraUsageRequest{
				EventID:    egressEventID(metric, appID, row.ModuleID, w.start),
				AppID:      appID,
				Metric:     metric,
				Value:      value,
				RecordedAt: w.start, // the window the egress occurred in, not now()
			})
			if err != nil {
				// Non-fatal: log + continue so one bad row doesn't abort the run.
				res.RowErrors++
				slog.ErrorContext(ctx, "record infra egress failed",
					"app_id", appID, "module_id", row.ModuleID, "metric", metric,
					"window_start", w.start, "bytes", row.Bytes, "error", err)
				continue
			}
			if resp.Recorded {
				res.Recorded++
			} else {
				res.Deduped++
			}
		}

		// THE REQUEST PASS (billing-engine#212). A second query over the same
		// closed window, grouped by tier and stage; a query error is fatal for
		// the same reason as above. Rows are folded per (app, module) in Go
		// so each metric stays ONE event per (app, module, window) — the same
		// id shape as the byte event, and idempotent the same way.
		requestRows, err := cf.QueryRequestWindow(ctx, egressDataset, w.start, w.end)
		if err != nil {
			slog.ErrorContext(ctx, "cloudflare request query failed",
				"window_start", w.start, "window_end", w.end, "error", err)
			res.Failed = true
			res.Err = err
			return res
		}
		syncRequestRows(ctx, svc, w, requestRows, &res)
	}
	return res
}

// requestKey is one (app, module) group of the request pass.
type requestKey struct {
	appID    uuid.UUID
	moduleID string
}

// requestTotals is what one (app, module) owes the two counts: every request,
// and the R2-tier subset.
type requestTotals struct {
	requests float64
	r2Reads  float64
}

// syncRequestRows folds one window's request rows per (app, module) and
// records infra.cdn.request.count and, when any, infra.cdn.r2.read.count —
// each as count / requestsPerUnit. Rows outside billedStage, with an
// unparseable app_id, or with no requests are skipped (counted, at debug).
func syncRequestRows(ctx context.Context, svc *usage.Service, w hourWindow, rows []cloudflare.RequestRow, res *syncResult) {
	totals := map[requestKey]*requestTotals{}
	order := []requestKey{}
	for _, row := range rows {
		res.RequestRows++
		if row.Stage != billedStage || row.Requests <= 0 {
			res.RequestSkipped++
			continue
		}
		appID, err := uuid.Parse(row.AppID)
		if err != nil || appID == uuid.Nil {
			res.RequestSkipped++
			slog.DebugContext(ctx, "skipping request row with unparseable app_id",
				"app_id", row.AppID, "module_id", row.ModuleID, "window_start", w.start)
			continue
		}
		key := requestKey{appID: appID, moduleID: row.ModuleID}
		t, ok := totals[key]
		if !ok {
			t = &requestTotals{}
			totals[key] = t
			order = append(order, key)
		}
		t.requests += row.Requests
		if row.Tier == r2HitTier {
			t.r2Reads += row.Requests
		}
	}
	for _, key := range order {
		t := totals[key]
		record := func(metric string, count float64) {
			resp, err := svc.RecordInfraUsage(ctx, usage.RecordInfraUsageRequest{
				EventID:    egressEventID(metric, key.appID, key.moduleID, w.start),
				AppID:      key.appID,
				Metric:     metric,
				Value:      count / requestsPerUnit,
				RecordedAt: w.start,
			})
			if err != nil {
				res.RowErrors++
				slog.ErrorContext(ctx, "record infra request count failed",
					"app_id", key.appID, "module_id", key.moduleID, "metric", metric,
					"window_start", w.start, "count", count, "error", err)
				return
			}
			if resp.Recorded {
				res.RequestRecorded++
			} else {
				res.Deduped++
			}
		}
		record(cdnRequestMetric, t.requests)
		if t.r2Reads > 0 {
			record(cdnR2ReadMetric, t.r2Reads)
		}
	}
}

// egressMetricAndValue decides, from a single row's blob2 (module_id) alone,
// which reserved metric it records under — the grouping/query logic upstream
// never changes. Both branches emit GiB: the per-byte COGS floors to 0 at the
// integer column (rule 5), so both keys are priced per GiB.
//
//   - blob2 == "ssr" (ssrModuleIDSentinel): SSR-origin egress (cdn-worker's
//     meter_ssr_egress) → ssrEgressMetric (migration 046).
//   - anything else (empty, or a real module_id): static-file egress →
//     cdnEgressMetric (migration 078). Before 078 this branch emitted raw
//     bytes under the price-0 egressMetric; the value scaling moved here with
//     the key, never applied to the old key.
func egressMetricAndValue(moduleID string, bytesTotal float64) (metric string, value float64) {
	if moduleID == ssrModuleIDSentinel {
		return ssrEgressMetric, bytesTotal / bytesPerGiB
	}
	return cdnEgressMetric, bytesTotal / bytesPerGiB
}

// egressEventID is the DETERMINISTIC idempotency key for one (metric, app,
// module, window) egress fact. It is a UUIDv5 (SHA-1) over the canonical
// colon-joined tuple (metric, app_id, module_id, window_start-RFC3339-UTC), so
// re-querying an already-ingested window produces the SAME id and
// RecordInfraUsage's ON CONFLICT(event_id) DO NOTHING dedupes the re-run. The
// window_start anchors the id to the bucket (not now()), so the id is stable
// across re-runs. moduleID is the raw CF blob2 string (kept verbatim so the id
// is reproducible from the dataset alone); an empty module_id still yields a
// stable per-app-per-window id. metric is included explicitly (rather than
// relying on the egressMetric constant, as before this PR) so the SSR and
// static-file metrics can never collide on event_id even if a future change
// made their (app, module, window) tuples otherwise overlap.
func egressEventID(metric string, appID uuid.UUID, moduleID string, windowStart time.Time) string {
	data := metric + ":" + appID.String() + ":" + moduleID + ":" + windowStart.UTC().Format(time.RFC3339)
	return uuid.NewSHA1(uuid.NameSpaceURL, []byte(data)).String()
}

// hourWindow is a half-open [start, end) hour bucket.
type hourWindow struct {
	start time.Time
	end   time.Time
}

// closedHourWindows returns the last `count` FULLY-CLOSED hour buckets ending at
// the top of the hour containing `at`. The current (partial) hour is EXCLUDED —
// its SUM is still growing, so recording it now then again next run would
// under-then-over count even with the deterministic id (the value would differ).
// e.g. at=12:37, count=3 → [09:00,10:00), [10:00,11:00), [11:00,12:00).
func closedHourWindows(at time.Time, count int) []hourWindow {
	topOfHour := at.UTC().Truncate(time.Hour) // start of the current partial hour = end of the last closed hour
	windows := make([]hourWindow, 0, count)
	for i := count; i >= 1; i-- {
		start := topOfHour.Add(time.Duration(-i) * time.Hour)
		windows = append(windows, hourWindow{start: start, end: start.Add(time.Hour)})
	}
	return windows
}

// logResult emits a single structured summary line for the sweep.
func logResult(ctx context.Context, msg string, res syncResult) {
	slog.InfoContext(ctx, msg,
		"windows", res.Windows, "rows", res.Rows,
		"recorded", res.Recorded, "deduped", res.Deduped,
		"skipped", res.Skipped, "row_errors", res.RowErrors,
		"org_rows", res.OrgRows, "org_bytes", res.OrgBytes,
		"request_rows", res.RequestRows, "request_recorded", res.RequestRecorded,
		"request_skipped", res.RequestSkipped,
		"failed", res.Failed)
}
