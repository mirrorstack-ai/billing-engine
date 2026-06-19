// Command infra-egress-sync is the scheduled CDN-egress PULLER for Milestone D
// PR #10c — the platform-infra (Plane 1) egress metering chokepoint.
//
// Egress is extreme-volume and the CDN edge runs OUTSIDE the VPC holding no
// platform secret, so the cdn-worker does NOT call billing per request. Instead
// the Rust worker accumulates bytes into a Cloudflare Analytics Engine dataset
// ("cdn_egress") via writeDataPoint, and THIS binary periodically PULLS the
// aggregated totals back: it holds a READ-ONLY Cloudflare API token, queries the
// CF GraphQL Analytics API for FULLY-CLOSED hour windows, and for each
// (app_id, module_id, window) calls billing-engine's RecordInfraUsage
// (metric="infra.egress.bytes"). Direction is billing-engine → Cloudflare
// (outbound pull); Cloudflare never calls back (design §3a / §5 PR #10c).
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

	"github.com/mirrorstack-ai/billing-engine/internal/account/usage"
	"github.com/mirrorstack-ai/billing-engine/internal/shared/cloudflare"
	"github.com/mirrorstack-ai/billing-engine/internal/shared/config"
)

// egressDataset is the Cloudflare Analytics Engine dataset name the cdn-worker
// writes to and this puller reads from. It MUST match the worker's
// wrangler.toml [[analytics_engine_datasets]] dataset binding exactly.
const egressDataset = "cdn_egress"

// egressMetric is the reserved platform-infra metric the puller records each
// window under. RecordInfraUsage resolves its kind (sum) + per-unit COGS from
// the platform-owned registry / seeded catalog (migration 017).
const egressMetric = "infra.egress.bytes"

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
	svc := usage.NewService(usage.NewStore(pool))
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
	Windows   int   // closed hour windows queried
	Rows      int   // total (app, module) rows returned across windows
	Recorded  int   // events newly inserted
	Deduped   int   // events that hit ON CONFLICT (already recorded)
	Skipped   int   // rows skipped for an empty / unparseable app_id
	RowErrors int   // per-row RecordInfraUsage errors (logged, non-fatal)
	Failed    bool  // a CF query error aborted a window (run exits non-zero)
	Err       error // the first fatal (CF query) error, for the Lambda return
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
				// Skip a row with an empty / garbage app_id — it can't be
				// attributed to a billing account. Logged at debug volume so a
				// flood of unattributable rows doesn't drown the log.
				res.Skipped++
				slog.DebugContext(ctx, "skipping egress row with unparseable app_id",
					"app_id", row.AppID, "module_id", row.ModuleID, "window_start", w.start)
				continue
			}

			// No owner: egress rows carry no principal; the event records as a
			// lazy NULL-account event backfilled on conversion (design §8). The
			// omitted OwnerUserID/OwnerOrgID (uuid.Nil) is deliberate, not missing.
			resp, err := svc.RecordInfraUsage(ctx, usage.RecordInfraUsageRequest{
				EventID:    egressEventID(appID, row.ModuleID, w.start),
				AppID:      appID,
				Metric:     egressMetric,
				Value:      row.Bytes,
				RecordedAt: w.start, // the window the egress occurred in, not now()
			})
			if err != nil {
				// Non-fatal: log + continue so one bad row doesn't abort the run.
				res.RowErrors++
				slog.ErrorContext(ctx, "record infra egress failed",
					"app_id", appID, "module_id", row.ModuleID,
					"window_start", w.start, "bytes", row.Bytes, "error", err)
				continue
			}
			if resp.Recorded {
				res.Recorded++
			} else {
				res.Deduped++
			}
		}
	}
	return res
}

// egressEventID is the DETERMINISTIC idempotency key for one (app, module,
// window) egress fact. It is a UUIDv5 (SHA-1) over the canonical colon-joined
// tuple (metric, app_id, module_id, window_start-RFC3339-UTC), so re-querying an
// already-ingested window produces the SAME id and RecordInfraUsage's ON
// CONFLICT(event_id) DO NOTHING dedupes the re-run. The window_start anchors the
// id to the bucket (not now()), so the id is stable across re-runs. moduleID is
// the raw CF blob2 string (kept verbatim so the id is reproducible from the
// dataset alone); an empty module_id still yields a stable per-app-per-window id.
func egressEventID(appID uuid.UUID, moduleID string, windowStart time.Time) string {
	data := egressMetric + ":" + appID.String() + ":" + moduleID + ":" + windowStart.UTC().Format(time.RFC3339)
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
		"failed", res.Failed)
}
