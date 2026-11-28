package main

import (
	"context"
	"log/slog"
	"time"

	cwtypes "github.com/aws/aws-sdk-go-v2/service/cloudwatch/types"
	"github.com/google/uuid"

	"github.com/mirrorstack-ai/billing-engine/internal/account/infrasync"
	"github.com/mirrorstack-ai/billing-engine/internal/account/usage"
	"github.com/mirrorstack-ai/billing-engine/internal/shared/awslambdainv"
)

// ssrGBSecondsMetric / ssrRequestCountMetric are the two reserved
// platform-infra metrics this puller records (migration 045, design doc
// §1/§4). RecordInfraUsage resolves their kind (both fixed in
// internal/account/usage/infra.go's platformInfraKind registry) + per-unit
// COGS from the seeded catalog.
const (
	ssrGBSecondsMetric    = "infra.compute.ssr.gb_seconds"
	ssrRequestCountMetric = "infra.compute.ssr.request.count"
)

// watermarkSource is this puller's row in ms_billing.infra_sync_watermarks and
// the Source dimension of its WatermarkLagSeconds metric (billing-engine#225).
// Each run sweeps from the watermark — at most seven days back, always the
// newest three sweepable hours, capped per run — instead of a fixed 3-hour
// lookback that lost every window of a longer outage (the 2026-09-13 403s had
// to be backfilled by hand).
const watermarkSource = "ssr-compute"

// lambdaLister is the AWS Lambda inventory surface the sync job depends on.
// awslambdainv.Client satisfies this; tests use a fake.
type lambdaLister interface {
	ListSSRFunctions(ctx context.Context) ([]awslambdainv.SSRFunction, error)
}

// metricsQuerier is the CloudWatch GetMetricData surface the sync job
// depends on. awslambdainv.Client satisfies this; tests use a fake.
type metricsQuerier interface {
	GetMetricData(ctx context.Context, queries []cwtypes.MetricDataQuery, start, end time.Time) ([]cwtypes.MetricDataResult, error)
}

// ssrSyncResult tallies one sweep for logging / exit code.
type ssrSyncResult struct {
	Functions   int  // ms-apphost-* functions enumerated
	SkippedIdle int  // functions pre-filtered as confirmed-idle (design §8 MEDIUM)
	Batches     int  // GetMetricData calls attempted
	BatchErrors int  // batches that errored (isolated, non-fatal — design §8 MEDIUM)
	NotReady    int  // (function results) skipped this run: unknown id or StatusCode != Complete
	Windows     int  // closed hour windows swept
	Recorded    int  // events newly inserted
	Deduped     int  // events that hit ON CONFLICT (already recorded)
	RowErrors   int  // per-row RecordInfraUsage errors (logged, non-fatal)
	Failed      bool // ListFunctions (enumeration) failed — no inventory, nothing else can proceed
	Err         error
	// Confirmed holds the start of every window the run fully recorded: the
	// inventory answered, no batch failed, every function's metrics were
	// Complete, and no row of that window errored. A batch error or a
	// not-ready function spans the whole queried range, so it confirms
	// nothing — the watermark stays and the next run asks again.
	Confirmed map[time.Time]bool
}

// runSSR sweeps the windows the watermark plans for `at` (closed hours aged
// past propagationLag) and advances the watermark over the ones the sweep
// confirmed.
func runSSR(ctx context.Context, svc *usage.Service, lister lambdaLister, querier metricsQuerier, idle idleChecker, tracker infrasync.Tracker, at time.Time) ssrSyncResult {
	var res ssrSyncResult
	tracker.Run(ctx, sweepableEnd(at, propagationLag), func(planned []infrasync.Window) func(infrasync.Window) bool {
		windows := make([]hourWindow, len(planned))
		for i, w := range planned {
			windows[i] = hourWindow{start: w.Start, end: w.End}
		}
		res = syncSSR(ctx, svc, lister, querier, idle, windows)
		return func(w infrasync.Window) bool { return res.Confirmed[w.Start] }
	})
	return res
}

// allWindowsConfirmedIdle reports whether EVERY window in this run's batch
// has its OWN immediately-preceding hour confirmed idle (a recorded
// request.count event with value == 0) for fn. This is a per-window check,
// not a single stale-hour check for the whole batch (design doc §8 MEDIUM;
// see the HIGH-severity fix this replaces): a function is only skipped for
// this run if NONE of its windows could possibly hold real activity, i.e.
// the hour immediately before window[0], AND the hour immediately before
// window[1] (which is window[0] itself), AND so on, were ALL confirmed idle.
//
// This matters because window[1]'s own preceding hour is window[0] — if
// window[0] hasn't been recorded yet (e.g. this is the very run that would
// first observe a resumption), WasIdle correctly returns "not confirmed
// idle" (not found defaults to not-idle), so the whole function stays
// eligible and every window — including the exact hour activity resumed in
// — is genuinely queried this run rather than silently dropped for the
// entire 3-window batch and permanently aging out of the lookback before any
// later run's (also single-hour) check could catch it.
//
// A WasIdle lookup error is treated the same conservative way as before:
// never let a lookup failure cause a function to be silently skipped.
func allWindowsConfirmedIdle(ctx context.Context, idle idleChecker, fn awslambdainv.SSRFunction, windows []hourWindow) bool {
	for _, w := range windows {
		priorEventID := ssrEventID(ssrRequestCountMetric, fn.AppID, fn.Env, w.start.Add(-time.Hour))
		idleYes, err := idle.WasIdle(ctx, priorEventID)
		if err != nil {
			slog.WarnContext(ctx, "ssr-compute-sync: idle pre-filter lookup failed, querying anyway",
				"function_name", fn.FunctionName, "error", err)
			return false
		}
		if !idleYes {
			return false
		}
	}
	return true
}

// syncSSR enumerates the ms-apphost-* Lambda fleet, pre-filters confirmed-
// idle functions, batches the rest into GetMetricData calls across the given
// closed hour windows, and records BOTH SSR compute metrics per (function,
// window) via RecordInfraUsage with a deterministic event_id.
//
// Enumeration failure is FATAL (no inventory, nothing else can run — mirrors
// egress's CF-query-error policy). A single batch's GetMetricData error is
// NON-FATAL: it is logged + counted and the remaining batches still run
// (design doc §8 MEDIUM "fatal-whole-run doesn't scale with inventory" — the
// fix egress's smaller usage-bounded row set didn't need). A per-row
// RecordInfraUsage error is logged + counted, never aborts the batch.
func syncSSR(ctx context.Context, svc *usage.Service, lister lambdaLister, querier metricsQuerier, idle idleChecker, windows []hourWindow) ssrSyncResult {
	res := ssrSyncResult{Confirmed: map[time.Time]bool{}}

	fns, err := lister.ListSSRFunctions(ctx)
	if err != nil {
		slog.ErrorContext(ctx, "ssr-compute-sync: ListSSRFunctions failed", "error", err)
		res.Failed = true
		res.Err = err
		return res
	}
	res.Functions = len(fns)

	res.Windows = len(windows)
	failedWindows := map[time.Time]bool{}
	if len(fns) == 0 || len(windows) == 0 {
		// An empty fleet has nothing to record: every window is confirmed.
		confirmWindows(&res, windows, failedWindows)
		return res
	}

	eligible := make([]awslambdainv.SSRFunction, 0, len(fns))
	for _, fn := range fns {
		if allWindowsConfirmedIdle(ctx, idle, fn, windows) {
			res.SkippedIdle++
			continue
		}
		eligible = append(eligible, fn)
	}

	for _, batch := range batchFunctions(eligible, ssrFunctionBatchSize) {
		res.Batches++
		rows, notReady, err := queryBatch(ctx, querier, batch, windows)
		res.NotReady += notReady
		if err != nil {
			// Per-batch isolation (design §8 MEDIUM): log + count, continue
			// with the remaining batches rather than aborting the whole run.
			// The deterministic event_id + next run's lookback re-sweep
			// backfill exactly this batch.
			res.BatchErrors++
			slog.ErrorContext(ctx, "ssr-compute-sync: GetMetricData batch failed, continuing with remaining batches",
				"batch_size", len(batch), "error", err)
			continue
		}

		for _, row := range rows {
			before := res.RowErrors
			recordSSRWindow(ctx, svc, row, &res)
			if res.RowErrors != before {
				failedWindows[row.WindowStart] = true
			}
		}
	}

	confirmWindows(&res, windows, failedWindows)
	return res
}

// confirmWindows marks every window confirmed unless the run hit a batch error
// or a not-ready function (both span the whole queried range) or a row of that
// window failed to record.
func confirmWindows(res *ssrSyncResult, windows []hourWindow, failedWindows map[time.Time]bool) {
	if res.BatchErrors > 0 || res.NotReady > 0 {
		return
	}
	for _, w := range windows {
		if !failedWindows[w.start] {
			res.Confirmed[w.start] = true
		}
	}
}

// recordSSRWindow prices one (function, window) usage row into the two SSR
// compute metrics and records each independently via RecordInfraUsage. Both
// calls carry AppID (parsed function name) and leave ModuleID/OwnerUserID/
// OwnerOrgID zero: SSR compute is app-level, not attributable to a specific
// installed module — it resolves through the platform-infra sentinel and
// records as a lazy NULL-account event, identical to egress's own
// attribution story (design doc §2.4).
func recordSSRWindow(ctx context.Context, svc *usage.Service, row ssrWindowUsage, res *ssrSyncResult) {
	gbSeconds := (row.DurationMsSum / 1000.0) * (float64(row.Function.MemoryMB) / 1024.0)
	requestCountK := row.Invocations / 1000.0

	record := func(metric string, value float64) {
		resp, err := svc.RecordInfraUsage(ctx, usage.RecordInfraUsageRequest{
			EventID:    ssrEventID(metric, row.Function.AppID, row.Function.Env, row.WindowStart),
			AppID:      row.Function.AppID,
			Metric:     metric,
			Value:      value,
			RecordedAt: row.WindowStart,
		})
		if err != nil {
			res.RowErrors++
			slog.ErrorContext(ctx, "ssr-compute-sync: record infra usage failed",
				"function_name", row.Function.FunctionName, "metric", metric,
				"window_start", row.WindowStart, "value", value, "error", err)
			return
		}
		if resp.Recorded {
			res.Recorded++
		} else {
			res.Deduped++
		}
	}

	record(ssrGBSecondsMetric, gbSeconds)
	record(ssrRequestCountMetric, requestCountK)
}

// ssrEventID is the DETERMINISTIC idempotency key for one (metric, app, env,
// window) SSR compute fact — the same UUIDv5 (SHA-1) scheme
// cmd/infra-egress-sync uses, extended with `env` (design doc §2.4) so the id
// stays well-formed once a second env value exists. Re-querying an
// already-ingested window produces the SAME id, and RecordInfraUsage's ON
// CONFLICT(event_id) DO NOTHING dedupes the re-run.
func ssrEventID(metric string, appID uuid.UUID, env string, windowStart time.Time) string {
	data := metric + ":" + appID.String() + ":" + env + ":" + windowStart.UTC().Format(time.RFC3339)
	return uuid.NewSHA1(uuid.NameSpaceURL, []byte(data)).String()
}
