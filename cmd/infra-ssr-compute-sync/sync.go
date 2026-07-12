package main

import (
	"context"
	"log/slog"
	"time"

	cwtypes "github.com/aws/aws-sdk-go-v2/service/cloudwatch/types"
	"github.com/google/uuid"

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

// ssrLookbackHours mirrors infra-egress-sync's lookbackHours contract: each
// run sweeps the last N CLOSED hour windows (after the propagationLag
// margin), so a missed/late run catches up and the deterministic event_id
// makes the overlap a no-op.
//
// SAFE SCHEDULE INTERVAL: same constraint as egress — the EventBridge
// schedule MUST be <= ssrLookbackHours-1 hours (hourly, per the design doc's
// CDK snippet), kept in sync with the IaC.
const ssrLookbackHours = 3

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
}

// syncSSR enumerates the ms-apphost-* Lambda fleet, pre-filters confirmed-
// idle functions, batches the rest into GetMetricData calls across the
// lookback's closed hour windows, and records BOTH SSR compute metrics per
// (function, window) via RecordInfraUsage with a deterministic event_id.
//
// Enumeration failure is FATAL (no inventory, nothing else can run — mirrors
// egress's CF-query-error policy). A single batch's GetMetricData error is
// NON-FATAL: it is logged + counted and the remaining batches still run
// (design doc §8 MEDIUM "fatal-whole-run doesn't scale with inventory" — the
// fix egress's smaller usage-bounded row set didn't need). A per-row
// RecordInfraUsage error is logged + counted, never aborts the batch.
func syncSSR(ctx context.Context, svc *usage.Service, lister lambdaLister, querier metricsQuerier, idle idleChecker, at time.Time) ssrSyncResult {
	var res ssrSyncResult

	fns, err := lister.ListSSRFunctions(ctx)
	if err != nil {
		slog.ErrorContext(ctx, "ssr-compute-sync: ListSSRFunctions failed", "error", err)
		res.Failed = true
		res.Err = err
		return res
	}
	res.Functions = len(fns)

	windows := closedHourWindowsWithLag(at, ssrLookbackHours, propagationLag)
	res.Windows = len(windows)
	if len(fns) == 0 || len(windows) == 0 {
		return res
	}

	priorWindowStart := windows[0].start.Add(-time.Hour)
	eligible := make([]awslambdainv.SSRFunction, 0, len(fns))
	for _, fn := range fns {
		priorEventID := ssrEventID(ssrRequestCountMetric, fn.AppID, fn.Env, priorWindowStart)
		idleYes, err := idle.WasIdle(ctx, priorEventID)
		if err != nil {
			// Conservative: a lookup failure must never silently DROP a
			// function from metering, so treat it as not-idle (query it).
			slog.WarnContext(ctx, "ssr-compute-sync: idle pre-filter lookup failed, querying anyway",
				"function_name", fn.FunctionName, "error", err)
			eligible = append(eligible, fn)
			continue
		}
		if idleYes {
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
			recordSSRWindow(ctx, svc, row, &res)
		}
	}

	return res
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
