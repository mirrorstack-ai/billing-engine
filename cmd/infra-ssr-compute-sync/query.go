package main

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	cwtypes "github.com/aws/aws-sdk-go-v2/service/cloudwatch/types"

	"github.com/mirrorstack-ai/billing-engine/internal/shared/awslambdainv"
)

const (
	lambdaNamespace       = "AWS/Lambda"
	metricNameDuration    = "Duration"
	metricNameInvocations = "Invocations"
	statSum               = "Sum"

	// ssrFunctionBatchSize bounds how many functions' queries go in a single
	// GetMetricData call: 2 MetricDataQuery entries per function (Duration +
	// Invocations), and GetMetricData's hard per-call cap is 500 entries — so
	// at most 250 functions per batch.
	ssrFunctionBatchSize = 250

	// ssrMetricPeriodSeconds is the GetMetricData Period: 3600 (one hour)
	// returns one datapoint PER HOUR BUCKET across the whole queried
	// StartTime/EndTime range in a single call — flatter than
	// infra-egress-sync's per-window loop (design doc §2.2).
	ssrMetricPeriodSeconds = 3600
)

// metricQueryTarget is what a synthetic MetricDataQuery.Id resolves back to,
// built at QUERY-CONSTRUCTION TIME (buildMetricQueries) so that
// correlateResults never has to guess: GetMetricData does NOT guarantee
// response order matches request order (up to 500 batched MetricDataQuery
// entries per call), so every result MUST be looked up by its own returned
// .Id field, never by array position (design doc §8, HIGH finding #1).
type metricQueryTarget struct {
	Function awslambdainv.SSRFunction
	Metric   string // metricNameDuration | metricNameInvocations
}

// batchFunctions splits fns into groups of at most size (ssrFunctionBatchSize
// in production; parameterized for tests).
func batchFunctions(fns []awslambdainv.SSRFunction, size int) [][]awslambdainv.SSRFunction {
	if size <= 0 {
		size = len(fns)
	}
	var batches [][]awslambdainv.SSRFunction
	for i := 0; i < len(fns); i += size {
		end := i + size
		if end > len(fns) {
			end = len(fns)
		}
		batches = append(batches, fns[i:end])
	}
	return batches
}

// buildMetricQueries builds one Duration-Sum and one Invocations-Sum
// MetricDataQuery per function, with a SYNTHETIC Id ("d0"/"i0"/"d1"/"i1"...)
// since MetricDataQuery.Id must match ^[a-z][a-zA-Z0-9_]*$ and a UUID app_id
// can't be used directly as an Id. Returns the queries alongside the explicit
// Id -> target map built AT THIS CONSTRUCTION TIME, which is the ONLY
// correlation mechanism correlateResults is allowed to use.
func buildMetricQueries(fns []awslambdainv.SSRFunction) ([]cwtypes.MetricDataQuery, map[string]metricQueryTarget) {
	queries := make([]cwtypes.MetricDataQuery, 0, len(fns)*2)
	ids := make(map[string]metricQueryTarget, len(fns)*2)
	for i, fn := range fns {
		durID := fmt.Sprintf("d%d", i)
		invID := fmt.Sprintf("i%d", i)
		queries = append(queries,
			metricDataQuery(durID, fn.FunctionName, metricNameDuration),
			metricDataQuery(invID, fn.FunctionName, metricNameInvocations),
		)
		ids[durID] = metricQueryTarget{Function: fn, Metric: metricNameDuration}
		ids[invID] = metricQueryTarget{Function: fn, Metric: metricNameInvocations}
	}
	return queries, ids
}

func metricDataQuery(id, functionName, metricName string) cwtypes.MetricDataQuery {
	return cwtypes.MetricDataQuery{
		Id: aws.String(id),
		MetricStat: &cwtypes.MetricStat{
			Metric: &cwtypes.Metric{
				Namespace:  aws.String(lambdaNamespace),
				MetricName: aws.String(metricName),
				Dimensions: []cwtypes.Dimension{
					{Name: aws.String("FunctionName"), Value: aws.String(functionName)},
				},
			},
			Period: aws.Int32(ssrMetricPeriodSeconds),
			Stat:   aws.String(statSum),
		},
		ReturnData: aws.Bool(true),
	}
}

// ssrWindowUsage is one (function, closed-hour-window) usage fact, ready to
// price into the two SSR compute metrics.
type ssrWindowUsage struct {
	Function      awslambdainv.SSRFunction
	WindowStart   time.Time
	DurationMsSum float64
	Invocations   float64
}

// funcSeries accumulates one function's per-hour Duration/Invocations
// datapoints across however many MetricDataResult entries reference it (one
// each, in the current design, but the map handles any shape) plus each
// metric's own StatusCode.
type funcSeries struct {
	fn                  awslambdainv.SSRFunction
	sawDuration         bool
	sawInvocations      bool
	durationComplete    bool
	invocationsComplete bool
	durationByHour      map[time.Time]float64
	invocationsByHour   map[time.Time]float64
}

// correlateResults reassembles a batched GetMetricData response into one
// ssrWindowUsage per (function, window) — correlating EVERY result by its own
// returned .Id field via idIndex (never by array position/index; see
// buildMetricQueries), and gating on each result's StatusCode.
//
// A function is INCLUDED for a window only if BOTH its Duration and its
// Invocations MetricDataResult carried StatusCode == Complete. Anything else
// (PartialData, InternalError, or a missing result entirely) means "not
// ready yet" for that FUNCTION across the whole queried range — it is
// dropped silently here (logged by the caller via the returned skip count)
// so the caller never records a partial/zero value under a window's
// permanent (ON CONFLICT DO NOTHING) idempotency key. The next run's
// lookback re-sweep will retry it.
//
// A missing datapoint for a specific hour (Timestamps/Values has no entry
// for that hour) within an otherwise-Complete result is NOT an error — it
// means CloudWatch has literally zero to report for that hour (e.g. zero
// invocations), which is a real, correctly-recorded zero.
func correlateResults(results []cwtypes.MetricDataResult, idIndex map[string]metricQueryTarget, windows []hourWindow) ([]ssrWindowUsage, int) {
	series := make(map[string]*funcSeries)

	skippedUnknownID := 0
	for _, res := range results {
		id := aws.ToString(res.Id)
		target, ok := idIndex[id]
		if !ok {
			// Should never happen (every Id we send is one we minted), but
			// AWS's own contract is "correlate by Id" — an Id we don't
			// recognize can't be safely attributed to anything, so it is
			// dropped rather than guessed at.
			skippedUnknownID++
			slog.Warn("ssr-compute-sync: GetMetricData result with unrecognized id", "id", id)
			continue
		}

		fs, ok := series[target.Function.FunctionName]
		if !ok {
			fs = &funcSeries{
				fn:                target.Function,
				durationByHour:    make(map[time.Time]float64),
				invocationsByHour: make(map[time.Time]float64),
			}
			series[target.Function.FunctionName] = fs
		}

		complete := res.StatusCode == cwtypes.StatusCodeComplete
		switch target.Metric {
		case metricNameDuration:
			fs.sawDuration = true
			fs.durationComplete = complete
			for i, ts := range res.Timestamps {
				fs.durationByHour[ts.UTC().Truncate(time.Hour)] = res.Values[i]
			}
		case metricNameInvocations:
			fs.sawInvocations = true
			fs.invocationsComplete = complete
			for i, ts := range res.Timestamps {
				fs.invocationsByHour[ts.UTC().Truncate(time.Hour)] = res.Values[i]
			}
		}
	}

	var rows []ssrWindowUsage
	notReady := 0
	for _, fs := range series {
		if !fs.sawDuration || !fs.sawInvocations || !fs.durationComplete || !fs.invocationsComplete {
			notReady++
			slog.Warn("ssr-compute-sync: skipping function, metrics not Complete this run",
				"function_name", fs.fn.FunctionName,
				"saw_duration", fs.sawDuration, "saw_invocations", fs.sawInvocations,
				"duration_complete", fs.durationComplete, "invocations_complete", fs.invocationsComplete)
			continue
		}
		for _, w := range windows {
			rows = append(rows, ssrWindowUsage{
				Function:      fs.fn,
				WindowStart:   w.start,
				DurationMsSum: fs.durationByHour[w.start], // 0 if no datapoint = 0 invocations that hour
				Invocations:   fs.invocationsByHour[w.start],
			})
		}
	}
	return rows, skippedUnknownID + notReady
}

// batchQuery is the minimal per-batch inputs/outputs the sync loop needs;
// kept as a function for testability without spinning up a real
// awslambdainv.Client.
func queryBatch(ctx context.Context, querier metricsQuerier, batch []awslambdainv.SSRFunction, windows []hourWindow) ([]ssrWindowUsage, int, error) {
	if len(batch) == 0 || len(windows) == 0 {
		return nil, 0, nil
	}
	queries, idIndex := buildMetricQueries(batch)
	results, err := querier.GetMetricData(ctx, queries, windows[0].start, windows[len(windows)-1].end)
	if err != nil {
		return nil, 0, err
	}
	rows, skipped := correlateResults(results, idIndex, windows)
	return rows, skipped, nil
}
