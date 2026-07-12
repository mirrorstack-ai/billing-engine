package main

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	cwtypes "github.com/aws/aws-sdk-go-v2/service/cloudwatch/types"
	"github.com/google/uuid"

	"github.com/mirrorstack-ai/billing-engine/internal/shared/awslambdainv"
)

func testFn(name string, memMB int32) awslambdainv.SSRFunction {
	return awslambdainv.SSRFunction{
		FunctionName: name,
		AppID:        uuid.New(),
		Env:          "prod",
		MemoryMB:     memMB,
	}
}

func TestBuildMetricQueries_SyntheticIdScheme(t *testing.T) {
	// MetricDataQuery.Id must match ^[a-z][a-zA-Z0-9_]*$ — a raw UUID app_id
	// (starts with a hex digit, contains hyphens) is not a legal Id. The
	// synthetic d{i}/i{i} scheme sidesteps that entirely.
	fns := []awslambdainv.SSRFunction{testFn("fn-a", 512), testFn("fn-b", 256)}
	queries, ids := buildMetricQueries(fns)

	if len(queries) != 4 {
		t.Fatalf("len(queries) = %d, want 4 (2 functions x 2 metrics)", len(queries))
	}
	wantIDs := []string{"d0", "i0", "d1", "i1"}
	for _, want := range wantIDs {
		if _, ok := ids[want]; !ok {
			t.Errorf("ids missing synthetic id %q", want)
		}
	}
	if ids["d0"].Function.FunctionName != "fn-a" || ids["d0"].Metric != metricNameDuration {
		t.Errorf("ids[d0] = %+v, want fn-a/Duration", ids["d0"])
	}
	if ids["i1"].Function.FunctionName != "fn-b" || ids["i1"].Metric != metricNameInvocations {
		t.Errorf("ids[i1] = %+v, want fn-b/Invocations", ids["i1"])
	}
}

// TestCorrelateResults_UsesIdNotPositionalOrder is the direct regression test
// for design doc §8 HIGH finding #1: AWS does not guarantee GetMetricData's
// response order matches request order. This test builds queries for two
// functions, then constructs the FAKE response with its result entries
// DELIBERATELY SHUFFLED (function B's data appears before function A's, and
// each function's own Duration/Invocations pair is also out of order) and
// asserts every value still lands on the correct function via its .Id field
// — a positional/index-based reassembly would swap the two functions' data.
func TestCorrelateResults_UsesIdNotPositionalOrder(t *testing.T) {
	fnA := testFn("ms-apphost-a", 512)
	fnB := testFn("ms-apphost-b", 1024)
	_, idIndex := buildMetricQueries([]awslambdainv.SSRFunction{fnA, fnB})

	windowStart := time.Date(2026, 7, 12, 10, 0, 0, 0, time.UTC)
	windows := []hourWindow{{start: windowStart, end: windowStart.Add(time.Hour)}}

	// Correct data: A has Duration=1000ms/Invocations=10, B has
	// Duration=5000ms/Invocations=50. The response below is shuffled AND
	// each function's own two metrics are non-adjacent, to make sure nothing
	// about call order is relied upon.
	results := []cwtypes.MetricDataResult{
		{Id: aws.String("i1"), StatusCode: cwtypes.StatusCodeComplete, Timestamps: []time.Time{windowStart}, Values: []float64{50}},   // B invocations
		{Id: aws.String("d0"), StatusCode: cwtypes.StatusCodeComplete, Timestamps: []time.Time{windowStart}, Values: []float64{1000}}, // A duration
		{Id: aws.String("i0"), StatusCode: cwtypes.StatusCodeComplete, Timestamps: []time.Time{windowStart}, Values: []float64{10}},   // A invocations
		{Id: aws.String("d1"), StatusCode: cwtypes.StatusCodeComplete, Timestamps: []time.Time{windowStart}, Values: []float64{5000}}, // B duration
	}

	rows, skipped := correlateResults(results, idIndex, windows)
	if skipped != 0 {
		t.Fatalf("skipped = %d, want 0", skipped)
	}
	if len(rows) != 2 {
		t.Fatalf("len(rows) = %d, want 2", len(rows))
	}

	byFn := map[string]ssrWindowUsage{}
	for _, r := range rows {
		byFn[r.Function.FunctionName] = r
	}
	a, ok := byFn["ms-apphost-a"]
	if !ok {
		t.Fatalf("no row for ms-apphost-a")
	}
	if a.DurationMsSum != 1000 || a.Invocations != 10 {
		t.Errorf("ms-apphost-a row = %+v, want Duration=1000/Invocations=10 (result misattribution — the HIGH-severity bug this test guards against)", a)
	}
	b, ok := byFn["ms-apphost-b"]
	if !ok {
		t.Fatalf("no row for ms-apphost-b")
	}
	if b.DurationMsSum != 5000 || b.Invocations != 50 {
		t.Errorf("ms-apphost-b row = %+v, want Duration=5000/Invocations=50 (result misattribution)", b)
	}
}

// TestCorrelateResults_SkipsNonCompleteStatus is the direct regression test
// for design doc §8 HIGH finding #2: a GetMetricData result with StatusCode
// PartialData (or InternalError) must NEVER be recorded — it must be dropped
// entirely for that function this run, since the deterministic event_id's ON
// CONFLICT DO NOTHING means a bad first write can never be corrected later.
func TestCorrelateResults_SkipsNonCompleteStatus(t *testing.T) {
	fnA := testFn("ms-apphost-a", 512)
	fnB := testFn("ms-apphost-b", 512)
	_, idIndex := buildMetricQueries([]awslambdainv.SSRFunction{fnA, fnB})

	windowStart := time.Date(2026, 7, 12, 10, 0, 0, 0, time.UTC)
	windows := []hourWindow{{start: windowStart, end: windowStart.Add(time.Hour)}}

	results := []cwtypes.MetricDataResult{
		// fnA: Duration is Complete but Invocations is PartialData — the
		// WHOLE function must be dropped, not just one of its two metrics.
		{Id: aws.String("d0"), StatusCode: cwtypes.StatusCodeComplete, Timestamps: []time.Time{windowStart}, Values: []float64{1000}},
		{Id: aws.String("i0"), StatusCode: cwtypes.StatusCodePartialData, Timestamps: []time.Time{windowStart}, Values: []float64{10}},
		// fnB: both Complete — must be recorded normally.
		{Id: aws.String("d1"), StatusCode: cwtypes.StatusCodeComplete, Timestamps: []time.Time{windowStart}, Values: []float64{2000}},
		{Id: aws.String("i1"), StatusCode: cwtypes.StatusCodeComplete, Timestamps: []time.Time{windowStart}, Values: []float64{20}},
	}

	rows, skipped := correlateResults(results, idIndex, windows)
	if skipped != 1 {
		t.Errorf("skipped = %d, want 1 (fnA dropped for non-Complete Invocations)", skipped)
	}
	if len(rows) != 1 {
		t.Fatalf("len(rows) = %d, want 1 (only fnB recorded)", len(rows))
	}
	if rows[0].Function.FunctionName != "ms-apphost-b" {
		t.Errorf("recorded function = %q, want ms-apphost-b; fnA (PartialData) must never be recorded", rows[0].Function.FunctionName)
	}
}

func TestCorrelateResults_MissingDatapointIsZeroNotSkip(t *testing.T) {
	// An hour with genuinely zero invocations has NO Timestamps/Values entry
	// at all (CloudWatch doesn't emit an explicit 0) — that is a real zero,
	// not a reason to drop the function.
	fn := testFn("ms-apphost-a", 512)
	_, idIndex := buildMetricQueries([]awslambdainv.SSRFunction{fn})

	w1 := time.Date(2026, 7, 12, 9, 0, 0, 0, time.UTC)
	w2 := time.Date(2026, 7, 12, 10, 0, 0, 0, time.UTC)
	windows := []hourWindow{{start: w1, end: w1.Add(time.Hour)}, {start: w2, end: w2.Add(time.Hour)}}

	results := []cwtypes.MetricDataResult{
		// Only w2 has a datapoint; w1 had nothing to report.
		{Id: aws.String("d0"), StatusCode: cwtypes.StatusCodeComplete, Timestamps: []time.Time{w2}, Values: []float64{100}},
		{Id: aws.String("i0"), StatusCode: cwtypes.StatusCodeComplete, Timestamps: []time.Time{w2}, Values: []float64{1}},
	}

	rows, skipped := correlateResults(results, idIndex, windows)
	if skipped != 0 {
		t.Fatalf("skipped = %d, want 0", skipped)
	}
	if len(rows) != 2 {
		t.Fatalf("len(rows) = %d, want 2 (both windows present, one at zero)", len(rows))
	}
	for _, r := range rows {
		if r.WindowStart.Equal(w1) && (r.DurationMsSum != 0 || r.Invocations != 0) {
			t.Errorf("w1 row = %+v, want zero-valued (no datapoint = no invocations that hour)", r)
		}
		if r.WindowStart.Equal(w2) && (r.DurationMsSum != 100 || r.Invocations != 1) {
			t.Errorf("w2 row = %+v, want Duration=100/Invocations=1", r)
		}
	}
}

func TestBatchFunctions_SplitsIntoGroups(t *testing.T) {
	fns := make([]awslambdainv.SSRFunction, 5)
	for i := range fns {
		fns[i] = testFn("fn", 512)
	}
	batches := batchFunctions(fns, 2)
	if len(batches) != 3 {
		t.Fatalf("len(batches) = %d, want 3", len(batches))
	}
	if len(batches[0]) != 2 || len(batches[1]) != 2 || len(batches[2]) != 1 {
		t.Errorf("batch sizes = %d/%d/%d, want 2/2/1", len(batches[0]), len(batches[1]), len(batches[2]))
	}
}
