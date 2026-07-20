package main

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	cwtypes "github.com/aws/aws-sdk-go-v2/service/cloudwatch/types"
	"github.com/google/uuid"

	"github.com/mirrorstack-ai/billing-engine/internal/account/usage"
	"github.com/mirrorstack-ai/billing-engine/internal/shared/awslambdainv"
)

// --- fake lambdaLister / metricsQuerier -----------------------------------

type fakeLister struct {
	fns []awslambdainv.SSRFunction
	err error
}

func (f *fakeLister) ListSSRFunctions(_ context.Context) ([]awslambdainv.SSRFunction, error) {
	return f.fns, f.err
}

// fakeQuerier returns a canned set of MetricDataResults per batch call, or an
// error on a configured call index (to exercise per-batch failure isolation).
// It records every batch's queries so a test can assert batching.
type fakeQuerier struct {
	resultsByCall map[int][]cwtypes.MetricDataResult
	errByCall     map[int]error
	calls         int
	batchSizes    []int
}

func (f *fakeQuerier) GetMetricData(_ context.Context, queries []cwtypes.MetricDataQuery, _, _ time.Time) ([]cwtypes.MetricDataResult, error) {
	idx := f.calls
	f.calls++
	f.batchSizes = append(f.batchSizes, len(queries)/2)
	if err, ok := f.errByCall[idx]; ok {
		return nil, err
	}
	return f.resultsByCall[idx], nil
}

// --- fake idleChecker ------------------------------------------------------

type fakeIdle struct {
	idle map[string]bool // event_id -> idle
	err  error
}

func (f *fakeIdle) WasIdle(_ context.Context, eventID string) (bool, error) {
	if f.err != nil {
		return false, f.err
	}
	return f.idle[eventID], nil
}

// storeBackedIdle is a REALISTIC idleChecker fake: unlike fakeIdle's static
// map, it derives WasIdle from a live *fakeStore's recorded events — exactly
// how the real pgxIdleChecker derives it from ms_billing.usage_events. This
// is what lets a test simulate MULTIPLE sequential runs sharing one store and
// observe how each run's recordings feed the NEXT run's idle pre-filter, the
// way production actually behaves across scheduled invocations.
type storeBackedIdle struct{ store *fakeStore }

func (s *storeBackedIdle) WasIdle(_ context.Context, eventID string) (bool, error) {
	ev, ok := s.store.events[eventID]
	if !ok {
		return false, nil
	}
	return ev.Value == 0, nil
}

// --- fake usage.Store (mirrors cmd/infra-egress-sync's fakeStore) --------

type fakeStore struct {
	events      map[string]usage.UsageEvent
	insertErr   error
	failEventID string
}

func newFakeStore() *fakeStore { return &fakeStore{events: map[string]usage.UsageEvent{}} }

func (f *fakeStore) InsertUsageEvent(_ context.Context, ev usage.UsageEvent) (bool, error) {
	if f.insertErr != nil && ev.EventID == f.failEventID {
		return false, f.insertErr
	}
	if _, exists := f.events[ev.EventID]; exists {
		return false, nil
	}
	f.events[ev.EventID] = ev
	return true, nil
}
func (f *fakeStore) AccountByOwner(_ context.Context, _ usage.Owner) (uuid.UUID, bool, error) {
	return uuid.Nil, false, nil
}
func (f *fakeStore) AccountAnchorDay(_ context.Context, _ uuid.UUID) (int, error) { return 1, nil }
func (f *fakeStore) LookupMetricDefinition(_ context.Context, _ uuid.UUID, _ string) (usage.MetricDefinition, bool, error) {
	return usage.MetricDefinition{}, false, nil
}
func (f *fakeStore) UpsertMetricDefinitions(_ context.Context, _ []usage.MetricDeclaration) error {
	return nil
}
func (f *fakeStore) UpsertMetricVersionPrices(_ context.Context, _ []usage.MetricVersionPrice) error {
	return nil
}
func (f *fakeStore) UpsertInfraPriceOverrides(_ context.Context, _ uuid.UUID, _ []usage.InfraPriceOverride) error {
	return nil
}
func (f *fakeStore) CurrentPeriodUsage(_ context.Context, _ uuid.UUID, _, _ time.Time) ([]usage.MetricUsageRaw, error) {
	return nil, nil
}
func (f *fakeStore) UpsertModuleVisibility(_ context.Context, _ uuid.UUID, _ usage.Visibility) error {
	return nil
}
func (f *fakeStore) UsageHistory(_ context.Context, _ uuid.UUID, _, _ time.Time) ([]usage.PeriodMetricUsageRaw, error) {
	return nil, nil
}
func (f *fakeStore) VersionBreakdown(_ context.Context, _ uuid.UUID, _ time.Time, _ uuid.UUID) ([]usage.VersionUsageRaw, error) {
	return nil, nil
}
func (f *fakeStore) AppUsage(_ context.Context, _, _ uuid.UUID, _, _ time.Time) ([]usage.AppMetricUsageRaw, error) {
	return nil, nil
}
func (f *fakeStore) AppBill(_ context.Context, _, _ uuid.UUID, _, _ time.Time) ([]usage.AppMetricUsageRaw, error) {
	return nil, nil
}
func (f *fakeStore) AppInfraBill(_ context.Context, _, _ uuid.UUID, _, _ time.Time) ([]usage.AppInfraUsage, error) {
	return nil, nil
}
func (f *fakeStore) AppModuleInfraBill(_ context.Context, _, _ uuid.UUID, _, _ time.Time) ([]usage.AppModuleInfraUsage, error) {
	return nil, nil
}
func (f *fakeStore) ListBillingPeriods(_ context.Context, _ uuid.UUID, _ time.Time) ([]usage.BillingPeriodRaw, error) {
	return nil, nil
}
func (f *fakeStore) BillingPeriodWindow(_ context.Context, _, _ uuid.UUID) (time.Time, time.Time, bool, error) {
	return time.Time{}, time.Time{}, false, nil
}
func (f *fakeStore) ListInvoices(_ context.Context, _ uuid.UUID, _ int32, _ *usage.InvoiceCursor) ([]usage.InvoiceMirrorRaw, error) {
	return nil, nil
}
func (f *fakeStore) AppMirror(_ context.Context, _ uuid.UUID) (usage.AppMirrorInfo, bool, error) {
	return usage.AppMirrorInfo{}, false, nil
}
func (f *fakeStore) AppBaseSnapshot(_ context.Context, _ uuid.UUID, _ time.Time) (usage.AppBaseSnapshotInfo, bool, error) {
	return usage.AppBaseSnapshotInfo{}, false, nil
}
func (f *fakeStore) AppIDsWithUsage(_ context.Context, _ uuid.UUID, _, _ time.Time) ([]uuid.UUID, error) {
	return nil, nil
}
func (f *fakeStore) MirroredAppIDs(_ context.Context, _ uuid.UUID, _, _ time.Time) ([]uuid.UUID, error) {
	return nil, nil
}
func (f *fakeStore) LiveModuleTimerCountForAccount(_ context.Context, _ uuid.UUID) (int, error) {
	return 0, nil
}

func (f *fakeStore) LiveDomainCountForAccount(_ context.Context, _ uuid.UUID) (int, error) {
	return 0, nil
}
func (f *fakeStore) SettledNewCreationCharges(_ context.Context, _ uuid.UUID, _, _ time.Time) ([]usage.SettledNewCreationChargeRaw, error) {
	return nil, nil
}
func (f *fakeStore) PendingNewCreationCharges(_ context.Context, _ uuid.UUID, _, _, _ time.Time) ([]usage.PendingNewCreationChargeRaw, error) {
	return nil, nil
}
func (f *fakeStore) PendingAddonModuleCharges(_ context.Context, _ uuid.UUID, _ int, _ time.Time) ([]usage.PendingAddonChargeRaw, error) {
	return nil, nil
}
func (f *fakeStore) CoCreatedOverModuleTimerCount(_ context.Context, _, _ uuid.UUID, _ time.Time, _ int) (int, error) {
	return 0, nil
}

func newSvc(store usage.Store) *usage.Service { return usage.NewService(store) }

// at is a fixed trigger time comfortably past the propagation-lag margin so
// the closed-window math is unambiguous.
var at = time.Date(2026, 7, 12, 12, 37, 0, 0, time.UTC)

func TestSyncSSR_RecordsBothMetricsPerFunctionWindow(t *testing.T) {
	fn := testFn("ms-apphost-x", 512) // 512MB
	store := newFakeStore()
	lister := &fakeLister{fns: []awslambdainv.SSRFunction{fn}}

	windows := closedHourWindowsWithLag(at, ssrLookbackHours, propagationLag)
	ts := windows[0].start
	querier := &fakeQuerier{resultsByCall: map[int][]cwtypes.MetricDataResult{
		0: {
			{Id: aws.String("d0"), StatusCode: cwtypes.StatusCodeComplete,
				Timestamps: []time.Time{ts}, Values: []float64{2048}}, // 2048ms
			{Id: aws.String("i0"), StatusCode: cwtypes.StatusCodeComplete,
				Timestamps: []time.Time{ts}, Values: []float64{2000}}, // 2000 invocations
		},
	}}
	idle := &fakeIdle{idle: map[string]bool{}}

	res := syncSSR(context.Background(), newSvc(store), lister, querier, idle, at)
	if res.Failed {
		t.Fatalf("res.Failed = true, err=%v", res.Err)
	}
	if res.Recorded != 2*len(windows) {
		t.Fatalf("Recorded = %d, want %d (2 metrics x %d windows)", res.Recorded, 2*len(windows), len(windows))
	}

	gbSecID := ssrEventID(ssrGBSecondsMetric, fn.AppID, fn.Env, ts)
	ev, ok := store.events[gbSecID]
	if !ok {
		t.Fatalf("no gb_seconds event recorded for window %v", ts)
	}
	wantGBSeconds := (2048.0 / 1000.0) * (512.0 / 1024.0) // duration_s * memory_GB
	if ev.Value != wantGBSeconds {
		t.Errorf("gb_seconds value = %v, want %v", ev.Value, wantGBSeconds)
	}
	if ev.Metric != ssrGBSecondsMetric || ev.AppID != fn.AppID {
		t.Errorf("gb_seconds event = %+v", ev)
	}

	reqID := ssrEventID(ssrRequestCountMetric, fn.AppID, fn.Env, ts)
	ev2, ok := store.events[reqID]
	if !ok {
		t.Fatalf("no request.count event recorded for window %v", ts)
	}
	if ev2.Value != 2.0 { // 2000 invocations / 1000
		t.Errorf("request.count value = %v, want 2.0", ev2.Value)
	}
}

func TestSyncSSR_IdlePrefilterSkipsConfirmedIdleFunctions(t *testing.T) {
	idleFn := testFn("ms-apphost-idle", 512)
	activeFn := testFn("ms-apphost-active", 512)
	lister := &fakeLister{fns: []awslambdainv.SSRFunction{idleFn, activeFn}}

	windows := closedHourWindowsWithLag(at, ssrLookbackHours, propagationLag)
	// A function is only pre-filtered out when EVERY window's own
	// immediately-preceding hour is confirmed idle — not just window[0]'s.
	// window[1]'s preceding hour is window[0] itself, window[2]'s is
	// window[1], etc., so all of them must be seeded here.
	idleEvents := map[string]bool{}
	prior := windows[0].start.Add(-time.Hour)
	idleEvents[ssrEventID(ssrRequestCountMetric, idleFn.AppID, idleFn.Env, prior)] = true
	for _, w := range windows {
		idleEvents[ssrEventID(ssrRequestCountMetric, idleFn.AppID, idleFn.Env, w.start)] = true
	}

	idle := &fakeIdle{idle: idleEvents}
	ts := windows[0].start
	querier := &fakeQuerier{resultsByCall: map[int][]cwtypes.MetricDataResult{
		0: {
			{Id: aws.String("d0"), StatusCode: cwtypes.StatusCodeComplete, Timestamps: []time.Time{ts}, Values: []float64{100}},
			{Id: aws.String("i0"), StatusCode: cwtypes.StatusCodeComplete, Timestamps: []time.Time{ts}, Values: []float64{10}},
		},
	}}

	res := syncSSR(context.Background(), newSvc(newFakeStore()), lister, querier, idle, at)
	if res.SkippedIdle != 1 {
		t.Errorf("SkippedIdle = %d, want 1", res.SkippedIdle)
	}
	if querier.calls != 1 {
		t.Fatalf("querier.calls = %d, want 1", querier.calls)
	}
	// Only the active function's 2 queries should have been sent — the idle
	// one must never reach GetMetricData at all (bounding the job's own AWS
	// API cost, design doc §8 MEDIUM).
	if querier.batchSizes[0] != 1 {
		t.Errorf("batch function count = %d, want 1 (idle function pre-filtered out)", querier.batchSizes[0])
	}
}

// TestSyncSSR_IdleThenResumeAcrossRuns is the direct regression test for the
// HIGH-severity permanent-data-loss bug: the OLD idle pre-filter computed
// eligibility ONCE per run from a single stale hour (window[0]'s own
// preceding hour) and skipped a function's ENTIRE 3-window batch if that one
// hour was idle — even when the function had already resumed activity in
// window[0] itself (or later). By the time a later run's own single-hour
// check would have re-included the function, the exact resumption window had
// already aged out of the shifted 3-hour lookback, and — because the event_id
// is idempotent/permanent (ON CONFLICT DO NOTHING) — that hour's usage could
// never be recorded, ever.
//
// This test uses storeBackedIdle (not the static fakeIdle) so run 2's idle
// check is driven by what run 1 ACTUALLY recorded, exactly as production's
// pgxIdleChecker reads back its own prior writes.
func TestSyncSSR_IdleThenResumeAcrossRuns(t *testing.T) {
	fn := testFn("ms-apphost-resume", 512)
	store := newFakeStore()
	idle := &storeBackedIdle{store: store}

	// --- Run 1: function has been idle for a long time (confirmed idle one
	// hour before this run's earliest window), then resumes activity starting
	// in exactly that earliest window and stays active through the rest of
	// the batch. ---
	run1At := at
	windows1 := closedHourWindowsWithLag(run1At, ssrLookbackHours, propagationLag)
	longIdleHour := windows1[0].start.Add(-time.Hour)
	priorEventID := ssrEventID(ssrRequestCountMetric, fn.AppID, fn.Env, longIdleHour)
	if _, err := store.InsertUsageEvent(context.Background(), usage.UsageEvent{
		EventID: priorEventID, AppID: fn.AppID, Metric: ssrRequestCountMetric,
		Value: 0, RecordedAt: longIdleHour,
	}); err != nil {
		t.Fatalf("seeding long-idle history event: %v", err)
	}

	lister := &fakeLister{fns: []awslambdainv.SSRFunction{fn}}
	querier1 := &fakeQuerier{resultsByCall: map[int][]cwtypes.MetricDataResult{
		0: {
			{Id: aws.String("d0"), StatusCode: cwtypes.StatusCodeComplete,
				Timestamps: []time.Time{windows1[0].start, windows1[1].start, windows1[2].start},
				Values:     []float64{1000, 1200, 1300}},
			{Id: aws.String("i0"), StatusCode: cwtypes.StatusCodeComplete,
				Timestamps: []time.Time{windows1[0].start, windows1[1].start, windows1[2].start},
				Values:     []float64{50, 60, 70}}, // resumption: 50 invocations in window[0]
		},
	}}

	res1 := syncSSR(context.Background(), newSvc(store), lister, querier1, idle, run1At)
	if res1.SkippedIdle != 0 {
		t.Fatalf("run1 SkippedIdle = %d, want 0 — the function must NOT be skipped: window[1]/window[2]'s own preceding hours (window[0]/window[1]) have no recorded history yet, so they cannot be confirmed idle", res1.SkippedIdle)
	}
	if querier1.calls != 1 {
		t.Fatalf("run1 querier.calls = %d, want 1 — the resumption window must be genuinely queried this run", querier1.calls)
	}

	resumptionEventID := ssrEventID(ssrRequestCountMetric, fn.AppID, fn.Env, windows1[0].start)
	ev, ok := store.events[resumptionEventID]
	if !ok {
		t.Fatalf("resumption window (%v) usage was never recorded — permanently dropped", windows1[0].start)
	}
	if ev.Value != 0.05 { // 50 invocations / 1000
		t.Errorf("resumption window request.count = %v, want 0.05", ev.Value)
	}

	// --- Run 2: one hour later, the lookback shifts. The function stays
	// active. Confirm the sweep continues normally and the run-1 resumption
	// data is still intact (nothing about run 2 should retroactively lose
	// it). ---
	run2At := run1At.Add(time.Hour)
	windows2 := closedHourWindowsWithLag(run2At, ssrLookbackHours, propagationLag)
	if windows2[0].start != windows1[1].start {
		t.Fatalf("test setup assumption broken: windows2[0]=%v, want windows1[1]=%v", windows2[0].start, windows1[1].start)
	}

	querier2 := &fakeQuerier{resultsByCall: map[int][]cwtypes.MetricDataResult{
		0: {
			{Id: aws.String("d0"), StatusCode: cwtypes.StatusCodeComplete,
				Timestamps: []time.Time{windows2[0].start, windows2[1].start, windows2[2].start},
				Values:     []float64{1200, 1300, 1400}},
			{Id: aws.String("i0"), StatusCode: cwtypes.StatusCodeComplete,
				Timestamps: []time.Time{windows2[0].start, windows2[1].start, windows2[2].start},
				Values:     []float64{60, 70, 80}},
		},
	}}

	res2 := syncSSR(context.Background(), newSvc(store), lister, querier2, idle, run2At)
	if res2.SkippedIdle != 0 {
		t.Fatalf("run2 SkippedIdle = %d, want 0 — the function is genuinely active", res2.SkippedIdle)
	}
	if querier2.calls != 1 {
		t.Fatalf("run2 querier.calls = %d, want 1", querier2.calls)
	}

	// The original resumption event from run 1 must still be present and
	// untouched — proving it was durably recorded, not just transiently
	// visible.
	if ev, ok := store.events[resumptionEventID]; !ok || ev.Value != 0.05 {
		t.Fatalf("resumption window event lost or changed after run 2: %+v, ok=%v", ev, ok)
	}
	// And the new window run 2 introduces (windows2[2], not seen by run 1)
	// must also be recorded.
	newWindowEventID := ssrEventID(ssrRequestCountMetric, fn.AppID, fn.Env, windows2[2].start)
	if _, ok := store.events[newWindowEventID]; !ok {
		t.Fatalf("run2's new window (%v) usage was never recorded", windows2[2].start)
	}
}

func TestSyncSSR_BatchErrorIsolatesRemainingBatches(t *testing.T) {
	// ssrFunctionBatchSize is 250 (GetMetricData's 500-query cap / 2 metrics
	// per function), so 251 functions forces exactly two batches: the first
	// 250 (batch 0, made to error) and the 251st alone (batch 1, made to
	// succeed) — this is the only way to exercise per-batch isolation
	// without parameterizing production's fixed batch size.
	fns := make([]awslambdainv.SSRFunction, 0, ssrFunctionBatchSize+1)
	for i := 0; i < ssrFunctionBatchSize+1; i++ {
		fns = append(fns, testFn("ms-apphost-batch", 512))
	}
	lister := &fakeLister{fns: fns}

	windows := closedHourWindowsWithLag(at, ssrLookbackHours, propagationLag)
	ts := windows[0].start
	querier := &fakeQuerier{
		errByCall: map[int]error{0: errors.New("throttled")},
		resultsByCall: map[int][]cwtypes.MetricDataResult{
			1: {
				{Id: aws.String("d0"), StatusCode: cwtypes.StatusCodeComplete, Timestamps: []time.Time{ts}, Values: []float64{100}},
				{Id: aws.String("i0"), StatusCode: cwtypes.StatusCodeComplete, Timestamps: []time.Time{ts}, Values: []float64{10}},
			},
		},
	}
	idle := &fakeIdle{idle: map[string]bool{}}

	res := syncSSR(context.Background(), newSvc(newFakeStore()), lister, querier, idle, at)
	if res.Failed {
		t.Fatalf("res.Failed = true — a single batch error must NOT fail the whole run")
	}
	if res.BatchErrors != 1 {
		t.Errorf("BatchErrors = %d, want 1", res.BatchErrors)
	}
	if querier.calls != 2 {
		t.Fatalf("querier.calls = %d, want 2 (the second batch must still run after the first errored)", querier.calls)
	}
	if res.Recorded != 2*len(windows) {
		t.Errorf("Recorded = %d, want %d (the surviving batch's one function is still recorded)", res.Recorded, 2*len(windows))
	}
}

func TestSyncSSR_EnumerationFailureIsFatal(t *testing.T) {
	lister := &fakeLister{err: errors.New("aws unreachable")}
	querier := &fakeQuerier{}
	idle := &fakeIdle{}

	res := syncSSR(context.Background(), newSvc(newFakeStore()), lister, querier, idle, at)
	if !res.Failed {
		t.Error("res.Failed = false, want true when ListSSRFunctions errors")
	}
	if querier.calls != 0 {
		t.Errorf("querier.calls = %d, want 0 — no inventory means nothing else should run", querier.calls)
	}
}

func TestSyncSSR_RowErrorIsNonFatalAndCounted(t *testing.T) {
	fn := testFn("ms-apphost-x", 512)
	store := newFakeStore()
	windows := closedHourWindowsWithLag(at, ssrLookbackHours, propagationLag)
	ts := windows[0].start
	store.insertErr = errors.New("transient db error")
	store.failEventID = ssrEventID(ssrGBSecondsMetric, fn.AppID, fn.Env, ts)

	lister := &fakeLister{fns: []awslambdainv.SSRFunction{fn}}
	querier := &fakeQuerier{resultsByCall: map[int][]cwtypes.MetricDataResult{
		0: {
			{Id: aws.String("d0"), StatusCode: cwtypes.StatusCodeComplete, Timestamps: []time.Time{ts}, Values: []float64{100}},
			{Id: aws.String("i0"), StatusCode: cwtypes.StatusCodeComplete, Timestamps: []time.Time{ts}, Values: []float64{10}},
		},
	}}
	idle := &fakeIdle{idle: map[string]bool{}}

	res := syncSSR(context.Background(), newSvc(store), lister, querier, idle, at)
	if res.Failed {
		t.Fatalf("res.Failed = true, want false (a row error is never fatal)")
	}
	if res.RowErrors != 1 {
		t.Errorf("RowErrors = %d, want 1", res.RowErrors)
	}
	// The OTHER metric for the same window must still have recorded fine.
	if res.Recorded != 2*len(windows)-1 {
		t.Errorf("Recorded = %d, want %d", res.Recorded, 2*len(windows)-1)
	}
}

func TestSSREventID_StableAndDistinct(t *testing.T) {
	appID := uuid.New()
	ws := time.Date(2026, 7, 12, 10, 0, 0, 0, time.UTC)

	id1 := ssrEventID(ssrGBSecondsMetric, appID, "prod", ws)
	id2 := ssrEventID(ssrGBSecondsMetric, appID, "prod", ws)
	if id1 != id2 {
		t.Errorf("ssrEventID not stable: %q != %q", id1, id2)
	}

	if id3 := ssrEventID(ssrRequestCountMetric, appID, "prod", ws); id3 == id1 {
		t.Errorf("different metric produced the same event_id")
	}
	if id4 := ssrEventID(ssrGBSecondsMetric, appID, "staging", ws); id4 == id1 {
		t.Errorf("different env produced the same event_id")
	}
	if id5 := ssrEventID(ssrGBSecondsMetric, appID, "prod", ws.Add(time.Hour)); id5 == id1 {
		t.Errorf("different window produced the same event_id")
	}
}

func TestClosedHourWindowsWithLag_ExcludesRecentlyClosedWindow(t *testing.T) {
	// at is only 5 minutes past the hour: the [11:00,12:00) window closed 5
	// minutes ago, well inside the 10-minute propagation-lag margin, so it
	// must NOT appear in the swept set yet.
	trigger := time.Date(2026, 7, 12, 12, 5, 0, 0, time.UTC)
	windows := closedHourWindowsWithLag(trigger, 3, 10*time.Minute)

	for _, w := range windows {
		if w.start.Hour() == 11 && w.start.Day() == 12 {
			t.Fatalf("windows = %+v, must not include the [11:00,12:00) window (only 5m past close, inside the 10m lag margin)", windows)
		}
	}
	last := windows[len(windows)-1]
	wantLastStart := time.Date(2026, 7, 12, 10, 0, 0, 0, time.UTC)
	if !last.start.Equal(wantLastStart) {
		t.Errorf("last window start = %v, want %v", last.start, wantLastStart)
	}
}

func TestClosedHourWindowsWithLag_IncludesWindowOnceLagElapses(t *testing.T) {
	// 11 minutes past the hour: now past the 10-minute margin, so
	// [11:00,12:00) should be the latest included window.
	trigger := time.Date(2026, 7, 12, 12, 11, 0, 0, time.UTC)
	windows := closedHourWindowsWithLag(trigger, 3, 10*time.Minute)
	last := windows[len(windows)-1]
	wantLastStart := time.Date(2026, 7, 12, 11, 0, 0, 0, time.UTC)
	if !last.start.Equal(wantLastStart) {
		t.Errorf("last window start = %v, want %v", last.start, wantLastStart)
	}
}
