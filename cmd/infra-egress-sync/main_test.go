package main

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/mirrorstack-ai/billing-engine/internal/account/usage"
	"github.com/mirrorstack-ai/billing-engine/internal/shared/cloudflare"
)

// --- fake Cloudflare analytics client -------------------------------------
//
// NEVER calls the real Cloudflare API (hard rule). It returns canned rows per
// window and can be told to error so the "CF query error fails the run" path is
// exercised. It also records every (datasetName, window) it was queried with so
// a test can assert ONLY closed windows are pulled (the current partial hour is
// excluded).

type fakeCF struct {
	rowsByStart map[time.Time][]cloudflare.EgressRow
	err         error
	queried     []hourWindow
	dataset     string
}

func (f *fakeCF) QueryEgressWindow(_ context.Context, datasetName string, start, end time.Time) ([]cloudflare.EgressRow, error) {
	f.dataset = datasetName
	f.queried = append(f.queried, hourWindow{start: start, end: end})
	if f.err != nil {
		return nil, f.err
	}
	return f.rowsByStart[start], nil
}

// --- fake usage.Store -----------------------------------------------------
//
// Minimal in-memory Store satisfying usage.Store so the test drives a REAL
// usage.Service through RecordInfraUsage (the production ingest path) — only the
// persistence is faked. The infra plane needs no metric_definitions row (kind is
// platform-owned), so LookupMetricDefinition is unused by this path.

type fakeStore struct {
	events      map[string]usage.UsageEvent // event_id → event (idempotency)
	insertErr   error                       // if set, InsertUsageEvent fails for failEventID only
	failEventID string                      // the one event_id whose insert errors (per-row RowErrors path)
}

func newFakeStore() *fakeStore {
	return &fakeStore{events: map[string]usage.UsageEvent{}}
}

func (f *fakeStore) InsertUsageEvent(_ context.Context, ev usage.UsageEvent) (bool, error) {
	if f.insertErr != nil && ev.EventID == f.failEventID {
		return false, f.insertErr // simulate a transient DB error for one row
	}
	if _, exists := f.events[ev.EventID]; exists {
		return false, nil // ON CONFLICT(event_id) DO NOTHING
	}
	f.events[ev.EventID] = ev
	return true, nil
}

func (f *fakeStore) AccountByOwner(_ context.Context, _ usage.Owner) (uuid.UUID, bool, error) {
	return uuid.Nil, false, nil // egress rows carry no owner → lazy event
}

func (f *fakeStore) AccountAnchorDay(_ context.Context, _ uuid.UUID) (int, error) {
	return 1, nil // egress sync never reads a period window; calendar-month default
}

func (f *fakeStore) LookupMetricDefinition(_ context.Context, _ uuid.UUID, _ string) (usage.MetricDefinition, bool, error) {
	return usage.MetricDefinition{}, false, nil
}
func (f *fakeStore) UpsertMetricDefinitions(_ context.Context, _ []usage.MetricDeclaration) error {
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

func newSvc(store usage.Store) *usage.Service { return usage.NewService(store) }

// at is a fixed trigger time mid-hour so the closed-window math is unambiguous.
var at = time.Date(2026, 6, 15, 12, 37, 0, 0, time.UTC)

func TestSyncEgress_AggregatesRowsIntoRecordInfraUsage(t *testing.T) {
	app1, app2 := uuid.New(), uuid.New()
	mod := uuid.New().String()
	// Put rows only in the most-recent closed hour [11:00, 12:00).
	win := time.Date(2026, 6, 15, 11, 0, 0, 0, time.UTC)
	cf := &fakeCF{rowsByStart: map[time.Time][]cloudflare.EgressRow{
		win: {
			{AppID: app1.String(), ModuleID: mod, Bytes: 1024},
			{AppID: app2.String(), ModuleID: "", Bytes: 2048},
		},
	}}
	store := newFakeStore()

	res := syncEgress(context.Background(), newSvc(store), cf, at)
	require.False(t, res.Failed)
	require.Equal(t, 2, res.Recorded)
	require.Equal(t, 2, len(store.events))

	// Every event is the reserved egress metric, stamped under the infra
	// sentinel module, with the byte SUM as the value and recorded_at = the
	// window start (when the egress occurred), not now().
	for _, ev := range store.events {
		require.Equal(t, egressMetric, ev.Metric)
		require.Equal(t, usage.PlatformInfraModuleID(), ev.ModuleID)
		require.Equal(t, usage.KindSum, ev.Kind)
		require.True(t, win.Equal(ev.RecordedAt), "recorded_at must be the window start")
	}
	// Values land on the right app.
	require.Equal(t, float64(1024), store.events[egressEventID(app1, mod, win)].Value)
	require.Equal(t, float64(2048), store.events[egressEventID(app2, "", win)].Value)
}

func TestSyncEgress_DeterministicEventIDIsIdempotent(t *testing.T) {
	app := uuid.New()
	mod := uuid.New().String()
	win := time.Date(2026, 6, 15, 11, 0, 0, 0, time.UTC)
	rows := map[time.Time][]cloudflare.EgressRow{
		win: {{AppID: app.String(), ModuleID: mod, Bytes: 4096}},
	}
	store := newFakeStore()
	svc := newSvc(store)

	first := syncEgress(context.Background(), svc, &fakeCF{rowsByStart: rows}, at)
	require.Equal(t, 1, first.Recorded)
	require.Equal(t, 0, first.Deduped)

	// Re-run the SAME window: deterministic event_id → ON CONFLICT dedupes,
	// nothing double-recorded.
	second := syncEgress(context.Background(), svc, &fakeCF{rowsByStart: rows}, at)
	require.Equal(t, 0, second.Recorded)
	require.Equal(t, 1, second.Deduped)
	require.Equal(t, 1, len(store.events), "re-run must not double-write")
}

func TestEgressEventID_StableAndDistinct(t *testing.T) {
	app := uuid.New()
	win := time.Date(2026, 6, 15, 11, 0, 0, 0, time.UTC)

	// Stable: same tuple → same id (the idempotency contract).
	require.Equal(t, egressEventID(app, "m", win), egressEventID(app, "m", win))
	// Distinct on each tuple component.
	require.NotEqual(t, egressEventID(app, "m", win), egressEventID(uuid.New(), "m", win))
	require.NotEqual(t, egressEventID(app, "m", win), egressEventID(app, "n", win))
	require.NotEqual(t, egressEventID(app, "m", win), egressEventID(app, "m", win.Add(time.Hour)))
}

func TestSyncEgress_OnlyClosedWindowsQueried(t *testing.T) {
	cf := &fakeCF{rowsByStart: map[time.Time][]cloudflare.EgressRow{}}
	syncEgress(context.Background(), newSvc(newFakeStore()), cf, at)

	require.Equal(t, egressDataset, cf.dataset)
	require.Len(t, cf.queried, lookbackHours)
	// The current partial hour [12:00, 13:00) must NEVER be queried — every
	// queried window ends at or before the top of the trigger hour.
	currentHourStart := at.Truncate(time.Hour) // 12:00
	for _, w := range cf.queried {
		require.True(t, w.end.Equal(currentHourStart) || w.end.Before(currentHourStart),
			"window %s–%s must be fully closed (end ≤ %s)", w.start, w.end, currentHourStart)
		require.True(t, w.end.Equal(w.start.Add(time.Hour)), "each window is one hour")
	}
	// The last (most recent) closed window is [11:00, 12:00).
	last := cf.queried[len(cf.queried)-1]
	require.True(t, last.start.Equal(time.Date(2026, 6, 15, 11, 0, 0, 0, time.UTC)))
	require.True(t, last.end.Equal(currentHourStart))
}

func TestSyncEgress_SkipsUnparseableAppID(t *testing.T) {
	good := uuid.New()
	win := time.Date(2026, 6, 15, 11, 0, 0, 0, time.UTC)
	cf := &fakeCF{rowsByStart: map[time.Time][]cloudflare.EgressRow{
		win: {
			{AppID: "", ModuleID: "m", Bytes: 1},                // empty → skip
			{AppID: "not-a-uuid", ModuleID: "m", Bytes: 2},      // garbage → skip
			{AppID: uuid.Nil.String(), ModuleID: "m", Bytes: 3}, // all-zeros → skip
			{AppID: good.String(), ModuleID: "m", Bytes: 4},     // valid → recorded
		},
	}}
	store := newFakeStore()

	res := syncEgress(context.Background(), newSvc(store), cf, at)
	require.False(t, res.Failed)
	require.Equal(t, 3, res.Skipped)
	require.Equal(t, 1, res.Recorded)
	require.Equal(t, 1, len(store.events))
	require.Equal(t, float64(4), store.events[egressEventID(good, "m", win)].Value)
}

func TestSyncEgress_RowErrorIsNonFatal(t *testing.T) {
	bad, good := uuid.New(), uuid.New()
	mod := uuid.New().String()
	win := time.Date(2026, 6, 15, 11, 0, 0, 0, time.UTC)
	cf := &fakeCF{rowsByStart: map[time.Time][]cloudflare.EgressRow{
		win: {
			{AppID: bad.String(), ModuleID: mod, Bytes: 100},  // insert errors → RowErrors
			{AppID: good.String(), ModuleID: mod, Bytes: 200}, // still lands
		},
	}}
	store := newFakeStore()
	store.insertErr = errors.New("transient db error")
	store.failEventID = egressEventID(bad, mod, win)

	res := syncEgress(context.Background(), newSvc(store), cf, at)
	// A per-row RecordInfraUsage error is logged + counted but never aborts the
	// sweep — the run is NOT marked Failed and the good row still records.
	require.False(t, res.Failed)
	require.Equal(t, 1, res.RowErrors)
	require.Equal(t, 1, res.Recorded)
	require.Equal(t, 1, len(store.events))
	require.Equal(t, float64(200), store.events[egressEventID(good, mod, win)].Value)
}

func TestSyncEgress_CFQueryErrorFailsCleanly(t *testing.T) {
	wantErr := errors.New("cloudflare 401 unauthorized")
	cf := &fakeCF{err: wantErr}
	store := newFakeStore()

	res := syncEgress(context.Background(), newSvc(store), cf, at)
	require.True(t, res.Failed)
	require.ErrorIs(t, res.Err, wantErr)
	// Abort on the FIRST window's error — no partial double-write.
	require.Equal(t, 0, res.Recorded)
	require.Empty(t, store.events)
	require.Len(t, cf.queried, 1, "must abort the sweep on the first query error")
}

func TestClosedHourWindows(t *testing.T) {
	got := closedHourWindows(at, 3)
	require.Len(t, got, 3)
	// Ascending, contiguous, ending at the top of the trigger hour.
	require.True(t, got[0].start.Equal(time.Date(2026, 6, 15, 9, 0, 0, 0, time.UTC)))
	require.True(t, got[2].end.Equal(time.Date(2026, 6, 15, 12, 0, 0, 0, time.UTC)))
	for i := 1; i < len(got); i++ {
		require.True(t, got[i].start.Equal(got[i-1].end), "windows must be contiguous")
	}
}
