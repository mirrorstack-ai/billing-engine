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
	// requestsByStart feeds QueryRequestWindow (billing-engine#212); a
	// window absent here answers with no rows, like a dataset with no
	// double2 yet.
	requestsByStart map[time.Time][]cloudflare.RequestRow
	requestErr      error
}

func (f *fakeCF) QueryRequestWindow(_ context.Context, _ string, start, _ time.Time) ([]cloudflare.RequestRow, error) {
	if f.requestErr != nil {
		return nil, f.requestErr
	}
	return f.requestsByStart[start], nil
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

func (f *fakeStore) CheckUsageEventID(context.Context, string, []byte) (bool, error) {
	return false, nil
}

func (f *fakeStore) InsertUsageObservation(ctx context.Context, ev usage.UsageEvent, _, _ time.Time, _ usage.UsageRejectionReason) (bool, float64, error) {
	recorded, err := f.InsertUsageEvent(ctx, ev)
	if !recorded || err != nil {
		return recorded, 0, err
	}
	return true, ev.Value, nil
}

func (f *fakeStore) DefaultCardCountry(context.Context, uuid.UUID) (string, bool, error) {
	return "", false, nil // no default card on file → tax not configured (migration 074)
}

func (f *fakeStore) AccountByOwner(_ context.Context, _ usage.Owner) (uuid.UUID, bool, error) {
	return uuid.Nil, false, nil // egress rows carry no owner → lazy event
}

func (f *fakeStore) AppOwnerOrg(context.Context, uuid.UUID) (uuid.UUID, bool, error) {
	return uuid.Nil, false, nil
}

func (f *fakeStore) AccountAnchorDay(_ context.Context, _ uuid.UUID) (int, error) {
	return 1, nil // egress sync never reads a period window; calendar-month default
}
func (f *fakeStore) AccountActivation(context.Context, uuid.UUID) (time.Time, bool, error) {
	return time.Time{}, false, nil
}

func (f *fakeStore) LookupMetricDefinition(_ context.Context, _ uuid.UUID, _ string) (usage.MetricDefinition, bool, error) {
	return usage.MetricDefinition{}, false, nil
}
func (f *fakeStore) UpsertMetricDefinitions(_ context.Context, _ []usage.MetricDeclaration) error {
	return nil
}
func (f *fakeStore) UpsertMetricVersionPrices(_ context.Context, _ []usage.MetricVersionPrice) error {
	return nil
}
func (f *fakeStore) SyncInfraPriceOverrides(_ context.Context, _ uuid.UUID, _ bool, _ []usage.InfraPriceOverride) error {
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

func (f *fakeStore) AppModuleInfraBill(_ context.Context, _, _ uuid.UUID, _, _ time.Time, _ bool) ([]usage.AppModuleInfraUsage, error) {
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

func (f *fakeStore) LiveOverModuleTimerCountForApp(_ context.Context, _, _ uuid.UUID, _ int) (int, error) {
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
func (f *fakeStore) UnresolvedOneTimeCharges(_ context.Context, _ uuid.UUID, _, _ int) ([]usage.UnresolvedOneTimeChargeRaw, error) {
	return nil, nil
}
func (f *fakeStore) CoCreatedOverModuleTimerCount(_ context.Context, _, _ uuid.UUID, _ time.Time, _ int) (int, error) {
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

	// Every event is the CDN egress metric (migration 078), stamped under the
	// infra sentinel module, with the byte SUM in GiB as the value and
	// recorded_at = the window start (when the egress occurred), not now().
	for _, ev := range store.events {
		require.Equal(t, cdnEgressMetric, ev.Metric)
		require.Equal(t, usage.PlatformInfraModuleID(), ev.ModuleID)
		require.Equal(t, usage.KindSum, ev.Kind)
		require.True(t, win.Equal(ev.RecordedAt), "recorded_at must be the window start")
	}
	// Values land on the right app.
	require.InDelta(t, 1024.0/bytesPerGiB, store.events[egressEventID(cdnEgressMetric, app1, mod, win)].Value, 1e-15)
	require.InDelta(t, 2048.0/bytesPerGiB, store.events[egressEventID(cdnEgressMetric, app2, "", win)].Value, 1e-15)
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
	require.Equal(t, egressEventID(egressMetric, app, "m", win), egressEventID(egressMetric, app, "m", win))
	// Distinct on each tuple component.
	require.NotEqual(t, egressEventID(egressMetric, app, "m", win), egressEventID(egressMetric, uuid.New(), "m", win))
	require.NotEqual(t, egressEventID(egressMetric, app, "m", win), egressEventID(egressMetric, app, "n", win))
	require.NotEqual(t, egressEventID(egressMetric, app, "m", win), egressEventID(egressMetric, app, "m", win.Add(time.Hour)))
	// Distinct on the metric itself — the SSR and static-file metrics must
	// never collide on event_id even for an otherwise-identical tuple.
	require.NotEqual(t, egressEventID(egressMetric, app, "m", win), egressEventID(ssrEgressMetric, app, "m", win))
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
	require.InDelta(t, 4.0/bytesPerGiB, store.events[egressEventID(cdnEgressMetric, good, "m", win)].Value, 1e-15)
}

func TestSyncEgress_OrgRowsAreCountedNotRecorded(t *testing.T) {
	org := uuid.New()
	win := time.Date(2026, 6, 15, 11, 0, 0, 0, time.UTC)
	cf := &fakeCF{rowsByStart: map[time.Time][]cloudflare.EgressRow{
		win: {
			{OrgID: org.String(), Bytes: 100},                    // org → counted
			{OrgID: uuid.New().String(), Bytes: 50},              // another org → counted
			{OrgID: "not-a-uuid", Bytes: 7},                      // garbage org → skip
			{OrgID: uuid.Nil.String(), Bytes: 8},                 // all-zeros org → skip
			{AppID: "not-a-uuid", OrgID: org.String(), Bytes: 9}, // bad app wins → skip
		},
	}}
	store := newFakeStore()

	res := syncEgress(context.Background(), newSvc(store), cf, at)
	require.False(t, res.Failed)
	require.Equal(t, 2, res.OrgRows)
	require.InDelta(t, 150.0, res.OrgBytes, 1e-9)
	require.Equal(t, 3, res.Skipped)
	require.Equal(t, 0, res.Recorded)
	require.Empty(t, store.events)
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
	store.failEventID = egressEventID(cdnEgressMetric, bad, mod, win)

	res := syncEgress(context.Background(), newSvc(store), cf, at)
	// A per-row RecordInfraUsage error is logged + counted but never aborts the
	// sweep — the run is NOT marked Failed and the good row still records.
	require.False(t, res.Failed)
	require.Equal(t, 1, res.RowErrors)
	require.Equal(t, 1, res.Recorded)
	require.Equal(t, 1, len(store.events))
	require.InDelta(t, 200.0/bytesPerGiB, store.events[egressEventID(cdnEgressMetric, good, mod, win)].Value, 1e-15)
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

// --- SSR-origin vs static-file egress routing (migration 046) -------------

func TestSyncEgress_SSRRowRecordsUnderNewMetricInGiB(t *testing.T) {
	app := uuid.New()
	win := time.Date(2026, 6, 15, 11, 0, 0, 0, time.UTC)
	const rawBytes = float64(2 * bytesPerGiB) // exactly 2 GiB, for an exact expected value
	cf := &fakeCF{rowsByStart: map[time.Time][]cloudflare.EgressRow{
		win: {{AppID: app.String(), ModuleID: ssrModuleIDSentinel, Bytes: rawBytes}},
	}}
	store := newFakeStore()

	res := syncEgress(context.Background(), newSvc(store), cf, at)
	require.False(t, res.Failed)
	require.Equal(t, 1, res.Recorded)
	require.Equal(t, 1, len(store.events))

	ev := store.events[egressEventID(ssrEgressMetric, app, ssrModuleIDSentinel, win)]
	require.Equal(t, ssrEgressMetric, ev.Metric)
	require.Equal(t, usage.KindSum, ev.Kind)
	require.Equal(t, usage.PlatformInfraModuleID(), ev.ModuleID)
	// Producer converts raw bytes to GiB for the ssr branch only.
	require.InDelta(t, 2.0, ev.Value, 1e-9)
	require.True(t, win.Equal(ev.RecordedAt))
}

// TestSyncEgress_StaticFileRowsRecordUnderTheCDNKeyInGiB pins migration 078:
// a row with blob2="" and a row with a real module_id record under
// infra.egress.cdn.bytes, converted to GiB, and NEVER under the retired
// price-0 infra.egress.bytes — even in a window that also contains an SSR row,
// which keeps its own key. A static row landing on the old key would be free;
// a static row landing on the new key in raw bytes would be a 2^30 overcharge.
func TestSyncEgress_StaticFileRowsRecordUnderTheCDNKeyInGiB(t *testing.T) {
	appEmpty, appMod, appSSR := uuid.New(), uuid.New(), uuid.New()
	mod := uuid.New().String()
	win := time.Date(2026, 6, 15, 11, 0, 0, 0, time.UTC)
	cf := &fakeCF{rowsByStart: map[time.Time][]cloudflare.EgressRow{
		win: {
			{AppID: appEmpty.String(), ModuleID: "", Bytes: float64(2 * bytesPerGiB)},
			{AppID: appMod.String(), ModuleID: mod, Bytes: float64(bytesPerGiB / 2)},
			{AppID: appSSR.String(), ModuleID: ssrModuleIDSentinel, Bytes: float64(bytesPerGiB)},
		},
	}}
	store := newFakeStore()

	res := syncEgress(context.Background(), newSvc(store), cf, at)
	require.False(t, res.Failed)
	require.Equal(t, 3, res.Recorded)
	require.Equal(t, 3, len(store.events))

	emptyEv := store.events[egressEventID(cdnEgressMetric, appEmpty, "", win)]
	require.Equal(t, cdnEgressMetric, emptyEv.Metric)
	require.InDelta(t, 2.0, emptyEv.Value, 1e-9, "static-file bytes are GiB-converted")

	modEv := store.events[egressEventID(cdnEgressMetric, appMod, mod, win)]
	require.Equal(t, cdnEgressMetric, modEv.Metric)
	require.InDelta(t, 0.5, modEv.Value, 1e-9, "static-file bytes are GiB-converted")

	ssrEv := store.events[egressEventID(ssrEgressMetric, appSSR, ssrModuleIDSentinel, win)]
	require.Equal(t, ssrEgressMetric, ssrEv.Metric)
	require.InDelta(t, 1.0, ssrEv.Value, 1e-9, "ssr bytes keep their own key")

	for _, ev := range store.events {
		require.NotEqual(t, egressMetric, ev.Metric, "nothing records under the retired price-0 key any more")
	}
}

// TestSyncEgress_SSRAndStaticEventIDsNeverCollide proves the idempotent
// event_id scheme holds independently for both metrics: re-running the same
// window dedupes each row exactly once (no double-recording), and the SSR row
// and a static row sharing the same app_id + window never collide on
// event_id even though they are, in the raw dataset, adjacent groups.
func TestSyncEgress_SSRAndStaticEventIDsNeverCollide(t *testing.T) {
	app := uuid.New() // SAME app_id emits both a static row and an ssr row
	mod := uuid.New().String()
	win := time.Date(2026, 6, 15, 11, 0, 0, 0, time.UTC)
	rows := map[time.Time][]cloudflare.EgressRow{
		win: {
			{AppID: app.String(), ModuleID: mod, Bytes: 4096},
			{AppID: app.String(), ModuleID: ssrModuleIDSentinel, Bytes: float64(bytesPerGiB)},
		},
	}
	store := newFakeStore()
	svc := newSvc(store)

	first := syncEgress(context.Background(), svc, &fakeCF{rowsByStart: rows}, at)
	require.Equal(t, 2, first.Recorded)
	require.Equal(t, 0, first.Deduped)
	require.Equal(t, 2, len(store.events), "the static and ssr rows must record as TWO distinct events")

	staticID := egressEventID(cdnEgressMetric, app, mod, win)
	ssrID := egressEventID(ssrEgressMetric, app, ssrModuleIDSentinel, win)
	require.NotEqual(t, staticID, ssrID)
	require.InDelta(t, 4096.0/bytesPerGiB, store.events[staticID].Value, 1e-15)
	require.InDelta(t, 1.0, store.events[ssrID].Value, 1e-9)

	// Re-run the SAME window: both dedupe via ON CONFLICT, neither
	// double-records, and no cross-metric collision is introduced.
	second := syncEgress(context.Background(), svc, &fakeCF{rowsByStart: rows}, at)
	require.Equal(t, 0, second.Recorded)
	require.Equal(t, 2, second.Deduped)
	require.Equal(t, 2, len(store.events), "re-run must not double-write either metric")
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

// --- The request pass (billing-engine#212, cdn-worker#58) -------------------

// Requests fold per (app, module) across tiers into ONE infra.cdn.request.count
// event, the r2-hit subset into ONE infra.cdn.r2.read.count event, both in
// units of 1k; rows outside the prod stage (dev, or "" from before the worker
// carried a stage) are dropped, and a group with no R2 reads records no
// R2 event at all.
func TestSyncEgress_RequestPassFoldsTiersPerAppAndBillsPer1k(t *testing.T) {
	app := uuid.New()
	win := time.Date(2026, 6, 15, 11, 0, 0, 0, time.UTC)
	cf := &fakeCF{
		rowsByStart: map[time.Time][]cloudflare.EgressRow{},
		requestsByStart: map[time.Time][]cloudflare.RequestRow{
			win: {
				{AppID: app.String(), ModuleID: "", Tier: "edge-hit", Stage: "prod", Requests: 1500},
				{AppID: app.String(), ModuleID: "", Tier: "r2-hit", Stage: "prod", Requests: 400},
				{AppID: app.String(), ModuleID: "", Tier: "s3-origin", Stage: "prod", Requests: 100},
				{AppID: app.String(), ModuleID: "", Tier: "edge-hit", Stage: "dev", Requests: 9999}, // dev: never billed
				{AppID: app.String(), ModuleID: "", Tier: "", Stage: "", Requests: 0},               // pre-#58 row
				{AppID: "not-a-uuid", ModuleID: "", Tier: "edge-hit", Stage: "prod", Requests: 7},   // unattributable
			},
		},
	}
	store := newFakeStore()

	res := syncEgress(context.Background(), newSvc(store), cf, at)
	require.False(t, res.Failed)
	require.Equal(t, 6, res.RequestRows)
	require.Equal(t, 3, res.RequestSkipped, "dev, pre-#58 and unattributable rows")
	require.Equal(t, 2, res.RequestRecorded, "one request event + one R2 event")
	require.Equal(t, 2, len(store.events))

	req := store.events[egressEventID(cdnRequestMetric, app, "", win)]
	require.Equal(t, cdnRequestMetric, req.Metric)
	require.Equal(t, usage.KindCount, req.Kind)
	require.InDelta(t, 2.0, req.Value, 1e-9, "(1500 + 400 + 100) / 1000 — all tiers, prod only")
	require.True(t, win.Equal(req.RecordedAt))

	r2 := store.events[egressEventID(cdnR2ReadMetric, app, "", win)]
	require.Equal(t, cdnR2ReadMetric, r2.Metric)
	require.InDelta(t, 0.4, r2.Value, 1e-9, "400 / 1000 — the r2-hit rows only")

	// Idempotent: the same window again dedupes both.
	again := syncEgress(context.Background(), newSvc(store), cf, at)
	require.Equal(t, 0, again.RequestRecorded)
	require.Equal(t, 2, again.Deduped)
	require.Equal(t, 2, len(store.events))

	// No R2 reads → no R2 event, and a request query error is fatal.
	store2 := newFakeStore()
	cf2 := &fakeCF{rowsByStart: map[time.Time][]cloudflare.EgressRow{}, requestsByStart: map[time.Time][]cloudflare.RequestRow{
		win: {{AppID: app.String(), ModuleID: "m", Tier: "edge-hit", Stage: "prod", Requests: 250}},
	}}
	res2 := syncEgress(context.Background(), newSvc(store2), cf2, at)
	require.Equal(t, 1, res2.RequestRecorded)
	_, hasR2 := store2.events[egressEventID(cdnR2ReadMetric, app, "m", win)]
	require.False(t, hasR2)
	require.InDelta(t, 0.25, store2.events[egressEventID(cdnRequestMetric, app, "m", win)].Value, 1e-9)

	cf3 := &fakeCF{rowsByStart: map[time.Time][]cloudflare.EgressRow{}, requestErr: errors.New("cf down")}
	res3 := syncEgress(context.Background(), newSvc(newFakeStore()), cf3, at)
	require.True(t, res3.Failed, "a request query failure aborts the sweep like an egress one")
}
