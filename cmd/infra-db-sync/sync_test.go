package main

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/mirrorstack-ai/billing-engine/internal/account/billing"
	"github.com/mirrorstack-ai/billing-engine/internal/account/usage"
)

// fakeReader drives the whole sweep with no database.
type fakeReader struct {
	tables     []tableSize
	tablesErr  error
	installs   map[uuid.UUID][]install
	installErr map[uuid.UUID]error
}

func (f *fakeReader) TableSizes(context.Context) ([]tableSize, error) {
	return f.tables, f.tablesErr
}

func (f *fakeReader) Installs(_ context.Context, app uuid.UUID) ([]install, error) {
	if err := f.installErr[app]; err != nil {
		return nil, err
	}
	return f.installs[app], nil
}

// fakeHistory answers the billing-side question "which (app, module) pairs
// last stood at a positive level", with no database.
type fakeHistory struct {
	pairs []pair
	err   error
}

func (f *fakeHistory) LivePairs(context.Context, time.Time) ([]pair, error) { return f.pairs, f.err }

// fakeRecorder keeps every request and can answer a chosen error per call.
type fakeRecorder struct {
	reqs    []usage.RecordInfraUsageRequest
	seen    map[string]bool
	errFor  func(usage.RecordInfraUsageRequest) error
	deduped bool
}

func (f *fakeRecorder) RecordInfraUsage(_ context.Context, req usage.RecordInfraUsageRequest) (*usage.RecordInfraUsageResponse, error) {
	if f.errFor != nil {
		if err := f.errFor(req); err != nil {
			return nil, err
		}
	}
	if f.seen == nil {
		f.seen = map[string]bool{}
	}
	f.reqs = append(f.reqs, req)
	dup := f.seen[req.EventID]
	f.seen[req.EventID] = true
	return &usage.RecordInfraUsageResponse{Recorded: !dup}, nil
}

var (
	appA = uuid.MustParse("11111111-1111-4111-8111-111111111111")
	appB = uuid.MustParse("22222222-2222-4222-8222-222222222222")
	modQ = uuid.MustParse("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa") // quiz
	modC = uuid.MustParse("bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb") // second module
	modE = uuid.MustParse("cccccccc-cccc-4ccc-8ccc-cccccccccccc") // installed, no tables
	modX = uuid.MustParse("dddddddd-dddd-4ddd-8ddd-dddddddddddd") // uninstalled
	appC = uuid.MustParse("33333333-3333-4333-8333-333333333333") // dropped app
)

const gib = int64(1) << 30

// phys is the REAL physical table prefix of a deployed module: m<32 hex of the
// module uuid>_ (api-platform modulePhysicalPrefix, app-module-sdk
// ids.NormalizeModuleID). Never "<username>_<slug>_": that is a display value.
func phys(m uuid.UUID) string { return "m" + strings.ReplaceAll(m.String(), "-", "") + "_" }

func sync0(rec recorder, rd dbReader) syncResult {
	return syncDB(context.Background(), rec, rd, &fakeHistory{}, at())
}

func at() time.Time { return time.Date(2026, 10, 5, 12, 30, 0, 0, time.UTC) }

func TestAppSchemaName_RoundTripsAndRejectsEverythingElse(t *testing.T) {
	require.Equal(t, "app_11111111_1111_4111_8111_111111111111", appSchemaName(appA))
	got, ok := parseAppSchema(appSchemaName(appA))
	require.True(t, ok)
	require.Equal(t, appA, got)

	for _, bad := range []string{
		"", "app_", "app_1", "ms_billing", "mod_m0123456789abcdef0123456789abcdef",
		"org_11111111_1111_4111_8111_111111111111",
		"app_11111111-1111-4111-8111-111111111111",  // hyphens: not the platform's form
		"APP_11111111_1111_4111_8111_111111111111",  // case-sensitive, as AppSchemaName lowercases
		"app_11111111_1111_4111_8111_11111111111g",  // not hex
		"app_11111111_1111_4111_8111_1111111111111", // one char too long
		"app_11111111_1111_4111_8111_111111111111_x",
	} {
		_, ok := parseAppSchema(bad)
		require.False(t, ok, "%q must not be mistaken for an app schema", bad)
	}
}

func TestModulePrefix_PinnedToThePlatformSpelling(t *testing.T) {
	// A literal, not derived: if the platform's naming ever moves, this fails
	// here rather than silently attributing nothing.
	m := uuid.MustParse("0194b3c2-7d1e-7a55-9c3f-5b8e2d4f6a10")
	require.Equal(t, "m0194b3c27d1e7a559c3f5b8e2d4f6a10_", modulePrefix(m))
	require.Len(t, modulePrefix(m), 34)
}

func TestAttribute_RealPhysicalNamesAndPlatformTablesAreNobodys(t *testing.T) {
	installs := []install{{Module: modQ}, {Module: modC}, {Module: modE}}
	tables := []tableSize{
		{Table: phys(modQ) + "attempts", Bytes: 3 * gib},
		{Table: phys(modC) + "answers", Bytes: 5 * gib},
		{Table: phys(modC) + "answers_dup", Bytes: gib},
		// a table name over 29 chars is stored truncated at 63; the prefix is intact
		{Table: (phys(modQ) + "a_very_long_table_name_that_overflows_the_limit")[:63], Bytes: gib},
		{Table: "members", Bytes: 7 * gib},                                                            // platform-owned
		{Table: "module_install", Bytes: 9 * gib},                                                     // platform-owned
		{Table: phys(modX) + "things", Bytes: gib},                                                    // uninstalled module's leftovers
		{Table: "m" + strings.ReplaceAll(modQ.String(), "-", "") + "noscore", Bytes: gib},             // no "_" after the id
		{Table: "M" + strings.ToUpper(strings.ReplaceAll(modQ.String(), "-", "")) + "_x", Bytes: gib}, // wrong case
	}
	per, unattributed := attribute(installs, tables)

	require.EqualValues(t, 4*gib, per[modQ])
	require.EqualValues(t, 6*gib, per[modC])
	v, ok := per[modE]
	require.True(t, ok, "an installed module with no tables must still be present: 0 is a level, and the rollup carries the last level across a gap")
	require.EqualValues(t, 0, v)
	require.EqualValues(t, (7+9+1+1+1)*gib, unattributed, "platform, orphan and mis-shaped names are counted aside, never billed to a module")
}

func TestAttribute_LegacyUsernameSlugNamesAreNotAttributed(t *testing.T) {
	// "<username>_<slug>_" is module_install.prefix, a display value. No deployed
	// schema names tables that way, so it is deliberately not a fallback: if one
	// ever did, its bytes would be loudly unattributed (syncDB fails on it).
	per, un := attribute([]install{{Module: modQ}}, []tableSize{{Table: "acme_quiz_attempts", Bytes: 2 * gib}})
	require.EqualValues(t, 0, per[modQ])
	require.EqualValues(t, 2*gib, un)
}

func TestAttribute_ATableIsChargedToExactlyOneModule(t *testing.T) {
	tables := []tableSize{{Table: phys(modQ) + "t", Bytes: 4 * gib}, {Table: phys(modC) + "t", Bytes: gib}}
	// the same install listed twice must not double a module's bytes
	per, un := attribute([]install{{Module: modQ}, {Module: modQ}, {Module: modC}}, tables)
	require.EqualValues(t, 4*gib, per[modQ])
	require.EqualValues(t, gib, per[modC])
	require.EqualValues(t, 0, un)
}

func twoApps() *fakeReader {
	return &fakeReader{
		tables: []tableSize{
			{Schema: appSchemaName(appA), Table: phys(modQ) + "attempts", Bytes: 2 * gib},
			{Schema: appSchemaName(appA), Table: "members", Bytes: gib},
			{Schema: appSchemaName(appB), Table: phys(modQ) + "attempts", Bytes: gib / 2},
			{Schema: "ms_billing", Table: "usage_events", Bytes: 99 * gib}, // never an app schema
		},
		installs: map[uuid.UUID][]install{
			appA: {{Module: modQ}, {Module: modE}},
			appB: {{Module: modQ}},
		},
	}
}

func TestSyncDB_EmitsALevelPerAppModuleAtEveryClosedHour(t *testing.T) {
	rec := &fakeRecorder{}
	res := syncDB(context.Background(), rec, twoApps(), &fakeHistory{}, at())

	require.False(t, res.Failed)
	require.Equal(t, lookbackHours, res.Samples)
	require.Equal(t, 3, res.Modules, "(A,quiz) (A,empty) (B,quiz)")
	require.Equal(t, 3*lookbackHours, res.Recorded)
	require.Len(t, rec.reqs, 3*lookbackHours)

	type k struct{ app, mod uuid.UUID }
	level := map[k]float64{}
	instants := map[time.Time]bool{}
	for _, r := range rec.reqs {
		require.Equal(t, "infra.db.gib_hours", r.Metric)
		require.Equal(t, uuid.Nil, r.OwnerUserID, "an infra sample carries no principal; it records lazily, exactly like storage-sync")
		require.Equal(t, uuid.Nil, r.OwnerOrgID)
		require.Equal(t, 0, r.RecordedAt.Minute()+r.RecordedAt.Second(), "samples sit on closed hour boundaries")
		require.True(t, r.RecordedAt.Before(at().Truncate(time.Hour).Add(time.Nanosecond)), "the open hour is never sampled")
		level[k{r.AppID, r.ModuleID}] = r.Value
		instants[r.RecordedAt] = true
	}
	require.Len(t, instants, lookbackHours)
	// The LEVEL in GiB, never GiB-hours: the rollup integrates it.
	require.EqualValues(t, 2.0, level[k{appA, modQ}])
	require.EqualValues(t, 0.0, level[k{appA, modE}])
	require.EqualValues(t, 0.5, level[k{appB, modQ}])
	require.NotContains(t, level, k{uuid.Nil, uuid.Nil}, "ms_billing and platform tables are nobody's")
}

func TestSyncDB_SecondRunDedupesAndEventIDsAreStableAndDistinct(t *testing.T) {
	rec := &fakeRecorder{}
	first := syncDB(context.Background(), rec, twoApps(), &fakeHistory{}, at())
	again := syncDB(context.Background(), rec, twoApps(), &fakeHistory{}, at())
	require.Equal(t, first.Recorded, 3*lookbackHours)
	require.Equal(t, 0, again.Recorded)
	require.Equal(t, 3*lookbackHours, again.Deduped)

	h := at().Truncate(time.Hour)
	require.Equal(t, dbEventID(appA, modQ, h), dbEventID(appA, modQ, h), "stable across runs: RecordWithID keys must not drift")
	require.NotEqual(t, dbEventID(appA, modQ, h), dbEventID(appB, modQ, h), "same module in two apps are two samples")
	require.NotEqual(t, dbEventID(appA, modQ, h), dbEventID(appA, modC, h))
	require.NotEqual(t, dbEventID(appA, modQ, h), dbEventID(appA, modQ, h.Add(time.Hour)))
	require.NotEqual(t, dbEventID(appA, modQ, h), storageLikeID(appA, modQ, h), "never collides with the storage sampler's ids")
}

func storageLikeID(app, mod uuid.UUID, at time.Time) string {
	return uuid.NewSHA1(uuid.NameSpaceURL, []byte("infra.storage.gib_hours/"+app.String()+"/"+mod.String()+"/"+at.UTC().Format(time.RFC3339Nano))).String()
}

func TestSyncDB_AStaleReplayConflictIsADedupeButANewestConflictIsARowError(t *testing.T) {
	newest := at().Truncate(time.Hour).Add(-time.Hour)
	rec := &fakeRecorder{errFor: func(r usage.RecordInfraUsageRequest) error {
		return billing.Conflict("event_id is already bound to a different canonical usage payload")
	}}
	res := syncDB(context.Background(), rec, twoApps(), &fakeHistory{}, at())
	require.True(t, res.Failed, "a conflict on the newest instant is a real row error")
	require.Equal(t, 3, res.RowErrors, "only the newest instant's conflict is real")
	require.Equal(t, 3*(lookbackHours-1), res.Deduped, "older instants keep the first, closer sample")
	_ = newest
}

func TestSyncDB_OneBadRowDoesNotDropTheOthers(t *testing.T) {
	rec := &fakeRecorder{errFor: func(r usage.RecordInfraUsageRequest) error {
		if r.AppID == appA && r.ModuleID == modQ {
			return billing.Internal("db down", errors.New("boom"))
		}
		return nil
	}}
	res := syncDB(context.Background(), rec, twoApps(), &fakeHistory{}, at())
	require.Equal(t, 2*lookbackHours, res.Recorded, "the other modules still record")
	require.Equal(t, lookbackHours, res.RowErrors)
	require.True(t, res.Failed, "a real row error fails the run so the alarm sees it, after every other row recorded")
	require.Error(t, res.Err)
}

func TestSyncDB_ReadFailureFailsTheRun(t *testing.T) {
	// A failed read that returned an empty set would look exactly like "no
	// databases this hour" and silently record nothing.
	rec := &fakeRecorder{}
	r := twoApps()
	r.tablesErr = errors.New("permission denied")
	res := syncDB(context.Background(), rec, r, &fakeHistory{}, at())
	require.True(t, res.Failed)
	require.Error(t, res.Err)
	require.Empty(t, rec.reqs)
}

func TestSyncDB_AnUnreadableAppIsSkippedButEveryAppUnreadableFails(t *testing.T) {
	// One app missing its read grant must not drop everyone else...
	r := twoApps()
	r.installErr = map[uuid.UUID]error{appA: errors.New("permission denied for schema")}
	rec := &fakeRecorder{}
	res := syncDB(context.Background(), rec, r, &fakeHistory{}, at())
	require.Equal(t, 1, res.AppErrors)
	require.Equal(t, lookbackHours, res.Recorded, "only (B,quiz) records")
	// ...yet the run is not green: an app that cannot be read is billed nothing
	// (see TestSyncDB_APartiallyUnreadableFleetFailsTheRunAfterRecording).
	require.True(t, res.Failed)

	// ...but if NO app can be read the role has no grant at all, and a green run
	// of zero rows is the failure this metric must never hide.
	r.installErr[appB] = errors.New("permission denied for schema")
	rec = &fakeRecorder{}
	res = syncDB(context.Background(), rec, r, &fakeHistory{}, at())
	require.True(t, res.Failed)
	require.Error(t, res.Err)
	require.Empty(t, rec.reqs)
}

func TestSyncDB_NoAppSchemasFailsTheRun(t *testing.T) {
	// An empty catalog read is indistinguishable from a broken one (wrong DB,
	// wrong role, a regex that matches nothing). A green run that measured nothing
	// is the failure this metric must never hide; the schedule ships disabled, so
	// a stage with no apps is not paged.
	rec := &fakeRecorder{}
	h := &fakeHistory{pairs: []pair{{appA, modQ}}}
	res := syncDB(context.Background(), rec, &fakeReader{}, h, at())
	require.True(t, res.Failed)
	require.Error(t, res.Err)
	require.Empty(t, rec.reqs, "and an empty read must never zero anybody")
}

func TestSyncDB_EveryRowFailingIsAFailedRun(t *testing.T) {
	rec := &fakeRecorder{errFor: func(usage.RecordInfraUsageRequest) error { return billing.Internal("db down", errors.New("boom")) }}
	res := sync0(rec, twoApps())
	require.True(t, res.Failed)
	require.Equal(t, 0, res.Recorded+res.Deduped)
	require.Greater(t, res.Modules, 0)
}

func TestSyncDB_AttributedZeroWithBytesUnattributedFails(t *testing.T) {
	// The naming-mismatch failure: installs exist, tables exist, nothing matches.
	// Every module would record an explicit 0 and the run would look green.
	r := &fakeReader{
		tables:   []tableSize{{Schema: appSchemaName(appA), Table: "acme_quiz_attempts", Bytes: 2 * gib}},
		installs: map[uuid.UUID][]install{appA: {{Module: modQ}}},
	}
	res := sync0(&fakeRecorder{}, r)
	require.True(t, res.Failed)
	require.EqualValues(t, 2*gib, res.UnattributedBytes)
	require.EqualValues(t, 0, res.AttributedBytes)
	require.ErrorContains(t, res.Err, "unattributed")
}

func TestSyncDB_NoInstallsAnywhereWithPlatformTablesIsNotAFailure(t *testing.T) {
	// Apps exist, only platform tables, no module installed yet: nothing to
	// attribute is not an attribution fault.
	r := &fakeReader{
		tables:   []tableSize{{Schema: appSchemaName(appA), Table: "members", Bytes: gib}},
		installs: map[uuid.UUID][]install{appA: nil},
	}
	res := sync0(&fakeRecorder{}, r)
	require.False(t, res.Failed, "%v", res.Err)
}

func TestSyncDB_AHighUnattributedShareIsLoggedAsAnAlarm(t *testing.T) {
	var buf bytes.Buffer
	old := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&buf, nil)))
	t.Cleanup(func() { slog.SetDefault(old) })

	r := &fakeReader{
		tables: []tableSize{
			{Schema: appSchemaName(appA), Table: phys(modQ) + "t", Bytes: gib},
			{Schema: appSchemaName(appA), Table: "members", Bytes: 3 * gib},
		},
		installs: map[uuid.UUID][]install{appA: {{Module: modQ}}},
	}
	res := sync0(&fakeRecorder{}, r)
	require.False(t, res.Failed, "attributed > 0: a warning, not a failure")
	require.Contains(t, buf.String(), "unattributed share")
}

// D3: the rollup carries the last observed level to the period end, so a module
// uninstalled (or an app dropped) mid-period would be billed its last level
// forever unless the sampler says 0.
func TestSyncDB_AModuleThatDisappearedIsZeroedAtEveryInstant(t *testing.T) {
	rec := &fakeRecorder{}
	h := &fakeHistory{pairs: []pair{
		{appA, modQ}, // still installed: sampled normally, not zeroed twice
		{appA, modX}, // uninstalled since the last sample
		{appB, modC}, // appB still has tables but modC left
		{appC, modX}, // the whole app was dropped: no tables, no schema
	}}
	res := syncDB(context.Background(), rec, twoApps(), h, at())
	require.False(t, res.Failed, "%v", res.Err)

	type k struct{ app, mod uuid.UUID }
	count := map[k]int{}
	level := map[k]float64{}
	for _, r := range rec.reqs {
		count[k{r.AppID, r.ModuleID}]++
		level[k{r.AppID, r.ModuleID}] = r.Value
	}
	for _, z := range []k{{appA, modX}, {appB, modC}, {appC, modX}} {
		require.Equal(t, lookbackHours, count[z], "an explicit 0 at every instant")
		require.EqualValues(t, 0.0, level[z])
	}
	require.Equal(t, lookbackHours, count[k{appA, modQ}], "a live module is emitted once per instant, never twice")
	require.EqualValues(t, 2.0, level[k{appA, modQ}])
	require.Equal(t, 3, res.Zeroed)
}

func TestSyncDB_AnUnreadableAppIsNeverZeroed(t *testing.T) {
	// An install read that failed says nothing about whether the module is still
	// installed: zeroing would under-bill a live module.
	r := twoApps()
	r.installErr = map[uuid.UUID]error{appA: errors.New("permission denied")}
	rec := &fakeRecorder{}
	h := &fakeHistory{pairs: []pair{{appA, modQ}, {appA, modX}}}
	syncDB(context.Background(), rec, r, h, at())
	for _, q := range rec.reqs {
		require.NotEqual(t, appA, q.AppID, "appA could not be read; nothing about it is recorded")
	}
}

func TestSyncDB_AHistoryReadFailureStillRecordsButFailsTheRun(t *testing.T) {
	rec := &fakeRecorder{}
	res := syncDB(context.Background(), rec, twoApps(), &fakeHistory{err: errors.New("billing db down")}, at())
	require.Equal(t, 3*lookbackHours, res.Recorded, "the measured sizes are not held back by a failed zero-check")
	require.True(t, res.Failed, "an unreadable history means a vanished module may still be charged: fail loudly")
	require.Error(t, res.Err)
}

func TestSyncDB_ZeroingIsIdempotent(t *testing.T) {
	rec := &fakeRecorder{}
	h := &fakeHistory{pairs: []pair{{appA, modX}}}
	first := syncDB(context.Background(), rec, twoApps(), h, at())
	again := syncDB(context.Background(), rec, twoApps(), h, at())
	require.Equal(t, 4*lookbackHours, first.Recorded)
	require.Equal(t, 0, again.Recorded)
	require.Equal(t, 4*lookbackHours, again.Deduped)
}

// D5: a table dropped between the catalog scan and the size call yields a NULL
// pg_total_relation_size. That row is skipped; it must not fail the whole read.
type fakeScanner struct {
	schema, table string
	size          *int64
}

func (f fakeScanner) Scan(dest ...any) error {
	*(dest[0].(*string)) = f.schema
	*(dest[1].(*string)) = f.table
	*(dest[2].(**int64)) = f.size
	return nil
}

func TestScanTableSize_ANullSizeIsASkipNotAnError(t *testing.T) {
	n := int64(42)
	got, ok, err := scanTableSize(fakeScanner{"app_x", "t", &n})
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, tableSize{Schema: "app_x", Table: "t", Bytes: 42}, got)

	_, ok, err = scanTableSize(fakeScanner{"app_x", "gone", nil})
	require.NoError(t, err)
	require.False(t, ok, "dropped between scan and size: skipped")
}

func TestReaderTimeouts_AreBoundedAndSetLocal(t *testing.T) {
	// SET LOCAL: scoped to the read-only transaction, so a pooled connection never
	// carries a timeout into anything else, and a stuck lock or scan cannot hang
	// the Lambda until its own timeout.
	require.Contains(t, readTimeoutsSQL, "SET LOCAL statement_timeout")
	require.Contains(t, readTimeoutsSQL, "SET LOCAL lock_timeout")
}

func TestSyncDB_LogsNeverCarryTableNamesOrPrefixes(t *testing.T) {
	// Table names and prefixes are a customer's handle. Counts and ids only.
	var buf bytes.Buffer
	old := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&buf, nil)))
	t.Cleanup(func() { slog.SetDefault(old) })

	r := twoApps()
	r.installErr = map[uuid.UUID]error{appA: errors.New("permission denied")}
	rec := &fakeRecorder{errFor: func(usage.RecordInfraUsageRequest) error { return billing.Internal("x", errors.New("y")) }}
	syncDB(context.Background(), rec, r, &fakeHistory{}, at())
	logs := buf.String()
	require.NotEmpty(t, logs)
	require.False(t, strings.Contains(logs, "acme"), "a prefix or table name reached the log: %s", logs)
	require.False(t, strings.Contains(logs, "members"), "a table name reached the log")
	require.False(t, strings.Contains(logs, phys(modQ)), "a physical table prefix reached the log")
}
