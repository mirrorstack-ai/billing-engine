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
	modC = uuid.MustParse("bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb") // quiz_core, shares quiz's prefix stem
	modE = uuid.MustParse("cccccccc-cccc-4ccc-8ccc-cccccccccccc") // installed, no tables
)

const gib = int64(1) << 30

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

func TestAttribute_LongestPrefixWinsAndPlatformTablesAreNobodys(t *testing.T) {
	installs := []install{
		{Module: modQ, Prefix: "acme_quiz_"},
		{Module: modC, Prefix: "acme_quiz_core_"},
		{Module: modE, Prefix: "acme_empty_"},
	}
	tables := []tableSize{
		{Table: "acme_quiz_attempts", Bytes: 3 * gib},
		{Table: "acme_quiz_core_answers", Bytes: 5 * gib},
		{Table: "acme_quiz_core_answers_pkey_dup", Bytes: gib},
		{Table: "members", Bytes: 7 * gib},         // platform-owned: no module prefix
		{Table: "module_install", Bytes: 9 * gib},  // platform-owned
		{Table: "acme_unknown_things", Bytes: gib}, // uninstalled module's leftovers
	}
	per, unattributed := attribute(installs, tables)

	require.EqualValues(t, 3*gib, per[modQ], "quiz keeps only its own tables")
	require.EqualValues(t, 6*gib, per[modC], "quiz_core is longer than quiz_ and takes the tables under it")
	v, ok := per[modE]
	require.True(t, ok, "an installed module with no tables must still be present: 0 is a level, and the rollup carries the last level across a gap")
	require.EqualValues(t, 0, v)
	require.EqualValues(t, (7+9+1)*gib, unattributed, "platform and orphan tables are counted aside, never billed to a module")
}

func TestAttribute_DuplicatePrefixIsAmbiguousAndNeverDoubleCharged(t *testing.T) {
	// Two installs sharing a prefix would charge the same table twice. The
	// lexically smaller module id takes it; the other is left at 0.
	installs := []install{{Module: modC, Prefix: "acme_x_"}, {Module: modQ, Prefix: "acme_x_"}}
	per, _ := attribute(installs, []tableSize{{Table: "acme_x_t", Bytes: 4 * gib}})
	require.EqualValues(t, 4*gib, per[modQ])
	require.EqualValues(t, 0, per[modC])
	require.EqualValues(t, 4*gib, per[modQ]+per[modC], "one table is charged exactly once")
}

func TestAttribute_EmptyPrefixNeverMatches(t *testing.T) {
	// An empty prefix would swallow every table in the schema, including the
	// platform's own.
	per, un := attribute([]install{{Module: modQ, Prefix: ""}}, []tableSize{{Table: "members", Bytes: gib}})
	require.EqualValues(t, 0, per[modQ])
	require.EqualValues(t, gib, un)
}

func twoApps() *fakeReader {
	return &fakeReader{
		tables: []tableSize{
			{Schema: appSchemaName(appA), Table: "acme_quiz_attempts", Bytes: 2 * gib},
			{Schema: appSchemaName(appA), Table: "members", Bytes: gib},
			{Schema: appSchemaName(appB), Table: "acme_quiz_attempts", Bytes: gib / 2},
			{Schema: "ms_billing", Table: "usage_events", Bytes: 99 * gib}, // never an app schema
		},
		installs: map[uuid.UUID][]install{
			appA: {{Module: modQ, Prefix: "acme_quiz_"}, {Module: modE, Prefix: "acme_empty_"}},
			appB: {{Module: modQ, Prefix: "acme_quiz_"}},
		},
	}
}

func TestSyncDB_EmitsALevelPerAppModuleAtEveryClosedHour(t *testing.T) {
	rec := &fakeRecorder{}
	res := syncDB(context.Background(), rec, twoApps(), at())

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
	first := syncDB(context.Background(), rec, twoApps(), at())
	again := syncDB(context.Background(), rec, twoApps(), at())
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
	res := syncDB(context.Background(), rec, twoApps(), at())
	require.False(t, res.Failed, "per-row errors never fail the sweep")
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
	res := syncDB(context.Background(), rec, twoApps(), at())
	require.False(t, res.Failed)
	require.Equal(t, lookbackHours, res.RowErrors)
	require.Equal(t, 2*lookbackHours, res.Recorded)
}

func TestSyncDB_ReadFailureFailsTheRun(t *testing.T) {
	// A failed read that returned an empty set would look exactly like "no
	// databases this hour" and silently record nothing.
	rec := &fakeRecorder{}
	r := twoApps()
	r.tablesErr = errors.New("permission denied")
	res := syncDB(context.Background(), rec, r, at())
	require.True(t, res.Failed)
	require.Error(t, res.Err)
	require.Empty(t, rec.reqs)
}

func TestSyncDB_AnUnreadableAppIsSkippedButEveryAppUnreadableFails(t *testing.T) {
	// One app missing its read grant must not drop everyone else...
	r := twoApps()
	r.installErr = map[uuid.UUID]error{appA: errors.New("permission denied for schema")}
	rec := &fakeRecorder{}
	res := syncDB(context.Background(), rec, r, at())
	require.False(t, res.Failed)
	require.Equal(t, 1, res.AppErrors)
	require.Equal(t, lookbackHours, res.Recorded, "only (B,quiz) records")

	// ...but if NO app can be read the role has no grant at all, and a green run
	// of zero rows is the failure this metric must never hide.
	r.installErr[appB] = errors.New("permission denied for schema")
	rec = &fakeRecorder{}
	res = syncDB(context.Background(), rec, r, at())
	require.True(t, res.Failed)
	require.Error(t, res.Err)
	require.Empty(t, rec.reqs)
}

func TestSyncDB_NoAppSchemasIsASuccessfulEmptyRun(t *testing.T) {
	rec := &fakeRecorder{}
	res := syncDB(context.Background(), rec, &fakeReader{}, at())
	require.False(t, res.Failed, "a fresh stage with no apps is not an outage")
	require.Equal(t, 0, res.Modules)
}

func TestSyncDB_LogsNeverCarryTableNamesOrPrefixes(t *testing.T) {
	// Prefixes are <username>_<slug>_: a customer's handle. Counts and ids only.
	var buf bytes.Buffer
	old := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&buf, nil)))
	t.Cleanup(func() { slog.SetDefault(old) })

	r := twoApps()
	r.installErr = map[uuid.UUID]error{appA: errors.New("permission denied")}
	rec := &fakeRecorder{errFor: func(usage.RecordInfraUsageRequest) error { return billing.Internal("x", errors.New("y")) }}
	syncDB(context.Background(), rec, r, at())
	logs := buf.String()
	require.NotEmpty(t, logs)
	require.False(t, strings.Contains(logs, "acme"), "a prefix or table name reached the log: %s", logs)
	require.False(t, strings.Contains(logs, "members"), "a table name reached the log")
}
