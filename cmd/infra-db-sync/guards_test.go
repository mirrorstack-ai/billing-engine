package main

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

// 🔴 Under-billing guards (#239 review follow-ups). The sampler's failure mode is
// silent: a wrong read says "0 bytes", the rollup carries 0 to the period end,
// and the customer is billed nothing while the platform pays. Every guard here
// must stop the run BEFORE a zero is recorded, because a recorded instant's
// first sample stands (event_id dedupe) and cannot be corrected later.

func priorPairs(n int) []pair {
	out := make([]pair, 0, n)
	for i := 0; i < n; i++ {
		out = append(out, pair{App: uuid.New(), Module: uuid.New()})
	}
	return out
}

func TestSyncDB_InstallsReadEmptyFleetWideWithPriorPairsFailsBeforeZeroing(t *testing.T) {
	// Tables exist in two apps, and every module_install read came back empty:
	// a revoked grant that returns zero rows, a wrong schema, an RLS filter. Every
	// prior pair would be zeroed and the whole fleet billed nothing.
	r := twoApps()
	r.installs = map[uuid.UUID][]install{}
	rec := &fakeRecorder{}
	res := syncDB(context.Background(), rec, r, &fakeHistory{pairs: []pair{{appA, modQ}, {appB, modQ}}}, at())
	require.True(t, res.Failed)
	require.Error(t, res.Err)
	require.Empty(t, rec.reqs, "not one zero may be recorded: a recorded instant cannot be corrected")
}

func TestSyncDB_AMassZeroingIsRefusedBeforeAnythingIsRecorded(t *testing.T) {
	// 3 pairs live in twoApps; 9 prior pairs have "vanished": three quarters of
	// the fleet. A real uninstall wave is never that large inside one hour.
	prior := append([]pair{{appA, modQ}, {appA, modE}, {appB, modQ}}, priorPairs(9)...)
	rec := &fakeRecorder{}
	res := syncDB(context.Background(), rec, twoApps(), &fakeHistory{pairs: prior}, at())
	require.True(t, res.Failed)
	require.Error(t, res.Err)
	require.Empty(t, rec.reqs)
}

func TestSyncDB_OrdinaryZeroingStillWorks(t *testing.T) {
	// 1 of 4 prior pairs gone: the normal uninstall. Guards must not stand in its
	// way, or a vanished module is billed its last level forever.
	prior := append([]pair{{appA, modQ}, {appA, modE}, {appB, modQ}}, pair{appB, modX})
	rec := &fakeRecorder{}
	res := syncDB(context.Background(), rec, twoApps(), &fakeHistory{pairs: prior}, at())
	require.False(t, res.Failed, "%v", res.Err)
	require.Equal(t, 1, res.Zeroed)
}

func TestSyncDB_ALargeFleetWithAMinorityGoneStillZeroes(t *testing.T) {
	// 10 prior pairs, 3 gone (30%): under the share, so the zeros are recorded.
	// The 7 others stay live by living in an app the fixture serves.
	r := twoApps()
	var tables []tableSize
	installs := map[uuid.UUID][]install{}
	app := uuid.New()
	var prior []pair
	for i := 0; i < 7; i++ {
		m := uuid.New()
		tables = append(tables, tableSize{Schema: appSchemaName(app), Table: phys(m) + "t", Bytes: gib})
		installs[app] = append(installs[app], install{Module: m})
		prior = append(prior, pair{app, m})
	}
	prior = append(prior, priorPairs(3)...)
	r.tables, r.installs = tables, installs
	res := syncDB(context.Background(), &fakeRecorder{}, r, &fakeHistory{pairs: prior}, at())
	require.False(t, res.Failed, "%v", res.Err)
	require.Equal(t, 3, res.Zeroed)
}

func TestSyncDB_UnreadableAppsDoNotCountTowardTheZeroedShare(t *testing.T) {
	// appA unreadable: its prior pairs are neither zeroed nor part of the share.
	r := twoApps()
	r.installErr = map[uuid.UUID]error{appA: errors.New("permission denied")}
	prior := []pair{{appA, modQ}, {appA, modE}, {appA, modX}, {appB, modQ}}
	res := syncDB(context.Background(), &fakeRecorder{}, r, &fakeHistory{pairs: prior}, at())
	require.Equal(t, 0, res.Zeroed)
}

func TestSyncDB_APartiallyUnreadableFleetFailsTheRunAfterRecording(t *testing.T) {
	r := twoApps()
	r.installErr = map[uuid.UUID]error{appA: errors.New("permission denied for schema")}
	rec := &fakeRecorder{}
	res := syncDB(context.Background(), rec, r, &fakeHistory{}, at())
	require.True(t, res.Failed, "an unreadable app is billed 0: the run must page, not look green")
	require.ErrorContains(t, res.Err, "unreadable")
	require.Equal(t, 1, res.AppErrors)
	require.Equal(t, lookbackHours, res.Recorded, "the readable app is still recorded: one bad app drops nobody else")
}

func TestSyncDB_AnAppWhoseTablesLackThePrefixIsAnAlarmedFailure(t *testing.T) {
	// appA has an install and 2 GiB of tables, none named m<hex>_: billed 0. appB is
	// healthy, so the fleet-wide "nothing attributed" check does not fire.
	var buf bytes.Buffer
	old := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&buf, nil)))
	t.Cleanup(func() { slog.SetDefault(old) })

	r := twoApps()
	r.tables = []tableSize{
		{Schema: appSchemaName(appA), Table: "legacy_quiz_attempts", Bytes: 2 * gib},
		{Schema: appSchemaName(appA), Table: "members", Bytes: gib / 100},
		{Schema: appSchemaName(appB), Table: phys(modQ) + "attempts", Bytes: gib / 2},
	}
	rec := &fakeRecorder{}
	res := sync0(rec, r)
	require.Equal(t, 1, res.PrefixMismatchApps)
	require.True(t, res.Failed)
	require.ErrorContains(t, res.Err, "prefix")
	require.Contains(t, buf.String(), appA.String(), "the alarm names the app")
	require.NotContains(t, buf.String(), "legacy_quiz_attempts", "a table name is a customer's handle and is never logged")
	require.NotEmpty(t, rec.reqs, "the verdict follows the recording; the healthy app is not held back")
}

func TestSyncDB_AnInstalledModuleWithNoTablesIsNotAPrefixMismatch(t *testing.T) {
	// A module that owns no tables leaves only the platform's small tables in the
	// app. That is a level of 0, not a naming fault.
	r := &fakeReader{
		tables: []tableSize{
			{Schema: appSchemaName(appA), Table: "members", Bytes: 64 << 10},
			{Schema: appSchemaName(appA), Table: "module_install", Bytes: 16 << 10},
			{Schema: appSchemaName(appB), Table: phys(modQ) + "attempts", Bytes: gib / 2},
		},
		installs: map[uuid.UUID][]install{appA: {{Module: modE}}, appB: {{Module: modQ}}},
	}
	res := sync0(&fakeRecorder{}, r)
	require.Equal(t, 0, res.PrefixMismatchApps)
	require.False(t, res.Failed, "%v", res.Err)
}
