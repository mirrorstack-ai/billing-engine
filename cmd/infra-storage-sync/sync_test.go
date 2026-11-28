package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/mirrorstack-ai/billing-engine/internal/account/billing"
)

type fakeLister struct {
	pages [][]s3types.Object
	err   error
	calls int
}

func (f *fakeLister) ListObjectsV2(_ context.Context, in *s3.ListObjectsV2Input, _ ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
	if f.err != nil {
		return nil, f.err
	}
	idx := 0
	if in.ContinuationToken != nil {
		idx = int((*in.ContinuationToken)[0] - '0')
	}
	f.calls++
	out := &s3.ListObjectsV2Output{Contents: f.pages[idx]}
	if idx+1 < len(f.pages) {
		tok := string(rune('0' + idx + 1))
		out.IsTruncated = ptr(true)
		out.NextContinuationToken = &tok
	}
	return out, nil
}

func obj(key string, size int64) s3types.Object {
	return s3types.Object{Key: &key, Size: &size}
}

func TestParseModulePrefix_RejectsAnythingNotAModuleObject(t *testing.T) {
	app, mod := uuid.New(), uuid.New()

	got, ok := parseModulePrefix("apps/" + app.String() + "/" + mod.String() + "/videos/a.mp4")
	require.True(t, ok)
	require.Equal(t, app, got.app)
	require.Equal(t, mod, got.module)

	// 🔴 EVERY REJECTION HERE IS A CUSTOMER NOT BEING BILLED FOR THE PLATFORM'S
	// OWN BYTES. A repaired guess would attribute deploy artifacts, avatars or a
	// malformed key to whichever module the parser talked itself into.
	for _, bad := range []string{
		"artifacts/" + app.String() + "/" + mod.String() + "/x", // not under apps/
		"apps/" + app.String() + "/" + mod.String() + "/",       // bare prefix, no object
		"apps/" + app.String() + "/" + mod.String(),             // no trailing segment
		"apps/" + app.String() + "/not-a-uuid/x",                // module not a uuid
		"apps/not-a-uuid/" + mod.String() + "/x",                // app not a uuid
		"apps/",
		"",
	} {
		_, ok := parseModulePrefix(bad)
		require.False(t, ok, "must reject %q", bad)
	}
}

func TestPrefixSizes_SumsPerModuleAcrossPages(t *testing.T) {
	appA, modA, modB := uuid.New(), uuid.New(), uuid.New()
	pa := "apps/" + appA.String() + "/" + modA.String() + "/"
	pb := "apps/" + appA.String() + "/" + modB.String() + "/"

	lister := &fakeLister{pages: [][]s3types.Object{
		{obj(pa+"one.mp4", 100), obj(pb+"x.bin", 7)},
		{obj(pa+"two.mp4", 23), obj("artifacts/junk", 999), obj("apps/bad/x/y", 5)},
	}}
	sizes, skipped, err := prefixSizes(context.Background(), lister, "bucket")
	require.NoError(t, err)
	require.Equal(t, 2, lister.calls, "pagination must be followed, not truncated")
	require.EqualValues(t, 123, sizes[prefixKey{app: appA, module: modA}])
	require.EqualValues(t, 7, sizes[prefixKey{app: appA, module: modB}])
	require.Equal(t, 1, skipped.SkippedNotModule, "artifacts/junk is outside the module layout")
	require.Equal(t, 1, skipped.SkippedMalformed, "apps/bad/x/y has the module shape and an id that is not a uuid")
	require.Equal(t, "apps/bad/x/y", skipped.FirstMalformed)
}

func TestPrefixSizes_SplitsSkipsByWhatTheyMean(t *testing.T) {
	// 🔴 billing-engine#226: one "skipped" read 13 on every healthy prod run, so
	// a real malformed key could never show. Each key runs ALONE so a swapped
	// classification fails both rows, not cancels out in a mixed total. Fixed
	// ids keep the subtest names stable for -run.
	app := uuid.MustParse("11111111-1111-4111-8111-111111111111")
	mod := uuid.MustParse("22222222-2222-4222-8222-222222222222")
	for _, tc := range []struct {
		key       string
		malformed bool
	}{
		// Routine: never an alarm, on a healthy bucket forever.
		{"apps/" + app.String() + "/uploads/cover/n1/01J.jpg", false}, // app branding (prod today)
		{"apps/" + app.String() + "/uploads/icon/01J.png", false},
		{"apps/" + app.String() + "/" + mod.String() + "/", false}, // bare folder marker, not an object
		{"apps/" + app.String() + "/icon.png", false},              // app-level, not module-shaped
		{"apps/", false},
		// Alarm: shaped like a module object, not metered.
		{"apps/" + app.String() + "/not-a-uuid/x", true},
		{"apps/not-a-uuid/" + mod.String() + "/x", true},
		{"apps/" + app.String() + "/" + mod.String()[:35] + "/x", true}, // truncated module id
	} {
		t.Run(tc.key, func(t *testing.T) {
			lister := &fakeLister{pages: [][]s3types.Object{{obj(tc.key, 42)}}}
			sizes, skipped, err := prefixSizes(context.Background(), lister, "bucket")
			require.NoError(t, err)
			require.Empty(t, sizes, "classification must never meter a rejected key")
			if tc.malformed {
				require.Equal(t, skipTally{SkippedMalformed: 1, FirstMalformed: tc.key}, skipped)
			} else {
				require.Equal(t, skipTally{SkippedNotModule: 1}, skipped)
			}
		})
	}
}

func TestPrefixSizes_EveryKeyLandsInExactlyOneBucket(t *testing.T) {
	// Reporting changed, metering did not: metered + not_module + malformed is
	// every listed key, once — nothing double-counted, nothing dropped.
	app, mod := uuid.New(), uuid.New()
	good := "apps/" + app.String() + "/" + mod.String() + "/"
	lister := &fakeLister{pages: [][]s3types.Object{
		{obj(good+"a.mp4", 10), obj("apps/"+app.String()+"/uploads/icon/x.png", 3), obj("apps/"+app.String()+"/nope/x", 5)},
		{obj(good+"b.mp4", 20), obj("apps/"+app.String()+"/uploads/cover/y.jpg", 4)},
	}}
	sizes, skipped, err := prefixSizes(context.Background(), lister, "bucket")
	require.NoError(t, err)
	require.Equal(t, map[prefixKey]int64{{app: app, module: mod}: 30}, sizes)
	require.Equal(t, 5, 2+skipped.SkippedNotModule+skipped.SkippedMalformed)
	require.Equal(t, skipTally{SkippedNotModule: 2, SkippedMalformed: 1, FirstMalformed: "apps/" + app.String() + "/nope/x"}, skipped)
}

// captureWarnings swaps the default logger for the test and returns the WARN
// records syncStorage emitted.
func captureWarnings(t *testing.T) func() []map[string]any {
	t.Helper()
	var buf bytes.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn})))
	t.Cleanup(func() { slog.SetDefault(prev) })
	return func() []map[string]any {
		var warns []map[string]any
		for _, line := range bytes.Split(bytes.TrimSpace(buf.Bytes()), []byte("\n")) {
			if len(line) == 0 {
				continue
			}
			var rec map[string]any
			require.NoError(t, json.Unmarshal(line, &rec))
			if rec["level"] == "WARN" {
				warns = append(warns, rec)
			}
		}
		return warns
	}
}

func TestSyncStorage_WarnsTheFirstMalformedKeyOncePerRun(t *testing.T) {
	app := uuid.New()
	first := "apps/" + app.String() + "/not-a-uuid/a"
	lister := &fakeLister{pages: [][]s3types.Object{
		{obj("apps/"+app.String()+"/uploads/icon/x.png", 1), obj(first, 1), obj("apps/"+app.String()+"/not-a-uuid/b", 1)},
		{obj("apps/zzz/"+uuid.NewString()+"/c", 1)},
	}}
	warnings := captureWarnings(t)

	// No key parses, so no prefix is recorded and the service is never reached.
	res := syncStorage(context.Background(), nil, lister, "bucket", time.Now())

	require.False(t, res.Failed)
	require.Equal(t, 3, res.SkippedMalformed)
	require.Equal(t, 1, res.SkippedNotModule)
	warns := warnings()
	require.Len(t, warns, 1, "one WARN per run, however many malformed keys and pages")
	require.Equal(t, first, warns[0]["first_key"], "the WARN carries the first offending key")
	require.EqualValues(t, 3, warns[0]["skipped_malformed"])
}

func TestSyncStorage_AHealthyBucketDoesNotWarn(t *testing.T) {
	// Today's prod shape: only app branding is skipped. That must stay silent,
	// or the WARN becomes the next signal that reads the same fine or broken.
	app := uuid.New()
	lister := &fakeLister{pages: [][]s3types.Object{{
		obj("apps/"+app.String()+"/uploads/cover/n1/x.jpg", 1),
		obj("apps/"+app.String()+"/uploads/icon/y.png", 1),
	}}}
	warnings := captureWarnings(t)

	res := syncStorage(context.Background(), nil, lister, "bucket", time.Now())

	require.Equal(t, skipTally{SkippedNotModule: 2}, res.skipTally)
	require.Empty(t, warnings())
}

func TestPrefixSizes_ListingErrorIsReturnedNotSwallowed(t *testing.T) {
	// A listing failure that returned an empty map would look exactly like "no
	// storage this hour" and silently zero every module's storage bill.
	_, _, err := prefixSizes(context.Background(), &fakeLister{err: errors.New("denied")}, "bucket")
	require.Error(t, err)
}

func TestClosedHours_NeverSamplesThePartialHour(t *testing.T) {
	at := time.Date(2026, 9, 10, 14, 37, 0, 0, time.UTC)
	got := closedHours(at, 3)
	require.Len(t, got, 3)
	require.Equal(t, time.Date(2026, 9, 10, 11, 0, 0, 0, time.UTC), got[0], "oldest first")
	require.Equal(t, time.Date(2026, 9, 10, 13, 0, 0, 0, time.UTC), got[2])
	for _, h := range got {
		require.True(t, h.Before(at.Truncate(time.Hour)),
			"the hour containing `at` is still open and must not be sampled")
	}
}

func TestStorageEventID_IsDeterministicPerInstantAndModule(t *testing.T) {
	// Determinism IS the dedupe: a catch-up run must re-mint the same id for an
	// instant it already recorded, or the overlap double-counts.
	app, mod := uuid.New(), uuid.New()
	at := time.Date(2026, 9, 10, 11, 0, 0, 0, time.UTC)
	require.Equal(t, storageEventID(app, mod, at), storageEventID(app, mod, at))
	require.NotEqual(t, storageEventID(app, mod, at), storageEventID(app, mod, at.Add(time.Hour)))
	require.NotEqual(t, storageEventID(app, mod, at), storageEventID(app, uuid.New(), at))
	require.NotEqual(t, storageEventID(app, mod, at), storageEventID(uuid.New(), mod, at))
}

func TestSyncStorage_EmitsAGiBLevelNotGiBHours(t *testing.T) {
	// 🔴 THE MISTAKE THIS EXISTS TO CATCH IS AN OVERCHARGE. infra.storage.gib_hours
	// is kind time_weighted and the rollup integrates it in SQL
	// (value × EXTRACT(EPOCH …) / 3600.0, queries/rollup.sql:323). The event must
	// therefore carry the GiB STANDING at the instant. Emitting GiB-hours would be
	// integrated a second time and bill a multiple of the truth — and migration
	// 020's "value = GiB-hours" note, which describes the ROLLED quantity, is
	// exactly the sentence that would talk someone into it.
	app, mod := uuid.New(), uuid.New()
	key := "apps/" + app.String() + "/" + mod.String() + "/big.mp4"

	lister := &fakeLister{pages: [][]s3types.Object{{obj(key, 2*bytesPerGiB)}}}
	sizes, _, err := prefixSizes(context.Background(), lister, "bucket")
	require.NoError(t, err)

	gib := float64(sizes[prefixKey{app: app, module: mod}]) / float64(bytesPerGiB)
	require.EqualValues(t, 2, gib, "the emitted value is the level in GiB")
	require.NotEqualValues(t, 2*lookbackHours, gib,
		"a pre-integrated value would be lookbackHours times too large")
}

func TestIsStaleReplayConflict_OnlyAnOlderInstantsConflictIsADedupe(t *testing.T) {
	// 09-15 13:48Z: the bucket level changed between runs, the 10:00 and 11:00
	// instants were re-emitted under their deterministic event_ids with the new
	// level, and RecordInfraUsage answered CONFLICT. The first sample of an older
	// instant stands; only the newest instant is this run's to record.
	newest := time.Date(2026, 9, 15, 12, 0, 0, 0, time.UTC)
	older := newest.Add(-2 * time.Hour)
	conflict := billing.Conflict("event_id is already bound to a different canonical usage payload")

	require.True(t, isStaleReplayConflict(conflict, older, newest), "an older instant's CONFLICT is the replay disagreeing with itself")
	require.False(t, isStaleReplayConflict(conflict, newest, newest), "a CONFLICT on the newest instant is real and stays a row error")
	require.False(t, isStaleReplayConflict(billing.Internal("db down", errors.New("boom")), older, newest), "only CONFLICT is a dedupe; any other error stays a row error")
	require.False(t, isStaleReplayConflict(errors.New("plain"), older, newest))
}
