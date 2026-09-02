package usage

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestCanonicalMetadataLimitsAndNormalization(t *testing.T) {
	canonical, err := canonicalMetadata(json.RawMessage(` {"z":-0.0,"a":1e0,"nested":{"ok":true}} `))
	require.NoError(t, err)
	require.Equal(t, `{"a":1,"nested":{"ok":true},"z":0}`, string(canonical))

	canonical, err = canonicalMetadata(json.RawMessage(
		`{"same":10e-1,"large":9007199254740993,"decimal":123.4500}`,
	))
	require.NoError(t, err)
	require.Equal(t, `{"decimal":12345e-2,"large":9007199254740993,"same":1}`, string(canonical))
	largeA, err := canonicalMetadata(json.RawMessage(`{"large":9007199254740993}`))
	require.NoError(t, err)
	largeB, err := canonicalMetadata(json.RawMessage(`{"large":9007199254740992}`))
	require.NoError(t, err)
	require.NotEqual(t, string(largeA), string(largeB),
		"adjacent integers above float64's exact range must not share canonical bytes")

	tests := map[string]json.RawMessage{
		"root scalar":      json.RawMessage(`"value"`),
		"invalid key":      json.RawMessage(`{"1bad":true}`),
		"too many members": json.RawMessage(`{"a":1,"b":1,"c":1,"d":1,"e":1,"f":1,"g":1,"h":1,"i":1,"j":1,"k":1,"l":1,"m":1,"n":1,"o":1,"p":1,"q":1,"r":1,"s":1,"t":1,"u":1,"v":1,"w":1,"x":1,"y":1,"z":1,"aa":1,"ab":1,"ac":1,"ad":1,"ae":1,"af":1,"ag":1}`),
		"array too long":   json.RawMessage(`{"items":[0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]}`),
		"string too long":  json.RawMessage(`{"value":"` + strings.Repeat("x", maxMetadataStringBytes+1) + `"}`),
		"raw too large":    json.RawMessage(`{"value":"` + strings.Repeat("x", maxMetadataBytes) + `"}`),
		"too deep":         json.RawMessage(`{"a":{"b":{"c":{"d":1}}}}`),
		"duplicate root":   json.RawMessage(`{"a":1,"a":2}`),
		"duplicate nested": json.RawMessage(`{"outer":{"a":1,"a":2}}`),
		"duplicate storm":  json.RawMessage(`{` + strings.Repeat(`"a":1,`, 32) + `"a":1}`),
	}
	for name, raw := range tests {
		t.Run(name, func(t *testing.T) {
			_, err := canonicalMetadata(raw)
			require.Error(t, err)
		})
	}
}

func TestObservationFingerprintCanonicalPayload(t *testing.T) {
	instant := time.Date(2026, 8, 30, 4, 5, 6, 123, time.FixedZone("offset", 8*60*60))
	base := UsageEvent{
		ObservationVersion: observationVersionV2,
		AppID:              uuid.MustParse("11111111-1111-1111-1111-111111111111"),
		ModuleID:           uuid.MustParse("22222222-2222-2222-2222-222222222222"),
		OwnerUserID:        uuid.MustParse("33333333-3333-3333-3333-333333333333"),
		Metric:             "users.monthly_active",
		Kind:               KindPeak,
		AggregationKey:     AggregationKeySubject,
		ModuleVersion:      "2.0.0",
		Value:              1,
		Subject:            "end-user-1",
		Metadata:           json.RawMessage(`{"a":1,"b":2}`),
		OccurredAt:         instant,
		RecordedAt:         instant.Add(time.Hour),
	}
	want := observationFingerprint(base)
	require.Len(t, want, 32)

	equivalent := base
	equivalent.OccurredAt = instant.UTC()
	equivalent.RecordedAt = instant.Add(48 * time.Hour) // receipt is excluded
	equivalent.Kind = KindCount                         // mutable catalog fields are excluded
	equivalent.AggregationKey = ""
	require.Equal(t, want, observationFingerprint(equivalent))

	changes := map[string]func(*UsageEvent){
		"version":        func(e *UsageEvent) { e.ObservationVersion = 1 },
		"app":            func(e *UsageEvent) { e.AppID = uuid.New() },
		"module":         func(e *UsageEvent) { e.ModuleID = uuid.New() },
		"owner":          func(e *UsageEvent) { e.OwnerUserID = uuid.New() },
		"metric":         func(e *UsageEvent) { e.Metric = "users.other" },
		"model":          func(e *UsageEvent) { e.Model = "priced-model" },
		"module version": func(e *UsageEvent) { e.ModuleVersion = "2.0.1" },
		"value":          func(e *UsageEvent) { e.Value = 2 },
		"subject":        func(e *UsageEvent) { e.Subject = "end-user-2" },
		"metadata":       func(e *UsageEvent) { e.Metadata = json.RawMessage(`{"a":2,"b":2}`) },
		"occurred at":    func(e *UsageEvent) { e.OccurredAt = e.OccurredAt.Add(time.Nanosecond) },
	}
	for name, mutate := range changes {
		t.Run(name, func(t *testing.T) {
			changed := base
			mutate(&changed)
			require.False(t, bytes.Equal(want, observationFingerprint(changed)))
		})
	}
}

func TestValidateSubjectOpaqueUTF8Bounds(t *testing.T) {
	require.NoError(t, validateSubject("opaque:使用者/42"))
	require.NoError(t, validateSubject(strings.Repeat("x", maxSubjectBytes)))
	for _, subject := range []string{
		strings.Repeat("x", maxSubjectBytes+1),
		"has\x00nul",
		"has\nnewline",
		string([]byte{0xff}),
	} {
		require.Error(t, validateSubject(subject))
	}
}
