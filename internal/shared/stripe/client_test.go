package stripe

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestNewClient is a smoke test: NewClient returns a non-nil Client
// satisfying the interface. End-to-end Customer / SetupIntent creation
// is exercised in integration tests against Stripe test mode.
func TestNewClient(t *testing.T) {
	c := NewClient("sk_test_dummy")
	require.NotNil(t, c)

	// Interface satisfaction is checked at compile time via the
	// assignment. This var declaration would fail to build if
	// NewClient's return type didn't satisfy Client.
	var _ Client = c
}

func TestNewVerifier(t *testing.T) {
	v := NewVerifier("whsec_dummy")
	require.NotNil(t, v)
	var _ Verifier = v
}

// TestVerifier_BadSignature pins the negative-path contract of the
// real verifier without needing a Stripe API call: ConstructEvent
// rejects malformed signatures locally. A genuine end-to-end Verify
// test (with a valid HMAC signature constructed against the secret)
// lives in the webhook integration suite, alongside payload fixtures.
func TestVerifier_BadSignature(t *testing.T) {
	v := NewVerifier("whsec_dev_secret_used_only_in_tests")

	_, err := v.Verify([]byte(`{"id":"evt_test","type":"payment_method.attached"}`), "t=12345,v1=garbage")

	require.Error(t, err)
}

func TestVerifier_EmptySignature(t *testing.T) {
	v := NewVerifier("whsec_dev_secret_used_only_in_tests")

	_, err := v.Verify([]byte(`{"id":"evt_test"}`), "")

	require.Error(t, err)
}

func TestItemPeriodParams(t *testing.T) {
	t.Run("populated", func(t *testing.T) {
		start := time.Date(2026, time.July, 17, 8, 30, 0, 0, time.FixedZone("UTC+8", 8*60*60))
		end := time.Date(2026, time.August, 10, 8, 30, 0, 0, time.FixedZone("UTC+8", 8*60*60))

		got := itemPeriodParams(LinePeriod{Start: start, End: end})

		require.NotNil(t, got)
		require.Equal(t, start.UTC().Unix(), *got.Start)
		require.Equal(t, end.UTC().Unix(), *got.End)
	})

	for _, tt := range []struct {
		name   string
		period LinePeriod
	}{
		{name: "zero value"},
		{name: "missing start", period: LinePeriod{End: time.Unix(200, 0)}},
		{name: "missing end", period: LinePeriod{Start: time.Unix(100, 0)}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			require.Nil(t, itemPeriodParams(tt.period))
		})
	}
}
