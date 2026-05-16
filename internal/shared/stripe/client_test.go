package stripe_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	billingstripe "github.com/mirrorstack-ai/billing-engine/internal/shared/stripe"
)

// TestNewClient is a smoke test: NewClient returns a non-nil Client
// satisfying the interface. End-to-end Customer / SetupIntent creation
// is exercised in integration tests against Stripe test mode.
func TestNewClient(t *testing.T) {
	c := billingstripe.NewClient("sk_test_dummy")
	require.NotNil(t, c)

	// Interface satisfaction is checked at compile time via the
	// assignment. This var declaration would fail to build if
	// NewClient's return type didn't satisfy Client.
	var _ billingstripe.Client = c
}

func TestNewVerifier(t *testing.T) {
	v := billingstripe.NewVerifier("whsec_dummy")
	require.NotNil(t, v)
	var _ billingstripe.Verifier = v
}

// TestVerifier_BadSignature pins the negative-path contract of the
// real verifier without needing a Stripe API call: ConstructEvent
// rejects malformed signatures locally. A genuine end-to-end Verify
// test (with a valid HMAC signature constructed against the secret)
// lives in the webhook integration suite, alongside payload fixtures.
func TestVerifier_BadSignature(t *testing.T) {
	v := billingstripe.NewVerifier("whsec_dev_secret_used_only_in_tests")

	_, err := v.Verify([]byte(`{"id":"evt_test","type":"payment_method.attached"}`), "t=12345,v1=garbage")

	require.Error(t, err)
}

func TestVerifier_EmptySignature(t *testing.T) {
	v := billingstripe.NewVerifier("whsec_dev_secret_used_only_in_tests")

	_, err := v.Verify([]byte(`{"id":"evt_test"}`), "")

	require.Error(t, err)
}
