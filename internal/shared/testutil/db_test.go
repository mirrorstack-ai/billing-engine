package testutil

import (
	"errors"
	"testing"
)

// A skipped integration package still prints "ok". These cases pin the
// one rule that keeps such a green honest: when a run asserts Docker is
// present, an unreachable daemon must fail rather than skip.
func TestDockerDisposition(t *testing.T) {
	unreachable := errors.New("Cannot connect to the Docker daemon at unix:///var/run/docker.sock")
	socketDenied := errors.New("dial unix /var/run/docker.sock: connect: permission denied")
	realFailure := errors.New("container exited with code 1: initdb: error: directory not empty")

	cases := []struct {
		name          string
		err           error
		requireDocker string
		want          disposition
	}{
		{"unreachable and tolerated skips", unreachable, "", dispositionSkip},
		{"unreachable but required fails", unreachable, "1", dispositionRequired},
		{"socket permission denied is a docker absence", socketDenied, "", dispositionSkip},
		{"socket permission denied under REQUIRE_DOCKER fails", socketDenied, "1", dispositionRequired},
		{"a real start failure never skips", realFailure, "", dispositionFail},
		{"a real start failure fails under REQUIRE_DOCKER too", realFailure, "1", dispositionFail},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := dockerDisposition(tc.err, tc.requireDocker); got != tc.want {
				t.Fatalf("dockerDisposition(%q, %q) = %v, want %v", tc.err, tc.requireDocker, got, tc.want)
			}
		})
	}
}

// The sandbox harness creates real Stripe objects, so its one safety
// property is that it cannot be pointed at live mode. An allow-list is
// used deliberately: an unanticipated credential form must be refused.
func TestIsStripeTestKey(t *testing.T) {
	admitted := []string{
		"sk_test_51abcDEF",
		"rk_test_51abcDEF",
	}
	refused := []string{
		"sk_live_51abcDEF",
		"rk_live_51abcDEF",
		"pk_test_51abcDEF", // publishable, not a secret key
		"whsec_abcDEF",     // webhook signing secret
		"sk_test",          // prefix without the separator
		"SK_TEST_51abcDEF", // Stripe prefixes are lowercase
		" sk_test_51abc",   // leading space would reach Stripe verbatim
		"",
	}

	for _, key := range admitted {
		if !IsStripeTestKey(key) {
			t.Errorf("IsStripeTestKey(%q) = false, want true", key)
		}
	}
	for _, key := range refused {
		if IsStripeTestKey(key) {
			t.Errorf("IsStripeTestKey(%q) = true, want false", key)
		}
	}
}
