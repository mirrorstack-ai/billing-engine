package testutil

import (
	"bufio"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// Stripe publishes test-mode credentials under two prefixes: a full
// secret key and a restricted key. Anything else — most importantly
// sk_live_ / rk_live_ — is a live-mode credential.
const (
	testSecretKeyPrefix     = "sk_test_"
	testRestrictedKeyPrefix = "rk_test_"
)

// IsStripeTestKey reports whether key is a Stripe TEST-mode credential.
//
// This is the whole safety property of the sandbox harness, so it is a
// prefix allow-list rather than a live-mode deny-list: a credential form
// nobody anticipated is refused, not admitted. An end-to-end test that
// creates customers, invoices and payment intents must be structurally
// incapable of doing so against live mode, and a deny-list inverts that
// default the first time Stripe introduces a new prefix.
func IsStripeTestKey(key string) bool {
	return strings.HasPrefix(key, testSecretKeyPrefix) ||
		strings.HasPrefix(key, testRestrictedKeyPrefix)
}

// SandboxStripeKey returns the Stripe sandbox secret key for end-to-end
// tests that drive the real provider, or skips the test when no sandbox
// is configured.
//
// Resolution order: STRIPE_SECRET_KEY from the environment, then
// STRIPE_SECRET_KEY from the repository's .env.local. The key is
// verified to be test-mode before it is returned; a live-mode key is a
// hard failure rather than a skip, because its presence means the run
// was pointed somewhere it must never reach.
//
// Set REQUIRE_STRIPE=1 to turn "no sandbox configured" into a failure,
// for the same reason REQUIRE_DOCKER exists: a skipped test still
// prints "ok".
func SandboxStripeKey(t *testing.T) string {
	t.Helper()

	key := os.Getenv("STRIPE_SECRET_KEY")
	if key == "" {
		key = envLocal(t)["STRIPE_SECRET_KEY"]
	}

	if key == "" {
		if os.Getenv("REQUIRE_STRIPE") != "" {
			t.Fatal("REQUIRE_STRIPE is set but no STRIPE_SECRET_KEY was found in the environment or .env.local")
		}
		t.Skip("no Stripe sandbox key configured (set STRIPE_SECRET_KEY or add it to .env.local)")
	}

	if !IsStripeTestKey(key) {
		// Never a skip. A non-test key here means the run is pointed at
		// live mode, and continuing would create real money objects.
		t.Fatalf("refusing to run against a non-test Stripe key (want %s… or %s…)",
			testSecretKeyPrefix, testRestrictedKeyPrefix)
	}
	return key
}

// envLocal parses the repository's .env.local into a map. It is a
// deliberately small parser — KEY=VALUE, optional surrounding quotes,
// # comments and blank lines ignored — because the file it reads holds
// credentials and a dependency here would widen what can see them.
//
// A missing file is not an error: SandboxStripeKey decides what an
// absent key means.
func envLocal(t *testing.T) map[string]string {
	t.Helper()
	out := map[string]string{}

	root, err := projectRoot()
	if err != nil {
		return out
	}
	f, err := os.Open(filepath.Join(root, ".env.local"))
	if err != nil {
		return out
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		line = strings.TrimPrefix(line, "export ")
		name, value, ok := strings.Cut(line, "=")
		if !ok {
			continue
		}
		value = strings.TrimSpace(value)
		value = strings.Trim(value, `"'`)
		out[strings.TrimSpace(name)] = value
	}
	return out
}
