package stripe

import (
	"fmt"
	"strings"
)

// Stripe secret/restricted key prefixes. Test and live keys are
// distinguished only by prefix; there is no other signal in the key
// itself that mode it authenticates as.
const (
	testSecretKeyPrefix     = "sk_test_"
	testRestrictedKeyPrefix = "rk_test_"
	liveSecretKeyPrefix     = "sk_live_"
	liveRestrictedKeyPrefix = "rk_live_"
)

// ExpectedLivemodeFromKey derives the Stripe mode a secret/restricted API key
// authenticates as, from its prefix: sk_test_/rk_test_ mean test mode
// (live=false), sk_live_/rk_live_ mean live mode (live=true).
//
// This is a prefix allow-list rather than a live-prefix-only check, for the
// same reason testutil.IsStripeTestKey's sandbox guard is one (it holds the
// identical allow-list for tests): a key form nobody anticipated must be
// refused, not silently defaulted to either mode. The production webhook
// livemode guard depends on this never guessing, so an unrecognized prefix —
// including a webhook signing secret, a publishable key, or a typo'd key —
// is an error, not a false/true default.
//
// Kept in this package (not internal/shared/testutil) so production code
// never imports the test-only package to reach it.
func ExpectedLivemodeFromKey(key string) (live bool, err error) {
	switch {
	case strings.HasPrefix(key, testSecretKeyPrefix), strings.HasPrefix(key, testRestrictedKeyPrefix):
		return false, nil
	case strings.HasPrefix(key, liveSecretKeyPrefix), strings.HasPrefix(key, liveRestrictedKeyPrefix):
		return true, nil
	default:
		return false, fmt.Errorf(
			"stripe: key has no recognized test/live prefix (want one of %s, %s, %s, %s)",
			testSecretKeyPrefix, testRestrictedKeyPrefix, liveSecretKeyPrefix, liveRestrictedKeyPrefix,
		)
	}
}
