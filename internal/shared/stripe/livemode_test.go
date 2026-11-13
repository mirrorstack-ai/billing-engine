package stripe_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	billingstripe "github.com/mirrorstack-ai/billing-engine/internal/shared/stripe"
)

func TestExpectedLivemodeFromKey(t *testing.T) {
	for _, tc := range []struct {
		name    string
		key     string
		want    bool
		wantErr bool
	}{
		{"secret test key is test mode", "sk_test_" + "abc123", false, false},
		{"restricted test key is test mode", "rk_test_" + "abc123", false, false},
		{"secret live key is live mode", "sk_live_" + "abc123", true, false},
		{"restricted live key is live mode", "rk_live_" + "abc123", true, false},
		{"empty string is refused", "", false, true},
		{"webhook signing secret is refused", "whsec_" + "abc123", false, true},
		{"publishable key is refused", "pk_test_" + "abc123", false, true},
		{"made-up prefix is refused", "sk_sandbox_" + "abc123", false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := billingstripe.ExpectedLivemodeFromKey(tc.key)
			if tc.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}
}
