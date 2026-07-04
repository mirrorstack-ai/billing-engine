package usage

// White-box pins for the ACCOUNT-level PaaS credit (accountPaasCreditMicros).
// The RPC-level tests can only observe 0 (the credit is subscription-gated OFF
// in v1), so the earn-path math and the usage-only-offset cap invariant are
// pinned here with the gate forced open.

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAccountPaasCreditMicros_GatedOffIsZero(t *testing.T) {
	// No ACTIVE SaaS subscription → no credit, regardless of the totals (the
	// platform never grants an unearned credit).
	credit, err := accountPaasCreditMicros(false, 1_000_000, 9_000_000)
	require.NoError(t, err)
	require.Zero(t, credit)
}

func TestAccountPaasCreditMicros_EarnedIsPctOfAccountInfra(t *testing.T) {
	// Gate forced open: the credit is PaasCreditPct% of the ACCOUNT-WIDE infra
	// total — a single account-level magnitude, not a per-app one, so it can
	// exceed any one app's usage (10 here) while staying under the account's
	// usage+infra cap.
	credit, err := accountPaasCreditMicros(true, 10, 200)
	require.NoError(t, err)
	require.EqualValues(t, 60, credit, "30% of the account infra total")
	require.LessOrEqual(t, credit, int64(10+200), "capped at usage + infra")
}

func TestAccountPaasCreditMicros_ZeroInfraZeroCredit(t *testing.T) {
	credit, err := accountPaasCreditMicros(true, 5_000_000, 0)
	require.NoError(t, err)
	require.Zero(t, credit, "the credit is infra-proportional; no infra → nothing to offset")
}

func TestAccountPaasCreditMicros_NeverExceedsUsagePlusInfra(t *testing.T) {
	// The cap invariant (credit ≤ moduleUsageTotal + infraTotal ⇒ the credit
	// can never eat base fees). Under today's pct formula the cap arm cannot
	// bind (pct% of infra ≤ infra); this sweep pins the INVARIANT so a future
	// flat-allowance credit formula inherits it rather than silently
	// discounting base fees.
	for _, tc := range []struct{ usage, infra int64 }{
		{0, 0},
		{0, 1},
		{0, 3},
		{0, 1_000_000_000},
		{1, 1},
		{123, 456_789},
		{5_000_000, 20_000_000},
	} {
		credit, err := accountPaasCreditMicros(true, tc.usage, tc.infra)
		require.NoError(t, err)
		require.GreaterOrEqual(t, credit, int64(0))
		require.LessOrEqual(t, credit, tc.usage+tc.infra,
			"usage=%d infra=%d: credit must never exceed the usage-plane charges", tc.usage, tc.infra)
	}
}
