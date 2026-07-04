package usage_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/mirrorstack-ai/billing-engine/internal/account/usage"
)

func day(y int, m time.Month, d int) time.Time {
	return time.Date(y, m, d, 0, 0, 0, 0, time.UTC)
}

// --- AppBaseFeeMicros: overage tier boundaries ------------------------------

func TestAppBaseFeeMicros_OverageBoundaries(t *testing.T) {
	// Owner spec 2026-07-05: base + $3 × max(0, count − 5). The boundary cases
	// pin the tier edges: 5 included modules cost nothing extra, the 6th costs
	// exactly one overage, 0 modules never discount below the base.
	for _, tc := range []struct {
		name  string
		count int
		want  int64
	}{
		{"zero modules → flat base (no negative overage)", 0, usage.BaseFeeMicros},
		{"exactly included (5) → flat base", usage.IncludedModules, usage.BaseFeeMicros},
		{"included+1 (6) → one $3 overage", usage.IncludedModules + 1, usage.BaseFeeMicros + usage.ModuleOverageFeeMicros},
		{"included+10 → ten overages", usage.IncludedModules + 10, usage.BaseFeeMicros + 10*usage.ModuleOverageFeeMicros},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, usage.AppBaseFeeMicros(usage.BaseFeeMicros, tc.count))
		})
	}
}

// --- ProratedBaseMicros: creation-period proration ---------------------------

func TestProratedBaseMicros(t *testing.T) {
	base := usage.BaseFeeMicros // $20

	for _, tc := range []struct {
		name       string
		base       int64
		createdAt  time.Time
		start, end time.Time
		want       int64
	}{
		{
			// The owner's worked example (anchor 4): create on the 1st with the
			// period ending on the 4th → days 1–3 inclusive = 3 of 30 days.
			// 20e6 × 3/30 = 2e6 exactly.
			name:      "owner example: 3 remaining days of a 30-day period",
			base:      base,
			createdAt: time.Date(2026, 7, 1, 8, 30, 0, 0, time.UTC), // mid-day → truncates to Jul 1
			start:     day(2026, 6, 4), end: day(2026, 7, 4),
			want: 2_000_000,
		},
		{
			name:      "created on/before period start → full base",
			base:      base,
			createdAt: day(2026, 6, 4),
			start:     day(2026, 6, 4), end: day(2026, 7, 4),
			want: base,
		},
		{
			name:      "created before the period (historical display) → full base",
			base:      base,
			createdAt: day(2026, 1, 15),
			start:     day(2026, 6, 4), end: day(2026, 7, 4),
			want: base,
		},
		{
			name:      "created on period end → zero (did not exist in the period)",
			base:      base,
			createdAt: day(2026, 7, 4),
			start:     day(2026, 6, 4), end: day(2026, 7, 4),
			want: 0,
		},
		{
			name:      "created on the LAST day → exactly one day billed",
			base:      base,
			createdAt: time.Date(2026, 7, 3, 23, 59, 59, 0, time.UTC),
			start:     day(2026, 6, 4), end: day(2026, 7, 4),
			want: 666_667, // round_half_up(20e6 × 1/30) = round(666_666.67)
		},
		{
			// Round-half-up at an exact .5: base 3 micros, 1 of 2 days → 1.5 → 2.
			// (Synthetic base; the formula (a×b + d/2) / d must tie-break UP.)
			name:      "half-up tie rounds up",
			base:      3,
			createdAt: day(2026, 6, 5),
			start:     day(2026, 6, 4), end: day(2026, 6, 6),
			want: 2,
		},
		{
			// Below-half remainder rounds DOWN: a 31-day period [May 4, Jun 4),
			// created Jun 3 → 1 remaining day. 20e6 × 1/31 = 645_161.29… →
			// 645_161 (the +d/2 term must not push a .29 fraction up).
			name:      "sub-half remainder rounds down",
			base:      base,
			createdAt: day(2026, 6, 3),
			start:     day(2026, 5, 4), end: day(2026, 6, 4),
			want: 645_161,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, usage.ProratedBaseMicros(tc.base, tc.createdAt, tc.start, tc.end))
		})
	}
}

func TestProratedBaseMicros_ClampedAnchorMonth(t *testing.T) {
	// A 31-anchor across February clamps to Feb 28 (2026 is not a leap year):
	// period [Jan 31, Feb 28) = 28 days. Created Feb 10 → remain = 18 days.
	// round_half_up(20e6 × 18/28) = round_half_up(12_857_142.857…) = 12_857_143.
	got := usage.ProratedBaseMicros(usage.BaseFeeMicros, day(2026, 2, 10), day(2026, 1, 31), day(2026, 2, 28))
	require.EqualValues(t, 12_857_143, got)

	// And the NEXT clamped window [Feb 28, Mar 31) = 31 days, created Mar 1 →
	// remain 30 → round_half_up(20e6 × 30/31) = 19_354_839 (…838.7 rounds up).
	got = usage.ProratedBaseMicros(usage.BaseFeeMicros, day(2026, 3, 1), day(2026, 2, 28), day(2026, 3, 31))
	require.EqualValues(t, 19_354_839, got)
}

func TestProratedBaseMicros_ProratesTheOverageToo(t *testing.T) {
	// The prorated amount is the FULL app base (base + overage), not the flat
	// fee: 7 modules → 26e6; half of a 30-day period (15 days) → 13e6.
	appBase := usage.AppBaseFeeMicros(usage.BaseFeeMicros, 7)
	got := usage.ProratedBaseMicros(appBase, day(2026, 6, 19), day(2026, 6, 4), day(2026, 7, 4))
	require.EqualValues(t, 13_000_000, got)
}
