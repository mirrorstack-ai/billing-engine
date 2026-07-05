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

// --- AccountOverageMicros: account-wide POOLED overage tier boundaries -------

func TestAccountOverageMicros_PooledBoundaries(t *testing.T) {
	// Owner spec 2026-07-05 / migration 032: $3 × max(0, pooledCount − 5), where
	// pooledCount is Σ live-app module_count for the WHOLE account. The boundary
	// cases pin the tier edges: ≤5 pooled modules cost nothing, the 6th costs
	// exactly one overage, and the overage is NOT the flat base (it is the
	// account-level surcharge only).
	for _, tc := range []struct {
		name   string
		pooled int
		want   int64
	}{
		{"zero pooled → no overage", 0, 0},
		{"under pool (3) → no overage", 3, 0},
		{"exactly included (5) → no overage", usage.IncludedModules, 0},
		{"included+1 (6) → one $3 overage", usage.IncludedModules + 1, usage.ModuleOverageFeeMicros},
		{"included+10 → ten overages", usage.IncludedModules + 10, 10 * usage.ModuleOverageFeeMicros},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, usage.AccountOverageMicros(tc.pooled))
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

func TestProratedOverageMicros_ProratesPooledOverageFromGraceEnd(t *testing.T) {
	// Migration 032: the mid-period sweep prorates the account-wide POOLED
	// overage from grace-end to the period end with the SAME day-count math as
	// ProratedBaseMicros. Pool of 7 → 2 over → $6/period; grace ends mid-period
	// (Jun 19 → 15 of a 30-day [Jun 4, Jul 4) period) → half → $3.
	overage := usage.AccountOverageMicros(7)
	require.EqualValues(t, 6_000_000, overage)
	got := usage.ProratedOverageMicros(overage, day(2026, 6, 19), day(2026, 6, 4), day(2026, 7, 4))
	require.EqualValues(t, 3_000_000, got)

	// grace-end on/before the period start → the FULL pooled overage (over the
	// whole period).
	require.EqualValues(t, overage, usage.ProratedOverageMicros(overage, day(2026, 6, 4), day(2026, 6, 4), day(2026, 7, 4)))

	// grace-end on/after the period end → 0 (grace ends after this period).
	require.EqualValues(t, 0, usage.ProratedOverageMicros(overage, day(2026, 7, 4), day(2026, 6, 4), day(2026, 7, 4)))
}
