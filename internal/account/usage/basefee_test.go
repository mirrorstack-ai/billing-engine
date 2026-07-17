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

func TestCreationChargeBaseMicros_MatchesSweepMath(t *testing.T) {
	periodStart := day(2026, 7, 11)
	periodEnd := day(2026, 8, 11)

	for _, tc := range []struct {
		name      string
		createdAt time.Time
		want      int64
	}{
		{
			name:      "period start is a full period without straddle",
			createdAt: day(2026, 7, 11),
			want:      20_000_000,
		},
		{
			name:      "reporter case is 25 of 31 days",
			createdAt: time.Date(2026, 7, 17, 12, 34, 0, 0, time.UTC),
			want:      16_129_032,
		},
		{
			name:      "grace ending exactly at period end adds the straddled full base",
			createdAt: day(2026, 8, 8),
			want:      21_935_484,
		},
		{
			name:      "period-end eve is one covered day plus the straddled full base",
			createdAt: time.Date(2026, 8, 10, 23, 0, 0, 0, time.UTC),
			want:      20_645_161,
		},
		{
			name:      "grace ending just before period end does not straddle",
			createdAt: time.Date(2026, 8, 7, 23, 59, 0, 0, time.UTC),
			want:      2_580_645,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, usage.CreationChargeBaseMicros(tc.createdAt, periodStart, periodEnd))
		})
	}
}

// TestCreationChargeAddonMicros_MatchesSweepOverageMath pins the pending
// preview's co-created add-on projection to the EXACT per-timer overage the
// sweep charges (cycle/proration.go): ProratedBaseMicros(ModuleOverageFeeMicros,
// created_at, window) plus the straddle top-up, times the over-count.
func TestCreationChargeAddonMicros_MatchesSweepOverageMath(t *testing.T) {
	periodStart := day(2026, 7, 11)
	periodEnd := day(2026, 8, 11)
	creationInstants := []time.Time{
		periodStart,
		time.Date(2026, 7, 17, 12, 34, 0, 0, time.UTC),
		day(2026, 7, 31),
		time.Date(2026, 8, 7, 23, 59, 0, 0, time.UTC), // no straddle (grace 8/10 < 8/11)
		day(2026, 8, 8),                               // straddles (grace 8/11 ≥ 8/11)
		time.Date(2026, 8, 10, 23, 0, 0, 0, time.UTC), // straddles
	}
	for _, createdAt := range creationInstants {
		for _, addonCount := range []int{0, 1, 3} {
			// Mirror the sweep's per-timer leg (proration.go overageMicros).
			perTimer := usage.ProratedBaseMicros(usage.ModuleOverageFeeMicros, createdAt, periodStart, periodEnd)
			if !usage.GraceExpiry(createdAt.UTC()).Before(periodEnd) {
				perTimer += usage.ModuleOverageFeeMicros
			}
			want := perTimer * int64(addonCount)
			require.Equal(t, want, usage.CreationChargeAddonMicros(createdAt, periodStart, periodEnd, addonCount),
				"%s ×%d", createdAt.Format(time.RFC3339), addonCount)
		}
	}
	// A non-positive over-count contributes nothing (an app at/under the pool).
	require.Zero(t, usage.CreationChargeAddonMicros(periodStart, periodStart, periodEnd, 0))
	require.Zero(t, usage.CreationChargeAddonMicros(periodStart, periodStart, periodEnd, -2))
}

func TestCreationChargeBaseMicros_EqualsSweepInputsAcrossWindow(t *testing.T) {
	periodStart := day(2026, 7, 11)
	periodEnd := day(2026, 8, 11)
	creationInstants := []time.Time{
		periodStart,
		time.Date(2026, 7, 11, 23, 59, 0, 0, time.UTC),
		time.Date(2026, 7, 17, 12, 34, 0, 0, time.UTC),
		day(2026, 7, 24),
		day(2026, 7, 31),
		day(2026, 8, 1),
		time.Date(2026, 8, 7, 23, 59, 0, 0, time.UTC),
		day(2026, 8, 8),
		time.Date(2026, 8, 9, 16, 0, 0, 0, time.UTC),
		time.Date(2026, 8, 10, 23, 0, 0, 0, time.UTC),
	}

	for _, createdAt := range creationInstants {
		want := usage.ProratedBaseMicros(usage.BaseFeeMicros, createdAt, periodStart, periodEnd)
		if !usage.GraceExpiry(createdAt.UTC()).Before(periodEnd) {
			want += usage.BaseFeeMicros
		}
		require.Equal(t, want, usage.CreationChargeBaseMicros(createdAt, periodStart, periodEnd), createdAt.Format(time.RFC3339))
	}
}
