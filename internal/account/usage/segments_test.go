package usage_test

import (
	"testing"
	"time"

	"github.com/mirrorstack-ai/billing-engine/internal/account/usage"
)

// The period every case below runs in: 30 whole UTC days, anchored on the 4th.
var (
	segPeriodStart = time.Date(2026, 6, 4, 0, 0, 0, 0, time.UTC)
	segPeriodEnd   = time.Date(2026, 7, 4, 0, 0, 0, 0, time.UTC)
)

// TestSegmentedProrationIsProratedBaseForOneSegment: an app that never changed
// plan must be billed to the micro as before — the segment helper is the same
// formula with an explicit end, and this pins that for every creation day in
// the period, on both a whole-dollar base and one that does not divide evenly.
func TestSegmentedProrationIsProratedBaseForOneSegment(t *testing.T) {
	t.Parallel()

	for _, base := range []int64{usage.BaseFeeMicros, 50_000_000, 7_777_777} {
		for day := segPeriodStart.AddDate(0, 0, -2); day.Before(segPeriodEnd.AddDate(0, 0, 2)); day = day.AddDate(0, 0, 1) {
			created := day.Add(13 * time.Hour)
			want := usage.ProratedBaseMicros(base, created, segPeriodStart, segPeriodEnd)
			got := usage.SegmentedProratedBaseMicros([]usage.BaseSegment{{From: created, BaseMicros: base}}, segPeriodStart, segPeriodEnd)
			if got != want {
				t.Errorf("base %d created %s: segmented %d, ProratedBaseMicros %d", base, created, got, want)
			}
		}
	}
}

// TestSegmentedProrationSplitsByDay pins the owner's example shape: created on
// the 10th on Free, upgraded to Pro at 15:00 on the 19th → days 10–18 at $0
// and days 19–3 (15 days) at $20 → 20 × 15/30 = $10.00 exactly, and the
// change DAY itself is at the new plan.
func TestSegmentedProrationSplitsByDay(t *testing.T) {
	t.Parallel()

	created := time.Date(2026, 6, 10, 9, 0, 0, 0, time.UTC)
	changed := time.Date(2026, 6, 19, 15, 0, 0, 0, time.UTC)
	segments := []usage.BaseSegment{
		{From: created, BaseMicros: 0},
		{From: changed, BaseMicros: usage.BaseFeeMicros},
	}
	if got := usage.SegmentedProratedBaseMicros(segments, segPeriodStart, segPeriodEnd); got != 10_000_000 {
		t.Errorf("free→pro on the 19th of a 30-day period = %d, want 10_000_000", got)
	}

	// Pro → Business on the same day: 20 × 9/30 + 50 × 15/30 = 6 + 25 = $31.
	segments = []usage.BaseSegment{
		{From: created, BaseMicros: usage.BaseFeeMicros},
		{From: changed, BaseMicros: 50_000_000},
	}
	if got := usage.SegmentedProratedBaseMicros(segments, segPeriodStart, segPeriodEnd); got != 31_000_000 {
		t.Errorf("pro→business on the 19th = %d, want 31_000_000", got)
	}

	// A change on the creation day prices the whole window at the new plan.
	sameDay := []usage.BaseSegment{
		{From: created, BaseMicros: 0},
		{From: created.Add(2 * time.Hour), BaseMicros: usage.BaseFeeMicros},
	}
	want := usage.ProratedBaseMicros(usage.BaseFeeMicros, created, segPeriodStart, segPeriodEnd)
	if got := usage.SegmentedProratedBaseMicros(sameDay, segPeriodStart, segPeriodEnd); got != want {
		t.Errorf("same-day change = %d, want the whole window at the new plan %d", got, want)
	}
}

// TestProratedSegmentMicrosClampsToThePeriod: a segment that starts before the
// period is clamped to its start; one that ends after it, to its end; one
// entirely outside contributes nothing; the whole period is the full base.
func TestProratedSegmentMicrosClampsToThePeriod(t *testing.T) {
	t.Parallel()

	base := usage.BaseFeeMicros
	cases := []struct {
		name     string
		from, to time.Time
		want     int64
	}{
		{"whole period", segPeriodStart, segPeriodEnd, base},
		{"starts before, covers all", segPeriodStart.AddDate(0, 0, -10), segPeriodEnd.AddDate(0, 0, 10), base},
		{"entirely before", segPeriodStart.AddDate(0, 0, -10), segPeriodStart, 0},
		{"entirely after", segPeriodEnd, segPeriodEnd.AddDate(0, 0, 3), 0},
		{"last 15 days", segPeriodStart.AddDate(0, 0, 15), segPeriodEnd, base / 2},
		{"first 15 days, end clamped", segPeriodStart, segPeriodStart.AddDate(0, 0, 15), base / 2},
		{"empty", segPeriodStart.AddDate(0, 0, 5), segPeriodStart.AddDate(0, 0, 5), 0},
		{"inverted", segPeriodStart.AddDate(0, 0, 9), segPeriodStart.AddDate(0, 0, 5), 0},
	}
	for _, c := range cases {
		if got := usage.ProratedSegmentMicros(base, c.from, c.to, segPeriodStart, segPeriodEnd); got != c.want {
			t.Errorf("%s: %d, want %d", c.name, got, c.want)
		}
	}
}

// TestUpgradeDeltaIsTheDifferenceNotTheNewPrice pins the owner's confirmation
// (2026-09-13): a mid-period upgrade charges (new − old) × remaining, so Free
// → Pro with half the period left is $10.00, never $20.00.
func TestUpgradeDeltaIsTheDifferenceNotTheNewPrice(t *testing.T) {
	t.Parallel()

	half := segPeriodStart.AddDate(0, 0, 15).Add(11 * time.Hour)
	delta := usage.TermsFor(usage.PlanPro).BaseFeeMicros - usage.TermsFor(usage.PlanFree).BaseFeeMicros
	if got := usage.ProratedSegmentMicros(delta, half, segPeriodEnd, segPeriodStart, segPeriodEnd); got != 10_000_000 {
		t.Errorf("free→pro at half period = %d, want 10_000_000", got)
	}
}
