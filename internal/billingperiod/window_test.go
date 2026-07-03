package billingperiod

import (
	"testing"
	"time"
)

func utc(y int, m time.Month, d int) time.Time {
	return time.Date(y, m, d, 0, 0, 0, 0, time.UTC)
}

func TestAnchorDay(t *testing.T) {
	// Anchor day is the day-of-month in UTC, derived from the stored instant.
	got := AnchorDay(time.Date(2026, 6, 2, 5, 30, 0, 0, time.UTC))
	if got != 2 {
		t.Fatalf("AnchorDay=%d want 2", got)
	}
	// A non-UTC zone is normalized to UTC before taking the day: 2026-06-02
	// 23:00 in UTC-4 is 2026-06-03 03:00 UTC → day 3.
	loc := time.FixedZone("UTC-4", -4*3600)
	got = AnchorDay(time.Date(2026, 6, 2, 23, 0, 0, 0, loc))
	if got != 3 {
		t.Fatalf("AnchorDay(cross-midnight)=%d want 3", got)
	}
}

func TestDaysIn(t *testing.T) {
	for _, tc := range []struct {
		y    int
		m    time.Month
		want int
	}{
		{2026, time.February, 28}, // non-leap
		{2024, time.February, 29}, // leap
		{2000, time.February, 29}, // leap (÷400)
		{1900, time.February, 28}, // NOT leap (÷100 not ÷400)
		{2026, time.April, 30},
		{2026, time.December, 31},
	} {
		if got := DaysIn(tc.y, tc.m); got != tc.want {
			t.Errorf("DaysIn(%d,%s)=%d want %d", tc.y, tc.m, got, tc.want)
		}
	}
}

func TestAnchoredPeriodWindow(t *testing.T) {
	for _, tc := range []struct {
		name               string
		at                 time.Time
		anchorDay          int
		wantStart, wantEnd time.Time
	}{
		{
			name: "card-binding example: anchor 2, mid-period",
			at:   time.Date(2026, 6, 15, 9, 0, 0, 0, time.UTC), anchorDay: 2,
			wantStart: utc(2026, 6, 2), wantEnd: utc(2026, 7, 2),
		},
		{
			name: "anchor 2, exactly on the boundary belongs to the NEW period",
			at:   utc(2026, 6, 2), anchorDay: 2,
			wantStart: utc(2026, 6, 2), wantEnd: utc(2026, 7, 2),
		},
		{
			name: "anchor 2, one instant before the boundary is still the OLD period",
			at:   utc(2026, 6, 2).Add(-time.Nanosecond), anchorDay: 2,
			wantStart: utc(2026, 5, 2), wantEnd: utc(2026, 6, 2),
		},
		{
			name: "anchor 1 reproduces the UTC calendar month",
			at:   time.Date(2026, 6, 19, 13, 30, 0, 0, time.UTC), anchorDay: 1,
			wantStart: utc(2026, 6, 1), wantEnd: utc(2026, 7, 1),
		},
		{
			name: "anchor 15 across a year boundary (Dec→Jan)",
			at:   utc(2025, 12, 20), anchorDay: 15,
			wantStart: utc(2025, 12, 15), wantEnd: utc(2026, 1, 15),
		},
		{
			name: "anchor 15 in early January windows back into December",
			at:   utc(2026, 1, 10), anchorDay: 15,
			wantStart: utc(2025, 12, 15), wantEnd: utc(2026, 1, 15),
		},
		// --- anchor 31 clamped across short months (the AddDate footgun) ---
		{
			name: "anchor 31, February (non-leap): boundary clamps to Feb 28",
			at:   utc(2026, 2, 10), anchorDay: 31,
			wantStart: utc(2026, 1, 31), wantEnd: utc(2026, 2, 28),
		},
		{
			name: "anchor 31, on the clamped Feb 28 boundary → March period",
			at:   utc(2026, 2, 28), anchorDay: 31,
			wantStart: utc(2026, 2, 28), wantEnd: utc(2026, 3, 31),
		},
		{
			name: "anchor 31, February (leap 2024): boundary clamps to Feb 29",
			at:   utc(2024, 2, 15), anchorDay: 31,
			wantStart: utc(2024, 1, 31), wantEnd: utc(2024, 2, 29),
		},
		{
			name: "anchor 31, April (30 days): boundary clamps to Apr 30",
			at:   utc(2026, 4, 10), anchorDay: 31,
			wantStart: utc(2026, 3, 31), wantEnd: utc(2026, 4, 30),
		},
		{
			name: "anchor 31, back in a 31-day month is NOT clamped",
			at:   utc(2026, 5, 15), anchorDay: 31,
			wantStart: utc(2026, 4, 30), wantEnd: utc(2026, 5, 31),
		},
		// --- anchor 29 / 30 clamps ---
		{
			name: "anchor 29, non-leap February clamps to 28",
			at:   utc(2026, 2, 14), anchorDay: 29,
			wantStart: utc(2026, 1, 29), wantEnd: utc(2026, 2, 28),
		},
		{
			name: "anchor 29, leap February is exactly the 29th",
			at:   utc(2024, 2, 14), anchorDay: 29,
			wantStart: utc(2024, 1, 29), wantEnd: utc(2024, 2, 29),
		},
		{
			name: "anchor 30, February clamps to 28; March is exactly the 30th",
			at:   utc(2026, 3, 15), anchorDay: 30,
			wantStart: utc(2026, 2, 28), wantEnd: utc(2026, 3, 30),
		},
		{
			name: "anchor 28 is literal every month (only 29/30/31 ever clamp)",
			at:   utc(2026, 2, 10), anchorDay: 28,
			wantStart: utc(2026, 1, 28), wantEnd: utc(2026, 2, 28),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			start, end := AnchoredPeriodWindow(tc.at, tc.anchorDay)
			if !start.Equal(tc.wantStart) {
				t.Errorf("start = %s want %s", start, tc.wantStart)
			}
			if !end.Equal(tc.wantEnd) {
				t.Errorf("end = %s want %s", end, tc.wantEnd)
			}
			// Invariants: non-empty, half-open contains at, UTC midnight boundaries.
			if !end.After(start) {
				t.Errorf("window must be non-empty: [%s, %s)", start, end)
			}
			at := tc.at.UTC()
			if at.Before(start) || !at.Before(end) {
				t.Errorf("at %s not in [%s, %s)", at, start, end)
			}
			for _, b := range []time.Time{start, end} {
				if b.Location() != time.UTC || b.Hour() != 0 || b.Minute() != 0 || b.Second() != 0 || b.Nanosecond() != 0 {
					t.Errorf("boundary %s is not midnight UTC", b)
				}
			}
		})
	}
}

// TestWindowsAreContiguous walks a full year of periods for the pathological
// anchor days and asserts each period's end is exactly the next period's start —
// no gaps, no overlaps — even across the Feb clamp (Jan31→Feb28→Mar31).
func TestWindowsAreContiguous(t *testing.T) {
	for _, anchorDay := range []int{1, 28, 29, 30, 31} {
		t.Run(time.Month(anchorDay).String(), func(t *testing.T) {
			// Start inside January 2024 (a leap year) and step 24 periods forward.
			cursor := utc(2024, 1, 10)
			_, prevEnd := AnchoredPeriodWindow(cursor, anchorDay)
			for i := 0; i < 24; i++ {
				// A timestamp one nanosecond into the next period.
				start, end := AnchoredPeriodWindow(prevEnd, anchorDay)
				if !start.Equal(prevEnd) {
					t.Fatalf("anchor %d: gap/overlap — prev end %s, next start %s", anchorDay, prevEnd, start)
				}
				if !end.After(start) {
					t.Fatalf("anchor %d: empty window [%s, %s)", anchorDay, start, end)
				}
				prevEnd = end
			}
		})
	}
}

// TestAnchoredJustClosedChainsToCurrent asserts the just-closed window's end
// equals the current window's start, so the cycle closes exactly the period that
// ended (and the contiguity means no usage falls between two runs).
func TestAnchoredJustClosed(t *testing.T) {
	for _, tc := range []struct {
		name               string
		at                 time.Time
		anchorDay          int
		wantStart, wantEnd time.Time
	}{
		{
			name: "anchor 2, mid-June → just-closed is May 2..Jun 2",
			at:   utc(2026, 6, 15), anchorDay: 2,
			wantStart: utc(2026, 5, 2), wantEnd: utc(2026, 6, 2),
		},
		{
			name: "anchor 31, mid-March → just-closed spans the Feb clamp (Jan31..Feb28)",
			at:   utc(2026, 3, 15), anchorDay: 31,
			wantStart: utc(2026, 1, 31), wantEnd: utc(2026, 2, 28),
		},
		{
			name: "anchor 31, mid-April → just-closed is Feb28..Mar31",
			at:   utc(2026, 4, 15), anchorDay: 31,
			wantStart: utc(2026, 2, 28), wantEnd: utc(2026, 3, 31),
		},
		{
			name: "anchor 15, early January → just-closed crosses the year (Nov15..Dec15)",
			at:   utc(2026, 1, 10), anchorDay: 15,
			wantStart: utc(2025, 11, 15), wantEnd: utc(2025, 12, 15),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			start, end := AnchoredJustClosed(tc.at, tc.anchorDay)
			if !start.Equal(tc.wantStart) {
				t.Errorf("start = %s want %s", start, tc.wantStart)
			}
			if !end.Equal(tc.wantEnd) {
				t.Errorf("end = %s want %s", end, tc.wantEnd)
			}
			// The just-closed end must equal the current period's start.
			curStart, _ := AnchoredPeriodWindow(tc.at, tc.anchorDay)
			if !end.Equal(curStart) {
				t.Errorf("just-closed end %s != current start %s", end, curStart)
			}
		})
	}
}

func TestShiftPeriods(t *testing.T) {
	for _, tc := range []struct {
		name      string
		from      time.Time
		months    int
		anchorDay int
		want      time.Time
	}{
		{
			name: "back 3 anchored periods from a June-2 boundary",
			from: utc(2026, 6, 2), months: -3, anchorDay: 2,
			want: utc(2026, 3, 2),
		},
		{
			name: "back 1 from a 31-anchor March boundary re-clamps Feb (not from Mar 31)",
			from: utc(2026, 3, 31), months: -1, anchorDay: 31,
			want: utc(2026, 2, 28),
		},
		{
			name: "forward 1 from the Feb-clamped boundary restores the ORIGINAL 31 (no ratchet)",
			from: utc(2026, 2, 28), months: 1, anchorDay: 31,
			want: utc(2026, 3, 31),
		},
		{
			name: "back 12 crosses the year",
			from: utc(2026, 6, 15), months: -12, anchorDay: 15,
			want: utc(2025, 6, 15),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := ShiftPeriods(tc.from, tc.months, tc.anchorDay)
			if !got.Equal(tc.want) {
				t.Errorf("ShiftPeriods = %s want %s", got, tc.want)
			}
		})
	}
}
