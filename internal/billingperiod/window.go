// Package billingperiod computes an account's billing-period windows, anchored
// to the day-of-month the account bound its first credit card (its
// billing-account activation) — NOT the UTC calendar month and NOT the signup
// date (ADR 0005). All boundaries are midnight UTC; every window is half-open
// [start, end) so a timestamp exactly on a boundary counts once, in the NEW
// period.
//
// The anchor day is derived in-process from the stored activated_at timestamp
// (AnchorDay); it is NEVER re-derived from a previously-clamped boundary. When a
// month is shorter than the anchor day the boundary CLAMPS to the last day of
// that month (anchor 31 → Feb 28/29, Apr 30, …); the ORIGINAL anchor day is
// re-clamped for EACH target month independently, so consecutive windows stay
// exactly contiguous and a 31-anchor never ratchets down to 28 permanently.
package billingperiod

import "time"

// DefaultAnchorDay is the fallback anchor when an account has no activated_at
// (never bound a card). Anchor day 1 reproduces the pre-025 UTC-calendar-month
// window exactly, so a NULL anchor degrades to the historical behavior.
const DefaultAnchorDay = 1

// AnchorDay returns the billing-period anchor day (1..31) for an activation
// instant: its day-of-month evaluated in UTC. Derive the anchor from the stored
// activated_at with this — never from an already-clamped period boundary.
func AnchorDay(activatedAt time.Time) int {
	return activatedAt.UTC().Day()
}

// DaysIn returns the number of days in month m of year y. Computed as "day 0 of
// the next month" (Go normalizes it to the last day of m), so it is correct for
// February in leap and non-leap years without a leap-year branch.
func DaysIn(y int, m time.Month) int {
	return time.Date(y, m+1, 0, 0, 0, 0, 0, time.UTC).Day()
}

// addMonths shifts (y, m) by d whole months using month-INDEX arithmetic only —
// it never touches a day-of-month, so it cannot trigger Go's Date-normalization
// overflow (e.g. Jan 31 + 1 month → Mar 3). d may be negative. Returns the
// resulting (year, month).
func addMonths(y int, m time.Month, d int) (int, time.Month) {
	// Zero-based month index arithmetic (Jan == 0) makes the modulo clean.
	t := (int(m) - 1) + d
	ny := y + t/12
	mi := t % 12
	if mi < 0 { // Go's % keeps the sign of the dividend; normalize into [0,11].
		mi += 12
		ny--
	}
	return ny, time.Month(mi + 1)
}

// clampedAnchor returns the midnight-UTC boundary for month (y, m) at the anchor
// day, clamping the day down to the month's last day when the month is shorter
// than anchorDay. The anchorDay passed in is always the ORIGINAL stored anchor,
// never a previously-clamped value, so clamping is idempotent and reversible.
func clampedAnchor(y int, m time.Month, anchorDay int) time.Time {
	d := anchorDay
	if dim := DaysIn(y, m); d > dim {
		d = dim
	}
	return time.Date(y, m, d, 0, 0, 0, 0, time.UTC)
}

// AnchoredPeriodWindow returns the [start, end) of the anchored billing period
// containing t, for an account whose anchor day is anchorDay. start is the most
// recent anchor boundary at or before t; end is the next anchor boundary. Both
// are clamped independently to their own month, so the window is exactly
// contiguous with its neighbors. t is normalized to UTC first; a t exactly on a
// boundary belongs to the period that boundary OPENS (half-open [start, end)).
func AnchoredPeriodWindow(t time.Time, anchorDay int) (start, end time.Time) {
	t = t.UTC()
	y, m := t.Year(), t.Month()
	a := clampedAnchor(y, m, anchorDay)
	if !t.Before(a) {
		// t is on/after this month's boundary → current period is [this, next).
		ny, nm := addMonths(y, m, 1)
		return a, clampedAnchor(ny, nm, anchorDay)
	}
	// t is before this month's boundary → current period is [prev, this).
	py, pm := addMonths(y, m, -1)
	return clampedAnchor(py, pm, anchorDay), a
}

// AnchoredJustClosed returns the [start, end) of the period that closed most
// recently as of t — i.e. the period immediately BEFORE the one containing t.
// Its end equals the current period's start, so cmd/billing-cycle rolls up and
// charges exactly the window that just ended. Both boundaries are clamped
// independently from the ORIGINAL anchorDay.
func AnchoredJustClosed(t time.Time, anchorDay int) (start, end time.Time) {
	curStart, _ := AnchoredPeriodWindow(t, anchorDay)
	py, pm := addMonths(curStart.Year(), curStart.Month(), -1)
	return clampedAnchor(py, pm, anchorDay), curStart
}

// ShiftPeriods returns the anchor boundary `months` periods away from the one in
// t's month — clampedAnchor(month(t) ± months, anchorDay). months may be
// negative. It steps by month-index arithmetic and re-clamps from the ORIGINAL
// anchorDay (never from t's own possibly-clamped day), so stepping a 31-anchor
// across a short month does not permanently ratchet the day down. Used to walk
// the multi-month usage-history window backward by whole anchored periods.
func ShiftPeriods(t time.Time, months, anchorDay int) time.Time {
	t = t.UTC()
	y, m := addMonths(t.Year(), t.Month(), months)
	return clampedAnchor(y, m, anchorDay)
}
