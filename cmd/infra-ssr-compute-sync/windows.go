package main

import "time"

// hourWindow is a half-open [start, end) hour bucket — same shape as
// cmd/infra-egress-sync's hourWindow.
type hourWindow struct {
	start time.Time
	end   time.Time
}

// propagationLag is the minimum age a closed hour window must have before
// this job will sweep it. CloudWatch does NOT guarantee full/instant
// datapoint propagation the instant an hour rolls over (design doc §8 HIGH
// finding #2) — sweeping too early risks a GetMetricData call returning
// PartialData for a window that will be Complete moments later, and (unlike
// a plain retry) recording ANYTHING under that window's ON CONFLICT DO
// NOTHING idempotency key would make a wrong/partial value permanent. This
// margin, plus the explicit StatusCode=="Complete" gate in correlateResults,
// are the two independent defenses against that failure mode.
const propagationLag = 10 * time.Minute

// closedHourWindowsWithLag returns the last `count` FULLY-CLOSED hour buckets
// ending at the top of the hour containing (at - propagationLag) — i.e. the
// current partial hour AND any hour that closed less than propagationLag ago
// are both excluded. Otherwise identical to infra-egress-sync's
// closedHourWindows: the deterministic event_id makes the lookback overlap
// across runs a no-op (already-recorded windows dedupe via ON CONFLICT DO
// NOTHING).
//
// e.g. at=12:05, count=3, propagationLag=10m: cutoff=11:55, so the boundary
// hour is 11:00 and windows are [08:00,09:00), [09:00,10:00), [10:00,11:00) —
// [11:00,12:00) has closed but is EXCLUDED because it closed only 5 minutes
// ago. Once at=12:11, cutoff=12:01, the boundary hour becomes 12:00 and
// [11:00,12:00) enters the swept set.
func closedHourWindowsWithLag(at time.Time, count int, lag time.Duration) []hourWindow {
	cutoff := at.UTC().Add(-lag)
	topOfHour := cutoff.Truncate(time.Hour)
	windows := make([]hourWindow, 0, count)
	for i := count; i >= 1; i-- {
		start := topOfHour.Add(time.Duration(-i) * time.Hour)
		windows = append(windows, hourWindow{start: start, end: start.Add(time.Hour)})
	}
	return windows
}
