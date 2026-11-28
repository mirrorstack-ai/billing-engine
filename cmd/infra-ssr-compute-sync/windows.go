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

// sweepableEnd is the exclusive end of the newest window this job may sweep:
// the top of the hour containing (at - lag). The current partial hour AND any
// hour that closed less than lag ago are both excluded.
//
// e.g. at=12:05, lag=10m: cutoff=11:55, so the newest sweepable window is
// [10:00,11:00) — [11:00,12:00) has closed but is EXCLUDED because it closed
// only 5 minutes ago. Once at=12:11, cutoff=12:01 and [11:00,12:00) enters.
func sweepableEnd(at time.Time, lag time.Duration) time.Time {
	return at.UTC().Add(-lag).Truncate(time.Hour)
}
