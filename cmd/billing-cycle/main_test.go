package main

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/mirrorstack-ai/billing-engine/internal/account/cycle"
)

func TestJustClosedCalendarMonth(t *testing.T) {
	for _, tc := range []struct {
		name      string
		at        time.Time
		wantStart time.Time
		wantEnd   time.Time
	}{
		{
			name:      "mid month",
			at:        time.Date(2026, 6, 19, 13, 30, 0, 0, time.UTC),
			wantStart: time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC),
			wantEnd:   time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC),
		},
		{
			name:      "first instant of month",
			at:        time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC),
			wantStart: time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC),
			wantEnd:   time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC),
		},
		{
			name:      "year rollover (January → prior December)",
			at:        time.Date(2026, 1, 15, 0, 0, 0, 0, time.UTC),
			wantStart: time.Date(2025, 12, 1, 0, 0, 0, 0, time.UTC),
			wantEnd:   time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		},
		{
			name:      "March (prior is 28-day February)",
			at:        time.Date(2026, 3, 31, 23, 59, 59, 0, time.UTC),
			wantStart: time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC),
			wantEnd:   time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			start, end := justClosedCalendarMonth(tc.at)
			require.True(t, tc.wantStart.Equal(start), "start: want %s got %s", tc.wantStart, start)
			require.True(t, tc.wantEnd.Equal(end), "end: want %s got %s", tc.wantEnd, end)
			require.True(t, end.After(start), "window must be non-empty")
		})
	}
}

// TestTally covers every classification arm — including the RunStatusFailed arm
// added so a failed run is never silently uncounted (a non-nil failed summary
// path, even though RunBillingCycle returns (nil, err) on charge failure today).
func TestTally(t *testing.T) {
	acct := uuid.New()
	for _, tc := range []struct {
		name    string
		summary *cycle.ChargeSummary
		check   func(t *testing.T, r cycleResult)
	}{
		{
			name:    "already run (invoiced exists)",
			summary: &cycle.ChargeSummary{FirstRun: false},
			check:   func(t *testing.T, r cycleResult) { require.Equal(t, 1, r.AlreadyRun) },
		},
		{
			name:    "skipped no pm",
			summary: &cycle.ChargeSummary{FirstRun: true, Status: cycle.RunStatusSkippedNoPM, ArrearsMicros: 1_000},
			check:   func(t *testing.T, r cycleResult) { require.Equal(t, 1, r.SkippedNoPM) },
		},
		{
			name:    "failed",
			summary: &cycle.ChargeSummary{FirstRun: true, Status: cycle.RunStatusFailed, ArrearsMicros: 1_000},
			check:   func(t *testing.T, r cycleResult) { require.Equal(t, 1, r.FailedRuns) },
		},
		{
			name:    "zero arrears",
			summary: &cycle.ChargeSummary{FirstRun: true, Status: cycle.RunStatusInvoiced, ArrearsMicros: 0},
			check:   func(t *testing.T, r cycleResult) { require.Equal(t, 1, r.ZeroArrears) },
		},
		{
			name:    "charged",
			summary: &cycle.ChargeSummary{FirstRun: true, Status: cycle.RunStatusInvoiced, ArrearsMicros: 1_000, ChargedCents: 1},
			check:   func(t *testing.T, r cycleResult) { require.Equal(t, 1, r.Charged) },
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var r cycleResult
			tally(&r, acct, tc.summary)
			tc.check(t, r)
		})
	}
}
