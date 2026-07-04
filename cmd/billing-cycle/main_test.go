package main

import (
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/mirrorstack-ai/billing-engine/internal/account/cycle"
)

// The per-account anchored window math (AnchoredJustClosed, clamping,
// contiguity) is unit-tested in internal/billingperiod; the cycle driver here
// only threads each account's anchor day into it. TestTally covers the
// charge-summary classification the driver still owns.

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
