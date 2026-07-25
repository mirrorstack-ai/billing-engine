package main

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/mirrorstack-ai/billing-engine/internal/account/cycle"
	"github.com/mirrorstack-ai/billing-engine/internal/account/legacyrestamp"
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

type fakeLegacyRestampRunner struct {
	inputs  []uuid.UUID
	results []legacyrestamp.Result
	errs    []error
}

func (r *fakeLegacyRestampRunner) RunPage(
	_ context.Context,
	after uuid.UUID,
) (legacyrestamp.Result, error) {
	r.inputs = append(r.inputs, after)
	index := len(r.inputs) - 1
	return r.results[index], r.errs[index]
}

func TestLegacyRestampHandlerStableCursorAndReleaseEvidence(t *testing.T) {
	after := uuid.MustParse("00000000-0000-4000-8000-000000000010")
	next := uuid.MustParse("00000000-0000-4000-8000-000000000020")
	cfg := legacyrestamp.Config{
		Enabled:    true,
		CoreSHA:    "1111111111111111111111111111111111111111",
		BillingSHA: "2222222222222222222222222222222222222222",
	}
	runner := &fakeLegacyRestampRunner{
		results: []legacyrestamp.Result{{
			Scanned: 10, Delivered: 10, TotalOwners: 20,
			NextCursor: next.String(),
		}},
		errs: []error{nil},
	}

	response, err := legacyRestampHandler(runner, cfg)(
		context.Background(),
		legacyRestampRequest{AfterAccountID: after.String()},
	)

	require.NoError(t, err)
	require.Equal(t, []uuid.UUID{after}, runner.inputs)
	require.Equal(t, legacyRestampResponse{
		NextAfterAccountID: next.String(),
		Attempted:          10,
		Succeeded:          10,
		TotalOwners:        20,
		CoreManifestSHA:    cfg.CoreSHA,
		BillingEngineSHA:   cfg.BillingSHA,
	}, response)

	payload, err := json.Marshal(response)
	require.NoError(t, err)
	var shape map[string]any
	require.NoError(t, json.Unmarshal(payload, &shape))
	require.ElementsMatch(t, []string{
		"complete",
		"next_after_account_id",
		"attempted",
		"succeeded",
		"failed",
		"blocked",
		"total_owners",
		"core_manifest_sha",
		"billing_engine_sha",
	}, mapKeys(shape))
}

func TestLegacyRestampHandlerMalformedCursorNeverRunsPage(t *testing.T) {
	runner := &fakeLegacyRestampRunner{}
	_, err := legacyRestampHandler(runner, legacyrestamp.Config{})(
		context.Background(),
		legacyRestampRequest{AfterAccountID: "not-a-uuid"},
	)
	require.Error(t, err)
	require.Empty(t, runner.inputs)
}

func TestLegacyRestampHandlerFailedPageReturnsErrorAndRetainedCursor(t *testing.T) {
	after := uuid.MustParse("00000000-0000-4000-8000-000000000010")
	pageErr := errors.New("one owner POST failed")
	runner := &fakeLegacyRestampRunner{
		results: []legacyrestamp.Result{{
			Scanned: 10, Delivered: 9, Failed: 1,
			NextCursor: after.String(),
		}},
		errs: []error{pageErr},
	}

	response, err := legacyRestampHandler(
		runner,
		legacyrestamp.Config{
			CoreSHA:    "1111111111111111111111111111111111111111",
			BillingSHA: "2222222222222222222222222222222222222222",
		},
	)(
		context.Background(),
		legacyRestampRequest{AfterAccountID: after.String()},
	)

	require.ErrorIs(t, err, pageErr)
	require.Equal(t, after.String(), response.NextAfterAccountID)
	require.Equal(t, 1, response.Failed)
}

func mapKeys(values map[string]any) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	return keys
}
