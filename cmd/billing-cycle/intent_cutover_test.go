package main

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/mirrorstack-ai/billing-engine/internal/account/cycle"
	"github.com/mirrorstack-ai/billing-engine/internal/intent"
	"github.com/mirrorstack-ai/billing-engine/internal/intent/evidence"
	"github.com/mirrorstack-ai/billing-engine/internal/intent/evidence/evidencetest"
	"github.com/mirrorstack-ai/billing-engine/internal/intent/proposer"
)

// The three states of the flag.
//
// The default no longer means "collect exactly as before": no leg has a
// collector left, so an unset flag arms nothing and buildService then
// refuses to start on it — see TestAnUnarmedWorkerRefusesToStart. What
// this function still owns is the refusal of a flag that is neither,
// which is why a truthy-looking value is an error and not a default.
func TestIntentCutoverDecision(t *testing.T) {
	for _, tc := range []struct {
		name    string
		flag    string
		arm     bool
		wantErr bool
	}{
		{"unset arms nothing (and the worker then refuses to start)", "", false, false},
		{"the exact armed value arms", intentCutoverArmed, true, false},
		{"a truthy-looking value is refused, not accepted", "true", false, true},
		{"1 is refused", "1", false, true},
		{"yes is refused", "yes", false, true},
		{"a near miss is refused", "propose-do-not-collect ", false, true},
		{"the env var name is refused", intentCutoverEnv, false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			arm, err := intentCutoverDecision(tc.flag)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("flag %q was accepted; a wrong belief about whether "+
						"money is moving is the failure this refusal exists to prevent", tc.flag)
				}
				if !errors.Is(err, errUnrecognisedCutoverFlag) {
					t.Fatalf("flag %q: got %v, want errUnrecognisedCutoverFlag", tc.flag, err)
				}
			} else if err != nil {
				t.Fatalf("flag %q: unexpected error %v", tc.flag, err)
			}
			if arm != tc.arm {
				t.Fatalf("flag %q: arm=%v, want %v", tc.flag, arm, tc.arm)
			}
		})
	}
}

// The regression that motivated all of this: WithIntentProposer had no
// non-test caller, so the cutover branch inside every leg was
// unreachable on a deployed worker. Proving the decision function is
// not enough — this proves the seam actually attaches.
func TestArmingActuallyAttachesTheSeam(t *testing.T) {
	svc := cycle.NewService(nil, nil)
	if svc.IntentProposerArmed() {
		t.Fatal("a freshly built service is already armed")
	}

	p, err := proposer.New(nilSaver{}, evidencetest.Recorder(t), func() time.Time { return evidencetest.At })
	if err != nil {
		t.Fatalf("proposer.New: %v", err)
	}
	armed := svc.WithIntentProposer(p)
	if !armed.IntentProposerArmed() {
		t.Fatal("WithIntentProposer did not attach the seam")
	}
}

// nilSaver is a Store that is never called: the test asserts the wiring,
// not a save. Giving it a body would test the proposer instead.
type nilSaver struct{}

func (nilSaver) IntentState(_ context.Context, _ string) (string, bool, error) { return "", false, nil }

func (nilSaver) PayerForAccount(_ context.Context, _ string) (intent.Subject, error) {
	panic("not reached")
}

func (nilSaver) SaveIntentWithEvidence(
	_ context.Context, _ intent.ChargeIntent, _ *evidence.Recorder, _ evidence.Event,
) error {
	panic("not reached")
}

func (nilSaver) SaveIntentGroupWithEvidence(
	context.Context, string, []intent.ChargeIntent, *evidence.Recorder, []evidence.Event,
) error {
	return nil
}

// 🔴 A WORKER THAT CAN BILL NOBODY MUST NOT START.
//
// The second half of the owner's 2026-09-12 decision on core-v2#1400. The first
// half is in the leg (boundary_charges.go): refuse cleanly instead of
// nil-dereferencing. This half is why that refusal should never be reached on a
// deployed worker — an unarmed billing-cycle bills nothing, and it should say so
// once, at deploy, rather than a month later when the first period boundary
// falls due and every run fails.
//
// The predicate is taken from the SERVICE, not from a bool this test invented:
// IntentProposerArmed is the same field every leg's nil guard reads, so a wiring
// change that stops attaching the proposer moves this test too. A test that
// passed its own `false` would pin the policy while the seam drifted underneath
// it — which is exactly how WithIntentProposer spent two legs with no caller.
func TestAnUnarmedWorkerRefusesToStart(t *testing.T) {
	svc := cycle.NewService(nil, nil)

	err := cutoverWiringDecision(svc.IntentProposerArmed())
	if err == nil {
		t.Fatal("a service with no proposer was allowed to start. Every charge leg proposes " +
			"and none has a collector, so this worker would run its whole schedule, bill " +
			"nobody, and refuse one boundary at a time")
	}
	if !errors.Is(err, errNoProposerNoCollector) {
		t.Fatalf("got %v, want errNoProposerNoCollector", err)
	}

	p, perr := proposer.New(nilSaver{}, evidencetest.Recorder(t), func() time.Time { return evidencetest.At })
	if perr != nil {
		t.Fatalf("proposer.New: %v", perr)
	}
	if err := cutoverWiringDecision(svc.WithIntentProposer(p).IntentProposerArmed()); err != nil {
		t.Fatalf("an armed worker was refused a start: %v. The refusal must gate the unwired "+
			"case only — gating the wired one stops billing entirely", err)
	}
}
