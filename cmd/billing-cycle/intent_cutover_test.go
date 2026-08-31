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

// The three states of the flag. The default matters most: a worker with
// no flag set must collect exactly as it did before, because that is
// what every deployed worker looks like today.
func TestIntentCutoverDecision(t *testing.T) {
	for _, tc := range []struct {
		name    string
		flag    string
		arm     bool
		wantErr bool
	}{
		{"unset stays on the legacy collecting path", "", false, false},
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

func (nilSaver) SaveIntentWithEvidence(
	_ context.Context, _ intent.ChargeIntent, _ *evidence.Recorder, _ evidence.Event,
) error {
	panic("not reached")
}
