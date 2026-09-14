package intentcutover

import (
	"encoding/hex"
	"errors"
	"testing"
	"time"

	"github.com/mirrorstack-ai/billing-engine/internal/shared/signing"
)

func TestDecision(t *testing.T) {
	for _, tc := range []struct {
		flag string
		arm  bool
		err  bool
	}{{"", false, false}, {Armed, true, false}, {"true", false, true}, {"1", false, true}, {Env, false, true}} {
		arm, err := Decision(tc.flag)
		if arm != tc.arm || (err != nil) != tc.err {
			t.Fatalf("Decision(%q) = %v, %v; want %v, err=%v", tc.flag, arm, err, tc.arm, tc.err)
		}
		if err != nil && !errors.Is(err, ErrUnrecognisedFlag) {
			t.Fatalf("Decision(%q): %v is not ErrUnrecognisedFlag", tc.flag, err)
		}
	}
}

// Arm is the one contract both Lambdas start through: unset arms nothing and
// errs nothing; an unrecognised flag is an ArmError at the flag stage; the
// armed value without the evidence key names that key; with it, a proposer.
func TestArmIsOneContract(t *testing.T) {
	now := func() time.Time { return time.Unix(0, 0).UTC() }
	env := func(vars map[string]string) func(string) string {
		return func(k string) string { return vars[k] }
	}
	if p, armed, err := Arm(nil, env(nil), now); p != nil || armed || err != nil {
		t.Fatalf("unset flag: %v %v %v", p, armed, err)
	}
	var ae *ArmError
	if _, armed, err := Arm(nil, env(map[string]string{Env: "true"}), now); !errors.As(err, &ae) || ae.Stage != "flag" || ae.Needs != Env || armed {
		t.Fatalf("unrecognised flag: %v armed=%v", err, armed)
	}
	if _, armed, err := Arm(nil, env(map[string]string{Env: Armed}), now); !errors.As(err, &ae) || ae.Needs != signing.EnvBillingEvidenceKey || !armed {
		t.Fatalf("armed without the evidence key must name it: %v armed=%v", err, armed)
	}
	seed := make([]byte, 32)
	for i := range seed {
		seed[i] = byte(i + 1)
	}
	p, armed, err := Arm(nil, env(map[string]string{Env: Armed, signing.EnvBillingEvidenceKey: hex.EncodeToString(seed)}), now)
	if err != nil || !armed || p == nil {
		t.Fatalf("armed with the evidence key: p=%v armed=%v err=%v", p, armed, err)
	}
}
