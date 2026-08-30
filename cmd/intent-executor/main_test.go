package main

import (
	"errors"
	"testing"

	"github.com/mirrorstack-ai/billing-engine/internal/account/capabilities"
	"github.com/mirrorstack-ai/billing-engine/internal/shared/buildinfo"
)

// An executor that starts anyway and declines every intent looks
// identical to one that is working and finding nothing to do. So each
// readiness condition refuses loudly, and each is checked here — a gate
// that cannot fail is not a gate.
func TestReadinessRefusesEachWayIndependently(t *testing.T) {
	ready := capabilities.Report{
		Build:            buildinfo.Info{Identified: true},
		LegacyMoneyPaths: 0,
	}

	if err := readiness(ready, "1"); err != nil {
		t.Fatalf("a fully ready deployment was refused: %v", err)
	}

	cases := []struct {
		name    string
		caps    capabilities.Report
		enabled string
		want    error
	}{
		{"not enabled", ready, "", errNotEnabled},
		{
			"build not stamped",
			capabilities.Report{Build: buildinfo.Info{Identified: false}, LegacyMoneyPaths: 0},
			"1", errBuildNotIdentified,
		},
		{
			"legacy money paths remain",
			capabilities.Report{Build: buildinfo.Info{Identified: true}, LegacyMoneyPaths: 1},
			"1", errLegacyPathsRemain,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := readiness(tc.caps, tc.enabled); !errors.Is(err, tc.want) {
				t.Fatalf("readiness = %v, want %v", err, tc.want)
			}
		})
	}
}

// 🔴 The condition that matters today: this tree has eleven legacy money
// paths, so the executor cannot start even if someone sets the flag.
//
// docs/DESIGN.md §11: "a strong new surface beside one weak legacy
// route is not a strong deployment. It is the weak route with better
// documentation." This is that sentence made into a startup check, and
// the test will start failing when the last legacy path is deleted —
// at which point deleting the test's premise is the correct fix.
func TestThisTreeCannotStartTheExecutor(t *testing.T) {
	stamped := capabilities.Report{
		Build:            buildinfo.Info{Identified: true},
		LegacyMoneyPaths: capabilities.LegacyMoneyPaths,
	}

	err := readiness(stamped, "1")
	if capabilities.LegacyMoneyPaths == 0 {
		if err != nil {
			t.Fatalf("no legacy paths remain but the executor is still refused: %v", err)
		}
		t.Fatal("legacyMoneyPaths reached zero — the executor can now start. " +
			"Delete this test and say so in the commit.")
	}
	if !errors.Is(err, errLegacyPathsRemain) {
		t.Fatalf("with %d legacy money paths the executor was not refused: %v",
			capabilities.LegacyMoneyPaths, err)
	}
}

// The environment this binary reports must not claim a gate whose
// evidence does not exist. Returning true for one would be the
// "declared but not implemented" failure, in the one place it decides
// whether money moves.
func TestTheReportedEnvironmentClaimsNothingUnbuilt(t *testing.T) {
	env := environment(nil)

	if env.PolicyDigestsMatch {
		t.Error("claimed policy digests match, and no policy store exists")
	}
	if env.TimeReady {
		t.Error("claimed time readiness, and no TimeReadinessPolicy exists")
	}
	if env.TaxIndependentlyReproducible {
		t.Error("claimed tax is independently reproducible, and nothing reproduces it")
	}
	if env.Unbuilt != (env.Unbuilt) || env.Unbuilt.ProofHeadCurrent || env.Unbuilt.AttemptFrozen ||
		env.Unbuilt.MerchantOfRecord || env.Unbuilt.EnclaveReady {
		t.Error("claimed evidence from a record that does not exist")
	}
}
