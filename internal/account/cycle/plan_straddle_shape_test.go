package cycle

import (
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/mirrorstack-ai/billing-engine/internal/account/usage"
)

// The Stripe rail's straddle SNAPSHOTS (proration.go:845 and :853) are built
// inside combinedProrationChargeShape, which the external tests reach only
// through a settled callback this package has no test path for. They are
// asserted here, in-package, against the same Business fixture — so a revert of
// either line to the flat usage.BaseFeeMicros fails a test rather than shipping
// a bill that charges $50 and displays $20.
//
// Mutation-proved 2026-09-13 alongside plan_straddle_test.go.
func TestCombinedProrationChargeShape_StraddleSnapshotsCarryThePlanBase(t *testing.T) {
	t.Parallel()

	const businessBase = 50_000_000
	if got := usage.TermsFor(usage.PlanBusiness).BaseFeeMicros; got != businessBase {
		t.Fatalf("fixture cannot discriminate: Business base = %d, flat base = %d", got, usage.BaseFeeMicros)
	}

	activatedAt := time.Date(2026, 5, 4, 9, 0, 0, 0, time.UTC) // anchor day 4

	// 🔴 BOTH STRADDLE BRANCHES, BECAUSE THEY ARE DIFFERENT LINES. The creation
	// period can already be CLOSED at activation (proration.go:845, where the
	// straddled period becomes the PRIMARY snapshot and StraddleSnapshot is
	// nil), or still OPEN while the grace crosses the boundary (:853, where a
	// prorated primary sits beside a full-base straddle snapshot).
	//
	// The first fixture alone left a revert of :853 alive through a whole
	// mutation run — it never reached that branch.
	for _, tc := range []struct {
		name    string
		created time.Time
	}{
		{"creation period closed at activation", time.Date(2026, 5, 2, 0, 0, 0, 0, time.UTC)},
		{"creation period still open", time.Date(2026, 6, 2, 0, 0, 0, 0, time.UTC)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			app := AppMirror{
				AppID:              uuid.New(),
				AccountID:          uuid.New(),
				CreatedAt:          tc.created,
				CreatedModuleCount: 0,
				Plan:               usage.PlanBusiness,
			}
			shape, err := combinedProrationChargeShape(app, activatedAt)
			if err != nil {
				t.Fatalf("combinedProrationChargeShape: %v", err)
			}

			// Snapshot is a value; StraddleSnapshot is a pointer, nil when the
			// straddled period IS the primary.
			snaps := []AppBaseSnapshot{shape.Snapshot}
			if shape.StraddleSnapshot != nil {
				snaps = append(snaps, *shape.StraddleSnapshot)
			}
			sawPlanBase := false
			for _, snap := range snaps {
				if snap.BaseMicros == businessBase {
					sawPlanBase = true
				}
				if snap.BaseMicros == usage.BaseFeeMicros {
					t.Errorf("a snapshot carries the FLAT base %d — a pricing site was reverted (%+v)",
						usage.BaseFeeMicros, snap)
				}
			}
			if !sawPlanBase {
				t.Errorf("no snapshot carried the Business base %d; shape = %+v", businessBase, shape)
			}
		})
	}
}
