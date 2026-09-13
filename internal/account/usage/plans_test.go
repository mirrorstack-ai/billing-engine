package usage_test

import (
	"testing"

	"github.com/mirrorstack-ai/billing-engine/internal/account/usage"
)

// TestPlanTermsMatchTheOwnersMatrix pins every plan term against the owner's
// matrix on core-v2#1412 (decided 2026-09-12). Changing a number here must be a
// change to that decision first; this test is what makes such an edit a visible
// diff rather than a quiet one.
func TestPlanTermsMatchTheOwnersMatrix(t *testing.T) {
	t.Parallel()

	want := map[usage.Plan]usage.PlanTerms{
		usage.PlanFree: {
			Plan: usage.PlanFree, BaseFeeMicros: 0,
			ModulesIncluded: 3, DomainsIncluded: 0, DeployAllowanceMicros: 1_000_000,
			MaxApps: 1, PersonalOnly: true,
		},
		usage.PlanPro: {
			Plan: usage.PlanPro, BaseFeeMicros: 20_000_000,
			ModulesIncluded: 10, DomainsIncluded: 1, DeployAllowanceMicros: 5_000_000,
			MaxApps: usage.Unlimited,
		},
		usage.PlanBusiness: {
			Plan: usage.PlanBusiness, BaseFeeMicros: 50_000_000,
			ModulesIncluded: usage.Unlimited, DomainsIncluded: 5, DeployAllowanceMicros: 15_000_000,
			MaxApps: usage.Unlimited,
		},
	}
	for plan, w := range want {
		if got := usage.TermsFor(plan); got != w {
			t.Errorf("TermsFor(%s) = %+v, want %+v", plan, got, w)
		}
	}
}

// TestNoPlanPauses pins the owner's never-pause rule at its source
// (2026-09-13, core-v2#1412). Free used to pause at its $1 部署費用 allowance,
// which punished an app for usage it was already paying for; it now bills the
// overage at the same rate Pro pays.
//
// 🔴 THIS IS ASSERTED OVER EVERY PLAN, NOT JUST FREE. api-platform reads these
// terms through GetAppPlan and the console renders from them, so a single plan
// left at true would put a "paused" state back in front of a customer after the
// decision that no such state exists. A new plan added with a hard cap has to
// fail here first.
func TestNoPlanPauses(t *testing.T) {
	t.Parallel()

	for _, plan := range []usage.Plan{usage.PlanFree, usage.PlanPro, usage.PlanBusiness} {
		if usage.TermsFor(plan).DeployCapHard {
			t.Errorf("TermsFor(%s).DeployCapHard = true; no plan may pause (owner 2026-09-13)", plan)
		}
	}
}

// TestDefaultPlanChangesNoBill: every existing app moves onto DefaultPlan at
// cut-over, so its base fee must be exactly the flat fee every app paid before
// plans.
func TestDefaultPlanChangesNoBill(t *testing.T) {
	t.Parallel()

	if usage.DefaultPlan != usage.PlanPro {
		t.Errorf("DefaultPlan = %q, want pro", usage.DefaultPlan)
	}
	if got := usage.TermsFor(usage.DefaultPlan).BaseFeeMicros; got != usage.BaseFeeMicros {
		t.Errorf("default plan base = %d, want BaseFeeMicros %d", got, usage.BaseFeeMicros)
	}
}

func TestParsePlan(t *testing.T) {
	t.Parallel()

	for _, s := range []string{"free", "pro", "business"} {
		if p, ok := usage.ParsePlan(s); !ok || string(p) != s {
			t.Errorf("ParsePlan(%q) = (%q, %v), want (%q, true)", s, p, ok, s)
		}
	}
	for _, s := range []string{"", "hobby", "default", "enterprise", "Pro", " pro"} {
		if _, ok := usage.ParsePlan(s); ok {
			t.Errorf("ParsePlan(%q) accepted a plan that does not exist", s)
		}
	}
}

// TestTermsForUnknownPlanIsTheDefaultNotFree: an unrecognised plan must never
// price an app as Free.
func TestTermsForUnknownPlanIsTheDefaultNotFree(t *testing.T) {
	t.Parallel()

	if got := usage.TermsFor("hobby"); got.Plan != usage.DefaultPlan || got.BaseFeeMicros != usage.BaseFeeMicros {
		t.Errorf("TermsFor(unknown) = %+v, want the default plan's terms", got)
	}
}
