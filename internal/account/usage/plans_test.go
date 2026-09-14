package usage_test

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/mirrorstack-ai/billing-engine/internal/account/usage"
)

// TestPlanTermsMatchTheOwnersMatrix pins every plan term against the owner's
// matrix: core-v2#1412 (2026-09-12) as amended on 2026-09-13 (core-v2#1458 and
// the ask/review rulings of that day — Free includes 4 modules, members are a
// priced allowance 3/10/25 at $2 each, Business includes 2 domains (09-14), the
// allowance is a usage allowance, three Free apps per personal account and one
// per org). Changing a number here must be a change to that decision first;
// this test is what makes such an edit a visible diff rather than a quiet one.
func TestPlanTermsMatchTheOwnersMatrix(t *testing.T) {
	t.Parallel()

	want := map[usage.Plan]usage.PlanTerms{
		usage.PlanFree: {
			Plan: usage.PlanFree, BaseFeeMicros: 0,
			ModulesIncluded: 4, DomainsIncluded: 0,
			MembersIncluded: 3, ExtraMemberFeeMicros: 2_000_000,
			UsageAllowanceMicros: 1_000_000,
			MaxApps:              3, MaxAppsPerOrg: 1,
		},
		usage.PlanPro: {
			Plan: usage.PlanPro, BaseFeeMicros: 20_000_000,
			ModulesIncluded: 10, DomainsIncluded: 1,
			MembersIncluded: 10, ExtraMemberFeeMicros: 2_000_000,
			UsageAllowanceMicros: 5_000_000,
			MaxApps:              usage.Unlimited, MaxAppsPerOrg: usage.Unlimited,
		},
		usage.PlanBusiness: {
			Plan: usage.PlanBusiness, BaseFeeMicros: 50_000_000,
			ModulesIncluded: usage.Unlimited, DomainsIncluded: 2,
			MembersIncluded: 25, ExtraMemberFeeMicros: 2_000_000,
			UsageAllowanceMicros: 15_000_000,
			MaxApps:              usage.Unlimited, MaxAppsPerOrg: usage.Unlimited,
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

// TestPlanRankOrdersTheLadder: an upgrade is a move to a higher rank and a
// downgrade to a lower one; the ladder is free < pro < business and an unknown
// plan ranks as the default.
func TestPlanRankOrdersTheLadder(t *testing.T) {
	t.Parallel()

	if !(usage.PlanRank(usage.PlanFree) < usage.PlanRank(usage.PlanPro) &&
		usage.PlanRank(usage.PlanPro) < usage.PlanRank(usage.PlanBusiness)) {
		t.Errorf("ladder is not free < pro < business: %d %d %d",
			usage.PlanRank(usage.PlanFree), usage.PlanRank(usage.PlanPro), usage.PlanRank(usage.PlanBusiness))
	}
	if usage.PlanRank("hobby") != usage.PlanRank(usage.DefaultPlan) {
		t.Errorf("an unknown plan must rank as the default")
	}
}

// TestMaxAppsForIsPerOwnerKind: the Free cap is 3 per personal account and 1
// per org (owner 2026-09-13); paid plans are uncapped for both.
func TestMaxAppsForIsPerOwnerKind(t *testing.T) {
	t.Parallel()

	free := usage.TermsFor(usage.PlanFree)
	if got := free.MaxAppsFor(false); got != 3 {
		t.Errorf("Free personal cap = %d, want 3", got)
	}
	if got := free.MaxAppsFor(true); got != 1 {
		t.Errorf("Free org cap = %d, want 1", got)
	}
	for _, plan := range []usage.Plan{usage.PlanPro, usage.PlanBusiness} {
		for _, org := range []bool{false, true} {
			if got := usage.TermsFor(plan).MaxAppsFor(org); got != usage.Unlimited {
				t.Errorf("TermsFor(%s).MaxAppsFor(org=%v) = %d, want Unlimited", plan, org, got)
			}
		}
	}
}

// TestExtraMembersMicros pins the per-unit member shape: nothing up to the
// included count, $2.00 for each member past it, on every plan.
func TestExtraMembersMicros(t *testing.T) {
	t.Parallel()

	cases := []struct {
		plan  usage.Plan
		count int
		want  int64
	}{
		{usage.PlanFree, 0, 0},
		{usage.PlanFree, 3, 0},
		{usage.PlanFree, 4, 2_000_000},
		{usage.PlanFree, 10, 14_000_000},
		{usage.PlanPro, 10, 0},
		{usage.PlanPro, 11, 2_000_000},
		{usage.PlanBusiness, 25, 0},
		{usage.PlanBusiness, 26, 2_000_000},
		{usage.PlanBusiness, 30, 10_000_000},
	}
	for _, c := range cases {
		if got := usage.ExtraMembersMicros(c.plan, c.count); got != c.want {
			t.Errorf("ExtraMembersMicros(%s, %d) = %d, want %d", c.plan, c.count, got, c.want)
		}
	}
}

// TestUsageAllowanceAccruesOnlyAfterGrace: no allowance inside the 3-day
// creation grace (an app cancelled there owes 100% of its usage); the
// allowance is in force from the instant the grace elapses.
func TestUsageAllowanceAccruesOnlyAfterGrace(t *testing.T) {
	t.Parallel()

	created := time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC)
	expiry := created.AddDate(0, 0, usage.GraceDays)
	for _, c := range []struct {
		at   time.Time
		want bool
	}{
		{created, false},
		{created.Add(time.Hour), false},
		{expiry.Add(-time.Second), false},
		{expiry, true},
		{expiry.Add(30 * 24 * time.Hour), true},
	} {
		if got := usage.UsageAllowanceAccrues(created, c.at); got != c.want {
			t.Errorf("UsageAllowanceAccrues(created, %s) = %v, want %v", c.at, got, c.want)
		}
	}
}

// TestPlanTermsWireCarriesTheDeployAllowanceAlias: api-platform decodes
// deploy_allowance_micros today; the wire carries it beside the new name with
// the same value until that reader moves.
func TestPlanTermsWireCarriesTheDeployAllowanceAlias(t *testing.T) {
	t.Parallel()

	raw, err := json.Marshal(usage.TermsFor(usage.PlanPro))
	if err != nil {
		t.Fatal(err)
	}
	var got map[string]json.RawMessage
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"usage_allowance_micros", "deploy_allowance_micros", "members_included",
		"extra_member_fee_micros", "max_apps", "max_apps_per_org", "modules_included", "base_fee_micros"} {
		if _, ok := got[key]; !ok {
			t.Errorf("wire is missing %q: %s", key, raw)
		}
	}
	if string(got["usage_allowance_micros"]) != string(got["deploy_allowance_micros"]) {
		t.Errorf("alias disagrees with the field: %s vs %s", got["usage_allowance_micros"], got["deploy_allowance_micros"])
	}
	if string(got["usage_allowance_micros"]) != "5000000" {
		t.Errorf("pro usage allowance on the wire = %s, want 5000000", got["usage_allowance_micros"])
	}
	if _, ok := got["personal_only"]; ok {
		t.Errorf("personal_only is retired (org apps may be Free, capped per org); it must not be emitted")
	}
}

// TestUsageDeduction_TheOwnersRule pins 用量減免 (billing-engine#202 PR-4,
// owner 2026-09-13): the plan's allowance nets module usage + 部署用量 as a
// category, never more than was used, the FULL allowance (never prorated),
// nothing inside the creation grace, nothing for an app the period's plan fee
// never covered.
func TestUsageDeduction_TheOwnersRule(t *testing.T) {
	t.Parallel()
	free, pro, biz := usage.TermsFor(usage.PlanFree), usage.TermsFor(usage.PlanPro), usage.TermsFor(usage.PlanBusiness)
	// Capped by the allowance: Free $1 against $3 of usage.
	if got := usage.UsageDeductionMicros(free, true, 2_000_000, 1_000_000); got != 1_000_000 {
		t.Fatalf("free: %d", got)
	}
	// Capped by the usage: an allowance is not a credit.
	if got := usage.UsageDeductionMicros(pro, true, 300_000, 200_000); got != 500_000 {
		t.Fatalf("pro under: %d", got)
	}
	if got := usage.UsageDeductionMicros(biz, true, 10_000_000, 9_000_000); got != 15_000_000 {
		t.Fatalf("business: %d", got)
	}
	// Not eligible → nothing; no usage → nothing; never negative.
	if got := usage.UsageDeductionMicros(pro, false, 3_000_000, 0); got != 0 {
		t.Fatalf("ineligible: %d", got)
	}
	if got := usage.UsageDeductionMicros(pro, true, 0, 0); got != 0 {
		t.Fatalf("no usage: %d", got)
	}
	if got := usage.UsageDeductionMicros(pro, true, -5, -5); got != 0 {
		t.Fatalf("negative usage: %d", got)
	}

	start := time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC)
	end := start.AddDate(0, 1, 0)
	graceEndsInside := end.Add(-24 * time.Hour).Add(-time.Duration(usage.GraceDays) * 24 * time.Hour) // created 4 days before the end: grace over by the end
	graceRunsPast := end.Add(-24 * time.Hour)                                                         // created the day before the end: still in grace at the end
	for _, tc := range []struct {
		name               string
		mirrored, deleted  bool
		created, deletedAt time.Time
		want               bool
	}{
		{"no roster row", false, false, start.AddDate(-1, 0, 0), time.Time{}, false},
		{"old app, live", true, false, start.AddDate(-1, 0, 0), time.Time{}, true},
		{"created inside the period, grace over by its end", true, false, graceEndsInside, time.Time{}, true},
		{"created inside the period, still in grace at its end", true, false, graceRunsPast, time.Time{}, false},
		{"created after the period", true, false, end.Add(time.Hour), time.Time{}, false},
		{"deleted before the period opened", true, true, start.AddDate(-1, 0, 0), start.Add(-time.Hour), false},
		{"deleted at the period's open instant", true, true, start.AddDate(-1, 0, 0), start, false},
		{"deleted inside the period after surviving grace", true, true, start.AddDate(-1, 0, 0), start.Add(10 * 24 * time.Hour), true},
		{"cancelled inside its grace", true, true, start.Add(2 * 24 * time.Hour), start.Add(3 * 24 * time.Hour), false},
		{"cancelled the moment its grace ended", true, true, start, usage.GraceExpiry(start), true},
	} {
		if got := usage.UsageAllowanceEligible(tc.mirrored, tc.created, tc.deleted, tc.deletedAt, start, end); got != tc.want {
			t.Errorf("%s: eligible = %v, want %v", tc.name, got, tc.want)
		}
	}
}
