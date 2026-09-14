package usage

import (
	"encoding/json"
	"time"
)

// Per-app billing plans (core-v2#1412; billing-engine#202). Every app is priced
// from ITS OWN plan — free | pro | business — never from its account.
//
// The TERMS below are reviewed constants and the one source of truth: a price is
// a code change like every other billing tunable in bill.go, never a data edit,
// and cycle.GetAppPlan hands them to api-platform so its gates never keep a
// second copy. TestPlanTermsMatchTheOwnersMatrix pins every number against the
// owner's matrix — core-v2#1412 plus the 2026-09-13 ladder changes recorded on
// core-v2#1458 — so changing one is a visible diff.
//
// Which plan an app is on lives in ms_billing.apps.plan (migration 075). A
// change of plan is a row in ms_billing.app_plan_changes (migration 076): an
// upgrade is charged its prorated difference at once, a downgrade waits for
// the period boundary. cycle.SetAppPlan owns both.

// Plan names a billing plan.
type Plan string

const (
	PlanFree     Plan = "free"
	PlanPro      Plan = "pro"
	PlanBusiness Plan = "business"
)

// DefaultPlan is every app's plan until its owner changes it: migration 075's
// column default, and the plan of an app the roster has not mirrored yet. Its
// base fee is BaseFeeMicros, the flat per-app fee every app paid before plans,
// so moving every existing app onto it changed no bill.
const DefaultPlan = PlanPro

// Unlimited marks a PlanTerms count with no ceiling.
const Unlimited = -1

// ExtraMemberFeeMicros is what one app member beyond the plan's included count
// costs per period, on every plan (owner 2026-09-13: "$2.00 each per period,
// the same per-unit shape as custom domains"). One constant rather than a
// per-plan field because the owner priced the extra once; the plans differ
// only in how many members they INCLUDE.
const ExtraMemberFeeMicros int64 = 2_000_000 // $2.00

// PlanTerms is what one plan includes and costs, in charged amounts.
type PlanTerms struct {
	Plan Plan `json:"plan"`
	// BaseFeeMicros is the plan's recurring per-app 基本費用.
	BaseFeeMicros int64 `json:"base_fee_micros"`
	// ModulesIncluded is how many installed modules the base fee covers;
	// Unlimited means no ceiling.
	ModulesIncluded int `json:"modules_included"`
	// DomainsIncluded is how many custom domains the base fee covers.
	DomainsIncluded int `json:"domains_included"`
	// MembersIncluded is how many app members the base fee covers. Every
	// member past it is charged ExtraMemberFeeMicros per period by the
	// boundary leg (cycle.RunBillingCycle, the advance-members component).
	// A NUMBER on every plan, Business included (owner 2026-09-13: 25, "and a
	// number rather than unlimited").
	MembersIncluded int `json:"members_included"`
	// ExtraMemberFeeMicros echoes the constant so a reader of the wire shape
	// sees the price beside the allowance it applies past.
	ExtraMemberFeeMicros int64 `json:"extra_member_fee_micros"`
	// UsageAllowanceMicros is the 用量額度: the charged value of usage the base
	// fee covers each period. It offsets tenant SSR compute AND module usage
	// (owner 2026-09-13 — it was the deploy-only 部署額度, and the name moved
	// with the meaning), and the FULL allowance is deducted, never a
	// prorated share (the owner's explicit choice).
	//
	// 🔴 IT DOES NOT ACCRUE DURING THE CREATION GRACE. Nothing has been
	// charged for the plan yet — that is what the grace is — so there is no
	// plan fee to have bought an allowance with. An app cancelled inside its
	// grace owes 100% of its metered usage. UsageAllowanceAccrues is the one
	// home of that rule.
	//
	// The allowance is a TERM here; no charge leg nets it yet (the boundary
	// leg's allowanceMicros is still the driver's 0). The netting is PR-4 of
	// billing-engine#202, and it must read this field, never a copy.
	UsageAllowanceMicros int64 `json:"usage_allowance_micros"`
	// DeployCapHard: past the usage allowance the site would pause and nothing
	// be charged.
	//
	// 🔴 NO PLAN SETS THIS, AND THE OWNER'S RULE IS THAT NONE EVER SHOULD
	// (2026-09-13, core-v2#1412). Free used to: it paused at its $1 allowance.
	// That punished an app for usage it was already paying for — modules are
	// charged by usage regardless of plan — so Free now bills its extras at the
	// same rates as Pro and simply includes less before charging starts.
	//
	// The field stays because the SHAPE is still meaningful (a future plan
	// could cap), but api-platform reads this through GetAppPlan, so leaving
	// Free at true would let a pause reach the console after the product
	// decision that there is no pause. A test asserts every plan is false.
	DeployCapHard bool `json:"deploy_cap_hard"`
	// MaxApps is how many apps on this plan one PERSONAL account may have;
	// Unlimited means no ceiling. Free: 3 (owner 2026-09-13, core-v2#1458).
	MaxApps int `json:"max_apps"`
	// MaxAppsPerOrg is how many apps on this plan one ORGANIZATION may have;
	// Unlimited means no ceiling. Free: 1 (owner 2026-09-13, via the
	// ask/review session: "per org, support 1 free app"). This replaced the
	// earlier personal-only rule, under which an org's apps could not be Free
	// at all.
	MaxAppsPerOrg int `json:"max_apps_per_org"`
}

// MarshalJSON emits the terms with one compatibility alias:
// deploy_allowance_micros carries the same value as usage_allowance_micros.
//
// api-platform's wire struct (internal/shared/billing/types.go PlanTerms)
// decodes deploy_allowance_micros, and it is deployed ahead of the reader that
// will learn the new name. Renaming the key alone would make every plan's
// allowance read as 0 in the console for the duration of the skew. The alias
// is the whole of the compatibility story: the Go field has one name, and the
// alias goes when api-platform reads usage_allowance_micros.
func (t PlanTerms) MarshalJSON() ([]byte, error) {
	type wire PlanTerms
	return json.Marshal(struct {
		wire
		DeployAllowanceMicros int64 `json:"deploy_allowance_micros"`
	}{wire(t), t.UsageAllowanceMicros})
}

// MaxAppsFor is the per-owner Free cap: MaxAppsPerOrg for an org-owned app,
// MaxApps for a personal account's. Unlimited on every plan but Free.
func (t PlanTerms) MaxAppsFor(orgOwned bool) int {
	if orgOwned {
		return t.MaxAppsPerOrg
	}
	return t.MaxApps
}

var planTerms = map[Plan]PlanTerms{
	PlanFree: {
		Plan:          PlanFree,
		BaseFeeMicros: 0,
		// 4 (owner 2026-09-13: 3 → 5 → 4). Free's ladder is what it INCLUDES
		// before charging starts, never a cap — the extras cost the same as
		// Pro's.
		ModulesIncluded:      4,
		DomainsIncluded:      0,
		MembersIncluded:      3,
		ExtraMemberFeeMicros: ExtraMemberFeeMicros,
		UsageAllowanceMicros: 1_000_000, // $1
		// Never pauses — see DeployCapHard. Past $1 the overage is billed at
		// the same rate Pro pays.
		DeployCapHard: false,
		MaxApps:       3,
		MaxAppsPerOrg: 1,
	},
	PlanPro: {
		Plan:                 PlanPro,
		BaseFeeMicros:        BaseFeeMicros, // $20
		ModulesIncluded:      10,
		DomainsIncluded:      1,
		MembersIncluded:      10,
		ExtraMemberFeeMicros: ExtraMemberFeeMicros,
		UsageAllowanceMicros: 5_000_000, // $5
		MaxApps:              Unlimited,
		MaxAppsPerOrg:        Unlimited,
	},
	PlanBusiness: {
		Plan:            PlanBusiness,
		BaseFeeMicros:   50_000_000, // $50
		ModulesIncluded: Unlimited,
		// 2 (owner 2026-09-14 via the ask/review session; it was 5 → 3 → 2):
		// extras are sold at $2 either way, so this is what the $50 includes
		// before charging starts — and it matches the platform's per-org hard
		// cap on app-domain claims, which stays at 2.
		DomainsIncluded:      2,
		MembersIncluded:      25,
		ExtraMemberFeeMicros: ExtraMemberFeeMicros,
		UsageAllowanceMicros: 15_000_000, // $15
		MaxApps:              Unlimited,
		MaxAppsPerOrg:        Unlimited,
	},
}

// planRank orders the ladder. A change to a higher rank is an upgrade (charged
// at once), to a lower rank a downgrade (effective at the boundary).
var planRank = map[Plan]int{PlanFree: 0, PlanPro: 1, PlanBusiness: 2}

// PlanRank is a plan's position on the ladder: free 0, pro 1, business 2. An
// unknown plan ranks as DefaultPlan, matching TermsFor.
func PlanRank(p Plan) int {
	if r, ok := planRank[p]; ok {
		return r
	}
	return planRank[DefaultPlan]
}

// ParsePlan returns the plan named s, and false for anything else — "hobby",
// "default" and every other retired vocabulary included.
func ParsePlan(s string) (Plan, bool) {
	p := Plan(s)
	_, ok := planTerms[p]
	return p, ok
}

// TermsFor returns a plan's terms. An unknown plan — which migration 075's CHECK
// makes unreachable from the database — reads as DefaultPlan, the price every
// app paid before plans: an unrecognised value must never price an app as Free.
func TermsFor(p Plan) PlanTerms {
	if t, ok := planTerms[p]; ok {
		return t
	}
	return planTerms[DefaultPlan]
}

// ExtraMembersMicros is what an app with memberCount members owes per period
// beyond its plan's included count: max(0, count − included) ×
// ExtraMemberFeeMicros. Per unit, not per block — a member is bought one at a
// time (owner 2026-09-13). Never negative.
func ExtraMembersMicros(p Plan, memberCount int) int64 {
	t := TermsFor(p)
	if t.MembersIncluded == Unlimited {
		return 0
	}
	over := int64(memberCount - t.MembersIncluded)
	if over <= 0 {
		return 0
	}
	return over * t.ExtraMemberFeeMicros
}

// UsageAllowanceAccrues reports whether an app created at createdAt has earned
// its plan's usage allowance as of `at`: false for the whole creation grace
// (owner 2026-09-13), true from the instant the grace elapses. The rule lives
// here, beside the term it qualifies, so the bill read and a future netting
// leg cannot disagree about it.
func UsageAllowanceAccrues(createdAt, at time.Time) bool {
	return !at.Before(GraceExpiry(createdAt.UTC()))
}

// UsageAllowanceEligible says whether an app earns its plan's usage allowance
// for [periodStart, periodEnd) (billing-engine#202 PR-4, owner 2026-09-13):
//
//   - no roster row (a pre-027 anomaly) → no plan of record, nothing;
//   - deleted at or before the period opened → the boundary never charged a
//     plan fee for this period, so there is no allowance to have bought;
//   - otherwise the app must have survived its creation grace by the period's
//     end — or, if it was deleted inside the period, by its deletion: an app
//     cancelled inside its grace owes 100% of what it metered
//     (UsageAllowanceAccrues, the owner's rule).
func UsageAllowanceEligible(mirrored bool, createdAt time.Time, deleted bool, deletedAt, periodStart, periodEnd time.Time) bool {
	if !mirrored {
		return false
	}
	instant := periodEnd
	if deleted && !deletedAt.IsZero() {
		if !deletedAt.After(periodStart) {
			return false
		}
		if deletedAt.Before(instant) {
			instant = deletedAt
		}
	}
	return UsageAllowanceAccrues(createdAt, instant)
}

// UsageDeductionMicros is the plan's 用量減免 for one app and one period: the
// plan's UsageAllowanceMicros offsets the app's 模組使用量 PLUS its 部署用量
// (the 'deploy' display group), as a category — never more than the app
// actually used (an allowance is not a credit) and the FULL allowance whenever
// it applies (never prorated, the owner's explicit choice); nothing when the
// app is not eligible (UsageAllowanceEligible).
func UsageDeductionMicros(terms PlanTerms, eligible bool, moduleUsageMicros, deployUsageMicros int64) int64 {
	if !eligible {
		return 0
	}
	base := moduleUsageMicros + deployUsageMicros
	if base < 0 {
		base = 0
	}
	if terms.UsageAllowanceMicros < base {
		return terms.UsageAllowanceMicros
	}
	return base
}
