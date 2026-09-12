package usage

// Per-app billing plans (core-v2#1412; billing-engine#202). Every app is priced
// from ITS OWN plan — free | pro | business — never from its account.
//
// The TERMS below are reviewed constants and the one source of truth: a price is
// a code change like every other billing tunable in bill.go, never a data edit,
// and cycle.GetAppPlan hands them to api-platform so its gates never keep a
// second copy. TestPlanTermsMatchTheOwnersMatrix pins every number against the
// owner's matrix on core-v2#1412, so changing one is a visible diff.
//
// Which plan an app is on lives in ms_billing.apps.plan (migration 075).

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
	// DeployAllowanceMicros is the 部署費用 (tenant SSR compute) the base fee
	// covers, as the amount a customer would otherwise be charged.
	DeployAllowanceMicros int64 `json:"deploy_allowance_micros"`
	// DeployCapHard: past the 部署費用 allowance the site pauses and nothing is
	// charged (Free). Otherwise the overage is billed.
	DeployCapHard bool `json:"deploy_cap_hard"`
	// MaxApps is how many apps on this plan one account may have; Unlimited
	// means no ceiling.
	MaxApps int `json:"max_apps"`
	// PersonalOnly: only a personal account's app may be on this plan (Free).
	// An org's apps start at Pro.
	PersonalOnly bool `json:"personal_only"`
}

var planTerms = map[Plan]PlanTerms{
	PlanFree: {
		Plan:                  PlanFree,
		BaseFeeMicros:         0,
		ModulesIncluded:       3,
		DomainsIncluded:       0,
		DeployAllowanceMicros: 1_000_000, // $1
		DeployCapHard:         true,
		MaxApps:               1,
		PersonalOnly:          true,
	},
	PlanPro: {
		Plan:                  PlanPro,
		BaseFeeMicros:         BaseFeeMicros, // $20
		ModulesIncluded:       10,
		DomainsIncluded:       1,
		DeployAllowanceMicros: 5_000_000, // $5
		MaxApps:               Unlimited,
	},
	PlanBusiness: {
		Plan:                  PlanBusiness,
		BaseFeeMicros:         50_000_000, // $50
		ModulesIncluded:       Unlimited,
		DomainsIncluded:       5,
		DeployAllowanceMicros: 15_000_000, // $15
		MaxApps:               Unlimited,
	},
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
