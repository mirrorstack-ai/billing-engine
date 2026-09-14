// Package budget implements the per-app spending-budget engine for
// billing-engine's cmd/account-api:
//
//	SetBudget         upsert a scope's spending cap + alert thresholds
//	GetBudgetStatus   live spend vs cap + which thresholds are crossed
//	GetBudgetAlerts   the recorded threshold crossings for a period
//	EvaluateAppBudget ingest-path hook: record newly-crossed thresholds
//
// billing-engine is CANONICAL for budget config (design
// docs-temp/budget-alerts/design.md §10): api-platform writes a budget over
// the internal-secret billing client and reads status/alerts back for in-app
// display; it never reads ms_billing SQL directly (trust boundary).
//
// SCOPE: the budget_scope enum carries 'org' and 'account' for forward-compat,
// but v1 WIRES ONLY scope='app'. SetBudget rejects org/account with
// INVALID_INPUT until those scopes are implemented.
//
// ALERT-ONLY by default (soft): crossing a threshold RECORDS an alert
// (idempotent per period+percent); it never stops metered work. billing NEVER
// sends mail — it records crossings and exposes them; email delivery is an
// api-platform follow-up.
//
// HARD CAP (migration 083, T101): a budget with HardCap=true additionally
// reports Exhausted on GetBudgetStatus once spend >= limit, and the CONSUMER
// refuses further work on it (api-platform's agent gate for CategoryAI).
// billing-engine itself never stops metered work — usage already incurred is
// always recorded — so a hard cap is a verdict the consumer enforces before
// the work, never a rejection at ingest.
//
// CATEGORY (migration 084): which spend a budget measures. CategoryAll is the
// scope's every usage event (the original budget); CategoryAI is its
// infra.ai.* events only, priced per model exactly like the bill. One row per
// (scope, scope_id, category, template_key), so an app can carry both, plus
// one AI row per ai-assistant TEMPLATE (owner 2026-09-14: the module's cap
// is per template, enforced, in money). A status read for (app, template)
// COMPOSES the template's row with the app's AI-wide row: exhausted if
// either is, the exhausted (else the more specific) row reported. A
// template row narrows the app cap; it never shadows it.
//
// SCOPES for CategoryAI: 'app' (scenario 1) and 'org' / 'account' (scenario
// 2: the console operator agent, billed to the org of an org-context
// conversation or to the personal account). CategoryAll stays 'app' only.
//
// Money is BIGINT micro-dollars (1e-6 USD), never float — the spend SUM is
// decoded through the same single-rounding-point helper the usage package
// uses (usage.MicrosFromNumeric).
//
// The package reuses the billing package's typed Error
// (INVALID_INPUT/NOT_FOUND/INTERNAL) so every billing surface speaks one
// wire-error vocabulary; cmd/account-api type-asserts to *billing.Error to
// fill the envelope.
package budget

import (
	"regexp"
	"time"

	"github.com/google/uuid"
)

// Scope mirrors ms_billing.budget_scope one-for-one. v1 wires ScopeApp only;
// ScopeOrg / ScopeAccount exist for forward-compat and are rejected by
// SetBudget for now.
type Scope string

const (
	ScopeApp     Scope = "app"
	ScopeOrg     Scope = "org"
	ScopeAccount Scope = "account"
)

// Category selects which spend a budget measures (migration 083).
type Category string

const (
	CategoryAll Category = "all"
	CategoryAI  Category = "ai"
	// CategoryExposure is the SYSTEM row billing-engine maintains per PaaS
	// account (migration 085) so budget_alerts can record crossings of the
	// risk-exposure pool. Never accepted from a caller.
	CategoryExposure Category = "exposure"
)

// normalizeCategory maps the wire value (empty = the pre-084 default) onto a
// known Category, or reports it unknown.
func normalizeCategory(c Category) (Category, bool) {
	switch c {
	case "", CategoryAll:
		return CategoryAll, true
	case CategoryAI:
		return CategoryAI, true
	}
	return "", false
}

// SetBudgetRequest is the payload of the SetBudget RPC — a platform
// CONTROL-PLANE call (internal secret, NOT the meter secret). api-platform
// fires it on app-settings save with the cap + thresholds.
//
// AccountID is the owner's billing account; it MAY be Nil (the owner has no
// account yet — a lazy budget, stored with a NULL account_id and backfilled
// on conversion). LimitMicros is the spending cap in micro-dollars.
// AlertPercents are the threshold percentages (1..100); empty defaults to
// {80,100}. The service dedupes + sorts them before persisting.
type SetBudgetRequest struct {
	Scope   Scope     `json:"scope"`
	ScopeID uuid.UUID `json:"scope_id"`
	// Category is which spend the cap measures: "all" (default when empty —
	// every pre-084 caller) or "ai" (infra.ai.* only, T101).
	Category Category `json:"category,omitempty"`
	// TemplateKey names the ai-assistant template this AI cap is for; empty
	// = the scope's AI-wide row. Only with Category "ai" and Scope "app".
	TemplateKey   string    `json:"template_key,omitempty"`
	AccountID     uuid.UUID `json:"account_id,omitempty"`
	LimitMicros   int64     `json:"limit_micros"`
	AlertPercents []int     `json:"alert_percents,omitempty"`
	Active        bool      `json:"active"`
	// HardCap true makes GetBudgetStatus report Exhausted at spend >= limit
	// so the consumer refuses further work; false keeps the cap alert-only.
	HardCap bool `json:"hard_cap,omitempty"`
	// AllowOverage is the customer's opt-out of their own hard cap (owner
	// 2026-09-14, scenario 2): alerts still fire, Exhausted stays false. It
	// never lifts the account's risk-graded exposure limit.
	AllowOverage bool `json:"allow_overage,omitempty"`
}

// SetBudgetResponse echoes the persisted budget, including the deduped+sorted
// thresholds the service actually stored.
type SetBudgetResponse struct {
	Category      Category `json:"category"`
	TemplateKey   string   `json:"template_key"`
	LimitMicros   int64    `json:"limit_micros"`
	AlertPercents []int    `json:"alert_percents"`
	Active        bool     `json:"active"`
	HardCap       bool     `json:"hard_cap"`
	AllowOverage  bool     `json:"allow_overage"`
}

// GetBudgetStatusRequest selects the budget whose live status to read. For
// Category "ai" on an app, TemplateKey asks for the verdict in force for
// THAT template: the template's own row composed with the app's AI-wide
// row (DecidedBy says which one bit). Empty TemplateKey reads the AI-wide
// row alone.
type GetBudgetStatusRequest struct {
	Scope       Scope     `json:"scope"`
	ScopeID     uuid.UUID `json:"scope_id"`
	Category    Category  `json:"category,omitempty"`
	TemplateKey string    `json:"template_key,omitempty"`
}

// DecidedBy names the row a status read resolved to.
type DecidedBy string

const (
	DecidedByTemplate DecidedBy = "template"
	DecidedByApp      DecidedBy = "app"
	DecidedByAccount  DecidedBy = "account"
	DecidedByOrg      DecidedBy = "org"
	DecidedByNone     DecidedBy = "none"
	// DecidedByExposureLimit: no customer cap bit, the PaaS risk-exposure
	// pool did (PR-B, migration 085).
	DecidedByExposureLimit DecidedBy = "exposure_limit"
	// DecidedByPaused: the incident kill-switch is on — every verdict is
	// allowed regardless of caps and pool (migration 085).
	DecidedByPaused DecidedBy = "paused"
)

// PoolSource says what bounds the pool: the risk-exposure curve, or nothing
// (not PaaS, or an unattributed scope).
type PoolSource string

const (
	PoolSourceExposureLimit PoolSource = "exposure_limit"
	PoolSourceNone          PoolSource = "none"
)

// Pool is the PaaS risk-exposure pool on a category='ai' status read (owner
// 2026-09-14): PaaS is pay-AFTER-use, so the platform's exposure is bounded
// by a risk-graded limit that REFUSES AI turns live. LimitMicros is the
// curve (risk_ramp_config × the account's signals), AccruedMicros the
// account's whole-period PaaS usage — AI + module usage + infra; plan/SaaS
// base fees are not usage and never count. Exhausted = Mode paas && accrued
// >= limit. The customer's allow_overage never lifts it. v1 gates AI only.
type Pool struct {
	// Mode is the account's billing mode: "paas" (the 'standard' mode), "credits", or "none" (unattributed).
	Mode            string     `json:"mode"`
	Source          PoolSource `json:"source"`
	LimitMicros     int64      `json:"limit_micros"`
	AccruedMicros   int64      `json:"accrued_micros"`
	RemainingMicros int64      `json:"remaining_micros"`
	Exhausted       bool       `json:"exhausted"`
	// HasUsableCard / PaidInvoices / DelinquentNow / LateCount are the curve's
	// inputs, echoed so a console can explain the limit ("$14.10: card on
	// file, 1 paid invoice").
	HasUsableCard bool `json:"has_usable_card"`
	PaidInvoices  int  `json:"paid_invoices"`
	DelinquentNow bool `json:"delinquent_now"`
	LateCount     int  `json:"late_count"`
	// EffectivePaid is k after the delinquency penalty — the k the limit was
	// computed from ("$14.14: 3 paid, 1 late × 2 = k 1").
	EffectivePaid int `json:"effective_paid"`
}

// ExposureSignals are the curve's inputs for one account. DelinquentNow and
// LateCount are read for the delinquency rule (owner's pick pending): an
// open/uncollectible invoice with a balance, and how many invoices ever
// needed a failed attempt or ended uncollectible/void.
type ExposureSignals struct {
	BillingMode   string
	HasUsableCard bool
	PaidInvoices  int
	DelinquentNow bool
	LateCount     int
}

// RiskRampConfig is the finance-owned curve row (085), read once per AI
// verdict together with the incident kill-switch.
type RiskRampConfig struct {
	NoCardMicros   int64
	CardBaseMicros int64
	CeilingMicros  int64
	// Exponent p in base × (1+k)^p: 0.5 = square root, 1 = linear; the DB
	// bounds it to (0, 1].
	Exponent float64
	// DelinquentFloor / LatePenaltyK are delinquency rule (b): floored at
	// NoCardMicros while delinquent; k_eff = max(0, paid − LatePenaltyK × late)
	// once settled (LatePenaltyK large = any late payment resets k).
	DelinquentFloor   bool
	LatePenaltyK      int
	EnforcementPaused bool
	PausedReason      string
	PausedBy          string
	PausedAt          time.Time // zero when not paused
}

// SetAIEnforcementPausedRequest flips the incident kill-switch (admin RPC,
// internal secret): Paused=true allows every AI verdict platform-wide until
// flipped back, with no deploy. Reason and ActorID are STORED on the row on
// a pause (and cleared on resume) so the audit answers "why was enforcement
// off, and who turned it off" without archaeology; both are required to pause.
type SetAIEnforcementPausedRequest struct {
	Paused  bool   `json:"paused"`
	Reason  string `json:"reason,omitempty"`
	ActorID string `json:"actor_id,omitempty"`
}

// AIEnforcementResponse is the switch's state (with its provenance) plus the
// curve, so an admin surface shows everything in one read.
type AIEnforcementResponse struct {
	Paused          bool      `json:"paused"`
	PausedReason    string    `json:"paused_reason,omitempty"`
	PausedBy        string    `json:"paused_by,omitempty"`
	PausedAt        time.Time `json:"paused_at,omitempty"`
	NoCardMicros    int64     `json:"no_card_micros"`
	CardBaseMicros  int64     `json:"card_base_micros"`
	CeilingMicros   int64     `json:"ceiling_micros"`
	Exponent        float64   `json:"exponent"`
	DelinquentFloor bool      `json:"delinquent_floor"`
	LatePenaltyK    int       `json:"late_penalty_k"`
}

// GetBudgetStatusResponse is the live spend-vs-cap status. Exists is false
// (with a nil error) when no budget is configured for the scope, so the
// caller can render "no budget" without treating it as an error.
type GetBudgetStatusResponse struct {
	Exists   bool     `json:"exists"`
	Category Category `json:"category"`
	// DecidedBy is the row this status came from ("none" when Exists is
	// false); TemplateKey is that row's template ("" for an AI-wide row).
	DecidedBy   DecidedBy `json:"decided_by"`
	TemplateKey string    `json:"template_key"`

	// PeriodStart / PeriodEnd bound the spend window (current calendar month —
	// the same window GetUsageSummary shows).
	PeriodStart time.Time `json:"period_start"`
	PeriodEnd   time.Time `json:"period_end"`

	LimitMicros int64 `json:"limit_micros"`
	SpendMicros int64 `json:"spend_micros"`

	// PercentUsed is spend/limit ×100, floored to a whole percent (0 when the
	// limit is 0). A DISPLAY value — the crossing decision uses integer micro
	// math, never this.
	PercentUsed int `json:"percent_used"`

	// Crossed is the subset of the budget's alert_percents the current spend
	// has reached, ascending.
	Crossed []int `json:"crossed"`

	Active bool `json:"active"`

	// HardCap / AllowOverage mirror the budget's settings. Exhausted is the
	// consumer's verdict: Exists && Active && HardCap && !AllowOverage &&
	// SpendMicros >= LimitMicros — an alert-only, inactive or overage-allowed
	// budget is never exhausted, whatever the spend. RemainingMicros is
	// max(limit - spend, 0), a display value.
	HardCap         bool  `json:"hard_cap"`
	AllowOverage    bool  `json:"allow_overage"`
	Exhausted       bool  `json:"exhausted"`
	RemainingMicros int64 `json:"remaining_micros"`

	// Pool is the PaaS risk-exposure pool for the scope's paying account on a
	// category='ai' read; nil for other categories. Exhausted above is the OR
	// of the cap rows and the pool; DecidedBy says which bit.
	Pool *Pool `json:"pool,omitempty"`
}

// GetBudgetAlertsRequest selects the recorded crossings for a budget + period.
// PeriodStart must be the first-of-month 00:00 UTC anchor (the evaluation
// window start); zero means "the current period".
type GetBudgetAlertsRequest struct {
	Scope       Scope     `json:"scope"`
	ScopeID     uuid.UUID `json:"scope_id"`
	Category    Category  `json:"category,omitempty"`
	TemplateKey string    `json:"template_key,omitempty"`
	PeriodStart time.Time `json:"period_start,omitempty"`
}

// GetBudgetAlertsResponse lists the recorded crossings for the period.
type GetBudgetAlertsResponse struct {
	Alerts []BudgetAlert `json:"alerts"`
}

// BudgetAlert is one recorded threshold crossing.
type BudgetAlert struct {
	Percent     int       `json:"percent"`
	SpendMicros int64     `json:"spend_micros"`
	LimitMicros int64     `json:"limit_micros"`
	PeriodStart time.Time `json:"period_start"`
	FiredAt     time.Time `json:"fired_at"`
}

// Budget is the persisted config the store resolves. AccountID is Nil for a
// lazy (account-less) budget.
type Budget struct {
	ID            uuid.UUID
	Scope         Scope
	ScopeID       uuid.UUID
	Category      Category
	TemplateKey   string
	AccountID     uuid.UUID
	LimitMicros   int64
	AlertPercents []int
	Active        bool
	HardCap       bool
	AllowOverage  bool
}

// templateKeyShape is the module's own template-key grammar (ai-assistant
// sql/app/0001_init: ^[a-z][a-z0-9_-]*$), mirrored by migration 084's CHECK.
var templateKeyShape = regexp.MustCompile(`^[a-z][a-z0-9_-]{0,63}$`)
