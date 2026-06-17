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
// ALERT-ONLY (soft): crossing a threshold RECORDS an alert (idempotent per
// period+percent); it never stops metered work. The hard-stop (action='cap')
// is a later phase. billing NEVER sends mail — it records crossings and
// exposes them; email delivery is an api-platform follow-up.
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
	Scope         Scope     `json:"scope"`
	ScopeID       uuid.UUID `json:"scope_id"`
	AccountID     uuid.UUID `json:"account_id,omitempty"`
	LimitMicros   int64     `json:"limit_micros"`
	AlertPercents []int     `json:"alert_percents,omitempty"`
	Active        bool      `json:"active"`
}

// SetBudgetResponse echoes the persisted budget, including the deduped+sorted
// thresholds the service actually stored.
type SetBudgetResponse struct {
	LimitMicros   int64 `json:"limit_micros"`
	AlertPercents []int `json:"alert_percents"`
	Active        bool  `json:"active"`
}

// GetBudgetStatusRequest selects the budget whose live status to read.
type GetBudgetStatusRequest struct {
	Scope   Scope     `json:"scope"`
	ScopeID uuid.UUID `json:"scope_id"`
}

// GetBudgetStatusResponse is the live spend-vs-cap status. Exists is false
// (with a nil error) when no budget is configured for the scope, so the
// caller can render "no budget" without treating it as an error.
type GetBudgetStatusResponse struct {
	Exists bool `json:"exists"`

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
}

// GetBudgetAlertsRequest selects the recorded crossings for a budget + period.
// PeriodStart must be the first-of-month 00:00 UTC anchor (the evaluation
// window start); zero means "the current period".
type GetBudgetAlertsRequest struct {
	Scope       Scope     `json:"scope"`
	ScopeID     uuid.UUID `json:"scope_id"`
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
	AccountID     uuid.UUID
	LimitMicros   int64
	AlertPercents []int
	Active        bool
}
