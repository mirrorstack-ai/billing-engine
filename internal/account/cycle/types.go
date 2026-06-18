// Package cycle implements the Milestone D period-rollup + developer-settlement
// surface for billing-engine: turning raw usage_events into the immutable,
// priced billable record (usage_aggregates) and accruing the developer
// revenue-share ledger (developer_settlements).
//
//	RollupPeriod      aggregate usage_events per (app, module, metric) by kind,
//	                  price per the pricing plane, snapshot onto usage_aggregates
//	SettleDevelopers  per module: platform_take + developer_owed from the
//	                  period's charged income, accrued into developer_settlements
//
// This is PR #5 of Milestone D: the RollupPeriod / SettleDevelopers Service
// methods + tests. The scheduled trigger (cmd/billing-cycle) and the Stripe
// charge rail are PR #6 — RollupPeriod is a plain Service method here, NOT a
// dispatcher action.
//
// PRICING PLANE (design §4 Axis 3, LOCKED):
//   - custom (third-party) metric → charged = quantity × unit_price_micros,
//     multiplier 10/10 (NO markup). The platform's cut on third-party usage is
//     the dev-side margin-share at settlement, not a customer markup.
//   - platform-infra / built-in metric (reserved infra.* / platform.* names) →
//     cost × 12/10. NOTE: infra.* metrics are not ingested until PR #10, so in
//     practice every metric now is custom (10/10) — but the plane logic is
//     implemented and defaults safely.
//
// SETTLEMENT (design §4 Axis 3 / §8): per module developer_owed =
// margin_share × (income − infra), where margin_share = 15% published /
// 30% private (default private 30% when unknown), income = Σ charged for the
// module's metrics, infra = COGS = 0 for now (lands with PR #10). developer_id
// is NOT known to billing-engine yet (no module→developer sync) so the row is
// keyed on module_id with developer_id NULL; status='accrued' (payout deferred
// to B2B2B).
//
// Money is integer micro-dollars end to end, round-half-up deterministically
// (matching the #16 precedent); NEVER float for money.
package cycle

import (
	"github.com/google/uuid"

	"github.com/mirrorstack-ai/billing-engine/internal/account/usage"
)

// Kind aliases usage.Kind so the cycle package speaks the same accumulation
// vocabulary as the ingest path without re-declaring the four constants.
type Kind = usage.Kind

// Visibility aliases usage.Visibility (the developer margin-share class).
type Visibility = usage.Visibility

// Customer markup multipliers (design §4 Axis 3). Custom metrics carry the
// identity 10/10 (no markup); platform-infra / built-in metrics carry 12/10
// (= 1.2×). The numerator/denominator are snapshotted onto each aggregate so a
// later rate change for a different metric class never rewrites a closed
// invoice.
const (
	customMarkupNum = 10 // custom metric: charge = raw_cost × 10/10 (= 1×)
	customMarkupDen = 10

	infraMarkupNum = 12 // platform-infra / built-in: charge = cost × 12/10 (= 1.2×)
	infraMarkupDen = 10
)

// Developer margin-share rates (design §4 Axis 3 / §8), as integer fractions
// of (income − infra). published = 15%, private = 30%. Default private (the
// higher take) when a module's visibility is unknown, so the platform never
// under-collects on a lagging publish (design §7-B).
const (
	publishedTakeNum = 15 // published module: platform takes 15%
	publishedTakeDen = 100

	privateTakeNum = 30 // private module (and unknown default): platform takes 30%
	privateTakeDen = 100
)

// MetricAggregate is one rolled-up, priced billable line — the in-memory form
// of a usage_aggregates row before it is upserted. All money is int64 micros.
type MetricAggregate struct {
	AppID    uuid.UUID
	ModuleID uuid.UUID
	Metric   string
	Kind     Kind

	// BillableQuantity is the per-kind aggregate (count/sum → SUM, peak → MAX,
	// time_weighted → integral). Carried as the exact NUMERIC string so the
	// store re-encodes it without a float round-trip.
	BillableQuantity string

	UnitPriceMicros int64
	MarkupNum       int
	MarkupDen       int
	RawCostMicros   int64
	ChargedMicros   int64
}

// RollupSummary reports what a RollupPeriod call wrote: the period it targeted
// and the per-metric aggregates it upserted (idempotent — a re-run upserts the
// identical set). TotalChargedMicros is the sum of every aggregate's
// charged_micros (the customer-billable total for the period).
type RollupSummary struct {
	PeriodID           uuid.UUID
	Aggregates         []MetricAggregate
	TotalChargedMicros int64
}

// ModuleSettlement is one accrued developer-settlement line — the in-memory
// form of a developer_settlements row. DeveloperID is the zero UUID (NULL in
// the DB) until a module→developer sync exists.
type ModuleSettlement struct {
	ModuleID            uuid.UUID
	IncomeMicros        int64
	InfraMicros         int64
	MarginShareClass    Visibility
	PlatformTakeMicros  int64
	DeveloperOwedMicros int64
}

// SettlementSummary reports what a SettleDevelopers call accrued.
type SettlementSummary struct {
	PeriodID    uuid.UUID
	Settlements []ModuleSettlement
}
