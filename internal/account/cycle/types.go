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
	// Model is the AI pricing dimension (migration 018): '' for non-AI metrics,
	// a roster model id for infra.ai.*. Part of the usage_aggregates idempotency
	// key so two models on one metric are distinct billable rows.
	Model string
	// ModuleVersion is the version-attribution dimension (migration 023): ''
	// for a version-less event, the emitting module's version otherwise.
	// Purely reporting — it never affects price — but it is part of the
	// usage_aggregates idempotency key so two versions on one metric are
	// distinct billable rows, exactly like two models are distinct under 018.
	ModuleVersion string
	Kind          Kind

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

// BillingRunStatus is the terminal state of a RunBillingCycle attempt. It
// mirrors the ms_billing.billing_runs.status CHECK one-for-one.
type BillingRunStatus string

const (
	// RunStatusInvoiced is the terminal success state: either the arrears
	// charge was created + the invoice mirrored, OR the netted arrears were 0
	// (nothing to charge — the run is "done" with no Stripe call).
	RunStatusInvoiced BillingRunStatus = "invoiced"

	// RunStatusSkippedNoPM means the account had positive arrears but no usable
	// default payment method. The usage is RETAINED (the usage_aggregates rows
	// are untouched); the next cycle re-attempts. NOT a failure, NOT lost usage.
	RunStatusSkippedNoPM BillingRunStatus = "skipped_no_pm"

	// RunStatusFailed means the charge errored after the PM gate (Stripe call or
	// mirror failed). Terminal; PR #7 webhook reconciliation + risk-graded
	// retry build on this. The run row stays so the failure is auditable.
	RunStatusFailed BillingRunStatus = "failed"

	// RunStatusSkippedPrepaid means the risk-graded gate (PR #9) did NOT
	// off-session-charge this cycle's accrued arrears because the account is in
	// 'prepaid' usage_billing_mode — either it was already prepaid, or the
	// risk-judge just tightened it to prepaid this cycle (delinquency / over
	// credit_limit / usage spike). The usage is RETAINED (usage_aggregates
	// untouched); it is NOT lost. The prepaid-credit WALLET (balance / top-ups)
	// that would settle it is a DEFERRED follow-up. NOT a failure, NOT lost usage.
	//
	// This status is reserved for a MODE-driven skip. A per-cycle spend_ceiling
	// breach (which does NOT change the mode) is RunStatusSkippedCeiling — the two
	// are kept distinct so an operator querying billing_runs can tell "account is
	// in prepaid mode" apart from "arrears exceeded the customer-set spend ceiling
	// for this one cycle".
	RunStatusSkippedPrepaid BillingRunStatus = "skipped_prepaid"

	// RunStatusSkippedCeiling means the netted arrears would breach the account's
	// customer-set spend_ceiling (the hard per-cycle bill-shock cap) so the
	// off-session charge was skipped this cycle — WITHOUT changing the account's
	// usage_billing_mode (the ceiling is a per-cycle cap, not a mode/trust
	// transition; the next cycle re-attempts once the ceiling is raised or the
	// arrears netted below it). The usage is RETAINED. Distinct from
	// RunStatusSkippedPrepaid so the skip reason is unambiguous in the audit trail.
	RunStatusSkippedCeiling BillingRunStatus = "skipped_ceiling"
)

// chargeCurrency is the Stripe charge currency. Fixed to usd for v1 (matching
// the card-on-file Checkout Session currency); multi-currency is a future
// concern that travels with per-account billing locale.
const chargeCurrency = "usd"

// ChargeSummary reports what a RunBillingCycle call did for one (account,
// period). It is the in-memory account of the run row + (when a charge
// happened) the mirrored invoice.
//
// FirstRun is false when InsertBillingRun hit the idempotency gate — the window
// already has an 'invoiced' (terminal-success) run — so the cycle did NOTHING
// this call (no arrears read, no Stripe charge). Callers MUST treat
// FirstRun=false as success, not an error: the work was already done. A
// non-terminal prior run (skipped_no_pm / failed / pending-died-mid-flight) is
// RECLAIMED and re-attempted, so it reports FirstRun=true.
type ChargeSummary struct {
	// FirstRun is true when this call performed a charge attempt — either it
	// inserted a fresh billing_runs row, or it reclaimed a non-terminal one
	// (skipped_no_pm / failed / pending). False = idempotent no-op: the window
	// already has an 'invoiced' run.
	FirstRun bool

	// Status is the terminal run status this call set. Zero ("") when
	// FirstRun is false (no status was set this call).
	Status BillingRunStatus

	// ArrearsMicros is the netted USAGE arrears = max(0, Σ charged_micros −
	// allowanceMicros) the cycle computed for the CLOSED period.
	ArrearsMicros int64

	// AdvanceBaseMicros is the NEW period's advance base fee (base-fee v1):
	// Σ over the account's live apps of BaseFee + Overage × max(0,
	// module_count − IncludedModules). 0 for a pre-backfill account (no
	// mirror rows). The invoice total is ArrearsMicros + AdvanceBaseMicros;
	// only when BOTH are 0 is the Stripe call skipped.
	AdvanceBaseMicros int64

	// ChargedCents is the whole-cent amount sent to Stripe (micros → cents
	// round-half-up over arrears + advance base). 0 when no charge happened.
	ChargedCents int64

	// StripeInvoiceID is the created Stripe invoice id, empty when no charge
	// happened (zero arrears or skipped_no_pm).
	StripeInvoiceID string
}
