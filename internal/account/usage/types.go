// Package usage implements the Milestone D metering ingest + live-summary
// RPC surface for billing-engine's cmd/account-api:
//
//	RecordUsage           idempotent raw usage_events write (the ingest seam)
//	GetUsageSummary       live current-period charged_micros per metric
//	SetMetricDefinitions  manifest-fed catalog sync (declaration-first)
//	SetModuleVisibility   developer margin-share mirror upsert
//
// Metering is DECLARATION-FIRST (design §1): a module declares each metric
// once via ms.Meter(name, kind, ms.Unit, ms.Price), api-platform syncs
// those declarations to the catalog via SetMetricDefinitions on
// install/publish, and RecordUsage REJECTS any metric not in the catalog.
//
// Trust model (design §3a): every identity field on a RecordUsage request
// is PLATFORM-RE-DERIVED by api-platform dispatch — the SDK's
// appIdHint/moduleIdHint are untrusted and never reach this package. The
// reserved `platform.*` / `infra.*` metric namespaces are rejected at this
// ingress (build rule 3) so a module can't self-report a platform-billable
// metric through the developer-controlled SDK meter.
//
// The package reuses the billing package's typed Error
// (INVALID_INPUT/NOT_FOUND/INTERNAL) so both surfaces speak one wire-error
// vocabulary; cmd/account-api type-asserts to *billing.Error to fill the
// envelope.
//
// JSON tags match the wire format both transports (lambda.Invoke + the
// local HTTP path) serialize against.
package usage

import (
	"time"

	"github.com/google/uuid"
)

// Kind mirrors ms_billing.metric_kind one-for-one: the four accumulation
// semantics carried end-to-end. It is snapshotted onto each usage_events
// row at ingest from metric_definitions, so a later catalog edit can't
// retro-change how a historical event rolls up.
type Kind string

const (
	KindCount        Kind = "count"
	KindSum          Kind = "sum"
	KindPeak         Kind = "peak"
	KindTimeWeighted Kind = "time_weighted"
)

// Visibility mirrors ms_billing.margin_share_class. It carries the
// DEVELOPER margin-share dimension (published 15% / private 30% of
// income−infra) settled OFF the customer's bill — NEVER a customer markup.
type Visibility string

const (
	VisibilityPrivate   Visibility = "private"
	VisibilityPublished Visibility = "published"
)

// RecordUsageRequest is the payload of the RecordUsage RPC.
//
// Every field except Value/Metric/EventID is dispatch-re-derived
// authoritative identity (design §3a Plane-1 trust): api-platform resolves
// AppID/ModuleID from the authenticated invoker and OwnerUserID/OwnerOrgID
// from the app's owner principal, then stamps RecordedAt with server time.
// The SDK's hint fields are discarded upstream and absent here.
//
// OwnerUserID / OwnerOrgID are the lazy-account anchor: exactly-one may be
// set, or BOTH empty when no billing account exists yet (the event is
// still recorded with a NULL account_id and backfilled on conversion —
// design §8 "Lazy account").
type RecordUsageRequest struct {
	// EventID is the SDK-minted idempotency key, stable across the call's
	// HTTP retry. RecordUsage inserts ON CONFLICT(event_id) DO NOTHING.
	EventID string `json:"event_id"`

	// Dispatch-re-derived attribution. Never the SDK hints.
	AppID    uuid.UUID `json:"app_id"`
	ModuleID uuid.UUID `json:"module_id"`

	// Owner principal (polymorphic). Exactly one set, or both Nil for a
	// lazy (account-less) event.
	OwnerUserID uuid.UUID `json:"owner_user_id,omitempty"`
	OwnerOrgID  uuid.UUID `json:"owner_org_id,omitempty"`

	Metric string  `json:"metric"`
	Value  float64 `json:"value"`

	// RecordedAt is dispatch's server-asserted event time (never the SDK
	// recordedAtHint). Empty is tolerated and defaulted to now() so a
	// malformed timestamp can't drop a billable fact.
	RecordedAt time.Time `json:"recorded_at,omitempty"`

	// ModuleVersion is the OPTIONAL version-attribution dimension (migration
	// 023): the version of the module that emitted the event (e.g. "2.1.0"),
	// dispatch-supplied like every other identity field. Purely a reporting
	// dimension — it never affects price. Empty is tolerated (every event
	// before this PR, and any module that doesn't report a version) → stored
	// as a NULL usage_events.module_version.
	ModuleVersion string `json:"module_version,omitempty"`
}

// RecordUsageResponse reports whether the event was newly recorded.
//
// Recorded is false when ON CONFLICT(event_id) DO NOTHING deduped an
// at-least-once retry — the call still succeeds (the fact is already
// stored). Callers MUST treat false as success, not as an error.
type RecordUsageResponse struct {
	Recorded bool `json:"recorded"`
}

// GetUsageSummaryRequest is the payload of GetUsageSummary. The owner
// principal selects the account whose live current-period usage is
// summed; exactly one of OwnerUserID / OwnerOrgID must be set.
type GetUsageSummaryRequest struct {
	OwnerUserID uuid.UUID `json:"owner_user_id,omitempty"`
	OwnerOrgID  uuid.UUID `json:"owner_org_id,omitempty"`
}

// GetUsageSummaryResponse is the live current-period summary.
//
// This is the aggregate-on-read fast path (design §4 Axis 3) — NOT the
// immutable billable record (that is usage_aggregates, written at rollup
// in PR #5). When no billing account exists yet, Metrics is an empty
// slice (not nil) and the call succeeds.
type GetUsageSummaryResponse struct {
	// PeriodStart / PeriodEnd bound the window the totals cover, echoed so
	// the caller can label the live estimate. Zero when no account exists.
	PeriodStart time.Time `json:"period_start"`
	PeriodEnd   time.Time `json:"period_end"`

	Metrics []MetricUsage `json:"metrics"`
}

// MetricUsage is one row of the live summary: the running quantity for a
// metric and its customer-facing charge. For a third-party custom metric
// the charge is quantity × the developer's declared per-unit price with NO
// blanket markup (design §1 / §4 Axis 1); the flat 1.2× is platform-infra-
// only and not in this PR's scope.
type MetricUsage struct {
	// ModuleID is the module that emitted the metric. Carried so a consumer
	// can pair a metric with its module's Visibility below without a second
	// round-trip; it was implicit (ungrouped) before this PR.
	ModuleID uuid.UUID `json:"module_id"`
	Metric   string    `json:"metric"`
	Kind     Kind      `json:"kind"`
	// Quantity is a DISPLAY value (the running metered amount), not a
	// billing-critical field — money is carried only in the *_micros int64
	// fields below. Float is deliberate here and must not be widened into a
	// money path; its precision is bounded by float64 exactness.
	Quantity        float64 `json:"quantity"`
	UnitPriceMicros int64   `json:"unit_price_micros"`
	RawCostMicros   int64   `json:"raw_cost_micros"`
	// ChargedMicros is the customer-facing live estimate. For custom
	// metrics it equals RawCostMicros (declared price, no markup).
	ChargedMicros int64 `json:"charged_micros"`
	// Group is the §11 display-group taxonomy bucket (compute / database /
	// storage / network / ai / requests / platform_security / other) the
	// billing UI rolls these rows up by. It is the AUTHORITATIVE catalog
	// classification (metric_definitions.display_group); api-platform proxies
	// it and the frontend groups on it — no name-prefix mapping. Defaults to
	// "other" for any metric not (yet) mapped, including every custom metric.
	Group string `json:"group"`
	// Visibility is the module's published/private margin-share class
	// (module_visibility, migration 010) as of now. Exposed here so a
	// consumer can compute the REAL platform-take share instead of hardcoding
	// an assumed rate; defaults to "private" (the safer, higher-take default)
	// when the module has no visibility row yet, matching the settlement
	// default (design §7-B). NEVER a customer markup — this is informational.
	Visibility Visibility `json:"visibility"`
}

// GetUsageHistoryRequest is the payload of GetUsageHistory: the owner
// principal (same shape as GetUsageSummary) plus how many trailing calendar
// months of ROLLED-UP history to return. Months <= 0 is rejected —
// GetUsageSummary already covers "the current period"; this RPC exists for
// the multi-month trend chart.
type GetUsageHistoryRequest struct {
	OwnerUserID uuid.UUID `json:"owner_user_id,omitempty"`
	OwnerOrgID  uuid.UUID `json:"owner_org_id,omitempty"`
	// Months is how many trailing CLOSED calendar months to return (e.g. 6).
	// The window excludes the current, still-open month (usage_aggregates for
	// it won't exist until cmd/billing-cycle rolls it up at month-end) —
	// GetUsageSummary is the live-estimate read for the in-progress month.
	Months int `json:"months"`
}

// PeriodUsage is one bucketed billing period's rolled-up metrics — the
// trend-chart unit GetUsageHistory returns one of per closed month.
type PeriodUsage struct {
	PeriodStart time.Time     `json:"period_start"`
	PeriodEnd   time.Time     `json:"period_end"`
	Metrics     []MetricUsage `json:"metrics"`
}

// GetUsageHistoryResponse is the multi-month trend-chart read. Periods is
// ordered oldest-to-newest; a calendar month with no rolled-up usage simply
// contributes no PeriodUsage entry (a gap, never an error — callers must not
// treat a missing month as a failure).
type GetUsageHistoryResponse struct {
	Periods []PeriodUsage `json:"periods"`
}

// GetVersionBreakdownRequest is the payload of GetVersionBreakdown: the owner
// principal plus an OPTIONAL module_id filter. A zero ModuleID (the default)
// means "every one of the owner's modules"; a non-zero ModuleID narrows to
// one module's versions. Always the CURRENT period (the same
// calendar-month-to-date window GetUsageSummary resolves) — there is no
// Months field here, unlike GetUsageHistory.
type GetVersionBreakdownRequest struct {
	OwnerUserID uuid.UUID `json:"owner_user_id,omitempty"`
	OwnerOrgID  uuid.UUID `json:"owner_org_id,omitempty"`
	ModuleID    uuid.UUID `json:"module_id,omitempty"`
}

// ModuleVersionUsage is one module_version's totals across every metric (and
// every model split) in the period: the per-version cost/income breakdown
// line. BillableQuantity is summed across metrics that may carry different
// units (e.g. request count + byte-hours) — it is a rough secondary total;
// RawCostMicros / ChargedMicros (both money, always comparable) are the
// authoritative cost/income figures this RPC exists to expose.
type ModuleVersionUsage struct {
	ModuleVersion    string  `json:"module_version"`
	BillableQuantity float64 `json:"billable_quantity"`
	RawCostMicros    int64   `json:"raw_cost_micros"`
	ChargedMicros    int64   `json:"charged_micros"`
}

// GetVersionBreakdownResponse is the per-version cost/income breakdown for
// the resolved period. PeriodStart/PeriodEnd echo the resolved window (zero
// when no billing account exists yet). Versions is empty (not nil) when the
// period hasn't been rolled up yet (data appears once cmd/billing-cycle's
// RollupPeriod runs for the window) — an empty result is not an error.
type GetVersionBreakdownResponse struct {
	PeriodStart time.Time            `json:"period_start"`
	PeriodEnd   time.Time            `json:"period_end"`
	Versions    []ModuleVersionUsage `json:"versions"`
}

// GetAppUsageSummaryRequest is the payload of GetAppUsageSummary: the app
// OWNER principal (the payer — same polymorphic shape as GetUsageSummary,
// exactly one of OwnerUserID / OwnerOrgID set) plus the ONE app whose current-
// period bill is requested. This is the read behind /apps/{appId}/settings/
// billing (what the app owner PAYS for this one app).
type GetAppUsageSummaryRequest struct {
	OwnerUserID uuid.UUID `json:"owner_user_id,omitempty"`
	OwnerOrgID  uuid.UUID `json:"owner_org_id,omitempty"`
	// AppID scopes the bill to a single app. Required — this RPC is per-app
	// (the account-wide read is GetUsageSummary).
	AppID uuid.UUID `json:"app_id"`
}

// AppMetricUsage is one line of an app's bill: a single (module, metric,
// model, module_version)'s current-period quantity and customer charge. The
// app owner pays the module's DECLARED unit_price per metered unit with NO
// customer markup by visibility (that developer margin-share is settled
// dev-side, off this bill), so ChargedMicros = UnitPriceMicros × the billable
// quantity. Model / ModuleVersion let the UI split per-model / per-version
// sub-lines; both are "" when the row carries neither dimension.
type AppMetricUsage struct {
	// ModuleID is the module that emitted the metric — the app owner pays for
	// each installed module's metered usage under this app.
	ModuleID uuid.UUID `json:"module_id"`
	Metric   string    `json:"metric"`
	Kind     Kind      `json:"kind"`
	// Model is the AI pricing dimension (migration 018); omitted for non-AI
	// rows. ModuleVersion is the attribution dimension (migration 023 — never
	// priced); omitted for a version-less row. Present so the billing UI can
	// render per-version sub-lines (data exists).
	Model         string `json:"model,omitempty"`
	ModuleVersion string `json:"module_version,omitempty"`
	// BillableQuantity is a DISPLAY value (the running / rolled-up metered
	// amount), not a money field — money is carried only in the *_micros int64
	// fields. Float is deliberate and must not be widened into a money path.
	BillableQuantity float64 `json:"billable_quantity"`
	UnitPriceMicros  int64   `json:"unit_price_micros"`
	// ChargedMicros is what the app owner pays for this line: declared
	// unit_price × billable quantity, NO markup.
	ChargedMicros int64 `json:"charged_micros"`
}

// AppInfraUsage is one declared infra metric's app-level line. Present for
// EVERY active sentinel metric_definitions row (0 quantity / 0 charged when
// unused), so the app-bill "顯示全部 / show all" can list every declared infra
// metric — including the $0 / not-recording ones — in the app-level 基礎設施
// section. Infra stays APP-level (attributed to the platform-infra sentinel
// module_id, never a real module).
//
// UnitPriceMicros is the RAW catalog COGS (pre-markup); ChargedMicros already
// includes the ×1.2 infra markup (qty × price × 12/10), applied exactly ONCE
// server-side in SQL — the wire's shown unit_price × quantity therefore does NOT
// equal charged_micros by design (the 1.2× lives only in charged_micros).
type AppInfraUsage struct {
	Metric           string  `json:"metric"`
	Kind             Kind    `json:"kind"`
	Unit             string  `json:"unit"`
	Group            string  `json:"group"`             // display_group
	UnitPriceMicros  int64   `json:"unit_price_micros"` // raw COGS (pre-markup)
	BillableQuantity float64 `json:"billable_quantity"`
	ChargedMicros    int64   `json:"charged_micros"` // qty × price × 12/10
}

// AppModuleInfraUsage is one per-MODULE infrastructure line on the app bill
// (decision 19): reserved infra.* / platform.* usage ATTRIBUTED to a real
// incurring module (module_id <> the platform-infra sentinel), rendered inside
// that module's card rather than the app-level 基礎設施 residual. It carries the
// DUAL price the UI needs to draw the "(default − module) ±delta" line:
//
//   - DefaultUnitPriceMicros is the platform-default raw COGS from the SENTINEL
//     metric_definitions row (pre-markup).
//   - ModuleUnitPriceMicros is the module's ms.Price(n) override raw COGS
//     (pre-markup), or NIL when the module declared no override. NIL is the wire
//     switch between the plain line (render at the default) and the adjusted line
//     (render "(default − module)"); it is deliberately NOT coalesced to the
//     default so the UI can tell "no override" from "override happens to equal
//     the default". ms.Price(0) → a non-nil 0 (full absorb), never nil.
//   - ChargedMicros is qty × COALESCE(module, default) × 12/10, the ×1.2 infra
//     markup applied ONCE server-side (same as AppInfraUsage) — the shown unit
//     prices are raw COGS, so unit_price × qty does NOT equal charged by design.
//
// Kind / Unit / Group all come from the SENTINEL row (the override row is
// price-only). Label is the friendly display name; billing-engine has no
// friendly-label registry, so it is the metric id itself (the web maps it to a
// display string exactly as it does for the residual AppInfraUsage lines, which
// carry no label). ModuleVersion buckets the line into a per-version sub-list
// when a module ran >1 version in the period; "" collapses to the module flat list.
type AppModuleInfraUsage struct {
	ModuleID      uuid.UUID `json:"module_id"`
	ModuleVersion string    `json:"module_version,omitempty"`
	Metric        string    `json:"metric"`
	Label         string    `json:"label"`
	Kind          Kind      `json:"kind"`
	Unit          string    `json:"unit"`
	Group         string    `json:"group"` // display_group, from the SENTINEL row
	// BillableQuantity is a DISPLAY value (the metered amount), not a money field —
	// money is carried only in the *_micros int64 fields.
	BillableQuantity       float64 `json:"billable_quantity"`
	DefaultUnitPriceMicros int64   `json:"default_unit_price_micros"` // SENTINEL row COGS (pre-markup) → UI unitPrice
	// ModuleUnitPriceMicros is the per-module override COGS (pre-markup), NIL when
	// no override row exists → UI renders the plain line. omitempty drops it from
	// the wire on nil, which the TS mirror reads as "no override" (plain mode).
	ModuleUnitPriceMicros *int64 `json:"module_unit_price_micros,omitempty"`
	ChargedMicros         int64  `json:"charged_micros"` // qty × COALESCE(module,default) × 12/10, rounded once
}

// GetAppUsageSummaryResponse is the app-owner bill for ONE app in the current
// period. PeriodStart / PeriodEnd bound the live window (zero when no billing
// account exists yet). Metrics is an empty slice (not nil) — never an error —
// when the app has no usage yet or the payer has no billing account.
type GetAppUsageSummaryResponse struct {
	// AppID echoes the requested app so the response is self-describing (this
	// is a per-app bill).
	AppID       uuid.UUID `json:"app_id"`
	PeriodStart time.Time `json:"period_start"`
	PeriodEnd   time.Time `json:"period_end"`

	Metrics []AppMetricUsage `json:"metrics"`
}

// GetAppBillRequest is the payload of GetAppBill: the app OWNER principal (the
// payer — exactly one of OwnerUserID / OwnerOrgID) plus the ONE app whose bill
// is requested, and an OPTIONAL period reference. This is the read behind the
// full 最終費用 bill (base fee + module usage + infrastructure − PaaS credit).
type GetAppBillRequest struct {
	OwnerUserID uuid.UUID `json:"owner_user_id,omitempty"`
	OwnerOrgID  uuid.UUID `json:"owner_org_id,omitempty"`
	// AppID scopes the bill to a single app. Required.
	AppID uuid.UUID `json:"app_id"`
	// PeriodID selects a PAST billing period (a real billing_periods row id from
	// GetBillingPeriods). OMIT it (leave zero) for the DEFAULT: the current,
	// still-open calendar-month period estimated live from usage_events. A set id
	// resolves to that closed period's frozen usage_aggregates. Do NOT send an
	// empty string — omit the field entirely to mean "current".
	PeriodID uuid.UUID `json:"period_id,omitempty"`
}

// GetAppBillResponse is the app-owner's FULL bill for ONE app in ONE period. The
// final total is:
//
//	TotalMicros = BaseFeeMicros + ModuleUsageTotalMicros + InfraTotalMicros − PaasCreditMicros
//
// Every amount is integer micro-dollars. There is NO customer markup by module
// visibility anywhere (that developer margin-share is settled dev-side, off this
// bill). When no billing account / usage exists yet the bill still carries the
// base fee (a per-app/period platform fee), with zero usage / infra / credit.
type GetAppBillResponse struct {
	// AppID echoes the requested app (self-describing per-app bill).
	AppID uuid.UUID `json:"app_id"`
	// Name is the app's frozen display name (migration 037), "" for pre-037 /
	// unnamed rows. IsDeleted is the server-authoritative removal flag — a
	// deleted app's bill still resolves (base spent, D1e) and carries its name.
	Name      string `json:"name,omitempty"`
	IsDeleted bool   `json:"is_deleted"`
	// PeriodID echoes the resolved period id — empty ("") for the current live
	// period (which has no billing_periods row yet), the real id for a past one.
	PeriodID string `json:"period_id"`
	// PeriodStart / PeriodEnd bound the billed window.
	PeriodStart time.Time `json:"period_start"`
	PeriodEnd   time.Time `json:"period_end"`

	// BaseFeeMicros is 基本費用 — the FLAT fixed per-app/period platform fee (see
	// the bill.go consts). Module overage is NO LONGER folded in here: it is
	// account-wide pooled (migration 032) and surfaced on GetAccountBill's
	// AccountOverageMicros. Bundles the PaaS infra credit surfaced below.
	BaseFeeMicros int64 `json:"base_fee_micros"`

	// ModuleUsage is 模組使用量 — one line per (module, metric, model,
	// module_version) of metered CUSTOM usage, quantity × declared unit price with
	// NO markup. EXCLUDES the reserved infra.* / platform.* metrics (those roll
	// into InfraTotalMicros). Empty slice (never nil) when the app has no module
	// usage this period.
	ModuleUsage []AppMetricUsage `json:"module_usage"`
	// ModuleUsageTotalMicros is Σ ModuleUsage[].ChargedMicros.
	ModuleUsageTotalMicros int64 `json:"module_usage_total_micros"`

	// InfraTotalMicros is 基礎設施 — the platform-infra plane charge (reserved
	// infra.* / platform.* metrics priced at the 1.2× infra markup) for this
	// app/period. It is Σ of InfraLines[].ChargedMicros; the per-metric
	// breakdown is InfraLines (kept as this scalar for back-compat).
	InfraTotalMicros int64 `json:"infra_total_micros"`

	// InfraLines is the per-metric 基礎設施 RESIDUAL breakdown: one line for EVERY
	// active declared infra metric (the platform-infra sentinel catalog rows),
	// including the ones with zero usage this period (0 quantity / $0), so "show
	// all" can list every declared infra metric. Since decision 19 it is the
	// SENTINEL-attributed residual only (genuinely-unattributable infra —
	// platform-agent AI, egress, async producers); infra attributed to a real
	// module moves to ModuleInfraLines below. UnitPriceMicros is raw catalog COGS
	// (pre-markup); ChargedMicros already includes the ×1.2 infra markup (applied
	// once, server-side). Empty slice (never nil).
	InfraLines []AppInfraUsage `json:"infra_lines"`

	// ModuleInfraLines is the per-MODULE 基礎設施 breakdown (decision 19): reserved
	// infra.* / platform.* usage attributed to a real incurring module, one line per
	// (module_id, module_version, metric), each carrying the dual default/override
	// price the UI draws "(default − module) ±delta" from. It is a pure DISPLAY
	// re-partition of the same InfraTotalMicros — attributed infra lives here,
	// unattributable overhead stays in InfraLines — so the reconciliation invariant
	// holds: InfraTotalMicros == Σ ModuleInfraLines[].ChargedMicros +
	// Σ InfraLines[].ChargedMicros (no double count). Empty slice (never nil) when
	// no module incurred attributed infra this period.
	ModuleInfraLines []AppModuleInfraUsage `json:"module_infra_lines"`

	// PaasCreditMicros is PaaS 額度 — the infra credit EARNED ONLY through an active
	// SaaS subscription (PaasCreditPct% of InfraTotalMicros). A NON-NEGATIVE
	// magnitude SUBTRACTED in TotalMicros. v1 has NO subscription system, so it is
	// ALWAYS 0 today: the field is retained on the wire for back-compat, and the web
	// hides the credit line when it is 0. It becomes non-zero once a subscription
	// resolver lands (see paasCreditMicros).
	PaasCreditMicros int64 `json:"paas_credit_micros"`

	// TotalMicros is 最終費用 = BaseFee + ModuleUsageTotal + InfraTotal − PaasCredit.
	TotalMicros int64 `json:"total_micros"`
}

// GetAccountBillRequest is the payload of GetAccountBill: the account OWNER
// principal (the payer — exactly one of OwnerUserID / OwnerOrgID) plus an
// OPTIONAL period reference. This is the read behind web-account /me/billing's
// bill summary + subscription card (DESIGN.md account-billing-read, wire 1).
type GetAccountBillRequest struct {
	OwnerUserID uuid.UUID `json:"owner_user_id,omitempty"`
	OwnerOrgID  uuid.UUID `json:"owner_org_id,omitempty"`
	// PeriodID selects a PAST billing period (a real billing_periods row id from
	// GetBillingPeriods). "" or omitted = the account's CURRENT anchored window
	// (the in-progress period, estimated live). Unlike GetAppBillRequest.PeriodID
	// (a uuid that must be omitted for "current") this is a STRING, because the
	// account proxy forwards the selector value verbatim and the current entry's
	// period_id IS "" on the wire (BillingPeriodRef). A non-empty value must be a
	// billing_periods id belonging to this account — unknown / other-account ids
	// are NOT_FOUND, a malformed (non-uuid) value is INVALID_INPUT.
	PeriodID string `json:"period_id,omitempty"`
}

// AccountPlan is the plan stub GetAccountBill serves — billing-engine's consts
// (PlanStub*) are the SINGLE SOURCE OF TRUTH for the account plan card,
// replacing api-platform's hardcode (internal/account/service/billing.go
// GetBillingSummary's "Hobby"/$0/"active"), which retires once the /bill proxy
// lands. v1 has NO subscriptions table (DESIGN.md D3): every account is on the
// $0 default tier, so this rides on the bill read instead of a standalone
// subscription endpoint / CRUD. When paid plans land these fields swap to a
// real subscription read; the wire shape here is the stable contract.
type AccountPlan struct {
	// Name is the human plan label (PlanStubName, "Hobby").
	Name string `json:"name"`
	// Status follows the subscription vocabulary; always "active" in v1.
	Status string `json:"status"`
	// PriceMicros is the recurring plan price in integer micro-USD ($0 in v1 —
	// the per-APP base fees are billed separately, see BaseFeeTotalMicros).
	PriceMicros int64 `json:"price_micros"`
	// Currency is the ISO code every *_micros amount is denominated in ("usd").
	Currency string `json:"currency"`
	// RenewsAt is when the plan (notionally) renews: the resolved period's end —
	// the next anchored boundary for the current window, the historical close
	// for a frozen one.
	RenewsAt time.Time `json:"renews_at"`
}

// AccountAppBill is one app's roster row on the account bill: the PRE-CREDIT
// per-app rollup of the SAME per-app pricing path GetAppBill serves (base fee +
// module usage + infra), collapsed to totals.
//
// NO app_name: billing-engine has no app names — the ms_billing.apps mirror
// stores only existence / module_count / lifecycle (migration 027), never
// display metadata. The web layer enriches these rows from its own app list by
// app_id (the same caller-resolves-display-names contract as module_id on the
// usage lines).
type AccountAppBill struct {
	AppID uuid.UUID `json:"app_id"`
	// Name is the app's frozen display name (migration 037), "" for pre-037 /
	// unnamed rows (the frontend then falls back to its own registry lookup).
	// Freezing it in billing is what lets a DELETED app's row still show its
	// name instead of "unknown app".
	Name string `json:"name,omitempty"`
	// IsDeleted is the server-authoritative removal flag — the bill page reads
	// it to show this app's charges in a dialog rather than linking to the
	// (now-gone) app page.
	IsDeleted bool `json:"is_deleted"`
	// BaseFeeMicros is this app's 基本費用 for the period, resolved SNAPSHOT-
	// FIRST exactly like GetAppBill (charged periods show what was invoiced;
	// un-charged ones the live mirror estimate, prorated for a creation period).
	BaseFeeMicros int64 `json:"base_fee_micros"`
	// ModuleUsageMicros is Σ of the app's non-reserved 模組使用量 line charges.
	ModuleUsageMicros int64 `json:"module_usage_micros"`
	// InfraMicros is the app's 基礎設施 total (residual + per-module attributed,
	// the 1.2× infra markup applied once, in SQL).
	InfraMicros int64 `json:"infra_micros"`
	// TotalMicros = BaseFee + ModuleUsage + Infra for THIS app, PRE-CREDIT: the
	// account-level PaaS credit (GetAccountBillResponse.PaasCreditMicros) is
	// applied once across the account, never allocated back per-app, so
	// Σ apps[].total_micros == the response's total_micros + paas_credit_micros.
	TotalMicros int64 `json:"total_micros"`
}

// GetAccountBillResponse is the account-owner's FULL bill for ONE period — the
// account-level aggregate of the per-app GetAppBill math (ONE pricing path,
// summed — never a second one):
//
//	TotalMicros = BaseFeeTotal + ModuleUsageTotal + InfraTotal − PaasCredit
//
// Every amount is integer micro-USD. Apps enumerates the UNION of (a) apps
// with usage in the window (the same rolled-up-else-live gate the per-app bill
// reads through) and (b) ms_billing.apps mirror rows overlapping the window —
// so a just-created zero-usage app still shows its base and a pre-mirror app
// with usage still appears — deduped and sorted by app_id (bytewise) for a
// stable response. A lazy account (no billing account row) gets a ZERO bill on
// the synthetic current calendar-month window with empty Apps — never an error.
type GetAccountBillResponse struct {
	// PeriodID echoes the resolved period id — "" for the current live window,
	// the real billing_periods id for a frozen one.
	PeriodID string `json:"period_id"`
	// PeriodStart / PeriodEnd bound the billed window, half-open [start, end).
	PeriodStart time.Time `json:"period_start"`
	PeriodEnd   time.Time `json:"period_end"`

	// Plan is the v1 plan stub (see AccountPlan — billing-engine consts, the
	// single source of truth replacing api-platform's hardcode).
	Plan AccountPlan `json:"plan"`

	// Apps is the per-app pre-credit rollup, one row per enumerated app, sorted
	// by app_id. Empty slice (never nil) for a lazy or app-less account. NO
	// app_name here — see AccountAppBill.
	Apps []AccountAppBill `json:"apps"`

	// BaseFeeTotalMicros / ModuleUsageTotalMicros / InfraTotalMicros are the
	// account-wide sums of the corresponding Apps[] columns.
	BaseFeeTotalMicros     int64 `json:"base_fee_total_micros"`
	ModuleUsageTotalMicros int64 `json:"module_usage_total_micros"`
	InfraTotalMicros       int64 `json:"infra_total_micros"`

	// AccountOverageMicros is the account's module overage for the period
	// (migration 033): $3 × max(0, Σ live-app module_count − IncludedModules),
	// an ACCOUNT line (NOT per app, NOT folded into any Apps[].base_fee_micros).
	// Under the per-module-instance model overage is billed per install on its
	// own grace timer (Leg 1); this display value is the steady-state estimate
	// from the CURRENT live pool. Included in TotalMicros below.
	AccountOverageMicros int64 `json:"account_overage_micros"`

	// PaasCreditMicros is the ACCOUNT-level PaaS credit, applied ONCE here
	// (never per-app): the same ACTIVE-SaaS-subscription gate as GetAppBill's
	// per-app credit (v1 has no subscription system → always 0), CAPPED at
	// ModuleUsageTotal + InfraTotal so a credit can never eat the base fees —
	// the same usage-only offset posture as the charge spine's allowance.
	PaasCreditMicros int64 `json:"paas_credit_micros"`

	// TotalMicros is 最終費用 = BaseFeeTotal + ModuleUsageTotal + InfraTotal +
	// AccountOverage − PaasCredit, ≥ 0 by the credit cap.
	TotalMicros int64 `json:"total_micros"`
}

// GetBillingPeriodsRequest is the payload of GetBillingPeriods: the owner
// principal whose billing cycles are listed for the web 週期 (period) selector.
type GetBillingPeriodsRequest struct {
	OwnerUserID uuid.UUID `json:"owner_user_id,omitempty"`
	OwnerOrgID  uuid.UUID `json:"owner_org_id,omitempty"`
}

// BillingPeriodRef is one entry of the period selector: a billing cycle the app
// bill can be rendered for. PeriodID is the real billing_periods row id for a
// past period, or "" for the synthetic CURRENT live period (which has no
// billing_periods row yet — request the current bill by OMITTING period_id).
type BillingPeriodRef struct {
	PeriodID    string    `json:"period_id"`
	PeriodStart time.Time `json:"period_start"`
	PeriodEnd   time.Time `json:"period_end"`
	// IsCurrent marks the in-progress calendar-month period (the default bill).
	IsCurrent bool `json:"is_current"`
}

// GetBillingPeriodsResponse lists the account's billing cycles newest-first, the
// current (live) period always first. It ALWAYS contains at least the current
// period (synthesized when no closed billing_periods row is current yet), so the
// selector is never empty even for a brand-new account.
type GetBillingPeriodsResponse struct {
	Periods []BillingPeriodRef `json:"periods"`
}

// SetModuleVisibilityRequest is the payload of SetModuleVisibility, fired
// by api-platform on a module publish/unpublish. It upserts the developer
// margin-share mirror; it NEVER affects the customer charge.
type SetModuleVisibilityRequest struct {
	ModuleID   uuid.UUID  `json:"module_id"`
	Visibility Visibility `json:"visibility"`
}

// SetModuleVisibilityResponse is the (empty) success body.
type SetModuleVisibilityResponse struct{}

// MetricDef is one metric declaration in a SetMetricDefinitions payload —
// the wire form of a module's manifest ms.Meter declaration. UnitPriceMicros
// is the developer's declared per-unit customer price; when Priced is false
// the metric is metered-but-unpriced (stored as a NULL price).
type MetricDef struct {
	Metric          string `json:"metric"`
	Kind            Kind   `json:"kind"`
	Unit            string `json:"unit,omitempty"`
	UnitPriceMicros int64  `json:"unit_price_micros,omitempty"`
	Priced          bool   `json:"priced"`
	Active          bool   `json:"active"`
}

// SetMetricDefinitionsRequest is the payload of SetMetricDefinitions, a
// platform CONTROL-PLANE call (gated by the internal secret, NOT the meter
// secret). api-platform fires it on module install/publish with every
// metric the module declared in its manifest, so the catalog exists before
// any event arrives (declaration-first — design §1 / §5).
type SetMetricDefinitionsRequest struct {
	ModuleID uuid.UUID   `json:"module_id"`
	Metrics  []MetricDef `json:"metrics"`
}

// SetMetricDefinitionsResponse reports how many declarations were synced.
type SetMetricDefinitionsResponse struct {
	Synced int `json:"synced"`
}

// InfraPriceOverride is one (metric, price) override in a
// SetInfraPriceOverrides payload — the wire form of a module's
// ms.Meter("infra.X", ms.Price(n)) manifest declaration for a RESERVED
// platform-infra metric. It carries PRICE ONLY: kind + unit are
// platform-owned and inherited from the SENTINEL base catalog row, never
// supplied by the caller (the INVERSE of MetricDef, which carries kind/unit
// for a module's own custom metric). UnitPriceMicros is the raw pre-markup
// COGS override; 0 is the "full absorb" price (ms.Price(0)), NOT "unpriced"
// — there is no Priced flag here (an override always carries a price).
type InfraPriceOverride struct {
	Metric          string `json:"metric"`
	UnitPriceMicros int64  `json:"unit_price_micros"`
}

// SetInfraPriceOverridesRequest is the payload of SetInfraPriceOverrides, a
// platform CONTROL-PLANE call (gated by the internal secret, NOT the meter
// secret). api-platform fires it on module publish with every RESERVED
// infra.* / platform.* metric the module re-priced via
// ms.Meter("infra.X", ms.Price(n)).
//
// It is the INVERSE of SetMetricDefinitions and the control-plane twin of
// RecordInfraUsage's gate: where SetMetricDefinitions REJECTS reserved names
// (a module may never DECLARE a platform metric), this call ACCEPTS ONLY
// reserved names registered in platformInfraKind (a module MAY re-PRICE one).
// Each override writes a price-only per-(module, metric) metric_definitions
// row under the REAL module_id (never the sentinel), so the app bill's
// dual-price resolution (decision 19 §4.2) resolves the module line at the
// override while the sentinel row stays the platform default.
type SetInfraPriceOverridesRequest struct {
	ModuleID  uuid.UUID            `json:"module_id"`
	Overrides []InfraPriceOverride `json:"overrides"`
}

// SetInfraPriceOverridesResponse reports how many overrides were synced.
type SetInfraPriceOverridesResponse struct {
	Synced int `json:"synced"`
}
