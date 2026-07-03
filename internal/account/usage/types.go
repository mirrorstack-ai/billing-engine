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
