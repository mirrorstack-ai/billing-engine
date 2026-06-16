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
	Metric string `json:"metric"`
	Kind   Kind   `json:"kind"`
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
