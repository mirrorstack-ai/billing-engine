package usage

import (
	"context"
	"math"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/mirrorstack-ai/billing-engine/internal/account/billing"
)

// PlatformInfraModuleIDString is the canonical text form of the reserved
// platform-infra sentinel module_id (Plane 1). It is the SINGLE source of truth
// the Go sentinel AND migration 017's seeded metric_definitions rows both derive
// from — keep this string and 017's INSERT literal identical or the ingest
// kind-lookup and the rollup price-lookup will resolve through different rows.
const PlatformInfraModuleIDString = "00000000-0000-0000-0000-000000000000"

// platformInfraModuleID is the parsed reserved sentinel module_id the
// platform-infra plane records under. ms_billing.metric_definitions.module_id is
// NOT NULL + UNIQUE(module_id, metric), so the platform-measured infra metrics
// (which belong to no real module) are catalogued under this all-zero sentinel.
// Migration 017 seeds the matching metric_definitions rows under the SAME
// sentinel, so both the ingest kind-lookup and the rollup price-lookup resolve
// through it. RecordInfraUsage stamps this as usage_events.module_id.
//
// It is an unexported package-level value (not an exported mutable var) so no
// caller — including a test — can reassign it and corrupt the sentinel for the
// rest of the process; read it through PlatformInfraModuleID(). uuid.UUID is not
// const-able ([16]byte), so this is the idiomatic immutable-value pattern.
//
// (Per-(app, module) infra ATTRIBUTION — keying the cost back to the specific
// module call that incurred it — is a producer-PR refinement; the foundation
// prices infra correctly under the sentinel.)
var platformInfraModuleID = uuid.MustParse(PlatformInfraModuleIDString)

// PlatformInfraModuleID returns the reserved platform-infra sentinel module_id.
// It is an accessor (not an exported var) precisely so the sentinel cannot be
// reassigned: every caller gets a fresh copy of the fixed architectural
// constant. See platformInfraModuleID for the no-mutation contract.
func PlatformInfraModuleID() uuid.UUID { return platformInfraModuleID }

// platformInfraKind returns the platform-owned accumulation KIND for a reserved
// infra.* / platform.* metric, and whether the metric is a registered platform
// infra metric at all. The platform — NOT the SDK and NOT a per-module catalog
// lookup — owns infra-metric semantics (design §3a / §5 "cdn-worker + module
// runtime"), so the kind is fixed HERE rather than read from metric_definitions:
//
//	infra.compute.walltime.ms    additive dispatch wall-time ms (fallback) → sum
//	infra.egress.bytes           additive CDN/egress bytes (retired)       → sum
//	infra.ai.input.tokens        additive provider INPUT tokens            → sum
//	infra.ai.output.tokens       additive provider OUTPUT tokens           → sum
//	infra.ai.cache_write.tokens  additive prompt-cache WRITE tokens        → sum
//	infra.ai.cache_read.tokens   additive prompt-cache READ tokens         → sum
//	infra.ai.requests            provider-API-call count                   → count
//	infra.request.count          per-invocation request fee                → count
//	infra.mcp.tool_call.count    MCP routing+auth fixed cost               → count
//	infra.cron.count             scheduler fire (the tick)                 → count
//	infra.event.count            per-subscriber fanout delivery            → count
//	infra.event.bytes            event-bus payload size (per-GiB)          → sum
//	infra.egress.api.bytes       non-CDN API egress bytes (per-GiB)        → sum
//	infra.storage.put.count      S3 tier-1 PUT/COPY ops (per-1k)           → count
//	infra.storage.list.count     S3 tier-1 LIST ops (per-1k)               → count
//	infra.storage.gib_hours      S3 stored volume (GiB-hours integral)     → time_weighted
//
// The eight P1 metrics (migration 020, design §2.2/§2.4/§2.5/§2.7) are
// PRODUCER-TARGET: seeded + registered here so the producer PRs (#5/#6/#7) emit
// names that already exist. Per design §3 rule 5, any per-unit COGS < 1 µ$ is
// priced in the COARSEST unit ≥ 1 µ$ and the PRODUCER emits the value pre-scaled
// to that unit — egress/event bytes per GiB (value = bytes/1024^3), event
// deliveries + storage PUT/LIST per 1k (value = n/1000); request/mcp/cron stay
// per-unit (≥ 1 µ$). The per-row contract lives in migration 020.
//
// The infra.ai.* family is priced PER MODEL: the producer (infra-metrics PR #2,
// in api-platform cmd/agent) stamps the model on RecordInfraUsageRequest.Model,
// the rollup resolves the per-(metric, model) price from metric_model_prices
// (migration 018) and falls back to the sentinel metric_definitions row when no
// model is carried. The token metrics are metered/priced in THOUSANDS of tokens
// (per-1k, design §3 rule 5) so the integer-micro price floor survives;
// infra.ai.requests is unpriced observability (price 0). Their kind is fixed
// here, never read from the catalog.
//
// A switch (not a package-level map) keeps this registry immutable by
// construction — there is no mutable global a test could corrupt. Adding a new
// platform-measured infra metric means a new case HERE plus a seeded
// metric_definitions row (migration 017/018) with its per-unit COGS.
// RecordInfraUsage rejects any reserved name without a case (an unregistered
// infra metric has no platform-owned kind).
//
// Catalog hygiene (infra-metrics design §1, migration 019): infra.compute.ms was
// RE-CHARTERED + RENAMED to infra.compute.walltime.ms (dispatch-observed
// wall-time, the FALLBACK-ONLY compute basis — never co-billed with a
// substrate-native metric). infra.egress.bytes was RETIRED to an unpriced
// reporting parent (price 0 in 019; its CDN children infra.egress.cdn.* are P2).
// infra.egress.bytes stays registered because it is still ingested.
//
// The old infra.compute.ms name was kept as a deprecated transition alias in 019
// but has now been DROPPED (migration 022 + this PR): api-platform #266 renamed
// the dispatch producer to emit infra.compute.walltime.ms exclusively, so nothing
// emits the old name. A RecordInfraUsage for infra.compute.ms is now rejected as
// an unregistered reserved metric — correct, because no producer emits it.
func platformInfraKind(metric string) (Kind, bool) {
	switch metric {
	case "infra.compute.walltime.ms":
		// Re-chartered fallback-only dispatch wall-time (design §1 / §2.1).
		return KindSum, true
	case "infra.egress.bytes":
		// RETIRED flat egress, kept as an unpriced reporting parent (design §2.5);
		// still ingested by cmd/infra-egress-sync, so it stays registered.
		return KindSum, true
	case "infra.ai.input.tokens":
		return KindSum, true
	case "infra.ai.output.tokens":
		return KindSum, true
	case "infra.ai.cache_write.tokens":
		return KindSum, true
	case "infra.ai.cache_read.tokens":
		return KindSum, true
	case "infra.ai.requests":
		return KindCount, true

	// --- P1 producer-target metrics (migration 020, design §2.2/§2.4/§2.5/§2.7).
	// Seeded + registered HERE so the downstream producer PRs (#5 dispatch
	// ingress, #6 async, #7 storage) emit names that already exist in BOTH this
	// registry and the catalog. No producer in this PR. The chosen BILLING unit +
	// the required producer value scaling live in migration 020's per-row comments
	// (rule-5 contract); the KIND is fixed here.
	case "infra.request.count":
		// §2.7 per-invocation APIGW+Lambda request fee. count; value = 1/request.
		return KindCount, true
	case "infra.mcp.tool_call.count":
		// §2.7 MCP routing+auth fixed cost. count; value = 1/call.
		return KindCount, true
	case "infra.cron.count":
		// §2.2 scheduler fire (the tick only). count; value = 1/fire.
		return KindCount, true
	case "infra.event.count":
		// §2.2 per-subscriber fanout delivery. count; priced per-1k → producer
		// value = deliveries/1000 (rule 5; 0.4 µ$/delivery floors per-unit).
		return KindCount, true
	case "infra.event.bytes":
		// §2.2 event-bus payload size. sum; NAMED bytes but priced/emitted PER GiB
		// (rule 5; per-byte floors) → producer value = bytes/1024^3. DISTINCT from
		// infra.egress.* (internal bus, different cost basis).
		return KindSum, true
	case "infra.egress.api.bytes":
		// §2.5 non-CDN egress (JSON API + proxied module bodies). sum; NAMED bytes
		// but priced/emitted PER GiB (rule 5; 0.0000838 µ$/byte floors) → producer
		// value = bytes/1024^3.
		return KindSum, true
	case "infra.storage.put.count":
		// §2.4 S3 tier-1 PUT/COPY ops. count; priced per-1k → producer value =
		// puts/1000 (rule 5; 0.005 µ$/PUT floors per-unit).
		return KindCount, true
	case "infra.storage.list.count":
		// §2.4 S3 tier-1 LIST ops. count; priced per-1k → producer value =
		// lists/1000 (rule 5; 0.005 µ$/LIST floors per-unit).
		return KindCount, true
	case "infra.storage.gib_hours":
		// §2.4 S3 stored VOLUME. time_weighted: producer (PR #7) emits the
		// GiB-hours integral of the stored-bytes gauge (how much × how long), NOT
		// a peak and NOT an op count. Priced per GiB-hour (~31.5 µ$ >= 1, no floor).
		return KindTimeWeighted, true

	default:
		return "", false
	}
}

// RecordInfraUsageRequest is the payload of the RecordInfraUsage RPC — the
// platform-trusted infra ingest (Plane 1). It is the INVERSE of
// RecordUsageRequest: the metric MUST be a reserved infra.* / platform.*
// namespace (everything else is rejected), and the kind is platform-owned
// (resolved via platformInfraKind, never supplied by the caller and never read
// from a module catalog).
//
// Only the platform's own measurement chokepoints reach this RPC (gated by the
// INTERNAL secret, never the meter secret), so AppID/OwnerUserID/OwnerOrgID/ModuleID
// are the trusted attribution of the call that incurred the infra cost. ModuleID is
// TRUSTED platform-re-derived attribution (decision 19): the dispatch producer stamps
// the incurring module UUID; a Nil ModuleID is the explicit "unattributed" signal
// (platform-agent AI, egress/async producers) and is mapped to the platform-infra
// sentinel. It is NEVER an SDK hint.
type RecordInfraUsageRequest struct {
	// EventID is the producer-minted idempotency key, stable across the
	// producer's retry. RecordInfraUsage inserts ON CONFLICT(event_id) DO NOTHING.
	EventID string `json:"event_id"`

	// AppID is the app the infra cost is attributed to (the call's app). The
	// owner principal anchors the lazy billing account. Nil is tolerated as the
	// "unattributed to a single app" signal (platform-agent AI events send it) —
	// recorded under the zero app_id, which no per-app bill query matches, so it
	// accrues at the account level only (parallel to the lazy-owner rule).
	AppID uuid.UUID `json:"app_id"`

	// ModuleID is the incurring module (decision 19): the dispatch producer stamps
	// the module UUID it already holds so the app bill can attribute this infra line
	// to that module's card. Nil = unattributed → mapped to the platform-infra
	// sentinel at insert (prices + rolls up exactly as before). TRUSTED
	// platform-re-derived attribution, never an SDK-supplied hint.
	ModuleID uuid.UUID `json:"module_id,omitempty"`

	// Owner principal (polymorphic). Exactly one set, or both Nil for a lazy
	// (account-less) event — recorded with a NULL account_id, backfilled on
	// conversion (design §8 "Lazy account"), identical to RecordUsage.
	OwnerUserID uuid.UUID `json:"owner_user_id,omitempty"`
	OwnerOrgID  uuid.UUID `json:"owner_org_id,omitempty"`

	// Metric MUST be a reserved infra.* / platform.* name registered in
	// platformInfraKind. A non-reserved or unregistered metric is rejected with
	// INVALID_INPUT (the inverse of the SDK gate).
	Metric string `json:"metric"`

	// Model is the OPTIONAL AI pricing dimension (migration 018). The producer
	// (infra-metrics PR #2) stamps the roster model id (e.g.
	// "anthropic.claude-sonnet-4-6") on an infra.ai.* event so the rollup prices
	// it from metric_model_prices PER MODEL; the catalog row is the fallback when
	// it is empty. Empty for every non-AI infra metric → stored as a NULL
	// usage_events.model. It is purely a pricing dimension: it does NOT gate
	// acceptance (an AI metric with no model still records and prices via the
	// catalog fallback), and it is never read for non-AI metrics.
	Model string `json:"model,omitempty"`

	// ModuleVersion is the OPTIONAL version-attribution dimension (migration
	// 023): purely a reporting dimension, never a pricing one (unlike Model,
	// it is not restricted to the infra.ai.* family — any infra metric may
	// carry a version). Empty when the producer doesn't report one → stored
	// as a NULL usage_events.module_version.
	ModuleVersion string `json:"module_version,omitempty"`

	// Value is the platform-MEASURED quantity (ms / bytes). Authoritative and
	// non-zeroable — it comes from the platform's chokepoint, never an SDK hint.
	//
	// NOTE: float64 carries integer byte/ms counts exactly only up to 2^53; a
	// single event reporting more than ~9 PB of egress (or ~285,000 years of ms)
	// loses sub-unit precision. This matches RecordUsageRequest.Value and is far
	// beyond any single producer window, so it is an accepted plane-wide choice.
	Value float64 `json:"value"`

	// RecordedAt is the producer's server-asserted measurement time. Empty is
	// tolerated and defaulted to now() so a malformed timestamp can't drop a
	// billable infra fact.
	RecordedAt time.Time `json:"recorded_at,omitempty"`
}

// RecordInfraUsageResponse reports whether the infra event was newly recorded.
// Recorded is false when ON CONFLICT(event_id) DO NOTHING deduped a retry — the
// call still succeeds (the fact is already stored).
type RecordInfraUsageResponse struct {
	Recorded bool `json:"recorded"`
}

// RecordInfraUsage is the platform-trusted infra ingest (Plane 1) — the INVERSE
// of RecordUsage. It:
//  1. validates the required fields + a finite, non-negative value,
//  2. REQUIRES the metric be a reserved infra.* / platform.* namespace AND a
//     registered platform infra metric (rejects everything else — the inverse
//     of the SDK gate which rejects reserved names),
//  3. resolves the accumulation KIND from the platform-owned registry (NOT a
//     per-module metric_definitions lookup — the platform owns infra semantics),
//  4. stamps the incurring module_id (decision 19): req.ModuleID when the producer
//     supplied a real incurring module, else the platform-infra SENTINEL so an
//     unattributed event prices + rolls up through the seeded catalog rows
//     (migrations 017/020) exactly as before,
//  5. resolves the owner's billing account (Nil = lazy, recorded NULL),
//  6. inserts ON CONFLICT(event_id) DO NOTHING (idempotent retry).
//
// AppID Nil is tolerated (recorded under the zero app_id): the platform-agent AI
// producer (ai_meter.go) sends Nil AppID for account-level, app-unattributed infra —
// rejecting it swallowed every such event. It accrues at the account level only (no
// per-app bill query matches the zero app_id), parallel to the lazy-owner rule.
//
// A module can NEVER reach this path: it is gated by the INTERNAL secret (not
// the meter secret the SDK ingress uses) AND it accepts ONLY reserved names the
// SDK ingress rejects. A deduped retry returns Recorded=false with a nil error.
func (s *Service) RecordInfraUsage(ctx context.Context, req RecordInfraUsageRequest) (*RecordInfraUsageResponse, error) {
	if req.EventID == "" {
		return nil, billing.InvalidInput("event_id required")
	}
	// AppID Nil is NOT rejected (decision 19): it is the explicit "unattributed to a
	// single app" signal the platform-agent AI producer sends. Recorded under the zero
	// app_id — no per-app bill query matches it, so it accrues account-level only.
	if req.Metric == "" {
		return nil, billing.InvalidInput("metric required")
	}
	if math.IsNaN(req.Value) || math.IsInf(req.Value, 0) {
		return nil, billing.InvalidInput("value must be finite")
	}
	if req.Value < 0 {
		return nil, billing.InvalidInput("value must be non-negative")
	}
	if req.OwnerUserID != uuid.Nil && req.OwnerOrgID != uuid.Nil {
		return nil, billing.InvalidInput("owner_user_id and owner_org_id are mutually exclusive")
	}

	// INVERSE gate: the platform-infra ingest accepts ONLY the reserved
	// namespaces the SDK ingress rejects. A non-reserved metric here is a
	// caller bug (a custom business metric belongs on RecordUsage), so reject it.
	if !isReservedMetric(req.Metric) {
		return nil, billing.InvalidInput("metric is not a platform-infra namespace (RecordInfraUsage accepts only infra.* / platform.* metrics)")
	}
	// The kind is platform-owned: resolved from the registry, never from a
	// per-module catalog and never from the caller. An unregistered reserved
	// name has no platform-owned semantics → reject.
	kind, registered := platformInfraKind(req.Metric)
	if !registered {
		return nil, billing.InvalidInput("unknown platform-infra metric: " + req.Metric)
	}

	// Model is a pricing dimension EXCLUSIVE to the infra.ai.* family (migration
	// 018). Reject it on any other infra metric: a stray model on, say,
	// infra.compute.walltime.ms would persist as a non-NULL usage_events.model and then
	// trigger a spurious per-model lookup at rollup that always misses and falls
	// back to the catalog — a silent footgun. The comment that model is "never
	// read for non-AI metrics" is now ENFORCED, not just documented.
	if req.Model != "" && !strings.HasPrefix(req.Metric, "infra.ai.") {
		return nil, billing.InvalidInput("model is only valid for infra.ai.* metrics")
	}

	// Resolve the owner's billing account. Nil owner (or no account yet)
	// records a lazy event with NULL account_id — retained and backfilled on
	// conversion (design §8), identical to RecordUsage.
	accountID := uuid.Nil
	owner := Owner{UserID: req.OwnerUserID, OrgID: req.OwnerOrgID}
	if !owner.IsZero() {
		id, ok, err := s.store.AccountByOwner(ctx, owner)
		if err != nil {
			return nil, billing.Internal("account lookup failed", err)
		}
		if ok {
			accountID = id
		}
	}

	recordedAt := req.RecordedAt
	if recordedAt.IsZero() {
		recordedAt = s.nowFn().UTC()
	}

	// Decision 19 attribution: stamp the producer-supplied incurring module_id, or
	// the platform-infra sentinel when Nil (genuinely unattributable — platform-agent
	// AI, egress/async producers). The sentinel keeps the seeded catalog rows as the
	// price/kind/unit/display_group anchor, so an unattributed event prices + rolls up
	// identically to before. Trusted platform-re-derived attribution, never an SDK hint.
	moduleID := req.ModuleID
	if moduleID == uuid.Nil {
		moduleID = platformInfraModuleID
	}

	recorded, err := s.store.InsertUsageEvent(ctx, UsageEvent{
		EventID:       req.EventID,
		AccountID:     accountID,
		AppID:         req.AppID,
		ModuleID:      moduleID,
		Metric:        req.Metric,
		Kind:          kind,
		Value:         req.Value,
		RecordedAt:    recordedAt,
		Model:         req.Model,         // empty for non-AI metrics → NULL usage_events.model
		ModuleVersion: req.ModuleVersion, // empty → NULL usage_events.module_version
	})
	if err != nil {
		return nil, billing.Internal("insert infra usage event failed", err)
	}

	return &RecordInfraUsageResponse{Recorded: recorded}, nil
}
