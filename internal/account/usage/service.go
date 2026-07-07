package usage

import (
	"context"
	"log/slog"
	"math"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/mirrorstack-ai/billing-engine/internal/account/billing"
	"github.com/mirrorstack-ai/billing-engine/internal/billingperiod"
)

// reservedMetricPrefixes are the platform-measured namespaces a module
// must NOT self-report through the developer-controlled SDK meter
// (design §3a build rule 3). Platform infra (egress/compute/storage/
// tokens) is metered at the platform's own chokepoints and fed to
// RecordUsage platform-side; accepting these from the SDK ingress would
// let a module forge or zero a platform-billable metric.
var reservedMetricPrefixes = []string{"platform.", "infra."}

// BudgetEvaluator is the per-app budget hook the ingest path fires after a
// fresh usage event. It is satisfied by *budget.Service (kept as an interface
// here to avoid a usage→budget import cycle — budget imports usage for the
// money helper). nil means budgets are not wired and the hook is skipped.
//
// EvaluateAppBudget recomputes the app's current-period spend and records any
// newly-crossed threshold; it is called BEST-EFFORT (its error never fails
// the usage ingest).
type BudgetEvaluator interface {
	EvaluateAppBudget(ctx context.Context, appID uuid.UUID, periodStart, periodEnd time.Time) ([]int, error)
}

// Service implements the RecordUsage / GetUsageSummary /
// SetMetricDefinitions / SetModuleVisibility RPCs. It composes a Store;
// nowFn is injectable for deterministic tests. budget is the optional per-app
// budget hook fired on a fresh usage event (nil = budgets not wired).
type Service struct {
	store  Store
	nowFn  func() time.Time
	budget BudgetEvaluator
}

// NewService wires a Service. store is required; passing nil panics at
// the first call site.
func NewService(store Store) *Service {
	return &Service{store: store, nowFn: time.Now}
}

// WithBudgetEvaluator attaches the per-app budget hook fired on the ingest
// path. Returns the receiver for chaining at construction. A nil evaluator
// leaves budgets unwired (the hook is skipped).
func (s *Service) WithBudgetEvaluator(b BudgetEvaluator) *Service {
	s.budget = b
	return s
}

// RecordUsage is the ingest seam. It:
//  1. validates the dispatch-asserted request (identity is trusted here;
//     it was re-derived upstream — design §3a),
//  2. rejects the reserved platform.* / infra.* namespaces (build rule 3),
//  3. resolves the DECLARED kind from metric_definitions and REJECTS an
//     undeclared metric with INVALID_INPUT (declaration-first — design §1):
//     the catalog (manifest-fed via SetMetricDefinitions) must declare the
//     metric before any event is accepted. A retired (active=false) metric
//     is also rejected — it no longer accepts events,
//  4. resolves the owner's billing account (Nil = lazy, recorded NULL),
//  5. inserts ON CONFLICT(event_id) DO NOTHING (idempotent retry).
//
// A deduped retry returns Recorded=false with a nil error — the fact is
// already stored; callers must treat false as success.
func (s *Service) RecordUsage(ctx context.Context, req RecordUsageRequest) (*RecordUsageResponse, error) {
	if req.EventID == "" {
		return nil, billing.InvalidInput("event_id required")
	}
	if req.AppID == uuid.Nil {
		return nil, billing.InvalidInput("app_id required")
	}
	if req.ModuleID == uuid.Nil {
		return nil, billing.InvalidInput("module_id required")
	}
	if req.Metric == "" {
		return nil, billing.InvalidInput("metric required")
	}
	// Non-negative + finite value guard (design Axis 1: meters never carry
	// negative or non-finite quantities; the SDK validates too, but this
	// is the authoritative server-side gate).
	if math.IsNaN(req.Value) || math.IsInf(req.Value, 0) {
		return nil, billing.InvalidInput("value must be finite")
	}
	if req.Value < 0 {
		return nil, billing.InvalidInput("value must be non-negative")
	}
	if isReservedMetric(req.Metric) {
		return nil, billing.InvalidInput("metric uses a reserved platform namespace (platform.* / infra.* are platform-measured and cannot be self-reported)")
	}
	if req.OwnerUserID != uuid.Nil && req.OwnerOrgID != uuid.Nil {
		return nil, billing.InvalidInput("owner_user_id and owner_org_id are mutually exclusive")
	}

	// Resolve the DECLARED kind from the manifest-fed catalog. Metering is
	// declaration-first (design §1): a metric must be declared (ms.Meter →
	// manifest → SetMetricDefinitions) before any event is accepted, so an
	// undeclared metric is REJECTED rather than recorded with a fallback
	// kind. A retired (active=false) metric likewise no longer accepts
	// events. The resolved kind is snapshotted onto the usage_events row so
	// a later catalog edit can't retro-change how the event rolls up.
	def, found, err := s.store.LookupMetricDefinition(ctx, req.ModuleID, req.Metric)
	if err != nil {
		return nil, billing.Internal("metric definition lookup failed", err)
	}
	if !found {
		return nil, billing.InvalidInput("metric not declared (declare it via ms.Meter so the platform can resolve its kind and price)")
	}
	if !def.Active {
		return nil, billing.InvalidInput("metric is retired and no longer accepts events")
	}
	kind := def.Kind

	// Resolve the owner's billing account. Nil owner (or no account yet)
	// records a lazy event with NULL account_id — retained and backfilled
	// on conversion (design §8 "Lazy account").
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

	recorded, err := s.store.InsertUsageEvent(ctx, UsageEvent{
		EventID:       req.EventID,
		AccountID:     accountID,
		AppID:         req.AppID,
		ModuleID:      req.ModuleID,
		Metric:        req.Metric,
		Kind:          kind,
		Value:         req.Value,
		RecordedAt:    recordedAt,
		ModuleVersion: req.ModuleVersion,
	})
	if err != nil {
		return nil, billing.Internal("insert usage event failed", err)
	}

	// Per-app budget evaluation (design §5 / §10). Fire ONLY on a fresh
	// insert: recorded=false is a deduped retry whose event was already
	// evaluated, so re-evaluating would re-walk the same spend (harmless —
	// the alert insert is idempotent — but wasteful). BEST-EFFORT: a budget
	// error must NOT fail the usage ingest, so we log and continue. The
	// window matches GetUsageSummary's (the payer account's anchored period the
	// event fell in) so the budget is checked against exactly what the user
	// sees. A lazy event (accountID == Nil) has no payer account to anchor on,
	// so it falls back to the calendar month (DefaultAnchorDay).
	if recorded && s.budget != nil {
		anchorDay := billingperiod.DefaultAnchorDay
		if accountID != uuid.Nil {
			if d, err := s.store.AccountAnchorDay(ctx, accountID); err != nil {
				slog.Error("anchor day lookup failed (budget windowed on calendar month)",
					"app_id", req.AppID, "account_id", accountID, "error", err)
			} else {
				anchorDay = d
			}
		}
		start, end := billingperiod.AnchoredPeriodWindow(recordedAt.UTC(), anchorDay)
		if _, err := s.budget.EvaluateAppBudget(ctx, req.AppID, start, end); err != nil {
			slog.Error("budget evaluation failed (usage still recorded)",
				"app_id", req.AppID, "module_id", req.ModuleID, "metric", req.Metric, "error", err)
		}
	}

	return &RecordUsageResponse{Recorded: recorded}, nil
}

// GetUsageSummary returns the live current-period charged_micros per
// metric for an owner. For a third-party custom metric the charge is
// quantity × the developer's declared per-unit price with NO blanket
// markup (design §1 / §4 Axis 1) — so charged == raw cost here. No
// billing account yet → an empty Metrics slice and a nil error.
func (s *Service) GetUsageSummary(ctx context.Context, req GetUsageSummaryRequest) (*GetUsageSummaryResponse, error) {
	if req.OwnerUserID == uuid.Nil && req.OwnerOrgID == uuid.Nil {
		return nil, billing.InvalidInput("owner_user_id or owner_org_id required")
	}
	if req.OwnerUserID != uuid.Nil && req.OwnerOrgID != uuid.Nil {
		return nil, billing.InvalidInput("owner_user_id and owner_org_id are mutually exclusive")
	}

	owner := Owner{UserID: req.OwnerUserID, OrgID: req.OwnerOrgID}
	accountID, found, err := s.store.AccountByOwner(ctx, owner)
	if err != nil {
		return nil, billing.Internal("account lookup failed", err)
	}
	if !found {
		return &GetUsageSummaryResponse{Metrics: []MetricUsage{}}, nil
	}

	// Live current-period window: the account's anchored period (card-binding
	// day, ADR 0005) containing now — the same window the rollup + charge cycle
	// close on, so the running estimate lines up with the eventual bill.
	anchorDay, err := s.store.AccountAnchorDay(ctx, accountID)
	if err != nil {
		return nil, billing.Internal("anchor day lookup failed", err)
	}
	start, end := billingperiod.AnchoredPeriodWindow(s.nowFn().UTC(), anchorDay)

	rows, err := s.store.CurrentPeriodUsage(ctx, accountID, start, end)
	if err != nil {
		return nil, billing.Internal("usage summary query failed", err)
	}

	metrics := make([]MetricUsage, 0, len(rows))
	for _, r := range rows {
		// Custom metrics charge at the declared price with no blanket markup,
		// so the customer charge equals the raw (quantity × unit_price) cost.
		// The flat 1.2× for platform-infra / built-in metrics is not in this
		// PR's scope (PR #5/#10).
		metrics = append(metrics, MetricUsage{
			ModuleID:        r.ModuleID,
			Metric:          r.Metric,
			Kind:            r.Kind,
			Quantity:        r.Quantity,
			UnitPriceMicros: r.UnitPriceMicros,
			RawCostMicros:   r.RawCostMicros,
			ChargedMicros:   r.RawCostMicros,
			Group:           r.Group,
			Visibility:      r.Visibility,
		})
	}
	return &GetUsageSummaryResponse{
		PeriodStart: start,
		PeriodEnd:   end,
		Metrics:     metrics,
	}, nil
}

// GetUsageHistory returns the multi-month ROLLED-UP usage history for an
// owner — the trend-chart read over the immutable billable record
// (usage_aggregates), never the raw usage_events. The window is the trailing
// req.Months CLOSED calendar months (excluding the current, still-open
// month; GetUsageSummary is the live estimate for that one). No billing
// account yet → an empty Periods slice and a nil error.
func (s *Service) GetUsageHistory(ctx context.Context, req GetUsageHistoryRequest) (*GetUsageHistoryResponse, error) {
	if req.Months <= 0 {
		return nil, billing.InvalidInput("months must be positive")
	}
	if req.OwnerUserID == uuid.Nil && req.OwnerOrgID == uuid.Nil {
		return nil, billing.InvalidInput("owner_user_id or owner_org_id required")
	}
	if req.OwnerUserID != uuid.Nil && req.OwnerOrgID != uuid.Nil {
		return nil, billing.InvalidInput("owner_user_id and owner_org_id are mutually exclusive")
	}

	owner := Owner{UserID: req.OwnerUserID, OrgID: req.OwnerOrgID}
	accountID, found, err := s.store.AccountByOwner(ctx, owner)
	if err != nil {
		return nil, billing.Internal("account lookup failed", err)
	}
	if !found {
		return &GetUsageHistoryResponse{Periods: []PeriodUsage{}}, nil
	}

	// windowEnd is the current (in-progress) anchored period's start — the
	// trailing window stops there so this RPC never returns a partial current
	// period. windowStart is Months ANCHORED periods before that (stepped by
	// month-index arithmetic + re-clamped from the original anchor day via
	// ShiftPeriods — never AddDate, which would drift a 31-anchor across a short
	// month).
	anchorDay, err := s.store.AccountAnchorDay(ctx, accountID)
	if err != nil {
		return nil, billing.Internal("anchor day lookup failed", err)
	}
	windowEnd, _ := billingperiod.AnchoredPeriodWindow(s.nowFn().UTC(), anchorDay)
	windowStart := billingperiod.ShiftPeriods(windowEnd, -req.Months, anchorDay)

	rows, err := s.store.UsageHistory(ctx, accountID, windowStart, windowEnd)
	if err != nil {
		return nil, billing.Internal("usage history query failed", err)
	}

	// Bucket rows into ordered periods. The store returns rows ordered
	// (period_start ASC, metric ASC), so a "new period when period_start
	// changes" scan preserves that order without a re-sort or a map (which
	// would need one anyway, plus lose ordering).
	periods := make([]PeriodUsage, 0)
	var cur *PeriodUsage
	for _, r := range rows {
		if cur == nil || !cur.PeriodStart.Equal(r.PeriodStart) {
			periods = append(periods, PeriodUsage{
				PeriodStart: r.PeriodStart,
				PeriodEnd:   r.PeriodEnd,
				Metrics:     []MetricUsage{},
			})
			cur = &periods[len(periods)-1]
		}
		cur.Metrics = append(cur.Metrics, MetricUsage{
			ModuleID:        r.ModuleID,
			Metric:          r.Metric,
			Kind:            r.Kind,
			Quantity:        r.Quantity,
			UnitPriceMicros: r.UnitPriceMicros,
			RawCostMicros:   r.RawCostMicros,
			ChargedMicros:   r.ChargedMicros,
			Group:           r.Group,
			Visibility:      r.Visibility,
		})
	}
	return &GetUsageHistoryResponse{Periods: periods}, nil
}

// GetVersionBreakdown returns the per-module_version cost/income breakdown
// for the CURRENT period — the same calendar-month-to-date window
// GetUsageSummary resolves. It reads the immutable billable record
// (usage_aggregates), so Versions is empty until cmd/billing-cycle's
// RollupPeriod has run for this window (not an error — usage_aggregates is
// written at rollup, not at ingest). No billing account yet → an empty
// Versions slice and a nil error.
func (s *Service) GetVersionBreakdown(ctx context.Context, req GetVersionBreakdownRequest) (*GetVersionBreakdownResponse, error) {
	if req.OwnerUserID == uuid.Nil && req.OwnerOrgID == uuid.Nil {
		return nil, billing.InvalidInput("owner_user_id or owner_org_id required")
	}
	if req.OwnerUserID != uuid.Nil && req.OwnerOrgID != uuid.Nil {
		return nil, billing.InvalidInput("owner_user_id and owner_org_id are mutually exclusive")
	}

	owner := Owner{UserID: req.OwnerUserID, OrgID: req.OwnerOrgID}
	accountID, found, err := s.store.AccountByOwner(ctx, owner)
	if err != nil {
		return nil, billing.Internal("account lookup failed", err)
	}
	if !found {
		return &GetVersionBreakdownResponse{Versions: []ModuleVersionUsage{}}, nil
	}

	anchorDay, err := s.store.AccountAnchorDay(ctx, accountID)
	if err != nil {
		return nil, billing.Internal("anchor day lookup failed", err)
	}
	start, end := billingperiod.AnchoredPeriodWindow(s.nowFn().UTC(), anchorDay)

	rows, err := s.store.VersionBreakdown(ctx, accountID, start, req.ModuleID)
	if err != nil {
		return nil, billing.Internal("version breakdown query failed", err)
	}

	versions := make([]ModuleVersionUsage, 0, len(rows))
	for _, r := range rows {
		versions = append(versions, ModuleVersionUsage{
			ModuleVersion:    r.ModuleVersion,
			BillableQuantity: r.BillableQuantity,
			RawCostMicros:    r.RawCostMicros,
			ChargedMicros:    r.ChargedMicros,
		})
	}
	return &GetVersionBreakdownResponse{
		PeriodStart: start,
		PeriodEnd:   end,
		Versions:    versions,
	}, nil
}

// GetAppUsageSummary returns the app-owner's bill for ONE app in the current
// period — the read behind /apps/{appId}/settings/billing. The owner principal
// selects the PAYER's billing account (account_id gates the payer, same lazy-
// account resolution as GetUsageSummary); AppID filters to the one app. Each
// line is a (module, metric, model, module_version)'s billable quantity + the
// module's declared unit price + the customer charge, read rolled-up-else-live
// (usage_aggregates once this app+period is rolled up, else live usage_events —
// the same fast path GetUsageSummary uses). The app owner pays the DECLARED
// price per metered unit with NO customer markup by visibility, so ChargedMicros
// = raw cost here. No billing account yet → an empty Metrics slice + nil error.
func (s *Service) GetAppUsageSummary(ctx context.Context, req GetAppUsageSummaryRequest) (*GetAppUsageSummaryResponse, error) {
	if req.OwnerUserID == uuid.Nil && req.OwnerOrgID == uuid.Nil {
		return nil, billing.InvalidInput("owner_user_id or owner_org_id required")
	}
	if req.OwnerUserID != uuid.Nil && req.OwnerOrgID != uuid.Nil {
		return nil, billing.InvalidInput("owner_user_id and owner_org_id are mutually exclusive")
	}
	if req.AppID == uuid.Nil {
		return nil, billing.InvalidInput("app_id required")
	}

	owner := Owner{UserID: req.OwnerUserID, OrgID: req.OwnerOrgID}
	accountID, found, err := s.store.AccountByOwner(ctx, owner)
	if err != nil {
		return nil, billing.Internal("account lookup failed", err)
	}
	if !found {
		return &GetAppUsageSummaryResponse{AppID: req.AppID, Metrics: []AppMetricUsage{}}, nil
	}

	// Same live current-period window as GetUsageSummary: the account's anchored
	// period (card-binding day) containing now, which also anchors the rolled-up
	// branch's billing_periods lookup (period_start).
	anchorDay, err := s.store.AccountAnchorDay(ctx, accountID)
	if err != nil {
		return nil, billing.Internal("anchor day lookup failed", err)
	}
	start, end := billingperiod.AnchoredPeriodWindow(s.nowFn().UTC(), anchorDay)

	rows, err := s.store.AppUsage(ctx, accountID, req.AppID, start, end)
	if err != nil {
		return nil, billing.Internal("app usage summary query failed", err)
	}

	metrics := make([]AppMetricUsage, 0, len(rows))
	for _, r := range rows {
		metrics = append(metrics, AppMetricUsage{
			ModuleID:         r.ModuleID,
			Metric:           r.Metric,
			Kind:             r.Kind,
			Model:            r.Model,
			ModuleVersion:    r.ModuleVersion,
			BillableQuantity: r.BillableQuantity,
			UnitPriceMicros:  r.UnitPriceMicros,
			ChargedMicros:    r.ChargedMicros,
		})
	}
	return &GetAppUsageSummaryResponse{
		AppID:       req.AppID,
		PeriodStart: start,
		PeriodEnd:   end,
		Metrics:     metrics,
	}, nil
}

// SetMetricDefinitions syncs a module's declared metrics into the catalog
// (declaration-first — design §1 / §5). It is a platform CONTROL-PLANE
// call: api-platform reads the manifest's declarations on install/publish
// and upserts each one so the catalog (kind/unit/price) exists before any
// event. Validates each declaration and rejects the reserved platform.* /
// infra.* namespaces a module must never self-declare (design §3a build
// rule 3). Idempotent per (module, metric).
func (s *Service) SetMetricDefinitions(ctx context.Context, req SetMetricDefinitionsRequest) (*SetMetricDefinitionsResponse, error) {
	if req.ModuleID == uuid.Nil {
		return nil, billing.InvalidInput("module_id required")
	}
	// Validate every declaration BEFORE touching the store, then upsert the
	// whole set in one transaction (UpsertMetricDefinitions is all-or-nothing).
	// A partial catalog would accept some declared metrics at ingest and
	// reject others until the next sync — declaration-first correctness
	// (design §1) requires the catalog be fully-or-nothing.
	defs := make([]MetricDeclaration, 0, len(req.Metrics))
	for _, m := range req.Metrics {
		if m.Metric == "" {
			return nil, billing.InvalidInput("metric required")
		}
		if isReservedMetric(m.Metric) {
			return nil, billing.InvalidInput("metric uses a reserved platform namespace (platform.* / infra.* are platform-measured and cannot be declared by a module): " + m.Metric)
		}
		if !isValidKind(m.Kind) {
			return nil, billing.InvalidInput("invalid metric kind: " + string(m.Kind))
		}
		if m.Priced && m.UnitPriceMicros < 0 {
			return nil, billing.InvalidInput("unit_price_micros must be non-negative")
		}
		defs = append(defs, MetricDeclaration{
			ModuleID:        req.ModuleID,
			Metric:          m.Metric,
			Kind:            m.Kind,
			Unit:            m.Unit,
			UnitPriceMicros: m.UnitPriceMicros,
			Priced:          m.Priced,
			Active:          m.Active,
		})
	}
	if err := s.store.UpsertMetricDefinitions(ctx, defs); err != nil {
		return nil, billing.Internal("upsert metric definitions failed", err)
	}
	return &SetMetricDefinitionsResponse{Synced: len(req.Metrics)}, nil
}

// SetMetricVersionPrices syncs a version's immutable per-metric price
// snapshot(s) into metric_version_prices (usage-time-pricing Phase 1,
// migration 044). It is a platform CONTROL-PLANE call: api-platform fires it
// at version PUBLISH time (mirroring SetMetricDefinitions' manifest sync),
// so the rollup can resolve a version-stamped event's price VERSION-FIRST
// (cycle.MetricPriceMicros → LookupMetricVersionPrice) instead of the
// version-blind metric_definitions catalog row — the fix for the
// mid-period-reprice bug (docs-temp/usage-time-pricing/design.md): a LATER
// version's re-price can never retroactively change an EARLIER version's
// already-snapshotted price.
//
// Semantics: written ONCE per (module_id, metric, module_version). A
// duplicate publish of the exact same version is a no-op (ON CONFLICT DO
// NOTHING at the store layer), never an error and never an overwrite.
func (s *Service) SetMetricVersionPrices(ctx context.Context, req SetMetricVersionPricesRequest) (*SetMetricVersionPricesResponse, error) {
	if req.ModuleID == uuid.Nil {
		return nil, billing.InvalidInput("module_id required")
	}
	// Validate every entry BEFORE touching the store, then upsert the whole
	// set in one transaction (UpsertMetricVersionPrices is all-or-nothing) —
	// mirrors SetMetricDefinitions' validate-then-upsert shape.
	prices := make([]MetricVersionPrice, 0, len(req.Prices))
	for _, p := range req.Prices {
		if p.Metric == "" {
			return nil, billing.InvalidInput("metric required")
		}
		if p.ModuleVersion == "" {
			return nil, billing.InvalidInput("module_version required")
		}
		if p.UnitPriceMicros < 0 {
			return nil, billing.InvalidInput("unit_price_micros must be non-negative")
		}
		prices = append(prices, MetricVersionPrice{
			ModuleID:        req.ModuleID,
			Metric:          p.Metric,
			ModuleVersion:   p.ModuleVersion,
			UnitPriceMicros: p.UnitPriceMicros,
		})
	}
	if err := s.store.UpsertMetricVersionPrices(ctx, prices); err != nil {
		return nil, billing.Internal("upsert metric version prices failed", err)
	}
	return &SetMetricVersionPricesResponse{Synced: len(req.Prices)}, nil
}

// SetInfraPriceOverrides writes a module's per-metric price OVERRIDES for the
// reserved platform-infra metrics it re-priced via ms.Meter("infra.X",
// ms.Price(n)) (decision 19 §4.3). It is the INVERSE of SetMetricDefinitions
// and the control-plane twin of RecordInfraUsage's gate:
//
//   - SetMetricDefinitions REJECTS reserved names (a module may never DECLARE a
//     platform metric); this RPC ACCEPTS ONLY reserved names REGISTERED in
//     platformInfraKind (a module MAY re-PRICE one). The reserved-name guard on
//     SetMetricDefinitions is deliberately left UNTOUCHED — the custom-metric
//     plane keeps rejecting reserved names; only this dedicated seam persists an
//     override (modeled on metric_model_prices / migration 018, the secondary
//     per-(metric, model) price layered over the sentinel row — this is the same
//     pattern one axis over, per-(module, metric)).
//   - The override row carries PRICE ONLY. kind + unit are platform-owned and
//     INHERITED from the SENTINEL base catalog row (the store copies them in one
//     INSERT ... SELECT) — never supplied by the caller.
//   - The row keys (module_id, metric) with the REAL module_id (NOT the
//     sentinel), so the bill's dual-price resolution (decision 19 §4.2, W1)
//     finds it via LEFT JOIN on the event's real module_id. ms.Price(0) →
//     override 0 → full absorb, no special case.
//
// Platform CONTROL-PLANE call (internal secret, not the meter secret),
// all-or-nothing (one transaction in the store), idempotent per
// (module, metric). No metric_definitions schema change: UNIQUE(module_id,
// metric) already supports the row.
func (s *Service) SetInfraPriceOverrides(ctx context.Context, req SetInfraPriceOverridesRequest) (*SetInfraPriceOverridesResponse, error) {
	if req.ModuleID == uuid.Nil {
		return nil, billing.InvalidInput("module_id required")
	}
	// The override row keys a REAL module_id. The all-zero sentinel is the
	// platform's BASE infra catalog (seeded by migration 017/018/020, the
	// authoritative default price + kind + unit + display_group), never
	// re-priced through this RPC — reject it so a caller can't clobber the base
	// price here.
	if req.ModuleID == platformInfraModuleID {
		return nil, billing.InvalidInput("module_id must be a real module, not the platform-infra sentinel")
	}
	// Validate every override BEFORE touching the store (the store upsert is
	// all-or-nothing, mirroring SetMetricDefinitions): a partial write would
	// leave the module's override set inconsistent until the next publish.
	overrides := make([]InfraPriceOverride, 0, len(req.Overrides))
	for _, o := range req.Overrides {
		if o.Metric == "" {
			return nil, billing.InvalidInput("metric required")
		}
		// INVERSE gate, mirroring RecordInfraUsage: accept ONLY reserved
		// infra.* / platform.* names (a custom metric belongs on
		// SetMetricDefinitions) that are REGISTERED platform infra metrics (an
		// unregistered reserved name has no platform-owned catalog row to
		// inherit kind/unit from, and nothing would ever emit it).
		if !isReservedMetric(o.Metric) {
			return nil, billing.InvalidInput("metric is not a platform-infra namespace (SetInfraPriceOverrides accepts only infra.* / platform.* metrics): " + o.Metric)
		}
		if _, registered := platformInfraKind(o.Metric); !registered {
			return nil, billing.InvalidInput("unknown platform-infra metric: " + o.Metric)
		}
		if o.UnitPriceMicros < 0 {
			return nil, billing.InvalidInput("unit_price_micros must be non-negative")
		}
		overrides = append(overrides, o)
	}
	if err := s.store.UpsertInfraPriceOverrides(ctx, req.ModuleID, overrides); err != nil {
		return nil, billing.Internal("upsert infra price overrides failed", err)
	}
	return &SetInfraPriceOverridesResponse{Synced: len(overrides)}, nil
}

// SetModuleVisibility upserts the developer margin-share mirror. It
// NEVER affects the customer charge; it governs only the developer
// settlement rate (PR #5). Fired by api-platform on every
// publish/unpublish.
func (s *Service) SetModuleVisibility(ctx context.Context, req SetModuleVisibilityRequest) (*SetModuleVisibilityResponse, error) {
	if req.ModuleID == uuid.Nil {
		return nil, billing.InvalidInput("module_id required")
	}
	if req.Visibility != VisibilityPrivate && req.Visibility != VisibilityPublished {
		return nil, billing.InvalidInput("visibility must be 'private' or 'published'")
	}
	if err := s.store.UpsertModuleVisibility(ctx, req.ModuleID, req.Visibility); err != nil {
		return nil, billing.Internal("set module visibility failed", err)
	}
	return &SetModuleVisibilityResponse{}, nil
}

// isValidKind reports whether k is one of the four catalog kinds. Guards
// the SetMetricDefinitions sync so a malformed manifest can't write an
// invalid enum (which the DB would reject anyway, with a worse error).
func isValidKind(k Kind) bool {
	switch k {
	case KindCount, KindSum, KindPeak, KindTimeWeighted:
		return true
	default:
		return false
	}
}

// isReservedMetric reports whether the metric name falls in a
// platform-measured namespace the SDK ingress must reject. Case-sensitive:
// the platform owns the exact lowercase prefixes.
func isReservedMetric(metric string) bool {
	for _, p := range reservedMetricPrefixes {
		if strings.HasPrefix(metric, p) {
			return true
		}
	}
	return false
}
