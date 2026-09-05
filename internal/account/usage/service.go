package usage

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/mirrorstack-ai/billing-engine/internal/account/billing"
	"github.com/mirrorstack-ai/billing-engine/internal/account/credit"
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
	credit credit.UsageEvaluator
}

// NewService wires a Service. store is required; passing nil panics at
// the first call site.
func NewService(store Store) *Service {
	return &Service{store: store, nowFn: time.Now}
}

// WithNow overrides the Service clock for deterministic tests.
func (s *Service) WithNow(now func() time.Time) *Service {
	s.nowFn = now
	return s
}

// WithBudgetEvaluator attaches the per-app budget hook fired on the ingest
// path. Returns the receiver for chaining at construction. A nil evaluator
// leaves budgets unwired (the hook is skipped).
func (s *Service) WithBudgetEvaluator(b BudgetEvaluator) *Service {
	s.budget = b
	return s
}

// WithCreditEvaluator attaches the disposable-wallet estimate + auto-top-up
// hook. Like the budget hook, it is invoked only for a newly inserted event and
// every error is logged and swallowed so metering never depends on Redis or a
// payment rail.
func (s *Service) WithCreditEvaluator(e credit.UsageEvaluator) *Service {
	s.credit = e
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
// The request's dev_served flag (migration 073) is carried onto the row
// verbatim: this ingress cannot tell a tunnel from a Lambda, only api-platform
// can. A dev_served event is stored, rolled up and priced like any other and
// charged never, so the one ingest-path behaviour it changes is the disposable
// wallet estimate, which is skipped for it (see below).
//
// A deduped retry returns Recorded=false with a nil error — the fact is
// already stored; callers must treat false as success.
func (s *Service) RecordUsage(ctx context.Context, req RecordUsageRequest) (*RecordUsageResponse, error) {
	return s.recordUsage(ctx, req, false)
}

func (s *Service) recordUsage(ctx context.Context, req RecordUsageRequest, timingRetried bool) (*RecordUsageResponse, error) {
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

	version, err := normalizedObservationVersion(req.Version)
	if err != nil {
		return nil, billing.InvalidInput(err.Error())
	}
	var metadata []byte
	switch version {
	case observationVersionLegacy:
		if req.Subject != "" || len(req.Metadata) != 0 || !req.OccurredAt.IsZero() {
			return nil, billing.InvalidInput("subject, metadata, and occurred_at require observation contract v=2")
		}
	case observationVersionV2:
		if req.OccurredAt.IsZero() {
			return nil, billing.InvalidInput("occurred_at required for observation contract v=2")
		}
		if err := validateSubject(req.Subject); err != nil {
			return nil, billing.InvalidInput(err.Error())
		}
		metadata, err = canonicalMetadata(req.Metadata)
		if err != nil {
			return nil, billing.InvalidInput(err.Error())
		}
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
	kind := def.Kind
	event := UsageEvent{
		ObservationVersion: version,
		EventID:            req.EventID,
		AppID:              req.AppID,
		ModuleID:           req.ModuleID,
		Metric:             req.Metric,
		Kind:               kind,
		AggregationKey:     def.AggregationKey,
		Value:              req.Value,
		OccurredAt:         req.OccurredAt.UTC(),
		Subject:            req.Subject,
		Metadata:           metadata,
		OwnerUserID:        req.OwnerUserID,
		OwnerOrgID:         req.OwnerOrgID,
		ModuleVersion:      req.ModuleVersion,
		// Carried VERBATIM from the request (migration 073). This ingress does
		// not — and cannot — decide whether a module was tunnel-served: only
		// api-platform sees which credential authenticated the call. Absent is
		// false is chargeable, today's behaviour.
		DevServed: req.DevServed,
	}
	event.PayloadFingerprint = observationFingerprint(event)
	accepted, err := s.store.CheckUsageEventID(ctx, req.EventID, event.PayloadFingerprint)
	if err != nil {
		if errors.Is(err, ErrUsageEventConflict) {
			return nil, billing.Conflict("event_id is already bound to a different canonical usage payload")
		}
		return nil, billing.Internal("usage event id lookup failed", err)
	}
	if accepted {
		return &RecordUsageResponse{Recorded: false}, nil
	}
	if !def.Active {
		return nil, billing.InvalidInput("metric is retired and no longer accepts events")
	}
	if def.AggregationKey == AggregationKeySubject {
		if version != observationVersionV2 {
			return nil, billing.InvalidInput("catalog aggregation_key subject requires observation contract v=2")
		}
		if req.Subject == "" {
			return nil, billing.InvalidInput("subject required for a subject-keyed peak metric")
		}
	}

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
		} else if owner.OrgID != uuid.Nil {
			rosterOrgID, found, err := s.store.AppOwnerOrg(ctx, req.AppID)
			if err != nil {
				return nil, billing.Internal("app billing registration lookup failed", err)
			}
			if !found || rosterOrgID != owner.OrgID {
				return nil, billing.InvalidInput("app is not registered for billing under this org — usage cannot be attributed and was NOT recorded (register the app before metering)")
			}
			slog.WarnContext(ctx, "org usage retained but unbilled pending funding designation",
				"org_id", owner.OrgID, "app_id", req.AppID, "module_id", req.ModuleID, "metric", req.Metric)
		}
	}

	now := s.nowFn().UTC()
	recordedAt := req.RecordedAt.UTC()
	if recordedAt.IsZero() {
		recordedAt = now
	}

	// v2 policy and every downstream read must agree on the account's anchored
	// billing window. For v1, preserve the old best-effort fallback when this
	// lookup is needed only by budget/credit hooks; for v2 a wrong anchor could
	// admit a closed observation into the wrong period, so lookup failure is loud.
	anchorDay := billingperiod.DefaultAnchorDay
	firstFundedStart := time.Time{}
	accountActivatedAt := time.Time{}
	accountActivated := false
	if accountID != uuid.Nil && (version == observationVersionV2 || s.budget != nil || s.credit != nil) {
		if version == observationVersionV2 {
			accountActivatedAt, accountActivated, err = s.store.AccountActivation(ctx, accountID)
			if err != nil {
				return nil, billing.Internal("account activation lookup failed", err)
			}
			if accountActivated {
				anchorDay = billingperiod.AnchorDay(accountActivatedAt)
				firstFundedStart, _ = billingperiod.AnchoredPeriodWindow(accountActivatedAt, anchorDay)
			}
		} else {
			d, anchorErr := s.store.AccountAnchorDay(ctx, accountID)
			if anchorErr != nil {
				slog.Error("anchor day lookup failed (budget windowed on calendar month)",
					"app_id", req.AppID, "account_id", accountID, "error", anchorErr)
			} else {
				anchorDay = d
			}
		}
	}

	eventTime := recordedAt
	occurredAt := time.Time{}
	policy := OccurrencePolicyV1IngestTime
	policyRejection := UsageRejectionReason("")
	if version == observationVersionV2 {
		occurredAt = req.OccurredAt.UTC()
		eventTime = occurredAt
		policy = OccurrencePolicyOnTime
		if occurredAt.After(now.Add(5 * time.Minute)) {
			policyRejection = UsageRejectionOccurredFuture
		} else if occurredAt.Before(now.Add(-35 * 24 * time.Hour)) {
			policyRejection = UsageRejectionOccurredTooOld
		}
		if policyRejection == "" && !firstFundedStart.IsZero() && occurredAt.Before(firstFundedStart) {
			// The cycle never closes a pre-activation window. Clamp into the
			// first funded period while preserving occurred_at as audit evidence.
			eventTime = firstFundedStart
			policy = OccurrencePolicyFirstFunded
		} else {
			currentStart, _ := billingperiod.AnchoredPeriodWindow(now, anchorDay)
			if policyRejection == "" && occurredAt.Before(currentStart) {
				policy = OccurrencePolicyLateOpen
			}
		}
	}
	periodStart, periodEnd := billingperiod.AnchoredPeriodWindow(eventTime, anchorDay)
	if version == observationVersionV2 && accountID != uuid.Nil && policyRejection == "" {
		// The standard cycle can still close the current and immediately prior
		// window. Anything older is logically closed even if an outage/missed
		// empty sweep left no billing_period row to record that fact.
		earliestClosableStart, _ := billingperiod.AnchoredJustClosed(now, anchorDay)
		if periodStart.Before(earliestClosableStart) {
			policyRejection = UsageRejectionPeriodClosed
		}
	}

	event.AccountID = accountID
	event.AccountActivatedAt = accountActivatedAt
	event.AccountActivated = accountActivated
	event.RecordedAt = recordedAt
	event.OccurredAt = occurredAt
	event.BillableAt = eventTime
	event.OccurrencePolicy = policy

	var recorded bool
	billableDelta := req.Value
	if version == observationVersionV2 {
		recorded, billableDelta, err = s.store.InsertUsageObservation(
			ctx, event, periodStart, periodEnd, policyRejection,
		)
	} else {
		recorded, err = s.store.InsertUsageEvent(ctx, event)
		if !recorded {
			billableDelta = 0
		}
	}
	if err != nil {
		switch {
		case errors.Is(err, ErrUsageAccountTimingChanged):
			if timingRetried {
				return nil, billing.Internal("account activation changed repeatedly during usage admission", err)
			}
			return s.recordUsage(ctx, req, true)
		case errors.Is(err, ErrUsageEventConflict):
			return nil, billing.Conflict("event_id is already bound to a different canonical usage payload")
		case errors.Is(err, ErrUsageOccurredFuture):
			return nil, billing.InvalidInput("occurred_at is more than 5 minutes in the future")
		case errors.Is(err, ErrUsageOccurredTooOld):
			return nil, billing.InvalidInput("occurred_at is older than the 35-day observation retry window")
		case errors.Is(err, ErrUsagePeriodClosed):
			return nil, billing.Conflict("the observation billing period is closing or invoiced")
		default:
			return nil, billing.Internal("insert usage event failed", err)
		}
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
	if recorded && (s.budget != nil || s.credit != nil) {
		start, end := periodStart, periodEnd
		if s.budget != nil {
			if _, err := s.budget.EvaluateAppBudget(ctx, req.AppID, start, end); err != nil {
				slog.Error("budget evaluation failed (usage still recorded)",
					"app_id", req.AppID, "module_id", req.ModuleID, "metric", req.Metric, "error", err)
			}
		}
		// 🔴 The disposable wallet is MONEY. A tunnel-served event is never
		// charged, so it must never estimate a debit, never draw down a
		// balance and never trigger an auto-top-up — the developer would be
		// buying credit to pay for usage that will be waived at rollup. The
		// budget hook above still fires: it RE-READS the app's spend from SQL
		// (AppPeriodSpendMicros, which excludes dev_served rows), so it stays
		// correct on its own and a dev event simply moves nothing.
		if s.credit != nil && accountID != uuid.Nil && !req.DevServed {
			delta, err := approximateUsageChargeMicros(req.Metric, billableDelta, def)
			if err != nil {
				slog.Error("credit estimate pricing failed (usage still recorded)",
					"app_id", req.AppID, "module_id", req.ModuleID, "metric", req.Metric, "error", err)
			} else if err := s.credit.EvaluateCreditUsage(ctx, credit.UsageEvent{
				AccountID:               accountID,
				AppID:                   req.AppID,
				EventID:                 req.EventID,
				ApproximateChargeMicros: delta,
				PeriodStart:             start,
				PeriodEnd:               end,
			}); err != nil {
				slog.Error("credit estimate/top-up evaluation failed (usage still recorded)",
					"app_id", req.AppID, "account_id", accountID, "metric", req.Metric, "error", err)
			}
		}
	}

	return &RecordUsageResponse{Recorded: recorded}, nil
}

// approximateUsageChargeMicros prices one newly accepted event for the
// disposable fast-path counter. The authoritative wallet debit still uses the
// rolled/boundary bill. For subject-keyed peak the store supplies only the
// positive increase over that subject's previous in-period MAX, preventing the
// disposable estimate from double-counting provider duplicates. Other peak and
// time-weighted events retain the established conservative approximation and
// are overwritten by authoritative live/rollup math at reconciliation.
func approximateUsageChargeMicros(metric string, value float64, def MetricDefinition) (int64, error) {
	if !def.Priced || def.UnitPriceMicros == 0 || value == 0 {
		return 0, nil
	}
	charge := value * float64(def.UnitPriceMicros)
	if isReservedMetric(metric) {
		charge = charge * 12 / 10
	}
	if math.IsNaN(charge) || math.IsInf(charge, 0) || charge > float64(math.MaxInt64) {
		return 0, fmt.Errorf("approximate charge is outside int64 micros")
	}
	return int64(math.Floor(charge + 0.5)), nil
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
			ActiveSeconds:    r.ActiveSeconds,
			PeriodDays:       r.PeriodDays,
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
		if m.AggregationKey != "" && m.AggregationKey != AggregationKeySubject {
			return nil, billing.InvalidInput("invalid aggregation_key: " + string(m.AggregationKey))
		}
		if m.AggregationKey == AggregationKeySubject && m.Kind != KindPeak {
			return nil, billing.InvalidInput("aggregation_key subject is only valid for peak metrics")
		}
		if m.Priced && m.UnitPriceMicros < 0 {
			return nil, billing.InvalidInput("unit_price_micros must be non-negative")
		}
		defs = append(defs, MetricDeclaration{
			ModuleID:        req.ModuleID,
			Metric:          m.Metric,
			Kind:            m.Kind,
			AggregationKey:  m.AggregationKey,
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
//   - AbsorbAll (ms.AbsorbInfra) does that for EVERY active sentinel metric at
//     once, expanded from the catalog in the store — never from a list of metric
//     names held by the caller, the SDK, or this package (see the request type).
//     Explicit overrides apply after it and win.
//
// 🔴 AUTHORITATIVE, NOT ADDITIVE. The set this call carries REPLACES the module's
// reserved overrides; anything absent is deleted. An EMPTY payload is therefore
// meaningful and must still reach the store — it is how a module withdraws the
// last ms.Price it declared. The predecessor upserted and returned early on an
// empty list, which made an override write-once: the price outlived the
// declaration and the app owner kept paying it.
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
	if err := s.store.SyncInfraPriceOverrides(ctx, req.ModuleID, req.AbsorbAll, overrides); err != nil {
		return nil, billing.Internal("sync infra price overrides failed", err)
	}
	return &SetInfraPriceOverridesResponse{Synced: len(overrides), AbsorbAll: req.AbsorbAll}, nil
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
