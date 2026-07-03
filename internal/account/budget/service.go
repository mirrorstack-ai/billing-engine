package budget

import (
	"context"
	"sort"
	"time"

	"github.com/google/uuid"

	"github.com/mirrorstack-ai/billing-engine/internal/account/billing"
	"github.com/mirrorstack-ai/billing-engine/internal/billingperiod"
)

// defaultAlertPercents is the threshold set applied when SetBudget is called
// with none: warn at 80% of the cap and again at the cap (design §9).
var defaultAlertPercents = []int{80, 100}

// Service implements the SetBudget / GetBudgetStatus / GetBudgetAlerts RPCs
// and the EvaluateAppBudget ingest-path hook. It composes a Store; nowFn is
// injectable for deterministic tests.
type Service struct {
	store Store
	nowFn func() time.Time
}

// NewService wires a Service. store is required; passing nil panics at the
// first call site.
func NewService(store Store) *Service {
	return &Service{store: store, nowFn: time.Now}
}

// WithNow overrides the clock the Service reads for the current-period window
// (GetBudgetStatus / GetBudgetAlerts default period). For deterministic tests
// at month boundaries; production never calls it. Returns the receiver for
// chaining at construction.
func (s *Service) WithNow(now func() time.Time) *Service {
	if now != nil {
		s.nowFn = now
	}
	return s
}

// SetBudget upserts a scope's spending cap + alert thresholds (design §10).
// It is a platform CONTROL-PLANE call (internal secret). v1 wires scope='app'
// only: org/account scopes are rejected with INVALID_INPUT (the enum carries
// them for forward-compat). Validates the limit + each percent (1..100), then
// dedupes + sorts the thresholds before persisting so a re-set is stable.
func (s *Service) SetBudget(ctx context.Context, req SetBudgetRequest) (*SetBudgetResponse, error) {
	if req.Scope != ScopeApp {
		if req.Scope == ScopeOrg || req.Scope == ScopeAccount {
			return nil, billing.InvalidInput("budget scope not yet supported (v1 wires 'app' only): " + string(req.Scope))
		}
		return nil, billing.InvalidInput("invalid budget scope: " + string(req.Scope))
	}
	if req.ScopeID == uuid.Nil {
		return nil, billing.InvalidInput("scope_id required")
	}
	if req.LimitMicros < 0 {
		return nil, billing.InvalidInput("limit_micros must be non-negative")
	}

	percents := req.AlertPercents
	if len(percents) == 0 {
		percents = defaultAlertPercents
	}
	clean, err := normalizePercents(percents)
	if err != nil {
		return nil, err
	}

	saved, err := s.store.UpsertBudget(ctx, Budget{
		Scope:         req.Scope,
		ScopeID:       req.ScopeID,
		AccountID:     req.AccountID,
		LimitMicros:   req.LimitMicros,
		AlertPercents: clean,
		Active:        req.Active,
	})
	if err != nil {
		return nil, billing.Internal("upsert budget failed", err)
	}
	return &SetBudgetResponse{
		LimitMicros:   saved.LimitMicros,
		AlertPercents: saved.AlertPercents,
		Active:        saved.Active,
	}, nil
}

// GetBudgetStatus returns the live spend-vs-cap status for a scope: the cap,
// the current-period spend, the floored percent-used, and which thresholds
// the spend has crossed. No budget configured → Exists=false with a nil
// error (the caller renders "no budget", not an error). v1 = app scope only.
func (s *Service) GetBudgetStatus(ctx context.Context, req GetBudgetStatusRequest) (*GetBudgetStatusResponse, error) {
	if req.Scope != ScopeApp {
		return nil, billing.InvalidInput("budget scope not yet supported (v1 wires 'app' only): " + string(req.Scope))
	}
	if req.ScopeID == uuid.Nil {
		return nil, billing.InvalidInput("scope_id required")
	}

	b, found, err := s.store.GetBudget(ctx, req.Scope, req.ScopeID)
	if err != nil {
		return nil, billing.Internal("get budget failed", err)
	}
	if !found {
		return &GetBudgetStatusResponse{Exists: false, Crossed: []int{}}, nil
	}

	anchorDay, err := s.store.AppAnchorDay(ctx, b.ScopeID)
	if err != nil {
		return nil, billing.Internal("anchor day lookup failed", err)
	}
	start, end := billingperiod.AnchoredPeriodWindow(s.nowFn().UTC(), anchorDay)
	spend, err := s.store.AppPeriodSpendMicros(ctx, b.ScopeID, start, end)
	if err != nil {
		return nil, billing.Internal("budget spend query failed", err)
	}

	return &GetBudgetStatusResponse{
		Exists:      true,
		PeriodStart: start,
		PeriodEnd:   end,
		LimitMicros: b.LimitMicros,
		SpendMicros: spend,
		PercentUsed: percentUsed(spend, b.LimitMicros),
		Crossed:     crossedThresholds(spend, b.LimitMicros, b.AlertPercents),
		Active:      b.Active,
	}, nil
}

// GetBudgetAlerts returns the recorded threshold crossings for a scope's
// budget in a period. PeriodStart zero defaults to the current period. No
// budget configured → an empty Alerts slice with a nil error. v1 = app scope.
func (s *Service) GetBudgetAlerts(ctx context.Context, req GetBudgetAlertsRequest) (*GetBudgetAlertsResponse, error) {
	if req.Scope != ScopeApp {
		return nil, billing.InvalidInput("budget scope not yet supported (v1 wires 'app' only): " + string(req.Scope))
	}
	if req.ScopeID == uuid.Nil {
		return nil, billing.InvalidInput("scope_id required")
	}

	b, found, err := s.store.GetBudget(ctx, req.Scope, req.ScopeID)
	if err != nil {
		return nil, billing.Internal("get budget failed", err)
	}
	if !found {
		return &GetBudgetAlertsResponse{Alerts: []BudgetAlert{}}, nil
	}

	periodStart := req.PeriodStart
	if periodStart.IsZero() {
		// ListBudgetAlerts keys on period_start only (the idempotency anchor),
		// so the window END is intentionally discarded here. Default to the app's
		// current ANCHORED period start (card-binding day) — the same value the
		// ingest-path evaluation records crossings under, so a default-period read
		// matches the stored alerts.
		anchorDay, err := s.store.AppAnchorDay(ctx, req.ScopeID)
		if err != nil {
			return nil, billing.Internal("anchor day lookup failed", err)
		}
		periodStart, _ = billingperiod.AnchoredPeriodWindow(s.nowFn().UTC(), anchorDay)
	} else {
		periodStart = periodStart.UTC()
	}

	alerts, err := s.store.ListBudgetAlerts(ctx, b.ID, periodStart)
	if err != nil {
		return nil, billing.Internal("list budget alerts failed", err)
	}
	return &GetBudgetAlertsResponse{Alerts: alerts}, nil
}

// EvaluateAppBudget is the ingest-path hook (design §5 / §10). After a usage
// event is inserted, it recomputes the app's current-period spend and records
// any newly-crossed threshold in budget_alerts (idempotent per period+percent
// via ON CONFLICT). It returns the percents it recorded THIS call (a fresh
// crossing); an already-recorded crossing is silently skipped.
//
// No budget configured (or inactive) for the app → a no-op with a nil error,
// so the caller can invoke it unconditionally. The caller runs it BEST-EFFORT
// off the usage write: an error here must NOT fail the ingest.
//
// The crossings are recorded as a single all-or-nothing batch (the store wraps
// the inserts in one transaction): if any insert fails the whole batch rolls
// back, so a partial set of alerts is never persisted. The caller logs the
// error best-effort and the NEXT usage event re-evaluates and records the
// batch atomically — the spend snapshot then reflects the retry-time spend,
// which is the closest faithful crossing-time value still observable.
//
// periodStart/periodEnd are passed in so evaluation uses the EXACT window the
// caller already derived for the event (the current calendar month — the same
// window GetUsageSummary shows).
func (s *Service) EvaluateAppBudget(ctx context.Context, appID uuid.UUID, periodStart, periodEnd time.Time) ([]int, error) {
	b, found, err := s.store.GetBudget(ctx, ScopeApp, appID)
	if err != nil {
		return nil, err
	}
	if !found || !b.Active {
		return nil, nil
	}

	spend, err := s.store.AppPeriodSpendMicros(ctx, appID, periodStart, periodEnd)
	if err != nil {
		return nil, err
	}

	crossed := crossedThresholds(spend, b.LimitMicros, b.AlertPercents)
	if len(crossed) == 0 {
		return nil, nil
	}

	records := make([]AlertRecord, len(crossed))
	for i, pct := range crossed {
		records[i] = AlertRecord{
			BudgetID:    b.ID,
			PeriodStart: periodStart,
			Percent:     pct,
			SpendMicros: spend,
			LimitMicros: b.LimitMicros,
		}
	}
	// All-or-nothing: a partial batch would snapshot an inconsistent spend
	// across thresholds, so the store commits the whole set in one transaction.
	fired, err := s.store.InsertBudgetAlerts(ctx, records)
	if err != nil {
		return nil, err
	}
	return fired, nil
}

// crossedThresholds returns the subset of percents the spend has reached
// against the limit, ascending. A threshold p is crossed when
// spend ≥ limit × p / 100. Integer micro math only — no float for money.
//
// A zero limit means every positive percent is at-or-over, so any spend
// crosses every threshold (a 0-cap budget is fully consumed).
//
// Overflow-safe: the limit is DB-capped at 1e15 micros (migration 014), so
// limit×p (p ≤ 100) ≤ 1e17 can never overflow int64. spend, however, is an
// unbounded SUM of usage and could exceed ~9.2e16, so we must NOT multiply it
// by 100. We compare spend against the precomputed per-threshold target
// (limit×p/100) instead — the target is in the safe range and spend stands
// alone, so neither side can wrap.
func crossedThresholds(spendMicros, limitMicros int64, percents []int) []int {
	out := make([]int, 0, len(percents))
	for _, p := range percents {
		// limitMicros*int64(p) is safe (≤ 1e17 by the DB CHECK); the /100 floors
		// the target so spend ≥ ⌊limit×p/100⌋ matches the spend×100 ≥ limit×p
		// integer relation without ever multiplying the unbounded spend.
		target := limitMicros * int64(p) / 100
		if spendMicros >= target {
			out = append(out, p)
		}
	}
	sort.Ints(out)
	return out
}

// maxPercentUsed caps the DISPLAY percentage. Spend can exceed the cap
// (alert-only budgets never stop accrual), so percent-used is unbounded in
// principle; clamping keeps the value sane for a progress bar and avoids any
// int64 overflow in the quotient×100 scaling for pathologically small caps.
const maxPercentUsed = 1_000_000 // 1,000,000% — far past any real over-spend

// percentUsed is spend/limit ×100 floored to a whole percent, for DISPLAY
// only (the crossing decision never uses it). A zero limit with positive spend
// reports 100 (fully consumed) so the display matches the crossed-set, which
// treats a 0-cap as fully crossed; a zero limit with zero spend reports 0.
//
// Overflow-safe: spend is unbounded, so we divide first (spend/limit) and
// scale the quotient + remainder by 100 rather than computing spend×100. The
// quotient×100 + remainder×100/limit reconstructs the floored percentage
// exactly, then clamps to maxPercentUsed so the quotient scaling can't wrap.
func percentUsed(spendMicros, limitMicros int64) int {
	if limitMicros <= 0 {
		if spendMicros > 0 {
			return 100
		}
		return 0
	}
	whole := spendMicros / limitMicros
	if whole >= maxPercentUsed/100 {
		return maxPercentUsed
	}
	rem := spendMicros % limitMicros
	return int(whole*100 + rem*100/limitMicros)
}

// normalizePercents validates each percent is 1..100 and returns them
// deduped + ascending. An out-of-range percent is rejected with
// INVALID_INPUT so a malformed budget can't store a meaningless threshold.
func normalizePercents(percents []int) ([]int, error) {
	seen := make(map[int]struct{}, len(percents))
	out := make([]int, 0, len(percents))
	for _, p := range percents {
		if p < 1 || p > 100 {
			return nil, billing.InvalidInput("alert percent must be between 1 and 100")
		}
		if _, dup := seen[p]; dup {
			continue
		}
		seen[p] = struct{}{}
		out = append(out, p)
	}
	sort.Ints(out)
	return out, nil
}
