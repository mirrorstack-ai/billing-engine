package budget

import (
	"context"
	"log/slog"
	"math"
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

// resolveScope validates a request's (scope, category, template) triple and
// normalizes it. CategoryAll stays app-only (the pre-084 budget); CategoryAI
// is allowed on app, org and account; a template key is only meaningful on
// an app's AI cap and must have the module's key shape.
func resolveScope(scope Scope, category Category, templateKey string) (Category, error) {
	cat, ok := normalizeCategory(category)
	if !ok || cat == CategoryExposure {
		return "", billing.InvalidInput("invalid budget category: " + string(category))
	}
	switch scope {
	case ScopeApp:
	case ScopeOrg, ScopeAccount:
		if cat != CategoryAI {
			return "", billing.InvalidInput("budget scope " + string(scope) + " supports category 'ai' only")
		}
	default:
		return "", billing.InvalidInput("invalid budget scope: " + string(scope))
	}
	if templateKey != "" {
		if cat != CategoryAI || scope != ScopeApp {
			return "", billing.InvalidInput("template_key applies to an app's 'ai' budget only")
		}
		if !templateKeyShape.MatchString(templateKey) {
			return "", billing.InvalidInput("template_key must match ^[a-z][a-z0-9_-]*$ (max 64)")
		}
	}
	return cat, nil
}

// SetBudget upserts a scope's spending cap + alert thresholds (design §10).
// It is a platform CONTROL-PLANE call (internal secret). Validates the limit
// + each percent (1..100), then dedupes + sorts the thresholds before
// persisting so a re-set is stable. CategoryAI rows may carry a template
// key (an ai-assistant template's own cap), a hard cap and the customer's
// overage opt-out (migration 084).
func (s *Service) SetBudget(ctx context.Context, req SetBudgetRequest) (*SetBudgetResponse, error) {
	cat, err := resolveScope(req.Scope, req.Category, req.TemplateKey)
	if err != nil {
		return nil, err
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
		Category:      cat,
		TemplateKey:   req.TemplateKey,
		AccountID:     req.AccountID,
		LimitMicros:   req.LimitMicros,
		AlertPercents: clean,
		Active:        req.Active,
		HardCap:       req.HardCap,
		AllowOverage:  req.AllowOverage,
	})
	if err != nil {
		return nil, billing.Internal("upsert budget failed", err)
	}
	return &SetBudgetResponse{
		Category:      saved.Category,
		TemplateKey:   saved.TemplateKey,
		LimitMicros:   saved.LimitMicros,
		AlertPercents: saved.AlertPercents,
		Active:        saved.Active,
		HardCap:       saved.HardCap,
		AllowOverage:  saved.AllowOverage,
	}, nil
}

// candidate is one budget row a status read consults, with its spend.
type candidate struct {
	b         Budget
	decidedBy DecidedBy
	spend     int64
}

// candidates lists the rows a status read COMPOSES, most specific first. For
// an app's AI cap with a template: the template's own row AND the app's
// AI-wide row — both, never one shadowing the other. The template is named
// by the widget in a client-supplied parameter that the agent cannot verify
// against the module today, so a template row that HID the app-wide row would
// let a caller escape both caps by naming the roomier template; composing
// them keeps a more specific budget able to say LESS than the general one,
// never more (94, review of billing-engine#221). Every other scope has one
// row. Spend is read per row: the template's events for its row, the whole
// app for the AI-wide row.
func (s *Service) candidates(ctx context.Context, scope Scope, scopeID uuid.UUID, cat Category, templateKey string, start, end time.Time) ([]candidate, error) {
	var out []candidate
	add := func(tpl string, decidedBy DecidedBy) error {
		b, found, err := s.store.GetBudget(ctx, scope, scopeID, cat, tpl)
		if err != nil || !found {
			return err
		}
		spend, err := s.spendFor(ctx, b, start, end)
		if err != nil {
			return err
		}
		out = append(out, candidate{b: b, decidedBy: decidedBy, spend: spend})
		return nil
	}
	if templateKey != "" {
		if err := add(templateKey, DecidedByTemplate); err != nil {
			return nil, err
		}
	}
	wide := DecidedByApp
	switch scope {
	case ScopeOrg:
		wide = DecidedByOrg
	case ScopeAccount:
		wide = DecidedByAccount
	}
	if err := add("", wide); err != nil {
		return nil, err
	}
	return out, nil
}

// decide composes the candidates into ONE verdict: exhausted if ANY row is
// exhausted; the row reported is the first exhausted one (the template wins
// the label when both are spent), else the most specific row. This is the
// single authority for the verdict — api-platform passes it through.
func decide(cands []candidate) (candidate, bool) {
	if len(cands) == 0 {
		return candidate{}, false
	}
	for _, c := range cands {
		if exhausted(c.b, c.spend) {
			return c, true
		}
	}
	return cands[0], false
}

// spendAccount is the billing account whose events an org/account-scoped
// budget sums: the account itself, or the org's own account. found=false is
// a lazy org with no account yet — its spend is 0.
func (s *Service) spendAccount(ctx context.Context, scope Scope, scopeID uuid.UUID) (uuid.UUID, bool, error) {
	if scope == ScopeAccount {
		return scopeID, true, nil
	}
	return s.store.OrgAccountID(ctx, scopeID)
}

// windowFor derives the anchored period window a budget is evaluated in:
// an app follows its payer's anchor, an account its own, an org its own
// account's (calendar month while it has none).
func (s *Service) windowFor(ctx context.Context, scope Scope, scopeID uuid.UUID) (time.Time, time.Time, error) {
	var (
		anchorDay int
		err       error
	)
	switch scope {
	case ScopeApp:
		anchorDay, err = s.store.AppAnchorDay(ctx, scopeID)
	default:
		acct, found, aerr := s.spendAccount(ctx, scope, scopeID)
		if aerr != nil {
			return time.Time{}, time.Time{}, aerr
		}
		if !found {
			anchorDay = billingperiod.DefaultAnchorDay
		} else {
			anchorDay, err = s.store.AccountAnchorDay(ctx, acct)
		}
	}
	if err != nil {
		return time.Time{}, time.Time{}, err
	}
	start, end := billingperiod.AnchoredPeriodWindow(s.nowFn().UTC(), anchorDay)
	return start, end, nil
}

// spendFor sums the spend a budget row governs in [start, end).
func (s *Service) spendFor(ctx context.Context, b Budget, start, end time.Time) (int64, error) {
	if b.Scope == ScopeApp {
		return s.store.AppPeriodSpendMicros(ctx, b.ScopeID, b.Category, b.TemplateKey, start, end)
	}
	acct, found, err := s.spendAccount(ctx, b.Scope, b.ScopeID)
	if err != nil {
		return 0, err
	}
	if !found {
		return 0, nil
	}
	return s.store.AccountPeriodAISpendMicros(ctx, acct, start, end)
}

// paasBillingMode is accounts.billing_mode's value for pay-after-use (the
// web's 隨用隨付 card maps to it); 'credits' is the prepaid mode.
const paasBillingMode = "standard"

// EffectivePaid is k after the delinquency memory (rule b): each late
// invoice costs LatePenaltyK paid invoices of trust, floored at zero. A
// LatePenaltyK of 0 keeps k as is; a huge one is "any late payment resets k".
func EffectivePaid(cfg RiskRampConfig, sig ExposureSignals) int {
	k := sig.PaidInvoices
	if k < 0 {
		k = 0
	}
	late := sig.LateCount
	if late < 0 {
		late = 0
	}
	penalty := cfg.LatePenaltyK
	if penalty < 0 {
		penalty = 0
	}
	if penalty > 0 && late > 0 {
		if late > k/penalty { // k − penalty×late < 0 without overflow
			return 0
		}
		k -= penalty * late
	}
	return k
}

// growth is the configured shape's fraction of the range earned at k paid
// invoices, in [0, 1]. Sigmoid (owner's final shape): a logistic
// normalised so it is 0 at k=0 and EXACTLY 1 at k >= KMax — the curve
// reaches base + range at KMax, never asymptotically. Exp: 1 − e^(−k/Tau).
// A row with unusable parameters (the DB CHECKs guard the real one) earns
// nothing: the base alone.
func growth(cfg RiskRampConfig, k int) float64 {
	if k < 0 {
		k = 0
	}
	switch cfg.Shape {
	case ShapeSigmoid:
		if cfg.KMax <= 0 || cfg.A <= 0 {
			return 0
		}
		if k >= cfg.KMax {
			return 1
		}
		g := func(x float64) float64 { return 1 / (1 + math.Exp(-cfg.A*(x-cfg.K0))) }
		den := g(float64(cfg.KMax)) - g(0)
		if den <= 0 {
			return 0
		}
		return (g(float64(k)) - g(0)) / den
	case ShapeExp:
		if cfg.Tau <= 0 {
			return 0
		}
		return 1 - math.Exp(-float64(k)/cfg.Tau)
	}
	return 0
}

// ExposureLimitMicros is the owner's curve (2026-09-14, migration 085): no
// usable card → NoCardMicros; a usable card with k paid invoices →
// CardBaseMicros + RangeMicros × growth(k) — the configured shape (sigmoid
// seeded: $10 + $990 × a normalised logistic, exactly $1,000 at k = 24) —
// and never above CeilingMicros, the hard clamp. Delinquency (owner's pick): while an
// invoice is open with a balance the limit is the curve divided by
// DelinquentDivisor, never below NoCardMicros — the risk control without a
// hard cliff (a huge divisor reproduces the cliff, 1 disables the
// reduction); once settled, k is EffectivePaid — the memory, applied before
// the divisor. Whole micros, rounded half up; the ceiling also bounds a
// zero-card floor so a misconfigured row cannot exceed it. A non-positive
// tau (a row the DB CHECK did not see) means "no growth": the base alone.
func ExposureLimitMicros(cfg RiskRampConfig, sig ExposureSignals) int64 {
	var limit int64
	if sig.HasUsableCard {
		k := EffectivePaid(cfg, sig)
		limit = int64(math.Round(float64(cfg.CardBaseMicros) + float64(cfg.RangeMicros)*growth(cfg, k)))
	} else {
		limit = cfg.NoCardMicros
	}
	if limit > cfg.CeilingMicros {
		limit = cfg.CeilingMicros
	}
	if sig.DelinquentNow && cfg.DelinquentDivisor > 1 {
		reduced := limit / int64(cfg.DelinquentDivisor)
		if reduced < cfg.NoCardMicros {
			reduced = cfg.NoCardMicros
		}
		if reduced < limit {
			limit = reduced
		}
	}
	if limit < 0 {
		limit = 0
	}
	return limit
}

// poolFor computes the PaaS risk-exposure pool for an account in [start,
// end). A non-PaaS account (credits mode) or an unattributed scope has
// Source none and never exhausts. On a PaaS account it also refreshes the
// account's SYSTEM 'exposure' budget row (limit = the curve, hard cap) so the
// ingest-path evaluation can record 80% / 100% crossings against it.
func (s *Service) poolFor(ctx context.Context, cfg RiskRampConfig, accountID uuid.UUID, found bool, start, end time.Time) (*Pool, error) {
	if !found {
		return &Pool{Mode: "none", Source: PoolSourceNone}, nil
	}
	sig, err := s.store.ExposureSignals(ctx, accountID)
	if err != nil {
		return nil, err
	}
	if sig.BillingMode != paasBillingMode {
		return &Pool{Mode: sig.BillingMode, Source: PoolSourceNone, HasUsableCard: sig.HasUsableCard, PaidInvoices: sig.PaidInvoices}, nil
	}
	limit := ExposureLimitMicros(cfg, sig)
	accrued, err := s.store.AccountPeriodSpendMicros(ctx, accountID, start, end)
	if err != nil {
		return nil, err
	}
	remaining := limit - accrued
	if remaining < 0 {
		remaining = 0
	}
	// The system row: what budget_alerts hangs the pool's crossings on.
	// Upserted here (not only at ingest) so the limit follows the curve as
	// the account earns trust; alert_percents are the defaults.
	if _, err := s.store.UpsertBudget(ctx, Budget{
		Scope: ScopeAccount, ScopeID: accountID, Category: CategoryExposure, AccountID: accountID,
		LimitMicros: limit, AlertPercents: defaultAlertPercents, Active: true, HardCap: true,
	}); err != nil {
		return nil, err
	}
	return &Pool{
		Mode:            "paas",
		Source:          PoolSourceExposureLimit,
		LimitMicros:     limit,
		AccruedMicros:   accrued,
		RemainingMicros: remaining,
		Exhausted:       accrued >= limit,
		HasUsableCard:   sig.HasUsableCard,
		PaidInvoices:    sig.PaidInvoices,
		DelinquentNow:   sig.DelinquentNow,
		LateCount:       sig.LateCount,
		EffectivePaid:   EffectivePaid(cfg, sig),
	}, nil
}

// payerAccount is the account whose exposure pool a scope draws on: an app's
// attributed payer, an org's own account, the account itself.
func (s *Service) payerAccount(ctx context.Context, scope Scope, scopeID uuid.UUID) (uuid.UUID, bool, error) {
	if scope == ScopeApp {
		return s.store.AppPayerAccountID(ctx, scopeID)
	}
	return s.spendAccount(ctx, scope, scopeID)
}

// exhausted is the consumer's verdict for a hard cap: active, hard, not
// overage-allowed, and spend at or over the limit. Integer micro math.
func exhausted(b Budget, spendMicros int64) bool {
	return b.Active && b.HardCap && !b.AllowOverage && spendMicros >= b.LimitMicros
}

// GetBudgetStatus returns the live spend-vs-cap status for a scope: the cap,
// the current-period spend, the floored percent-used, which thresholds the
// spend has crossed, and — for a hard cap — whether the consumer must refuse
// further work. No budget configured → Exists=false with a nil error (the
// caller renders "no budget", not an error). An app's AI request with a
// template resolves template row → AI-wide row (DecidedBy says which).
func (s *Service) GetBudgetStatus(ctx context.Context, req GetBudgetStatusRequest) (*GetBudgetStatusResponse, error) {
	cat, err := resolveScope(req.Scope, req.Category, req.TemplateKey)
	if err != nil {
		return nil, err
	}
	if req.ScopeID == uuid.Nil {
		return nil, billing.InvalidInput("scope_id required")
	}

	start, end, err := s.windowFor(ctx, req.Scope, req.ScopeID)
	if err != nil {
		return nil, billing.Internal("anchor day lookup failed", err)
	}
	cands, err := s.candidates(ctx, req.Scope, req.ScopeID, cat, req.TemplateKey, start, end)
	if err != nil {
		return nil, billing.Internal("budget lookup failed", err)
	}
	c, isExhausted := decide(cands)

	// The PaaS exposure pool rides every category='ai' read (PR-B): OR'd into
	// the verdict, named by decided_by only when no customer cap bit. The
	// curve row is read ONCE per verdict and carries the incident
	// kill-switch: while paused, every AI verdict is allowed and says so —
	// the figures (caps, pool) are still reported so the console can show
	// what WOULD have refused.
	var pool *Pool
	decidedBy := c.decidedBy
	if cat == CategoryAI {
		cfg, err := s.store.RiskRampConfig(ctx)
		if err != nil {
			return nil, billing.Internal("risk ramp config read failed", err)
		}
		acct, found, err := s.payerAccount(ctx, req.Scope, req.ScopeID)
		if err != nil {
			return nil, billing.Internal("payer account lookup failed", err)
		}
		pool, err = s.poolFor(ctx, cfg, acct, found, start, end)
		if err != nil {
			return nil, billing.Internal("exposure pool evaluation failed", err)
		}
		if pool.Exhausted && !isExhausted {
			isExhausted = true
			decidedBy = DecidedByExposureLimit
		}
		if cfg.EnforcementPaused && isExhausted {
			isExhausted = false
			decidedBy = DecidedByPaused
		}
	}
	if len(cands) == 0 {
		return &GetBudgetStatusResponse{
			Exists:    false,
			Category:  cat,
			DecidedBy: map[bool]DecidedBy{true: DecidedByExposureLimit, false: DecidedByNone}[isExhausted],
			Exhausted: isExhausted,
			Crossed:   []int{},
			Pool:      pool,
		}, nil
	}
	b, spend := c.b, c.spend

	remaining := b.LimitMicros - spend
	if remaining < 0 {
		remaining = 0
	}
	return &GetBudgetStatusResponse{
		Exists:          true,
		Category:        b.Category,
		DecidedBy:       decidedBy,
		TemplateKey:     b.TemplateKey,
		PeriodStart:     start,
		PeriodEnd:       end,
		LimitMicros:     b.LimitMicros,
		SpendMicros:     spend,
		PercentUsed:     percentUsed(spend, b.LimitMicros),
		Crossed:         crossedThresholds(spend, b.LimitMicros, b.AlertPercents),
		Active:          b.Active,
		HardCap:         b.HardCap,
		AllowOverage:    b.AllowOverage,
		Exhausted:       isExhausted,
		RemainingMicros: remaining,
		Pool:            pool,
	}, nil
}

// GetBudgetAlerts returns the recorded threshold crossings for a scope's
// budget in a period. PeriodStart zero defaults to the current period. No
// budget configured → an empty Alerts slice with a nil error. Reads exactly
// the requested row (no template precedence: alerts belong to the row that
// recorded them).
func (s *Service) GetBudgetAlerts(ctx context.Context, req GetBudgetAlertsRequest) (*GetBudgetAlertsResponse, error) {
	cat, err := resolveScope(req.Scope, req.Category, req.TemplateKey)
	if err != nil {
		return nil, err
	}
	if req.ScopeID == uuid.Nil {
		return nil, billing.InvalidInput("scope_id required")
	}

	b, found, err := s.store.GetBudget(ctx, req.Scope, req.ScopeID, cat, req.TemplateKey)
	if err != nil {
		return nil, billing.Internal("get budget failed", err)
	}
	if !found {
		return &GetBudgetAlertsResponse{Alerts: []BudgetAlert{}}, nil
	}

	periodStart := req.PeriodStart
	if periodStart.IsZero() {
		// ListBudgetAlerts keys on period_start only (the idempotency anchor),
		// so the window END is intentionally discarded here. Default to the
		// scope's current ANCHORED period start — the same value the
		// ingest-path evaluation records crossings under.
		periodStart, _, err = s.windowFor(ctx, req.Scope, req.ScopeID)
		if err != nil {
			return nil, billing.Internal("anchor day lookup failed", err)
		}
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
// event is inserted, it recomputes the app's current-period spend for every
// budget row the event can move — the app's 'all' row, its AI-wide row, and
// (for an infra.ai.* event stamped with a template) that template's row —
// and records any newly-crossed threshold in budget_alerts (idempotent per
// period+percent via ON CONFLICT). It returns the percents it recorded THIS
// call across all rows; an already-recorded crossing is silently skipped.
//
// No budget configured (or inactive) for a row → that row is skipped, so the
// caller can invoke it unconditionally. The caller runs it BEST-EFFORT off
// the usage write: an error here must NOT fail the ingest.
//
// Each row's crossings are recorded as a single all-or-nothing batch (the
// store wraps the inserts in one transaction).
//
// periodStart/periodEnd are passed in so evaluation uses the EXACT window the
// caller already derived for the event.
func (s *Service) EvaluateAppBudget(ctx context.Context, appID uuid.UUID, templateKey string, periodStart, periodEnd time.Time) ([]int, error) {
	rows := []struct {
		cat Category
		tpl string
	}{{CategoryAll, ""}, {CategoryAI, ""}}
	if templateKey != "" {
		rows = append(rows, struct {
			cat Category
			tpl string
		}{CategoryAI, templateKey})
	}
	var fired []int
	for _, r := range rows {
		b, found, err := s.store.GetBudget(ctx, ScopeApp, appID, r.cat, r.tpl)
		if err != nil {
			return fired, err
		}
		if !found || !b.Active {
			continue
		}
		spend, err := s.store.AppPeriodSpendMicros(ctx, appID, r.cat, r.tpl, periodStart, periodEnd)
		if err != nil {
			return fired, err
		}
		got, err := s.recordCrossings(ctx, b, spend, periodStart)
		if err != nil {
			return fired, err
		}
		fired = append(fired, got...)
	}
	return fired, nil
}

// EvaluateAccountBudget is EvaluateAppBudget's twin for the account-scoped
// AI caps (scenario 2): after an infra.ai.* event landed on an account, the
// account's own 'ai' row and, if the account is an org's, the org's 'ai' row
// are re-evaluated. Same best-effort contract.
func (s *Service) EvaluateAccountBudget(ctx context.Context, accountID, ownerOrgID uuid.UUID, periodStart, periodEnd time.Time) ([]int, error) {
	var fired []int
	targets := []struct {
		scope Scope
		id    uuid.UUID
	}{{ScopeAccount, accountID}}
	if ownerOrgID != uuid.Nil {
		targets = append(targets, struct {
			scope Scope
			id    uuid.UUID
		}{ScopeOrg, ownerOrgID})
	}
	// The pool's crossings (085): refresh the system row against the curve and
	// record 80% / 100% of the account's whole-period usage — the pre-cliff
	// warning the console shows before the assistant stops.
	cfg, err := s.store.RiskRampConfig(ctx)
	if err != nil {
		return nil, err
	}
	pool, err := s.poolFor(ctx, cfg, accountID, true, periodStart, periodEnd)
	if err != nil {
		return nil, err
	}
	if pool.Source == PoolSourceExposureLimit {
		row, found, err := s.store.GetBudget(ctx, ScopeAccount, accountID, CategoryExposure, "")
		if err != nil {
			return nil, err
		}
		if found {
			got, err := s.recordCrossings(ctx, row, pool.AccruedMicros, periodStart)
			if err != nil {
				return nil, err
			}
			fired = append(fired, got...)
		}
	}
	var (
		spend    int64
		spendSet bool
	)
	for _, t := range targets {
		b, found, err := s.store.GetBudget(ctx, t.scope, t.id, CategoryAI, "")
		if err != nil {
			return fired, err
		}
		if !found || !b.Active {
			continue
		}
		if !spendSet {
			spend, err = s.store.AccountPeriodAISpendMicros(ctx, accountID, periodStart, periodEnd)
			if err != nil {
				return fired, err
			}
			spendSet = true
		}
		got, err := s.recordCrossings(ctx, b, spend, periodStart)
		if err != nil {
			return fired, err
		}
		fired = append(fired, got...)
	}
	return fired, nil
}

// SetAIEnforcementPaused flips the incident kill-switch (migration 085): a
// platform CONTROL-PLANE call (internal secret). Paused=true makes every AI
// verdict allowed (decided_by "paused") on the next read — no deploy, no
// cache on this side. Logged with the caller's reason.
func (s *Service) SetAIEnforcementPaused(ctx context.Context, req SetAIEnforcementPausedRequest) (*AIEnforcementResponse, error) {
	if req.Paused && (req.Reason == "" || req.ActorID == "") {
		return nil, billing.InvalidInput("pausing AI enforcement requires reason and actor_id")
	}
	paused, err := s.store.SetAIEnforcementPaused(ctx, req.Paused, req.Reason, req.ActorID)
	if err != nil {
		return nil, billing.Internal("set ai enforcement paused failed", err)
	}
	slog.WarnContext(ctx, "AI budget enforcement kill-switch changed", "paused", paused, "reason", req.Reason, "actor_id", req.ActorID)
	return s.GetAIEnforcement(ctx)
}

// GetAIEnforcement reads the kill-switch and the curve in one call.
func (s *Service) GetAIEnforcement(ctx context.Context) (*AIEnforcementResponse, error) {
	cfg, err := s.store.RiskRampConfig(ctx)
	if err != nil {
		return nil, billing.Internal("risk ramp config read failed", err)
	}
	return &AIEnforcementResponse{
		Paused:            cfg.EnforcementPaused,
		PausedReason:      cfg.PausedReason,
		PausedBy:          cfg.PausedBy,
		PausedAt:          cfg.PausedAt,
		NoCardMicros:      cfg.NoCardMicros,
		CardBaseMicros:    cfg.CardBaseMicros,
		CeilingMicros:     cfg.CeilingMicros,
		RangeMicros:       cfg.RangeMicros,
		Shape:             cfg.Shape,
		A:                 cfg.A,
		K0:                cfg.K0,
		KMax:              cfg.KMax,
		Tau:               cfg.Tau,
		DelinquentDivisor: cfg.DelinquentDivisor,
		LatePenaltyK:      cfg.LatePenaltyK,
	}, nil
}

// recordCrossings inserts the thresholds spend has reached for one budget
// row, all-or-nothing, and returns the freshly recorded percents.
func (s *Service) recordCrossings(ctx context.Context, b Budget, spend int64, periodStart time.Time) ([]int, error) {
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
	return s.store.InsertBudgetAlerts(ctx, records)
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
