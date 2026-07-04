package usage

import (
	"context"
	"math/big"
	"time"

	"github.com/google/uuid"

	"github.com/mirrorstack-ai/billing-engine/internal/account/billing"
	"github.com/mirrorstack-ai/billing-engine/internal/billingperiod"
)

// ============================================================================
// APP-OWNER BILL PRICING CONSTANTS
//
// The app-owner (customer) bill for ONE app for ONE period is:
//
//	最終費用 TotalMicros = BaseFee + ModuleUsageTotal + InfraTotal − PaasCredit
//
// ALL of the amounts below are TUNABLE post-build — the user adjusts them after
// this ships. The billed STRUCTURE + the mechanism (tiering, infra split, credit
// offset) is the deliverable, NOT these specific numbers. Keep them in ONE place.
//
// They are also PLAN-AWARE: a Pro org plan may change the base fee (see
// resolveBaseFeeMicros). The PaaS credit is gated separately on an ACTIVE SaaS
// subscription (see paasCreditMicros) — not on the plan, and never default-on.
// There is deliberately NO full plan/subscription system here (design: do not
// build one) — just the const seam + a TODO. All money is integer micro-dollars
// (1e-6 USD); NEVER float for money.
// ============================================================================

const (
	// BaseFeeMicros is 基本費用 — the fixed per-app/period platform base fee on the
	// DEFAULT plan. It BUNDLES the PaaS infra credit (surfaced as PaasCreditMicros)
	// and INCLUDES up to IncludedModules installed modules; each installed module
	// beyond that adds ModuleOverageFeeMicros. Tunable. Default $20.
	BaseFeeMicros int64 = 20_000_000 // $20.00

	// ProBaseFeeMicros is the base fee on the Pro org plan. TODO(plan): wire a real
	// plan resolver (ms_account.orgs / a subscription row) into resolveBaseFeeMicros;
	// v1 has no plan system so this const is the seam, not yet reached. Placeholder
	// value — tune with the real Pro plan.
	ProBaseFeeMicros int64 = 50_000_000 // $50.00 (placeholder)

	// IncludedModules is how many installed modules the base fee bundles before the
	// per-module surcharge kicks in. Owner spec 2026-07-05 (base-fee v1, DESIGN.md
	// D1): 5 included. Tunable ("may change"); becomes plan-resolved later.
	IncludedModules = 5

	// ModuleOverageFeeMicros is the surcharge added to the base fee for EACH
	// installed module beyond IncludedModules. Owner spec 2026-07-05 (base-fee v1):
	// $3.00/module/period. Tunable; becomes plan-resolved later. App base for a
	// period = BaseFee + Overage × max(0, module_count − IncludedModules) — see
	// AppBaseFeeMicros, the ONE place that formula lives.
	ModuleOverageFeeMicros int64 = 3_000_000 // $3.00

	// PaasCreditPct is PaaS 額度 — the percentage of the 基礎設施 InfraTotal credited
	// back (offsetting infra) as a SaaS-subscription benefit. Tunable. Default 30%.
	// (It replaced an earlier flat −$7 credit with an infra-proportional one.)
	// Only reached once a subscription earns it; subscription-gated OFF in v1 (see
	// paasCreditMicros), so today the credit is always 0.
	PaasCreditPct = 30
)

// Plan is the account/org billing plan. v1 has NO real plan system — this is the
// plan-aware SEAM the bill's base fee + PaaS credit hang off. TODO(plan): resolve
// the real plan (ms_account.orgs / a subscription row) instead of always using
// planDefault.
type Plan string

const (
	// PlanDefault is the only plan v1 resolves. resolveBaseFeeMicros returns
	// BaseFeeMicros for it.
	PlanDefault Plan = "default"
	// PlanPro is the Pro org plan hook: resolveBaseFeeMicros returns ProBaseFeeMicros
	// for it. Not reached until a real plan resolver exists.
	PlanPro Plan = "pro"
)

// resolveBaseFeeMicros returns the plan-aware base fee (before the per-module
// surcharge). TODO(plan): a Pro plan returns ProBaseFeeMicros; with no plan
// system yet every account is on PlanDefault → BaseFeeMicros.
func resolveBaseFeeMicros(plan Plan) int64 {
	if plan == PlanPro {
		return ProBaseFeeMicros
	}
	return BaseFeeMicros
}

// paasCreditMicros returns the PaaS infra credit that offsets the InfraTotal:
// round_half_up(infraTotalMicros × PaasCreditPct / 100), a NON-NEGATIVE magnitude
// the caller SUBTRACTS. The credit is EARNED ONLY through an active SaaS
// subscription — it is NOT a plan-default freebie. v1 has NO subscription system,
// so subscriptionActive is always false and the credit resolves to 0: the platform
// never grants an unearned credit. The PaasCreditPct + pctMicros seam is kept live
// (subscription-GATED, not deleted) so wiring a real subscription resolver flips it
// on without re-deriving the math. Zero infra → zero credit regardless.
func paasCreditMicros(subscriptionActive bool, infraTotalMicros int64) (int64, error) {
	if !subscriptionActive || infraTotalMicros <= 0 {
		return 0, nil
	}
	return pctMicros(infraTotalMicros, PaasCreditPct)
}

// pctMicros = round_half_up(base × pct / 100) in exact big.Rat, rounded once at
// the boundary through the shared money rounding point (roundRatHalfUp). Mirrors
// cycle.takeMicros — money never flows through float.
func pctMicros(base int64, pct int) (int64, error) {
	r := new(big.Rat).SetFrac(
		new(big.Int).Mul(big.NewInt(base), big.NewInt(int64(pct))),
		big.NewInt(100),
	)
	return roundRatHalfUp(r)
}

// GetAppBill returns the app-owner's FULL bill for ONE app in ONE period — the
// read behind the app billing page's 最終費用 breakdown. It:
//  1. resolves the PAYER's billing account from the owner principal (lazy-account
//     safe — no account yet still yields a base-fee-only bill),
//  2. resolves the period window: the current calendar month (default, live from
//     usage_events) or a past billing_periods row when PeriodID is set (frozen
//     usage_aggregates),
//  3. reads the module-usage lines (AppBill), keeping ONLY the non-reserved ones
//     → 模組使用量 module usage (declared price, NO markup); the reserved infra.* /
//     platform.* lines are dropped here and 基礎設施 is instead sourced per-metric
//     from the CATALOG (AppInfraBill) so every declared infra metric renders,
//     including the $0 / unused ones — InfraTotalMicros = Σ InfraLines[].charged
//     (the 1.2× infra markup applied once, in SQL),
//  4. computes 基本費用 base fee SNAPSHOT-FIRST: a charged period reads the
//     frozen per-app-period snapshot the charge leg wrote at billing time
//     (ms_billing.app_base_snapshots, migration 028 — exact period_start
//     match), so the displayed base IS what the invoice charged even after
//     later SyncAppModules count changes. Only an un-snapshotted period
//     (pre-feature history, unactivated account, in-progress period) falls
//     back to the live ESTIMATE from the mirror (migration 027):
//     AppBaseFeeMicros(resolveBaseFeeMicros(plan), module_count), PRORATED via
//     ProratedBaseMicros when the app's created_at falls inside the period.
//     An app ABSENT from the mirror (pre-backfill) falls back to the usage-proxy
//     count below — today's behavior, until the api-platform backfill lands,
//  5. computes PaaS 額度 credit = PaasCreditPct% of the infra total, but ONLY when
//     an active SaaS subscription earns it — v1 has no subscription system, so the
//     credit is subscription-gated OFF and is 0 (the wire field stays for back-compat),
//  6. TotalMicros = base + module usage + infra − credit.
//
// UNINSTALL-SAFE: usage is billed + displayed from the immutable ledgers only
// (AppBill never joins an install table), so an uninstalled module's accrued
// usage still appears. The module DISPLAY NAME is the caller's to resolve from the
// module catalog (module_versions), never from module_install; this bill carries
// module_id.
//
// INSTALLED-MODULE-COUNT: the authoritative count is the ms_billing.apps mirror's
// module_count (synced by api-platform via SyncAppModules — an installed-but-idle
// module counts, an uninstalled one stops counting at the next sync). Apps not
// yet mirrored (pre-backfill) keep the pre-027 documented PROXY: the count of
// DISTINCT modules with metered usage this period; the api-platform backfill PR
// retires that path.
func (s *Service) GetAppBill(ctx context.Context, req GetAppBillRequest) (*GetAppBillResponse, error) {
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

	// TODO(plan): resolve the real account/org plan. v1 has no plan system, so the
	// base fee is the default-plan fee and the PaaS credit applies by default.
	plan := PlanDefault

	// Resolve the billed window + the echoed period id.
	periodID := ""
	var periodStart, periodEnd time.Time
	if req.PeriodID != uuid.Nil {
		// A past period is addressed by a real billing_periods row id, which is
		// account-scoped — an unknown / other-account id is NOT_FOUND. Without a
		// billing account the caller owns no periods at all.
		if !found {
			return nil, billing.NotFound("billing period not found")
		}
		start, end, ok, err := s.store.BillingPeriodWindow(ctx, accountID, req.PeriodID)
		if err != nil {
			return nil, billing.Internal("billing period lookup failed", err)
		}
		if !ok {
			return nil, billing.NotFound("billing period not found")
		}
		periodStart, periodEnd = start, end
		periodID = req.PeriodID.String()
	} else {
		// Default: the account's current anchored period (card-binding day, ADR
		// 0005), live from events. No billing account yet → the calendar-month
		// default (DefaultAnchorDay) so the base-fee-only bill still shows a window.
		anchorDay := billingperiod.DefaultAnchorDay
		if found {
			d, err := s.store.AccountAnchorDay(ctx, accountID)
			if err != nil {
				return nil, billing.Internal("anchor day lookup failed", err)
			}
			anchorDay = d
		}
		periodStart, periodEnd = billingperiod.AnchoredPeriodWindow(s.nowFn().UTC(), anchorDay)
	}

	// Read the usage lines (empty when no billing account exists yet — the bill is
	// then base-fee-only).
	var lines []AppMetricUsageRaw
	if found {
		lines, err = s.store.AppBill(ctx, accountID, req.AppID, periodStart, periodEnd)
		if err != nil {
			return nil, billing.Internal("app bill query failed", err)
		}
	}

	// Keep only the non-reserved rows → module usage (displayed lines); the
	// reserved infra.* / platform.* rows are dropped here (infra is sourced
	// per-metric from the catalog below). Count DISTINCT non-reserved modules as
	// the installed-module proxy for the base-fee tier.
	moduleUsage := make([]AppMetricUsage, 0, len(lines))
	installedModules := make(map[uuid.UUID]struct{})
	var moduleUsageTotal int64
	for _, r := range lines {
		if isReservedMetric(r.Metric) {
			// Reserved infra.* / platform.* lines are sourced AUTHORITATIVELY from
			// the catalog-anchored AppInfraBill query below (so every declared infra
			// metric renders as its own line, including the $0 / unused ones).
			// Summing them here too would DOUBLE-COUNT, so skip them on this scan.
			continue
		}
		moduleUsage = append(moduleUsage, AppMetricUsage{
			ModuleID:         r.ModuleID,
			Metric:           r.Metric,
			Kind:             r.Kind,
			Model:            r.Model,
			ModuleVersion:    r.ModuleVersion,
			BillableQuantity: r.BillableQuantity,
			UnitPriceMicros:  r.UnitPriceMicros,
			ChargedMicros:    r.ChargedMicros,
		})
		moduleUsageTotal += r.ChargedMicros
		installedModules[r.ModuleID] = struct{}{}
	}

	// 基礎設施: source infra per-metric from the CATALOG (metric_definitions), NOT
	// the usage ledger — so EVERY active declared infra metric renders as its own
	// line, including declared-but-unused ones at qty 0 · $0 ("show all"). Run
	// UNCONDITIONALLY (even with no billing account, accountID == uuid.Nil): the
	// usage side then matches nothing and every declared metric COALESCEs to $0,
	// so a lazy/no-account app still shows all 16 infra lines. Each line's
	// ChargedMicros already carries the ×1.2 infra markup (applied once in SQL);
	// InfraTotalMicros is their sum, keeping the back-compat scalar exactly
	// reconcilable (infra_total == Σ infra_lines[].charged).
	infraLines, err := s.store.AppInfraBill(ctx, accountID, req.AppID, periodStart, periodEnd)
	if err != nil {
		return nil, billing.Internal("app infra bill query failed", err)
	}

	// 基礎設施, per-MODULE split (decision 19): reserved infra attributed to a real
	// incurring module renders inside that module's card, dual-priced (SENTINEL default
	// vs per-module override). This is USAGE-anchored, so — unlike the catalog-anchored
	// residual above — it is skipped when there is no billing account (no account → no
	// attributed usage → empty split), mirroring the module-usage read. Non-nil empty
	// slice otherwise so the wire never carries null.
	moduleInfraLines := []AppModuleInfraUsage{}
	if found {
		moduleInfraLines, err = s.store.AppModuleInfraBill(ctx, accountID, req.AppID, periodStart, periodEnd)
		if err != nil {
			return nil, billing.Internal("app module infra bill query failed", err)
		}
	}

	// InfraTotalMicros stays the FULL reconciliation scalar: the per-module split is a
	// pure display re-partition of the same infra total (attributed → moduleInfraLines,
	// unattributable → infraLines), so it is Σ of BOTH so that base fee / PaaS credit /
	// TotalMicros math downstream is unchanged.
	var infraTotal int64
	for _, l := range infraLines {
		infraTotal += l.ChargedMicros
	}
	for _, l := range moduleInfraLines {
		infraTotal += l.ChargedMicros
	}

	// 基本費用: the per-app-period SNAPSHOT (ms_billing.app_base_snapshots,
	// migration 028) is authoritative whenever this period's base was actually
	// charged — it freezes the exact amount the charge leg invoiced (advance
	// full base, or the creation-period proration), so a later SyncAppModules
	// count change can never drift the displayed base away from the invoice
	// (the spec's "never disagree"). Only when NO snapshot exists — pre-feature
	// periods, unactivated accounts (never charged), or an in-progress period
	// whose boundary hasn't billed yet — does the display fall back to the
	// live-count math below, which is then a DISPLAY-ONLY ESTIMATE computed
	// from the mirror's CURRENT module_count (or, with no mirror row at all,
	// the pre-027 flat fee + usage-proxy overage — see the
	// INSTALLED-MODULE-COUNT note above).
	var baseFee int64
	snap, snapped, err := s.store.AppBaseSnapshot(ctx, req.AppID, periodStart)
	if err != nil {
		return nil, billing.Internal("app base snapshot lookup failed", err)
	}
	if snapped {
		// This period's base was charged: display EXACTLY what was invoiced.
		// The snapshot alone decides — the mirror is only read on the
		// un-snapshotted estimate paths below.
		baseFee = snap.BaseMicros
	} else {
		mirror, mirrored, err := s.store.AppMirror(ctx, req.AppID)
		if err != nil {
			return nil, billing.Internal("app mirror lookup failed", err)
		}
		switch {
		case mirrored && mirror.Deleted && !mirror.DeletedAt.After(periodStart):
			// Deleted BEFORE this period opened → no base was (or will be) charged
			// for it (D1e: deletion stops FUTURE base fees). A deletion DURING the
			// period leaves that period's base spent, so it falls through below.
			baseFee = 0
		case mirrored:
			// No snapshot → estimate from the mirror's current count, the SAME
			// AppBaseFeeMicros + ProratedBaseMicros math the charge legs bill.
			baseFee = ProratedBaseMicros(
				AppBaseFeeMicros(resolveBaseFeeMicros(plan), mirror.ModuleCount),
				mirror.CreatedAt, periodStart, periodEnd,
			)
		default:
			baseFee = AppBaseFeeMicros(resolveBaseFeeMicros(plan), len(installedModules))
		}
	}

	// TODO(subscription): resolve whether this account has an ACTIVE SaaS
	// subscription — the ONLY thing that earns the PaaS infra credit. v1 has no
	// subscription system, so the credit is subscription-gated OFF and resolves to
	// 0; the wire field (paas_credit_micros) is retained at 0 for back-compat.
	const subscriptionActive = false
	paasCredit, err := paasCreditMicros(subscriptionActive, infraTotal)
	if err != nil {
		return nil, billing.Internal("compute paas credit failed", err)
	}

	total := baseFee + moduleUsageTotal + infraTotal - paasCredit

	return &GetAppBillResponse{
		AppID:                  req.AppID,
		PeriodID:               periodID,
		PeriodStart:            periodStart,
		PeriodEnd:              periodEnd,
		BaseFeeMicros:          baseFee,
		ModuleUsage:            moduleUsage,
		ModuleUsageTotalMicros: moduleUsageTotal,
		InfraTotalMicros:       infraTotal,
		InfraLines:             infraLines,
		ModuleInfraLines:       moduleInfraLines,
		PaasCreditMicros:       paasCredit,
		TotalMicros:            total,
	}, nil
}

// GetBillingPeriods lists the account's billing cycles for the web 週期 (period)
// selector — the CURRENT (live) period first, then every closed billing_periods
// row newest-first. It ALWAYS returns at least the current period: a brand-new
// account (no closed rows, or no billing account at all) still gets the synthetic
// current entry, so the selector is never empty. The current entry carries an
// empty period_id — request its bill by OMITTING GetAppBillRequest.PeriodID.
func (s *Service) GetBillingPeriods(ctx context.Context, req GetBillingPeriodsRequest) (*GetBillingPeriodsResponse, error) {
	if req.OwnerUserID == uuid.Nil && req.OwnerOrgID == uuid.Nil {
		return nil, billing.InvalidInput("owner_user_id or owner_org_id required")
	}
	if req.OwnerUserID != uuid.Nil && req.OwnerOrgID != uuid.Nil {
		return nil, billing.InvalidInput("owner_user_id and owner_org_id are mutually exclusive")
	}

	// Resolve the account (and its anchor) BEFORE the window: the synthetic
	// current period's start is the account's anchored boundary (card-binding
	// day, ADR 0005), and that SAME value is passed to ListBillingPeriods where
	// the SQL flags IsCurrent via period_start = currentStart — so it MUST equal
	// the period_start the rollup/charge cycle writes, or "current" never matches.
	owner := Owner{UserID: req.OwnerUserID, OrgID: req.OwnerOrgID}
	accountID, found, err := s.store.AccountByOwner(ctx, owner)
	if err != nil {
		return nil, billing.Internal("account lookup failed", err)
	}

	anchorDay := billingperiod.DefaultAnchorDay
	if found {
		d, err := s.store.AccountAnchorDay(ctx, accountID)
		if err != nil {
			return nil, billing.Internal("anchor day lookup failed", err)
		}
		anchorDay = d
	}
	currentStart, currentEnd := billingperiod.AnchoredPeriodWindow(s.nowFn().UTC(), anchorDay)
	syntheticCurrent := BillingPeriodRef{
		PeriodID:    "", // no billing_periods row for the in-progress period → live
		PeriodStart: currentStart,
		PeriodEnd:   currentEnd,
		IsCurrent:   true,
	}

	if !found {
		// No billing account yet → only the current live period.
		return &GetBillingPeriodsResponse{Periods: []BillingPeriodRef{syntheticCurrent}}, nil
	}

	rows, err := s.store.ListBillingPeriods(ctx, accountID, currentStart)
	if err != nil {
		return nil, billing.Internal("list billing periods failed", err)
	}

	periods := make([]BillingPeriodRef, 0, len(rows)+1)
	hasCurrent := false
	for _, r := range rows {
		if r.IsCurrent {
			hasCurrent = true
		}
		periods = append(periods, BillingPeriodRef{
			PeriodID:    r.ID.String(),
			PeriodStart: r.PeriodStart,
			PeriodEnd:   r.PeriodEnd,
			IsCurrent:   r.IsCurrent,
		})
	}
	// Prepend the synthetic current live period unless a closed row already covers
	// the in-progress month (rows are newest-first, so the current month — if
	// present — is already at the front).
	if !hasCurrent {
		periods = append([]BillingPeriodRef{syntheticCurrent}, periods...)
	}

	return &GetBillingPeriodsResponse{Periods: periods}, nil
}
