package usage

import (
	"context"
	"math/big"
	"sort"
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
	// DEFAULT plan. It BUNDLES the PaaS infra credit (surfaced as PaasCreditMicros).
	// It is FLAT per app: the IncludedModules allowance + the ModuleOverageFeeMicros
	// surcharge are ACCOUNT-WIDE POOLED (migration 032 — see AccountOverageMicros),
	// NOT folded into this per-app fee. Tunable. Default $20.
	BaseFeeMicros int64 = 20_000_000 // $20.00

	// ProBaseFeeMicros is the base fee on the Pro org plan. TODO(plan): wire a real
	// plan resolver (ms_account.orgs / a subscription row) into resolveBaseFeeMicros;
	// v1 has no plan system so this const is the seam, not yet reached. Placeholder
	// value — tune with the real Pro plan.
	ProBaseFeeMicros int64 = 50_000_000 // $50.00 (placeholder)

	// IncludedModules is the ACCOUNT-WIDE POOL of installed modules the base fee
	// bundles before the per-module surcharge kicks in (migration 032 — ONE pool
	// of 5 for the whole account, summed over its live apps, NOT 5 per app). Owner
	// spec 2026-07-05. Tunable ("may change"); becomes plan-resolved later.
	IncludedModules = 5

	// ModuleBlockSize is how many over-modules one purchased block covers (owner
	// spec 2026-08-27, superseding the 2026-07-05 per-module rate). Overage is
	// sold in WHOLE BLOCKS, not per module: an account pays for
	// ceil(over_count / ModuleBlockSize) blocks. Tunable.
	ModuleBlockSize = 5

	// ModuleBlockFeeMicros is the RECURRING price of one such block — the money
	// the customer actually sees each period. Account overage for a period =
	// ModuleBlockFeeMicros × ceil(max(0, Σ live-app module_count −
	// IncludedModules) / ModuleBlockSize); see AccountOverageMicros /
	// ModuleBlockMicros (basefee.go), the ONE place that formula lives. Tunable.
	//
	// Why blocks: the per-module rate priced the PLATFORM'S decomposition
	// granularity rather than customer value — splitting one module into two for
	// architectural reasons raised a tenant's bill while delivering them nothing.
	// A block absorbs that churn: inside a block, a split is free.
	ModuleBlockFeeMicros int64 = 5_000_000 // $5.00 per block of ModuleBlockSize

	// ModuleOverageFeeMicros is the per-module AMORTIZED rate, DERIVED from the
	// block price — never tuned independently, so the two can never drift.
	//
	// Block pricing is NON-ADDITIVE (the price of a set of modules is not the sum
	// of their individual prices), but the ONE-TIME charge legs are inherently
	// per-timer: Leg 1's grace sweep, the combined creation invoice, and their
	// crash-recovery replays each price a SINGLE install timer, and
	// cycle.moduleOverageChargeShape is load-bearingly built on every input being
	// IMMUTABLE so a fresh charge, an idem-key replay and the post-idem-window
	// recovery all recompute the identical amount. A timer's live FIFO rank is
	// MUTABLE, so pricing a one-time leg in blocks would let a replay reuse a
	// Stripe idempotency key with a different amount.
	//
	// So the split is by LEG, not by call site:
	//   - RECURRING money (the period bill line + the boundary advance leg) is
	//     priced in whole blocks — that is the customer-visible monthly price;
	//   - ONE-TIME mid-period stubs (a module installed partway through a period)
	//     stay per-module at this amortized rate, keeping every immutability and
	//     idempotency property intact.
	// The account settles onto the exact block price at the next boundary. The
	// stub can under-collect within a period (6 modules installed mid-period bill
	// 6 × this rate, not 2 blocks); that is bounded to one period, self-corrects
	// at the boundary, and errs in the customer's favour — the same direction as
	// the D1e no-refund-on-shrink asymmetry.
	ModuleOverageFeeMicros int64 = ModuleBlockFeeMicros / ModuleBlockSize // $1.00

	// DomainFeeMicros is the recurring fee for EACH active custom domain.
	// Domains have no included allowance; every live domain contributes this
	// full steady-state amount to the account bill. Tunable. Default $2.
	DomainFeeMicros int64 = 2_000_000 // $2.00

	// PaasCreditPct is PaaS 額度 — the percentage of the 基礎設施 InfraTotal credited
	// back (offsetting infra) as a SaaS-subscription benefit. Tunable. Default 30%.
	// (It replaced an earlier flat −$7 credit with an infra-proportional one.)
	// Only reached once a subscription earns it; subscription-gated OFF in v1 (see
	// paasCreditMicros), so today the credit is always 0.
	PaasCreditPct = 30

	// GraceDays is the creation grace window (owner spec 2026-07-05, D1e
	// follow-up). A newly created app is NOT charged its creation-period base
	// synchronously; a periodic sweep (cmd/billing-cycle) charges it only once it
	// has SURVIVED this many whole days past created_at, so an app soft-deleted
	// within the window is NEVER billed. A survivor is charged the SAME
	// creation-period proration (identical ProratedBaseMicros math, anchored to
	// the TRUE created_at) — grace delays WHEN the charge fires, never WHAT it
	// covers. Tunable.
	GraceDays = 3
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
//     back to the live ESTIMATE from the mirror (migration 027): the FLAT
//     plan-resolved base (resolveBaseFeeMicros(plan)) — full fee even when
//     created_at falls inside the period (the creation proration is the
//     sweep's one-time charge, never the recurring line; issue #63).
//     Module overage is NOT in the per-app base anymore — it is account-wide
//     pooled (migration 032, surfaced on GetAccountBill). An app ABSENT from
//     the mirror (pre-backfill) falls back to the flat plan base below,
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

	periodID, periodStart, periodEnd, err := s.resolveBillPeriod(ctx, accountID, found, req.PeriodID)
	if err != nil {
		return nil, err
	}

	parts, err := s.computeAppBill(ctx, accountID, found, plan, req.AppID, periodStart, periodEnd)
	if err != nil {
		return nil, err
	}

	// TODO(subscription): resolve whether this account has an ACTIVE SaaS
	// subscription — the ONLY thing that earns the PaaS infra credit. v1 has no
	// subscription system, so the credit is subscription-gated OFF and resolves to
	// 0; the wire field (paas_credit_micros) is retained at 0 for back-compat.
	const subscriptionActive = false
	paasCredit, err := paasCreditMicros(subscriptionActive, parts.InfraTotalMicros)
	if err != nil {
		return nil, billing.Internal("compute paas credit failed", err)
	}

	// The charge model pools the first IncludedModules timers account-wide, then
	// charges each later timer independently. GetAccountBill shows that as one
	// account line; the per-app page attributes the same LIVE over rows to the app
	// that owns them and folds that share into its 基本費用. Only the current
	// period uses this steady-state estimate: historical periods retain their
	// exact base snapshot because there is no frozen per-timer allocation ledger.
	//
	// Priced at the AMORTIZED per-module rate, NOT in blocks — deliberately. A
	// block is ACCOUNT-wide and its timers can span several apps, so there is no
	// non-arbitrary way to hand one app a whole block. The amortized rate is the
	// right per-timer primitive: each over-timer carries exactly one module's
	// share of a block, so the per-app shares sum to the account total whenever
	// the account's blocks are full. The account bill stays authoritative — see
	// BaseFeeMicros' doc: callers must not add the two APIs together.
	baseFee := parts.BaseFeeMicros
	var moduleOverage int64
	if req.PeriodID == uuid.Nil && found && !parts.IsDeleted {
		overCount, err := s.store.LiveOverModuleTimerCountForApp(ctx, accountID, req.AppID, IncludedModules)
		if err != nil {
			return nil, billing.Internal("app module overage attribution failed", err)
		}
		moduleOverage = int64(overCount) * ModuleOverageFeeMicros
		baseFee += moduleOverage
	}

	total := baseFee + parts.ModuleUsageTotalMicros + parts.InfraTotalMicros - paasCredit

	return &GetAppBillResponse{
		AppID:                  req.AppID,
		Name:                   parts.Name,
		IsDeleted:              parts.IsDeleted,
		PeriodID:               periodID,
		PeriodStart:            periodStart,
		PeriodEnd:              periodEnd,
		BaseFeeMicros:          baseFee,
		InstalledModuleCount:   parts.InstalledModuleCount,
		ModuleOverageMicros:    moduleOverage,
		ModuleUsage:            parts.ModuleUsage,
		ModuleUsageTotalMicros: parts.ModuleUsageTotalMicros,
		// Reported, never added: TotalMicros above is BaseFee + the NON-dev
		// module usage + Infra − PaasCredit (migration 073).
		ModuleUsageDevServedMicros: parts.ModuleUsageDevServedMicros,
		InfraTotalMicros:           parts.InfraTotalMicros,
		InfraLines:                 parts.InfraLines,
		ModuleInfraLines:           parts.ModuleInfraLines,
		PaasCreditMicros:           paasCredit,
		TotalMicros:                total,
	}, nil
}

// resolveBillPeriod resolves the billed window + the echoed period id for a
// bill read (GetAppBill / GetAccountBill — ONE resolution for both, so the two
// bills can never window differently):
//
//   - periodID == uuid.Nil → the account's CURRENT anchored period (card-
//     binding day, ADR 0005), live from events; with no billing account yet
//     (found=false) the calendar-month default (DefaultAnchorDay), so a lazy
//     bill still shows a window. Echoed id is "" (the in-progress period has
//     no billing_periods row).
//   - periodID set → that account-scoped billing_periods row's frozen [start,
//     end) window. An unknown / other-account id is NOT_FOUND; without a
//     billing account the caller owns no periods at all → NOT_FOUND too.
func (s *Service) resolveBillPeriod(ctx context.Context, accountID uuid.UUID, found bool, periodID uuid.UUID) (string, time.Time, time.Time, error) {
	if periodID != uuid.Nil {
		if !found {
			return "", time.Time{}, time.Time{}, billing.NotFound("billing period not found")
		}
		start, end, ok, err := s.store.BillingPeriodWindow(ctx, accountID, periodID)
		if err != nil {
			return "", time.Time{}, time.Time{}, billing.Internal("billing period lookup failed", err)
		}
		if !ok {
			return "", time.Time{}, time.Time{}, billing.NotFound("billing period not found")
		}
		return periodID.String(), start, end, nil
	}
	anchorDay := billingperiod.DefaultAnchorDay
	if found {
		d, err := s.store.AccountAnchorDay(ctx, accountID)
		if err != nil {
			return "", time.Time{}, time.Time{}, billing.Internal("anchor day lookup failed", err)
		}
		anchorDay = d
	}
	start, end := billingperiod.AnchoredPeriodWindow(s.nowFn().UTC(), anchorDay)
	return "", start, end, nil
}

// appBillParts is the pre-credit bill core for ONE (account, app, window),
// computed by computeAppBill — the SINGLE per-app pricing path shared by
// GetAppBill (which wires every part verbatim, then applies the per-app PaaS
// credit) and GetAccountBill (which sums the totals into one roster row per
// app, then applies the credit ONCE at the account level). The PaaS credit is
// deliberately NOT in here: which scope it offsets is the caller's semantic.
type appBillParts struct {
	// BaseFeeMicros is 基本費用 resolved SNAPSHOT-FIRST (see computeAppBill).
	BaseFeeMicros int64
	// InstalledModuleCount is the billing mirror's authoritative live snapshot.
	// It includes installed-but-idle modules, unlike the usage-ledger lines.
	InstalledModuleCount int
	// ModuleUsage are the non-reserved 模組使用量 lines, dev_served ones
	// included and flagged; ModuleUsageTotalMicros is the NON-dev total (the
	// one that enters the bill) and ModuleUsageDevServedMicros is the priced
	// tunnel figure that never does. ModuleUsageTotalMicros
	// is Σ their ChargedMicros.
	ModuleUsage            []AppMetricUsage
	ModuleUsageTotalMicros int64
	// ModuleUsageDevServedMicros is Σ of the dev_served lines' charges
	// (migration 073): priced, displayed, and never a term in any total.
	ModuleUsageDevServedMicros int64
	// InfraTotalMicros is 基礎設施 = Σ InfraLines + Σ ModuleInfraLines (the
	// 1.2× infra markup already applied once, in SQL).
	InfraTotalMicros int64
	InfraLines       []AppInfraUsage
	ModuleInfraLines []AppModuleInfraUsage
	// ModelLines is the display-only per-model rollup of every non-reserved line
	// carrying a model plus reserved model lines attributed to the platform-infra
	// sentinel. Reserved lines attributed to real modules are excluded because
	// their authoritative per-module infra charge has no model dimension. The
	// rollup is charge-reconciled so its sum never exceeds the usage total; it is
	// never re-priced.
	ModelLines []AgentModelUsage
	// Name is the frozen app display name (migration 037) — "" when the app was
	// never mirrored or was registered pre-037. IsDeleted is the server-
	// authoritative removal flag; the bill page reads it to show a deleted app's
	// charges in a dialog instead of linking to the (gone) app page.
	Name      string
	IsDeleted bool
}

// clampModelChargesToTotal reconciles the display-only per-model rollup with the
// authoritative usage total. The model buckets sum AppBill line charges rounded
// per (metric, model, version); the usage total sums the catalog-anchored infra
// queries rounded per metric (model collapsed) plus the module-usage lines. On a
// LIVE period the two rounding granularities can disagree by a few micros, and an
// operator deactivating a reserved metric against a frozen period drops it from
// the catalog-anchored total while the ledger-sourced model rows keep it — either
// way Σ models[].charged can drift ABOVE the total. models[] is a decomposition OF
// that total, so trim it from the TAIL (smallest charges first; lines are sorted
// charged desc) until Σ ≤ limit, dropping any row trimmed to a non-positive
// charge. It is a no-op when the rollup already fits (the common case: exact
// reconciliation on the frozen path, sub-cent drift on live). Returns the
// possibly-shortened slice.
func clampModelChargesToTotal(lines []AgentModelUsage, limit int64) []AgentModelUsage {
	if limit < 0 {
		limit = 0
	}

	var sum int64
	for _, line := range lines {
		sum += line.ChargedMicros
	}
	if sum <= limit {
		return lines
	}

	trimmed := append([]AgentModelUsage(nil), lines...)
	excess := sum - limit
	for i := len(trimmed) - 1; i >= 0 && excess > 0; i-- {
		if trimmed[i].ChargedMicros <= 0 {
			continue
		}
		reduction := trimmed[i].ChargedMicros
		if reduction > excess {
			reduction = excess
		}
		trimmed[i].ChargedMicros -= reduction
		excess -= reduction
	}

	compacted := trimmed[:0]
	for _, line := range trimmed {
		if line.ChargedMicros > 0 {
			compacted = append(compacted, line)
		}
	}
	return compacted
}

// computeAppBill computes one app's pre-credit bill parts for the resolved
// window — steps 3–4 of the GetAppBill contract (see its doc: module-usage
// partition, catalog-anchored infra residual + per-module infra split, and the
// snapshot-first 基本費用 resolution with its mirror-estimate and legacy
// usage-proxy fallbacks). found=false (no billing account yet) yields the
// base-fee-only bill: no usage/module-infra reads, but the catalog-anchored
// infra residual still renders every declared metric at $0.
func (s *Service) computeAppBill(ctx context.Context, accountID uuid.UUID, found bool, plan Plan, appID uuid.UUID, periodStart, periodEnd time.Time) (*appBillParts, error) {
	// Read the usage lines (empty when no billing account exists yet — the bill is
	// then base-fee-only).
	var lines []AppMetricUsageRaw
	var err error
	if found {
		lines, err = s.store.AppBill(ctx, accountID, appID, periodStart, periodEnd)
		if err != nil {
			return nil, billing.Internal("app bill query failed", err)
		}
	}

	// Keep only the non-reserved rows → module usage (displayed lines); the
	// reserved infra.* / platform.* rows are dropped here (infra is sourced
	// per-metric from the catalog below). For the display-only model rollup,
	// exclude reserved rows attributed to real modules: their authoritative
	// charge lives in the model-less per-module infra plane below. Sentinel-
	// attributed reserved rows and all non-reserved model rows still accrue, then
	// the completed rollup is reconciled to the usage total so Σ model charges ≤
	// that total by construction. Module overage is now account-wide pooled
	// (migration 032), so the per-app base no longer needs a distinct-module
	// proxy count here.
	moduleUsage := make([]AppMetricUsage, 0, len(lines))
	modelBuckets := make(map[string]AgentModelUsage)
	var moduleUsageTotal int64
	var moduleUsageDevServed int64
	for _, r := range lines {
		reserved := isReservedMetric(r.Metric)
		reservedRealModule := reserved && r.ModuleID != PlatformInfraModuleID()
		// 🔴 A dev_served line (migration 073) is DISPLAYED, never SPENT. It is
		// kept out of the per-model rollup for the same reason it is kept out
		// of the total: ModelLines is a breakdown OF the charged bill, clamped
		// to moduleUsageTotal+infraTotal below, so letting tunnel usage accrue
		// there would either inflate the agent-model display past what was
		// billed or be silently trimmed by the clamp — both of which report a
		// number no invoice contains.
		if r.Model != "" && !reservedRealModule && !r.DevServed {
			modelLine := modelBuckets[r.Model]
			modelLine.Model = r.Model
			modelLine.BillableQuantity += r.BillableQuantity
			modelLine.ChargedMicros += r.ChargedMicros
			modelBuckets[r.Model] = modelLine
		}
		if reserved {
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
			DevServed:        r.DevServed,
			BillableQuantity: r.BillableQuantity,
			UnitPriceMicros:  r.UnitPriceMicros,
			ChargedMicros:    r.ChargedMicros,
			ActiveSeconds:    r.ActiveSeconds,
			PeriodDays:       r.PeriodDays,
		})
		// 🔴 THE SPLIT THAT KEEPS A DEV TUNNEL OFF THE BILL. The LINE is
		// emitted either way, carrying its real ChargedMicros — a developer
		// testing a paid meter has to see what it would have cost. Only the
		// TOTAL forks, and only the non-dev total reaches TotalMicros.
		if r.DevServed {
			moduleUsageDevServed += r.ChargedMicros
			continue
		}
		moduleUsageTotal += r.ChargedMicros
	}
	modelLines := make([]AgentModelUsage, 0, len(modelBuckets))
	for _, modelLine := range modelBuckets {
		modelLines = append(modelLines, modelLine)
	}
	sort.Slice(modelLines, func(i, j int) bool {
		if modelLines[i].ChargedMicros != modelLines[j].ChargedMicros {
			return modelLines[i].ChargedMicros > modelLines[j].ChargedMicros
		}
		return modelLines[i].Model < modelLines[j].Model
	})

	// 基礎設施: source infra per-metric from the CATALOG (metric_definitions), NOT
	// the usage ledger — so EVERY active declared infra metric renders as its own
	// line, including declared-but-unused ones at qty 0 · $0 ("show all"). Run
	// UNCONDITIONALLY (even with no billing account, accountID == uuid.Nil): the
	// usage side then matches nothing and every declared metric COALESCEs to $0,
	// so a lazy/no-account app still shows all 16 infra lines. Each line's
	// ChargedMicros already carries the ×1.2 infra markup (applied once in SQL);
	// InfraTotalMicros is their sum, keeping the back-compat scalar exactly
	// reconcilable (infra_total == Σ infra_lines[].charged).
	infraLines, err := s.store.AppInfraBill(ctx, accountID, appID, periodStart, periodEnd)
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
		moduleInfraLines, err = s.store.AppModuleInfraBill(ctx, accountID, appID, periodStart, periodEnd)
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
	//
	// The mirror is read UNCONDITIONALLY (migration 037): it carries the frozen
	// name + the deleted flag, which the bill must surface on EVERY period —
	// including already-charged (snapshotted) periods — so a deleted app's
	// historical rows still show its name. (Pre-037 this read was nested in the
	// un-snapshotted estimate branch only.) The base-fee resolution below still
	// prefers the snapshot; the mirror only drives the estimate fallbacks.
	mirror, mirrored, err := s.store.AppMirror(ctx, appID)
	if err != nil {
		return nil, billing.Internal("app mirror lookup failed", err)
	}
	var baseFee int64
	snap, snapped, err := s.store.AppBaseSnapshot(ctx, appID, periodStart)
	if err != nil {
		return nil, billing.Internal("app base snapshot lookup failed", err)
	}
	if snapped {
		// This period's base was charged: display EXACTLY what was invoiced.
		// The snapshot alone decides — the mirror only drives the estimate paths.
		baseFee = snap.BaseMicros
	} else {
		switch {
		case mirrored && mirror.Deleted:
			// Deleted → no base will be charged (D1e: deletion stops FUTURE base
			// fees). The un-snapshotted estimate previews the charge legs, and
			// BOTH skip deleted apps: the advance leg rosters LIVE apps only
			// (cycle/charge.go LiveAppsCreatedBefore) and the creation sweep
			// re-checks not-deleted under lock (cycle/proration.go) — so a $20
			// preview here would never materialize on the invoice, whenever the
			// deletion happened. A base that WAS charged before the deletion is
			// the snapshot branch above and still displays what was invoiced;
			// usage arrears still render (and bill) below regardless.
			baseFee = 0
		case mirrored:
			// No snapshot → estimate the FLAT per-app base from the plan (module
			// overage is now account-wide pooled, migration 032 — never folded
			// into the per-app base here). FULL fee, never prorated: the
			// recurring line previews the advance-base charge leg
			// (cycle/charge.go), which bills the full next-period fee per live
			// app. The (created_at → period-end) proration is exclusively the
			// one-time "New creation" charge (cycle/proration.go) — applying it
			// here double-derived it and showed $14.19 for a $20 plan on every
			// mid-period app (issue #63; the Aug-1 invoice was already correct).
			baseFee = resolveBaseFeeMicros(plan)
		default:
			// No mirror row (pre-backfill): the flat per-app base. The pre-032
			// usage-proxy overage is gone — overage is pooled at the account
			// level, so a per-app read never estimates it.
			baseFee = resolveBaseFeeMicros(plan)
		}
	}

	modelLines = clampModelChargesToTotal(modelLines, moduleUsageTotal+infraTotal)

	return &appBillParts{
		BaseFeeMicros:              baseFee,
		InstalledModuleCount:       mirror.ModuleCount,
		ModuleUsage:                moduleUsage,
		ModuleUsageTotalMicros:     moduleUsageTotal,
		ModuleUsageDevServedMicros: moduleUsageDevServed,
		InfraTotalMicros:           infraTotal,
		InfraLines:                 infraLines,
		ModuleInfraLines:           moduleInfraLines,
		ModelLines:                 modelLines,
		Name:                       mirror.Name, // "" when not mirrored / pre-037
		IsDeleted:                  mirrored && mirror.Deleted,
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
