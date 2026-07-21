package usage

import (
	"bytes"
	"context"
	"slices"
	"sort"

	"github.com/google/uuid"

	"github.com/mirrorstack-ai/billing-engine/internal/account/billing"
	"github.com/mirrorstack-ai/billing-engine/internal/account/credit"
)

// ============================================================================
// ACCOUNT-LEVEL BILL (GetAccountBill) — the aggregate of the per-app bill.
//
// One pricing path: every number here is computeAppBill's per-app math summed
// across the account's apps for the window. This file adds ONLY (a) the app
// roster (usage ∪ mirror), (b) the account-level PaaS credit placement, and
// (c) the v1 plan stub. It must never grow a second way to price a line.
// ============================================================================

// Plan stub consts — the v1 account plan card GetAccountBill serves, and the
// SINGLE SOURCE OF TRUTH for it: api-platform's GetBillingSummary hardcode
// (internal/account/service/billing.go — "Hobby" / $0 / "active") retires in
// favor of proxying this once the /bill route lands (DESIGN.md D3). There is
// deliberately NO subscriptions table / plan CRUD in this wave; when paid
// plans land, these swap to a real subscription read behind the same wire
// shape (AccountPlan). Distinct from the Plan/PlanDefault BASE-FEE seam in
// bill.go: that resolves what an app's base fee is, this is what the plan
// card displays.
const (
	// PlanStubName is the human label of the only tier v1 has.
	PlanStubName = "Hobby"
	// PlanStubStatus is the subscription-vocabulary status; with no
	// subscription system every account is simply "active".
	PlanStubStatus = "active"
	// PlanStubPriceMicros is the recurring PLAN price (integer micro-USD) —
	// $0: the platform charges per-APP base fees (BaseFeeMicros), not a plan
	// subscription.
	PlanStubPriceMicros int64 = 0
	// PlanStubCurrency is the ISO currency code all bill money is in.
	PlanStubCurrency = "usd"
)

// GetAccountBill returns the account-owner's FULL bill for ONE period — the
// read behind web-account /me/billing's summary + subscription card. It:
//
//  1. resolves the PAYER's billing account from the owner principal and the
//     billed window via resolveBillPeriod (the SAME resolution GetAppBill
//     uses: "" → the current anchored window; a real billing_periods id →
//     that frozen window, account-scoped else NOT_FOUND),
//  2. LAZY account (no billing account row): returns a ZERO bill on the
//     synthetic current calendar-month window (DefaultAnchorDay) with empty
//     Apps — never an error, mirroring GetBillingPeriods' lazy posture,
//  3. enumerates the window's apps as the UNION of (a) app_ids with usage via
//     the same rolled-up-else-live gate the per-app bill reads through
//     (AppIDsWithUsage) and (b) ms_billing.apps mirror rows overlapping the
//     window (MirroredAppIDs) — so a just-created zero-usage app still shows
//     its base and a pre-mirror app with usage still appears — deduped and
//     sorted by app_id (bytewise) for a deterministic response; the zero-UUID
//     account-agent sentinel is excluded in SQL and defensively after the union,
//  4. computes each real app through computeAppBill — EXACTLY what GetAppBill
//     computes for that (owner, app, window): snapshot-first base fee, module
//     usage, infra with the same 1.2× markup source — and sums the totals
//     (apps[].total_micros are PRE-credit, see AccountAppBill); a DELETED app
//     whose row totals zero (base estimate zeroed, no usage/infra arrears) is
//     dropped from the roster — nothing of it will ever reach an invoice,
//  5. computes the zero-UUID account-agent scope through that same pricing path,
//     exposes module usage + infra in Agent, and deliberately discards its app
//     base-fee result because agent activity is not an app,
//  6. applies the PaaS credit ONCE at the ACCOUNT level: the same ACTIVE-SaaS
//     gate as the per-app credit (v1 has no subscription system → always 0),
//     capped at the combined app + agent usage plane so it never eats base fees,
//  7. adds the account-wide POOLED module overage (migration 032) once,
//     snapshot-first (the frozen charge, else a live estimate from the pool),
//  8. adds the live custom-domain account line ($2 per domain) once,
//  9. TotalMicros = BaseFeeTotal + ModuleUsageTotal + InfraTotal +
//     AccountOverage + CustomDomains + Agent.TotalMicros − PaasCredit (≥ 0 by
//     the cap), plus the v1 plan stub with RenewsAt = the period end.
func (s *Service) GetAccountBill(ctx context.Context, req GetAccountBillRequest) (*GetAccountBillResponse, error) {
	if req.OwnerUserID == uuid.Nil && req.OwnerOrgID == uuid.Nil {
		return nil, billing.InvalidInput("owner_user_id or owner_org_id required")
	}
	if req.OwnerUserID != uuid.Nil && req.OwnerOrgID != uuid.Nil {
		return nil, billing.InvalidInput("owner_user_id and owner_org_id are mutually exclusive")
	}
	// "" (or omitted) means the current window; a non-empty value must be a
	// billing_periods id. Malformed ids are INVALID_INPUT here (they can't be
	// anyone's period); unknown/foreign ids are NOT_FOUND below, same as
	// GetAppBill.
	periodRef := uuid.Nil
	if req.PeriodID != "" {
		id, err := uuid.Parse(req.PeriodID)
		if err != nil {
			return nil, billing.InvalidInput("period_id must be a billing period id (empty for the current period)")
		}
		periodRef = id
	}

	owner := Owner{UserID: req.OwnerUserID, OrgID: req.OwnerOrgID}
	accountID, found, err := s.store.AccountByOwner(ctx, owner)
	if err != nil {
		return nil, billing.Internal("account lookup failed", err)
	}

	// TODO(plan): resolve the real account/org plan (shared with GetAppBill's
	// base-fee seam). v1 has no plan system → PlanDefault.
	plan := PlanDefault

	periodID, periodStart, periodEnd, err := s.resolveBillPeriod(ctx, accountID, found, periodRef)
	if err != nil {
		return nil, err
	}

	planStub := AccountPlan{
		Name:        PlanStubName,
		Status:      PlanStubStatus,
		PriceMicros: PlanStubPriceMicros,
		Currency:    PlanStubCurrency,
		RenewsAt:    periodEnd,
	}

	if !found {
		// Lazy / never-activated account: a zero bill on the synthetic current
		// window (resolveBillPeriod already fell back to the calendar month).
		// Unlike the per-app bill — where a known app_id carries its base fee
		// even account-less — there is no roster to enumerate here (no mirror
		// rows without an account), so every total is 0 and Apps is empty
		// (never nil), matching GetBillingPeriods' lazy posture.
		return &GetAccountBillResponse{
			PeriodID:    periodID,
			PeriodStart: periodStart,
			PeriodEnd:   periodEnd,
			Plan:        planStub,
			Agent:       AccountAgentBill{Models: []AgentModelUsage{}},
			Apps:        []AccountAppBill{},
		}, nil
	}

	usageApps, err := s.store.AppIDsWithUsage(ctx, accountID, periodStart, periodEnd)
	if err != nil {
		return nil, billing.Internal("app usage enumeration failed", err)
	}
	mirrorApps, err := s.store.MirroredAppIDs(ctx, accountID, periodStart, periodEnd)
	if err != nil {
		return nil, billing.Internal("app mirror enumeration failed", err)
	}

	// Union the two halves, sort by app_id bytes, then compact adjacent
	// duplicates — one pass gives the deterministic deduped roster
	// regardless of which half (or which store scan order) produced an app.
	appIDs := make([]uuid.UUID, 0, len(usageApps)+len(mirrorApps))
	appIDs = append(appIDs, usageApps...)
	appIDs = append(appIDs, mirrorApps...)
	sort.Slice(appIDs, func(i, j int) bool {
		return bytes.Compare(appIDs[i][:], appIDs[j][:]) < 0
	})
	appIDs = slices.Compact(appIDs)
	// Account-level agent usage is stored under uuid.Nil for metering, but the
	// sentinel is never an app. SQL excludes it from both ledgers; this protects
	// alternate stores and test fakes from ever routing it through app base fees.
	appIDs = slices.DeleteFunc(appIDs, func(appID uuid.UUID) bool {
		return appID == uuid.Nil
	})

	apps := make([]AccountAppBill, 0, len(appIDs))
	var baseFeeTotal, moduleUsageTotal, infraTotal int64
	for _, appID := range appIDs {
		parts, err := s.computeAppBill(ctx, accountID, found, plan, appID, periodStart, periodEnd)
		if err != nil {
			return nil, err
		}
		total := parts.BaseFeeMicros + parts.ModuleUsageTotalMicros + parts.InfraTotalMicros
		// A deleted app contributes NOTHING to this bill once its estimated base
		// zeroes (computeAppBill) and it has no usage/infra arrears — drop the
		// row rather than rendering a dead $0 line. A deleted app WITH arrears
		// stays: its usage still bills at the boundary (cycle/charge.go).
		if parts.IsDeleted && total == 0 {
			continue
		}
		apps = append(apps, AccountAppBill{
			AppID:             appID,
			Name:              parts.Name,
			IsDeleted:         parts.IsDeleted,
			BaseFeeMicros:     parts.BaseFeeMicros,
			ModuleUsageMicros: parts.ModuleUsageTotalMicros,
			InfraMicros:       parts.InfraTotalMicros,
			// PRE-credit by contract: the account-level credit below is never
			// allocated back per-app.
			TotalMicros: total,
		})
		baseFeeTotal += parts.BaseFeeMicros
		moduleUsageTotal += parts.ModuleUsageTotalMicros
		infraTotal += parts.InfraTotalMicros
	}

	// Account-level agent scope (metered under uuid.Nil): the SAME pricing path
	// as an app, but its resolved app base fee is DISCARDED — agent activity is
	// not an app and never incurs a base fee. Runs unconditionally like the app
	// loop above: a lazy (!found) owner already returned a zero-Agent bill.
	agentParts, err := s.computeAppBill(ctx, accountID, found, plan, uuid.Nil, periodStart, periodEnd)
	if err != nil {
		return nil, err
	}
	agent := AccountAgentBill{
		ModuleUsageMicros: agentParts.ModuleUsageTotalMicros,
		InfraMicros:       agentParts.InfraTotalMicros,
		TotalMicros:       agentParts.ModuleUsageTotalMicros + agentParts.InfraTotalMicros,
		Models:            agentParts.ModelLines,
	}

	// Account overage line (migration 033): the steady-state monthly estimate
	// from the CURRENT live install timers — $3 × max(0, live − IncludedModules).
	// Under the per-module-instance model overage is billed per install on its own
	// grace timer (Leg 1) / at the boundary (Leg 2); the display sums the live
	// "over" timer rows and prices them at the steady-state $3 each — the timer
	// table is the overage model's source of truth. The display does not prorate
	// (the charge legs own proration), so this is an estimate of the period's
	// overage, not a per-line replay of the exact prorated amounts invoiced (that
	// would need a per-timer charged-micros column — a follow-up). It is an
	// ACCOUNT line, never allocated back per app.
	accountOverage, err := s.accountOverageMicros(ctx, accountID)
	if err != nil {
		return nil, err
	}
	customDomains, err := s.customDomainsMicros(ctx, accountID)
	if err != nil {
		return nil, err
	}

	// TODO(subscription): same gate as GetAppBill — v1 has no subscription
	// system, so the credit is subscription-gated OFF and resolves to 0.
	const subscriptionActive = false
	paasCredit, err := accountPaasCreditMicros(
		subscriptionActive,
		moduleUsageTotal+agent.ModuleUsageMicros,
		infraTotal+agent.InfraMicros,
	)
	if err != nil {
		return nil, billing.Internal("compute paas credit failed", err)
	}

	return &GetAccountBillResponse{
		PeriodID:               periodID,
		PeriodStart:            periodStart,
		PeriodEnd:              periodEnd,
		Plan:                   planStub,
		Apps:                   apps,
		Agent:                  agent,
		BaseFeeTotalMicros:     baseFeeTotal,
		ModuleUsageTotalMicros: moduleUsageTotal,
		InfraTotalMicros:       infraTotal,
		AccountOverageMicros:   accountOverage,
		CustomDomainsMicros:    customDomains,
		PaasCreditMicros:       paasCredit,
		TotalMicros:            baseFeeTotal + moduleUsageTotal + infraTotal + accountOverage + customDomains + agent.TotalMicros - paasCredit,
	}, nil
}

// ProjectedCreditCharge exposes the existing account-bill calculation through
// the narrow wallet reconciliation interface. It deliberately delegates to
// GetAccountBill so the real-time credit gate and UI standing never grow a
// second pricing implementation.
func (s *Service) ProjectedCreditCharge(ctx context.Context, ownerUserID, ownerOrgID uuid.UUID) (credit.Projection, error) {
	bill, err := s.GetAccountBill(ctx, GetAccountBillRequest{
		OwnerUserID: ownerUserID,
		OwnerOrgID:  ownerOrgID,
	})
	if err != nil {
		return credit.Projection{}, err
	}
	return credit.Projection{
		AmountMicros: bill.TotalMicros,
		PeriodStart:  bill.PeriodStart,
		PeriodEnd:    bill.PeriodEnd,
	}, nil
}

// accountOverageMicros resolves the account overage shown on the bill: the
// steady-state monthly estimate AccountOverageMicros(live timer count) = $3 ×
// max(0, live − IncludedModules), where `live` is the count of the account's
// currently-live install timers (migration 033) — the overage model's source of
// truth. The first IncludedModules live timers (by FIFO) are "included"; the rest
// are "over", so max(0, live − included) is exactly the live over-count the charge
// legs tier on. The PaaS credit never offsets it (overage rides ON TOP, like the
// base fee), so it is resolved outside the credit math.
func (s *Service) accountOverageMicros(ctx context.Context, accountID uuid.UUID) (int64, error) {
	live, err := s.store.LiveModuleTimerCountForAccount(ctx, accountID)
	if err != nil {
		return 0, billing.Internal("live module timer count lookup failed", err)
	}
	return AccountOverageMicros(live), nil
}

// customDomainsMicros resolves the account's steady-state custom-domain line:
// every currently-live custom domain costs DomainFeeMicros, with no included
// allowance. Proration belongs exclusively to the activation charge sweep; the
// bill display is the full-period live estimate, matching account overage above.
func (s *Service) customDomainsMicros(ctx context.Context, accountID uuid.UUID) (int64, error) {
	live, err := s.store.LiveDomainCountForAccount(ctx, accountID)
	if err != nil {
		return 0, billing.Internal("live domain count lookup failed", err)
	}
	return int64(live) * DomainFeeMicros, nil
}

// accountPaasCreditMicros is the ACCOUNT-level PaaS credit: the same
// subscription-gated pct-of-infra magnitude as GetAppBill's per-app
// paasCreditMicros, computed over the combined app + agent ACCOUNT-WIDE infra
// total and applied exactly once, then CAPPED at their combined module usage +
// infra totals — the credit offsets usage-plane charges only and can never eat
// the base fees, matching the charge spine's allowance posture (arrears =
// max(0, Σcharged − allowance), base fees added ON TOP — DESIGN.md "Base fee —
// v1 spec"). The cap keeps TotalMicros ≥ BaseFeeTotal ≥ 0 by construction.
//
// Under today's pct formula the cap cannot bind (PaasCreditPct% of infra ≤
// infra ≤ usage + infra), but it is pinned here — not left to the formula —
// so a future flat-allowance credit inherits the usage-only offset invariant
// instead of silently discounting base fees.
func accountPaasCreditMicros(subscriptionActive bool, moduleUsageTotalMicros, infraTotalMicros int64) (int64, error) {
	credit, err := paasCreditMicros(subscriptionActive, infraTotalMicros)
	if err != nil {
		return 0, err
	}
	if maxCredit := moduleUsageTotalMicros + infraTotalMicros; credit > maxCredit {
		credit = maxCredit
	}
	return credit, nil
}
