package cycle

// QuoteChange (billing-engine#255, core-v2#1829 step 1c/1e) prices a sequence
// of hypothetical plan steps — create an app, install modules, change a
// plan — WITHOUT writing anything, so api-platform can show a price on the
// plan confirm page and the install preview without holding a second copy of
// the pricing rules. It is the read-only twin of RegisterApp / SetAppPlan:
// every number it reports is derived from the SAME constants and functions
// those legs use (usage.TermsFor, usage.ModuleBlockMicros,
// usage.ProratedBaseMicros), never a re-typed price.
//
// This is NOT billing-engine#231's app-less platform price-terms echo: this
// call is per owner and per step (it needs the owner's current standing and
// live module count to price cumulatively), where #231 is a constants-only
// read with no owner at all.

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/mirrorstack-ai/billing-engine/internal/account/billing"
	"github.com/mirrorstack-ai/billing-engine/internal/account/usage"
	"github.com/mirrorstack-ai/billing-engine/internal/billingperiod"
)

// QuoteChangeStepKind names one step of a hypothetical plan change sequence.
type QuoteChangeStepKind string

const (
	QuoteStepCreateApp      QuoteChangeStepKind = "create_app"
	QuoteStepInstallModules QuoteChangeStepKind = "install_modules"
	QuoteStepSetPlan        QuoteChangeStepKind = "set_plan"
)

// QuoteChangeStep is one step of the sequence QuoteChange prices, in order.
// Steps are cumulative: modules an earlier install_modules step added count
// against the account's pool of usage.IncludedModules for every later step.
type QuoteChangeStep struct {
	Kind QuoteChangeStepKind `json:"kind"`
	// Plan is required for create_app and set_plan: free | pro | business.
	Plan string `json:"plan,omitempty"`
	// AppID names an existing, already-registered app for install_modules /
	// set_plan. Omit it and set StepRef instead to target an app this SAME
	// request creates with an earlier create_app step.
	AppID uuid.UUID `json:"app_id,omitempty"`
	// StepRef is the 0-based index of an earlier create_app step in this
	// request, naming the app that step would create. Mutually usable with
	// AppID (exactly one of the two identifies the target app for
	// install_modules / set_plan).
	StepRef *int `json:"step_ref,omitempty"`
	// ModuleCount is install_modules' N — how many modules that step adds.
	ModuleCount int `json:"module_count,omitempty"`
}

// QuoteChangeRequest is QuoteChange's payload: an owner (user XOR org, same
// convention as RegisterApp) and the ordered steps to price.
type QuoteChangeRequest struct {
	OwnerUserID uuid.UUID         `json:"owner_user_id,omitempty"`
	OwnerOrgID  uuid.UUID         `json:"owner_org_id,omitempty"`
	Steps       []QuoteChangeStep `json:"steps"`
}

// QuoteChangeStanding is the owner's funding posture as QuoteChange sees it —
// deliberately coarser than billing.Code: a preview must never fail loud for
// an owner who simply has not funded yet.
type QuoteChangeStanding string

const (
	// QuoteStandingOK: the owner is funded; the amounts are the real
	// numbers RegisterApp / SetAppPlan would charge for this sequence today.
	QuoteStandingOK QuoteChangeStanding = "ok"
	// QuoteStandingFundingRequired: the owner (or the sponsor it resolves
	// through) is not funded yet. Amounts are still the real derived prices
	// — they just cannot be collected until the owner funds.
	QuoteStandingFundingRequired QuoteChangeStanding = "funding_required"
	// QuoteStandingUnknown: a store read failed. No amount on any step can be
	// trusted, so every step reports zero rather than a guessed price.
	QuoteStandingUnknown QuoteChangeStanding = "unknown"
)

// QuoteChangeStepResult is QuoteChange's answer for one step, priced
// cumulatively against the steps before it.
type QuoteChangeStepResult struct {
	Kind QuoteChangeStepKind `json:"kind"`
	// OneTimeMicros is charged now (always 0 for create_app and
	// install_modules — both price under a grace window — and for a
	// downgrade, which waits for the period boundary; an upgrade's prorated
	// difference is the one case this is nonzero).
	OneTimeMicros int64 `json:"one_time_micros"`
	// RecurringMicros is the step's monthly-recurring delta from here on.
	RecurringMicros int64 `json:"recurring_micros"`
	// ModulesIncludedUsed / ModulesIncludedTotal describe the account-wide
	// free module pool's state immediately after this step (not per app):
	// usage.IncludedModules total, min(running module count, that total)
	// used. A block of usage.ModuleBlockSize absorbs churn within it.
	ModulesIncludedUsed  int `json:"modules_included_used"`
	ModulesIncludedTotal int `json:"modules_included_total"`
	// GraceDays / GraceAmountMicros: for a step priced under a creation or
	// install grace window, how many days the grace holds and what becomes
	// due — as a recurring charge — once it elapses without the app or
	// module being removed. Zero for a plan change, which has no grace.
	GraceDays         int   `json:"grace_days,omitempty"`
	GraceAmountMicros int64 `json:"grace_amount_micros,omitempty"`
	// Standing is the owner's funding posture (same value on every step).
	Standing QuoteChangeStanding `json:"standing"`
}

// QuoteChangeResponse is QuoteChange's answer: one priced result per
// requested step, in request order.
type QuoteChangeResponse struct {
	Steps []QuoteChangeStepResult `json:"steps"`
}

// QuoteChange prices a sequence of hypothetical plan steps for one owner. It
// is deliberately READ-ONLY (docs/DESIGN.md "a specification, not
// production" posture applies to every money-adjacent surface this repo
// serves): it never writes a row, and — like fundedOwnerAccount — it never
// get-or-creates an accounts row for an owner who has none. A store failure
// never produces a guessed price: every step in that response reports
// QuoteStandingUnknown and zero amounts instead.
func (s *Service) QuoteChange(ctx context.Context, req QuoteChangeRequest) (*QuoteChangeResponse, error) {
	if req.OwnerUserID == uuid.Nil && req.OwnerOrgID == uuid.Nil {
		return nil, billing.InvalidInput("owner_user_id or owner_org_id required")
	}
	if req.OwnerUserID != uuid.Nil && req.OwnerOrgID != uuid.Nil {
		return nil, billing.InvalidInput("owner_user_id and owner_org_id are mutually exclusive")
	}
	if len(req.Steps) == 0 {
		return nil, billing.InvalidInput("steps required")
	}

	parsedPlan := make([]usage.Plan, len(req.Steps))
	for i, step := range req.Steps {
		switch step.Kind {
		case QuoteStepCreateApp, QuoteStepSetPlan:
			plan, ok := usage.ParsePlan(step.Plan)
			if !ok {
				return nil, billing.InvalidInput(fmt.Sprintf("step %d: plan must be one of free, pro, business", i))
			}
			if plan == usage.PlanBusiness {
				return nil, billing.PlanNotAvailable("plan business is not available yet: " +
					"its per-app module allowance is not billed yet (billing-engine#202 PR-2c)")
			}
			parsedPlan[i] = plan
			if step.Kind == QuoteStepSetPlan && step.AppID == uuid.Nil && step.StepRef == nil {
				return nil, billing.InvalidInput(fmt.Sprintf("step %d: app_id or step_ref required", i))
			}
		case QuoteStepInstallModules:
			if step.ModuleCount <= 0 {
				return nil, billing.InvalidInput(fmt.Sprintf("step %d: module_count must be positive", i))
			}
			if step.ModuleCount > maxModuleCount {
				return nil, billing.InvalidInput(fmt.Sprintf("step %d: module_count exceeds the maximum supported count (100000)", i))
			}
			if step.AppID == uuid.Nil && step.StepRef == nil {
				return nil, billing.InvalidInput(fmt.Sprintf("step %d: app_id or step_ref required", i))
			}
		default:
			return nil, billing.InvalidInput(fmt.Sprintf("step %d: unknown step kind %q", i, step.Kind))
		}
		if step.StepRef != nil {
			ref := *step.StepRef
			if ref < 0 || ref >= i || req.Steps[ref].Kind != QuoteStepCreateApp {
				return nil, billing.InvalidInput(fmt.Sprintf("step %d: step_ref must name an earlier create_app step", i))
			}
		}
	}

	now := s.nowFn().UTC()
	accountID, activatedAt, standing, err := s.quoteOwnerStanding(ctx, req.OwnerUserID, req.OwnerOrgID)
	if err != nil {
		return unknownQuoteResponse(req.Steps), nil
	}

	moduleCount := 0
	if accountID != uuid.Nil {
		n, err := s.store.LiveModuleTimerCountForAccount(ctx, accountID)
		if err != nil {
			return unknownQuoteResponse(req.Steps), nil
		}
		moduleCount = n
	}
	anchorDay := billingperiod.AnchorDay(now)
	if !activatedAt.IsZero() {
		anchorDay = billingperiod.AnchorDay(activatedAt)
	}
	periodStart, periodEnd := billingperiod.AnchoredPeriodWindow(now, anchorDay)

	existingAppPlan := make(map[uuid.UUID]usage.Plan)
	stepAppPlan := make([]usage.Plan, len(req.Steps))

	resolveCurrentPlan := func(step QuoteChangeStep) (usage.Plan, error) {
		if step.StepRef != nil {
			return stepAppPlan[*step.StepRef], nil
		}
		if p, ok := existingAppPlan[step.AppID]; ok {
			return p, nil
		}
		app, found, err := s.store.AppMirror(ctx, step.AppID)
		if err != nil {
			return "", err
		}
		plan := usage.DefaultPlan
		if found {
			plan = effectivePlan(app)
		}
		existingAppPlan[step.AppID] = plan
		return plan, nil
	}

	results := make([]QuoteChangeStepResult, len(req.Steps))
	for i, step := range req.Steps {
		result := QuoteChangeStepResult{Kind: step.Kind, Standing: standing}
		result.ModulesIncludedTotal = usage.IncludedModules

		switch step.Kind {
		case QuoteStepCreateApp:
			plan := parsedPlan[i]
			terms := usage.TermsFor(plan)
			// Creation grace (RegisterApp, apps.go): nothing is charged now.
			// The base fee becomes due — as the recurring charge — once the
			// app survives GraceDays.
			result.RecurringMicros = terms.BaseFeeMicros
			result.GraceDays = usage.GraceDays
			result.GraceAmountMicros = terms.BaseFeeMicros
			result.ModulesIncludedUsed = min(moduleCount, usage.IncludedModules)
			stepAppPlan[i] = plan

		case QuoteStepInstallModules:
			before := moduleCount
			moduleCount += step.ModuleCount
			// The account-wide pool is priced in whole blocks of
			// usage.ModuleBlockSize; this step's delta is the recurring
			// block cost it adds on top of what was already over the
			// included pool.
			delta := usage.ModuleBlockMicros(int64(moduleCount-usage.IncludedModules)) -
				usage.ModuleBlockMicros(int64(before-usage.IncludedModules))
			result.RecurringMicros = delta
			result.GraceDays = usage.GraceDays
			result.GraceAmountMicros = delta
			result.ModulesIncludedUsed = min(moduleCount, usage.IncludedModules)

		case QuoteStepSetPlan:
			newPlan := parsedPlan[i]
			current, planErr := resolveCurrentPlan(step)
			if planErr != nil {
				return unknownQuoteResponse(req.Steps), nil
			}
			terms := usage.TermsFor(newPlan)
			result.RecurringMicros = terms.BaseFeeMicros
			result.ModulesIncludedUsed = min(moduleCount, usage.IncludedModules)
			if usage.PlanRank(newPlan) > usage.PlanRank(current) {
				// An upgrade charges the DIFFERENCE for the remaining days
				// of the current period, never the new plan's full price
				// (plan_changes.go's upgradeDeltaMicros rule) — the SAME
				// remaining-days proration usage.ProratedBaseMicros applies
				// to a creation charge.
				delta := terms.BaseFeeMicros - usage.TermsFor(current).BaseFeeMicros
				result.OneTimeMicros = usage.ProratedBaseMicros(delta, now, periodStart, periodEnd)
			}
			// A downgrade (or a no-op re-request of the current plan)
			// charges nothing now: it takes effect only at the next period
			// boundary (scheduleDowngrade).
			if step.StepRef != nil {
				stepAppPlan[*step.StepRef] = newPlan
			} else {
				existingAppPlan[step.AppID] = newPlan
			}
		}
		results[i] = result
	}

	return &QuoteChangeResponse{Steps: results}, nil
}

// quoteOwnerStanding mirrors fundedOwnerAccount's checks exactly — the SAME
// store reads, in the SAME order — but reports a QuoteChangeStanding instead
// of failing closed, since a preview must answer for an owner who has not
// funded yet rather than erroring. activatedAt is zero whenever no activation
// anchor was found (including every non-OK standing except a non-activated
// account that otherwise resolved). It never get-or-creates an accounts row.
func (s *Service) quoteOwnerStanding(ctx context.Context, ownerUserID, ownerOrgID uuid.UUID) (
	accountID uuid.UUID, activatedAt time.Time, standing QuoteChangeStanding, err error,
) {
	if ownerUserID != uuid.Nil {
		id, found, e := s.store.AccountIDByUser(ctx, ownerUserID)
		if e != nil {
			return uuid.Nil, time.Time{}, QuoteStandingUnknown, e
		}
		if !found {
			return uuid.Nil, time.Time{}, QuoteStandingFundingRequired, nil
		}
		at, activated, e := s.store.AccountActivation(ctx, id)
		if e != nil {
			return uuid.Nil, time.Time{}, QuoteStandingUnknown, e
		}
		if !activated {
			return id, time.Time{}, QuoteStandingFundingRequired, nil
		}
		accountID, activatedAt = id, at
	} else {
		id, funded, e := s.store.ResolveOrgFundedAccount(ctx, ownerOrgID)
		if e != nil {
			return uuid.Nil, time.Time{}, QuoteStandingUnknown, e
		}
		if !funded {
			return uuid.Nil, time.Time{}, QuoteStandingFundingRequired, nil
		}
		at, activated, e := s.store.AccountActivation(ctx, id)
		if e != nil {
			return uuid.Nil, time.Time{}, QuoteStandingUnknown, e
		}
		if !activated {
			// ResolveOrgFundedAccount already requires activation ("the
			// pointer never flips to an unfunded account") — this mirrors
			// the user leg's shape rather than being reachable.
			return id, time.Time{}, QuoteStandingFundingRequired, nil
		}
		accountID, activatedAt = id, at
	}

	fundingID, e := s.store.ChargeFundingAccount(ctx, accountID)
	if e != nil {
		return accountID, activatedAt, QuoteStandingUnknown, e
	}
	cards, e := s.store.UsableNonFraudCardCount(ctx, fundingID)
	if e != nil {
		return accountID, activatedAt, QuoteStandingUnknown, e
	}
	if cards < 1 {
		return accountID, activatedAt, QuoteStandingFundingRequired, nil
	}
	return accountID, activatedAt, QuoteStandingOK, nil
}

// unknownQuoteResponse is QuoteChange's answer when a store read fails: every
// step reports QuoteStandingUnknown and no amount, never a guessed price.
func unknownQuoteResponse(steps []QuoteChangeStep) *QuoteChangeResponse {
	results := make([]QuoteChangeStepResult, len(steps))
	for i, step := range steps {
		results[i] = QuoteChangeStepResult{Kind: step.Kind, Standing: QuoteStandingUnknown}
	}
	return &QuoteChangeResponse{Steps: results}
}
