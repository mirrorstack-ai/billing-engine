package cycle

// Plan changes (core-v2#1412, billing-engine#202 PR-2b). An app moves between
// free | pro | business through SetAppPlan; every move is a row in
// ms_billing.app_plan_changes (migration 076) and the row is the idempotency
// key of every money step of the move. The owner's rules (2026-09-12/13, via
// the ask/review session, recorded on core-v2#1412):
//
//   - an UPGRADE takes effect at once and charges the DIFFERENCE between the
//     two bases for the remaining days of the current period — the current
//     period was already paid at the plan in force, so charging the new base
//     in full would bill those days twice (Free → Pro at half period = $10,
//     never $20). Collected at once on the account's rail: in credits mode the
//     wallet is drawn for what it holds and the REMAINDER goes to the card;
//     the upgrade is refused only when there is no usable card either;
//   - an upgrade INSIDE THE CREATION GRACE charges nothing separately. The
//     creation charge, when it runs, prices each day at the plan in force that
//     day (days before the change at the old plan, the rest at the new —
//     usage.SegmentedProratedBaseMicros), still as one charge when the grace
//     ends;
//   - a DOWNGRADE takes effect at the next period boundary, with no refund,
//     and can be cancelled for free until then (downgrading and then upgrading
//     back cancels the pending downgrade, no charge). The boundary leg reads
//     the plan in force at the boundary (ApplyDuePlanChanges runs first);
//   - FREE: allowed for a personal account (at most usage.PlanTerms.MaxApps
//     Free apps) and for an org (at most MaxAppsPerOrg); the payer keeps a
//     usable card on file. A Free app never pauses at its limits.
//
// 🔴 NOTHING HERE COLLECTS. The card remainder is SEALED as a ChargeIntent
// through the same proposer every other leg uses; the wallet draw is an
// append-only ledger row. A crash between the two leaves a PENDING row, and
// SweepPendingPlanChanges (driven by cmd/billing-cycle) finishes it — the
// wallet decision is taken once (wallet_decided_at) so a retry seals the same
// document rather than a second one.

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"

	"github.com/mirrorstack-ai/billing-engine/internal/account/billing"
	"github.com/mirrorstack-ai/billing-engine/internal/account/usage"
	"github.com/mirrorstack-ai/billing-engine/internal/billingperiod"
	"github.com/mirrorstack-ai/billing-engine/internal/intent"
	"github.com/mirrorstack-ai/billing-engine/internal/intent/proposer"
)

// PlanChangeKind is the direction of a change: up the ladder or down it.
type PlanChangeKind string

const (
	PlanChangeUpgrade   PlanChangeKind = "upgrade"
	PlanChangeDowngrade PlanChangeKind = "downgrade"
)

// PlanChangeStatus mirrors the ms_billing.app_plan_changes.status CHECK.
//
//	upgrade:   pending  → settled
//	downgrade: scheduled → applied | cancelled
type PlanChangeStatus string

const (
	// PlanChangePending: an upgrade is in force but its money steps have not
	// all committed. The reconciler finishes it.
	PlanChangePending PlanChangeStatus = "pending"
	// PlanChangeSettled: every rail of an upgrade is recorded — or there was
	// nothing to record (a $0 delta, or a change folded into the creation
	// charge).
	PlanChangeSettled PlanChangeStatus = "settled"
	// PlanChangeScheduled: a downgrade waiting for its boundary.
	PlanChangeScheduled PlanChangeStatus = "scheduled"
	// PlanChangeApplied: the boundary arrived and the plan moved.
	PlanChangeApplied PlanChangeStatus = "applied"
	// PlanChangeCancelled: the downgrade was withdrawn before its boundary.
	PlanChangeCancelled PlanChangeStatus = "cancelled"
)

// PlanChange is one ms_billing.app_plan_changes row.
type PlanChange struct {
	ID        uuid.UUID
	AppID     uuid.UUID
	AccountID uuid.UUID
	FromPlan  usage.Plan
	ToPlan    usage.Plan
	Kind      PlanChangeKind
	// RequestedAt is when the owner asked; EffectiveAt when the plan takes
	// (or took) effect — RequestedAt for an upgrade, the boundary for a
	// downgrade.
	RequestedAt time.Time
	EffectiveAt time.Time
	// PeriodStart / PeriodEnd is the anchored period the change was priced
	// against.
	PeriodStart time.Time
	PeriodEnd   time.Time
	// FoldedIntoCreation: an upgrade inside the creation grace; the creation
	// charge prices the split window and this row charged nothing.
	FoldedIntoCreation bool
	// AmountMicros is what the change costs (derived micros); WalletMicros
	// what the wallet drew; CardMicros the whole-cent remainder sealed for
	// the card.
	AmountMicros int64
	WalletMicros int64
	CardMicros   int64
	// WalletDecided is set once the wallet leg has run, even when it drew 0.
	WalletDecided   bool
	WalletDecidedAt time.Time
	// CardRef is "intent:<digest>" once the card remainder is sealed.
	CardRef string
	// CardWindowStart is the card intent's execution-window anchor, stored at
	// the first seal attempt; zero until then.
	CardWindowStart time.Time
	Status          PlanChangeStatus
	SettledAt       time.Time
}

// IntentDigest is the sealed card intent's digest, or "" when no card leg was
// needed (or it has not run yet).
func (c PlanChange) IntentDigest() string {
	const prefix = "intent:"
	if len(c.CardRef) > len(prefix) && c.CardRef[:len(prefix)] == prefix {
		return c.CardRef[len(prefix):]
	}
	return ""
}

// PlanChangeShape is one priced shape of a change: when it takes effect, the
// period it was priced against, and what it costs.
type PlanChangeShape struct {
	EffectiveAt  time.Time
	PeriodStart  time.Time
	PeriodEnd    time.Time
	AmountMicros int64
}

// PlanChangeWalletParams is the wallet leg's input for a charged upgrade.
type PlanChangeWalletParams struct {
	// Credits: the account was classified credits-mode; the store draws the
	// lots (and re-reads the mode under the lock).
	Credits bool
	// AllowRemainder: a usable card exists to take what the wallet does not
	// hold. false + a short wallet = the whole open is rolled back.
	AllowRemainder bool
}

// OpenPlanChangeParams is what the service derived for a new change. The
// store decides the rest under the app row lock — see Store.OpenPlanChange.
type OpenPlanChangeParams struct {
	AppID       uuid.UUID
	AccountID   uuid.UUID
	FromPlan    usage.Plan
	ToPlan      usage.Plan
	Kind        PlanChangeKind
	RequestedAt time.Time
	// Downgrade is the scheduled shape (EffectiveAt = the boundary, amount 0).
	Downgrade *PlanChangeShape
	// Folded is the upgrade's shape when the creation window is still
	// unbilled (amount 0, the creation period); Charged when it is billed
	// (the prorated delta, the current period). The store picks from the
	// locked markers.
	Folded  *PlanChangeShape
	Charged *PlanChangeShape
	// Wallet is the wallet leg for a charged upgrade with money; nil = card
	// only.
	Wallet *PlanChangeWalletParams
	// ChargeAllowed: the service's gates (H10 prepaid, a usable card or a
	// covering wallet) passed for the CHARGED shape. When the store decides
	// charged and this is false — the caller's unlocked read expected a fold
	// — the open is rolled back as PlanChangeChargeRefused rather than
	// leaving a flipped plan nobody can collect for.
	ChargeAllowed bool
}

// OpenPlanChangeOutcome is the store's report from OpenPlanChange, decided
// under the app row lock.
type OpenPlanChangeOutcome int

const (
	// PlanChangeOpened: the row was inserted (and the plan flipped for an
	// upgrade).
	PlanChangeOpened OpenPlanChangeOutcome = iota
	// PlanChangeExisting: the app already has an open change; it is returned
	// instead. A retry of SetAppPlan resumes it.
	PlanChangeExisting
	// PlanChangeAppStale: the locked app row no longer matches the derivation
	// (deleted, absent, or not on FromPlan any more). Nothing was written.
	PlanChangeAppStale
	// PlanChangeCapReached: the destination plan's per-owner cap is full
	// (apps on it plus downgrades scheduled to it). Nothing was written.
	PlanChangeCapReached
	// PlanChangeOpenWalletShort: credits mode, the wallet cannot cover the
	// amount and no card can take the remainder. The whole open was rolled
	// back: no row, no flip, no draw.
	PlanChangeOpenWalletShort
	// PlanChangeChargeRefused: the store decided the upgrade must be charged
	// but the caller's gates had not passed for that shape. Rolled back.
	PlanChangeChargeRefused
)

// PlanChangeWalletOutcome is the store's report from DrawPlanChangeFromWallet.
type PlanChangeWalletOutcome int

const (
	// PlanChangeWalletDecided: the wallet decision is recorded (drawn may be
	// 0 — the account is not in credits mode, or the wallet held nothing).
	PlanChangeWalletDecided PlanChangeWalletOutcome = iota
	// PlanChangeWalletShort: the wallet cannot cover the amount and the
	// caller allowed no card remainder. NOTHING was drawn or decided.
	PlanChangeWalletShort
	// PlanChangeWalletAlreadyDecided: a concurrent retry decided first, or the
	// row is no longer pending. Re-read the row.
	PlanChangeWalletAlreadyDecided
)

// PlanChangeSummary is a change on the wire: what a SetAppPlan call did, or
// the scheduled downgrade GetAppPlan reports.
type PlanChangeSummary struct {
	ID                 uuid.UUID        `json:"id"`
	Kind               PlanChangeKind   `json:"kind"`
	FromPlan           usage.Plan       `json:"from_plan"`
	ToPlan             usage.Plan       `json:"to_plan"`
	Status             PlanChangeStatus `json:"status"`
	RequestedAt        time.Time        `json:"requested_at"`
	EffectiveAt        time.Time        `json:"effective_at"`
	FoldedIntoCreation bool             `json:"folded_into_creation"`
	AmountMicros       int64            `json:"amount_micros"`
	WalletMicros       int64            `json:"wallet_micros"`
	CardMicros         int64            `json:"card_micros"`
	IntentDigest       string           `json:"intent_digest,omitempty"`
}

func summarizePlanChange(c PlanChange) *PlanChangeSummary {
	return &PlanChangeSummary{
		ID: c.ID, Kind: c.Kind, FromPlan: c.FromPlan, ToPlan: c.ToPlan, Status: c.Status,
		RequestedAt: c.RequestedAt, EffectiveAt: c.EffectiveAt, FoldedIntoCreation: c.FoldedIntoCreation,
		AmountMicros: c.AmountMicros, WalletMicros: c.WalletMicros, CardMicros: c.CardMicros,
		IntentDigest: c.IntentDigest(),
	}
}

// GetAppPlanRequest is the payload of GetAppPlan.
type GetAppPlanRequest struct {
	AppID uuid.UUID `json:"app_id"`
}

// AppPlanResponse is GetAppPlan's and SetAppPlan's answer: the app's plan and
// what it includes, so api-platform enforces its gates from the one copy of the
// terms (usage/plans.go) instead of keeping its own.
type AppPlanResponse struct {
	AppID uuid.UUID       `json:"app_id"`
	Terms usage.PlanTerms `json:"terms"`
	// Change is what THIS SetAppPlan call did: the upgrade it charged, the
	// downgrade it scheduled or cancelled. nil on a read and on a no-op.
	Change *PlanChangeSummary `json:"change,omitempty"`
	// PendingChange is the app's scheduled downgrade, if one is waiting for
	// the boundary — the console shows "moves to Free on <date>" from it.
	PendingChange *PlanChangeSummary `json:"pending_change,omitempty"`
	// UsageAllowanceAccrues is false inside the creation grace: the plan's
	// usage allowance is a term but not yet earned (owner 2026-09-13), so a
	// bill shown then deducts nothing. CreationGraceEndsAt says when that
	// changes; zero for an app the roster has not mirrored.
	UsageAllowanceAccrues bool      `json:"usage_allowance_accrues"`
	CreationGraceEndsAt   time.Time `json:"creation_grace_ends_at"`
}

// SetAppPlanRequest is the payload of SetAppPlan.
type SetAppPlanRequest struct {
	AppID uuid.UUID `json:"app_id"`
	Plan  string    `json:"plan"`
}

// effectivePlan is the plan an AppMirror row prices at: its column, or
// DefaultPlan for a row the migration-075 default has not reached (never in
// production; the unit fakes register without one).
func effectivePlan(app AppMirror) usage.Plan {
	if app.Plan == "" {
		return usage.DefaultPlan
	}
	return app.Plan
}

// createdPlan is the plan an app was registered on (migration 077), or the
// plan in force for a row the column has not reached.
func createdPlan(app AppMirror) usage.Plan {
	if app.CreatedPlan == "" {
		return effectivePlan(app)
	}
	return app.CreatedPlan
}

// GetAppPlan reads an app's plan (core-v2#1412). An app the roster has not
// mirrored yet (RegisterApp is fire-and-forget) reads as usage.DefaultPlan, the
// plan migration 075 gives every row, with no pending change and no allowance
// in force.
func (s *Service) GetAppPlan(ctx context.Context, req GetAppPlanRequest) (*AppPlanResponse, error) {
	if req.AppID == uuid.Nil {
		return nil, billing.InvalidInput("app_id required")
	}
	app, found, err := s.store.AppMirror(ctx, req.AppID)
	if err != nil {
		return nil, billing.Internal("app mirror lookup failed", err)
	}
	if !found {
		return &AppPlanResponse{AppID: req.AppID, Terms: usage.TermsFor(usage.DefaultPlan)}, nil
	}
	return s.planResponse(ctx, app, nil)
}

// planResponse builds the answer for a mirrored app: its terms, the scheduled
// downgrade if any, and whether its usage allowance is in force.
func (s *Service) planResponse(ctx context.Context, app AppMirror, change *PlanChange) (*AppPlanResponse, error) {
	resp := &AppPlanResponse{
		AppID:                 app.AppID,
		Terms:                 usage.TermsFor(effectivePlan(app)),
		UsageAllowanceAccrues: usage.UsageAllowanceAccrues(app.CreatedAt, s.nowFn().UTC()),
		CreationGraceEndsAt:   usage.GraceExpiry(app.CreatedAt.UTC()),
	}
	if change != nil {
		resp.Change = summarizePlanChange(*change)
	}
	open, hasOpen, err := s.store.OpenPlanChangeForApp(ctx, app.AppID)
	if err != nil {
		return nil, billing.Internal("open plan change lookup failed", err)
	}
	if hasOpen && open.Status == PlanChangeScheduled {
		resp.PendingChange = summarizePlanChange(open)
	}
	return resp, nil
}

// SetAppPlan moves a live app onto a plan (core-v2#1412). api-platform calls it
// from its change-plan endpoint, after owner/admin authorization and step-up,
// and retries it fire-and-forget: a retry finds the app's open change and
// resumes it, so the same request can never charge twice.
//
// 🔴 `business` IS STILL REFUSED, DELIBERATELY. Its per-app included-module
// pool is not billed yet (the module legs still price the account-wide pool
// of usage.IncludedModules, usage/bill.go), so a Business app would be billed
// module overage its plan includes. PR-2c of billing-engine#202 lifts it.
//
// Outcomes, in the order they are decided:
//
//   - the requested plan is the current one: a scheduled downgrade is
//     cancelled (owner: upgrading back before the boundary cancels it, no
//     charge); otherwise a no-op that returns the terms;
//   - `free` checks the card half of the Free rules (freeEligible); the cap
//     half is the store's, under the owner lock;
//   - a lower rank is a downgrade, scheduled for the period boundary;
//   - a higher rank is an upgrade: folded into the creation charge while the
//     app's creation period is still unbilled, otherwise charged its prorated
//     difference at once (upgradeNow). Which of the two is decided by the
//     store under the app lock, from the row's creation markers.
func (s *Service) SetAppPlan(ctx context.Context, req SetAppPlanRequest) (*AppPlanResponse, error) {
	if req.AppID == uuid.Nil {
		return nil, billing.InvalidInput("app_id required")
	}
	plan, ok := usage.ParsePlan(req.Plan)
	if !ok {
		return nil, billing.InvalidInput("plan must be one of free, pro, business")
	}
	if plan == usage.PlanBusiness {
		return nil, billing.PlanNotAvailable("plan business is not available yet: " +
			"its per-app module allowance is not billed yet (billing-engine#202 PR-2c)")
	}
	app, found, err := s.store.AppMirror(ctx, req.AppID)
	if err != nil {
		return nil, billing.Internal("app mirror lookup failed", err)
	}
	if !found || app.Deleted {
		return nil, billing.NotFound("app not registered or deleted")
	}
	if app.AccountID == uuid.Nil {
		// An UNBILLED org roster row (migration 041): no payer, so no plan to
		// bill a change against. Designating funding attaches it.
		return nil, billing.PaymentRequired("organization has no funding: designate funding before changing the plan")
	}
	now := s.nowFn().UTC()

	open, hasOpen, err := s.store.OpenPlanChangeForApp(ctx, app.AppID)
	if err != nil {
		return nil, billing.Internal("open plan change lookup failed", err)
	}
	if hasOpen && open.Status == PlanChangePending {
		// A retry of an upgrade whose money steps did not all commit — or a
		// different request while one is in flight, which is refused: one
		// change at a time, and this one is not finished.
		if open.ToPlan != plan {
			return nil, billing.Conflict(fmt.Sprintf(
				"a change to plan %s is still being settled; retry after it completes", open.ToPlan))
		}
		settled, err := s.settlePlanChange(ctx, open)
		if err != nil {
			return nil, err
		}
		return s.planResponse(ctx, app, &settled)
	}

	current := effectivePlan(app)
	if plan == current {
		if hasOpen && open.Status == PlanChangeScheduled {
			cancelled, err := s.store.CancelScheduledPlanChange(ctx, app.AppID, now)
			if err != nil {
				return nil, billing.Internal("cancel scheduled plan change failed", err)
			}
			if cancelled {
				open.Status = PlanChangeCancelled
				open.SettledAt = now
				return s.planResponse(ctx, app, &open)
			}
		}
		return s.planResponse(ctx, app, nil)
	}
	if plan == usage.PlanFree {
		if err := s.freeEligible(ctx, app.AccountID); err != nil {
			return nil, err
		}
	}
	if usage.PlanRank(plan) < usage.PlanRank(current) {
		return s.scheduleDowngrade(ctx, app, current, plan, now, open, hasOpen)
	}
	return s.upgradeNow(ctx, app, current, plan, now, hasOpen)
}

// freeEligible is the card half of the Free gate: the payer keeps a usable
// non-fraud card on its FUNDING account (the same predicate RegisterApp's
// create gate applies — Free is cheaper to start, not card-less). The cap
// half — usage.PlanTerms.MaxApps per personal account, MaxAppsPerOrg per org
// (owner 2026-09-13), counting apps already Free AND downgrades scheduled to
// Free — is the STORE's, taken under the owner lock inside the transaction
// that commits the change (OpenPlanChange / InsertFreeAppMirror), so two
// concurrent commitments cannot both take the last slot.
func (s *Service) freeEligible(ctx context.Context, accountID uuid.UUID) error {
	fundingID, err := s.store.ChargeFundingAccount(ctx, accountID)
	if err != nil {
		return billing.Internal("funding account lookup failed", err)
	}
	cards, err := s.store.UsableNonFraudCardCount(ctx, fundingID)
	if err != nil {
		return billing.Internal("usable card count read failed", err)
	}
	if cards < 1 {
		return billing.PaymentRequired("no usable payment card on file: a Free app still needs a card on file")
	}
	return nil
}

// planLimitError is the cap refusal, worded per owner kind.
func planLimitError(plan usage.Plan, orgOwned bool) error {
	terms := usage.TermsFor(plan)
	scope := "personal account"
	if orgOwned {
		scope = "organization"
	}
	return billing.PlanLimit(fmt.Sprintf("plan %s allows at most %d app(s) per %s", plan, terms.MaxAppsFor(orgOwned), scope))
}

// scheduleDowngrade opens a downgrade for the boundary of the account's
// current anchored period. Nothing is charged and the plan stays where it is
// until the apply moves it at that boundary. Idempotent: a repeat of the same
// request returns the scheduled row.
func (s *Service) scheduleDowngrade(
	ctx context.Context, app AppMirror, from, to usage.Plan, now time.Time, open PlanChange, hasOpen bool,
) (*AppPlanResponse, error) {
	if hasOpen {
		if open.ToPlan == to {
			return s.planResponse(ctx, app, &open)
		}
		return nil, billing.Conflict(fmt.Sprintf(
			"a downgrade to plan %s is already scheduled; cancel it by asking for the current plan first", open.ToPlan))
	}
	activatedAt, activated, err := s.store.AccountActivation(ctx, app.AccountID)
	if err != nil {
		return nil, billing.Internal("account activation lookup failed", err)
	}
	if !activated {
		return nil, billing.PaymentRequired("billing account not activated: add a payment card before changing the plan")
	}
	periodStart, periodEnd := billingperiod.AnchoredPeriodWindow(now, billingperiod.AnchorDay(activatedAt))
	change, outcome, err := s.store.OpenPlanChange(ctx, OpenPlanChangeParams{
		AppID: app.AppID, AccountID: app.AccountID, FromPlan: from, ToPlan: to,
		Kind: PlanChangeDowngrade, RequestedAt: now,
		Downgrade: &PlanChangeShape{EffectiveAt: periodEnd, PeriodStart: periodStart, PeriodEnd: periodEnd},
	})
	if err != nil {
		return nil, billing.Internal("open plan change failed", err)
	}
	switch outcome {
	case PlanChangeAppStale:
		return nil, billing.Conflict("the app changed while the plan change was being opened; retry")
	case PlanChangeCapReached:
		return nil, planLimitError(to, app.OwnerOrgID != uuid.Nil)
	}
	return s.planResponse(ctx, app, &change)
}

// upgradeNow moves the app up the ladder at once. Both shapes are derived
// here — folded (the creation window is still unbilled: charge nothing, let
// the creation charge price the days) and charged (the prorated difference,
// collected now) — and the STORE picks one under the app lock from the row's
// creation markers, never from this function's unlocked read of them.
//
// Every gate runs BEFORE the open, and the store refuses a charged decision
// the gates did not pass (ChargeAllowed): the refusal the owner allowed ("no
// usable card either") must leave no trace, and a plan that flipped without
// the money being collectable would be exactly the partial state the ledger
// exists to prevent. Once the row is open the money steps are resumable
// (settlePlanChange), so a crash after this point is a pending row, never a
// lost charge.
func (s *Service) upgradeNow(
	ctx context.Context, app AppMirror, from, to usage.Plan, now time.Time, hasOpen bool,
) (*AppPlanResponse, error) {
	if hasOpen {
		// A scheduled downgrade exists and the request is for a plan ABOVE the
		// current one. Unreachable while business is refused (the only
		// downgrade is pro → free and the only upgrade from pro is business);
		// refused rather than silently stacked when the ladder grows.
		return nil, billing.Conflict("a downgrade is scheduled; cancel it by asking for the current plan first")
	}
	activatedAt, activated, err := s.store.AccountActivation(ctx, app.AccountID)
	if err != nil {
		return nil, billing.Internal("account activation lookup failed", err)
	}
	if !activated {
		return nil, billing.PaymentRequired("billing account not activated: add a payment card before changing the plan")
	}
	anchorDay := billingperiod.AnchorDay(activatedAt)
	periodStart, periodEnd := billingperiod.AnchoredPeriodWindow(now, anchorDay)
	creationStart, creationEnd := billingperiod.AnchoredPeriodWindow(app.CreatedAt.UTC(), anchorDay)

	// The difference between the bases for the remaining days, the change day
	// inclusive at the NEW plan (split by day, like the fold).
	amount, err := upgradeDeltaMicros(from, to, now, periodStart, periodEnd)
	if err != nil {
		return nil, billing.Internal("upgrade delta derivation failed", err)
	}
	folded := &PlanChangeShape{EffectiveAt: now, PeriodStart: creationStart, PeriodEnd: creationEnd}
	charged := &PlanChangeShape{EffectiveAt: now, PeriodStart: periodStart, PeriodEnd: periodEnd, AmountMicros: amount}

	// The unlocked HINT of which shape applies — only to pick the refusal
	// message when the gates fail. The store decides for real.
	expectFold := app.ProrationInvoiceID == "" && !app.ProrationSkipped && !app.ProrationAttempted

	// Gates for the CHARGED shape, all before the write. H10: a prepaid
	// account is never charged off-session by any leg, and an upgrade's charge
	// is an off-session debit on a stored instrument however it was requested.
	chargeAllowed := true
	var wallet *PlanChangeWalletParams
	var refusal error
	if amount > 0 {
		permitted, err := s.offSessionChargePermitted(ctx, app.AccountID)
		if err != nil {
			return nil, err
		}
		walletState, walletAllowed, err := s.creditWalletChargeState(ctx, app.AccountID, periodStart, periodEnd)
		if err != nil {
			return nil, billing.Internal("wallet route classification failed", err)
		}
		credits := walletAllowed && walletState.Mode == CreditBillingModeCredits
		_, cardOK, err := s.resolveChargeableCustomer(ctx, app.AccountID)
		if err != nil {
			return nil, err
		}
		wallet = &PlanChangeWalletParams{Credits: credits, AllowRemainder: cardOK}
		switch {
		case !permitted:
			chargeAllowed = false
			refusal = billing.PaymentRequired("account is in prepaid collection mode: an upgrade cannot be charged off-session")
		case !cardOK && !credits:
			// Owner: draw what the wallet holds and charge the remainder to
			// the card; refuse only with no usable card either. A standard
			// account has no wallet leg here (its credit applies at the
			// boundary spine, like the creation charge), so for it the card
			// is the only rail.
			chargeAllowed = false
			refusal = billing.PaymentRequired("no usable payment card on file: an upgrade needs a card for the remainder")
		}
		if !chargeAllowed && !expectFold {
			return nil, refusal
		}
	}

	change, outcome, err := s.store.OpenPlanChange(ctx, OpenPlanChangeParams{
		AppID: app.AppID, AccountID: app.AccountID, FromPlan: from, ToPlan: to,
		Kind: PlanChangeUpgrade, RequestedAt: now,
		Folded: folded, Charged: charged, Wallet: wallet, ChargeAllowed: chargeAllowed,
	})
	if err != nil {
		return nil, billing.Internal("open plan change failed", err)
	}
	switch outcome {
	case PlanChangeAppStale:
		return nil, billing.Conflict("the app changed while the plan change was being opened; retry")
	case PlanChangeCapReached:
		return nil, planLimitError(to, app.OwnerOrgID != uuid.Nil)
	case PlanChangeOpenWalletShort:
		return nil, billing.PaymentRequired("no usable payment card on file and the credit wallet does not cover the upgrade")
	case PlanChangeChargeRefused:
		if refusal == nil {
			refusal = billing.PaymentRequired("the upgrade must be charged now and no rail can collect it")
		}
		return nil, refusal
	case PlanChangeExisting:
		if change.Status != PlanChangePending || change.ToPlan != to {
			return nil, billing.Conflict(fmt.Sprintf("a change to plan %s is already open", change.ToPlan))
		}
	}
	app.Plan = to
	if change.Status == PlanChangeSettled {
		return s.planResponse(ctx, app, &change)
	}
	settled, err := s.settlePlanChange(ctx, change)
	if err != nil {
		return nil, err
	}
	return s.planResponse(ctx, app, &settled)
}

// settlePlanChange runs a pending upgrade's remaining money steps, from
// whatever state the row is in. Each step is idempotent against the row:
//
//  1. the H10 gate, on EVERY resume: a prepaid account is never charged
//     off-session, so a row resumed after the account tightened stays
//     pending (reported as PaymentRequired; the reconciler counts it skipped)
//     until the account relaxes;
//  2. the WALLET DECISION, if the row was written without one (a row that
//     predates the decision moving into the open): the same lots-only draw,
//     once;
//  3. the CARD REMAINDER, sealed as one intent whose lines total the gross
//     and whose wallet allocation is the draw — so the provider is handed
//     exactly the whole-cent remainder. The execution window opens at the
//     anchor STORED on the row at the first seal attempt (the request instant,
//     or the first seal instant if the window since closed) and the line's
//     label carries the app id only, so a retry after a crash — or after a
//     rename — seals the same digest, and the row settles with its reference.
//
// A row left pending — the card vanished between the gate and the seal, the
// account went prepaid, or the proposal failed — is finished by
// SweepPendingPlanChanges. It is never reversed: the wallet draw was money
// the customer owed for a plan they are on, and the plan is in force.
func (s *Service) settlePlanChange(ctx context.Context, change PlanChange) (PlanChange, error) {
	if change.Status == PlanChangeSettled {
		return change, nil
	}
	if change.Kind != PlanChangeUpgrade || change.Status != PlanChangePending {
		return change, billing.Internal(fmt.Sprintf("plan change %s is %s %s and cannot be settled", change.ID, change.Kind, change.Status), nil)
	}
	now := s.nowFn().UTC()
	if permitted, err := s.offSessionChargePermitted(ctx, change.AccountID); err != nil {
		return change, err
	} else if !permitted {
		return change, billing.PaymentRequired("account is in prepaid collection mode: the upgrade's charge waits until it relaxes")
	}
	_, cardOK, err := s.resolveChargeableCustomer(ctx, change.AccountID)
	if err != nil {
		return change, err
	}

	if !change.WalletDecided {
		outcome, drawn, err := s.store.DrawPlanChangeFromWallet(ctx, change, cardOK, now)
		if err != nil {
			if _, ok := err.(*billing.Error); ok {
				return change, err
			}
			return change, billing.Internal("plan change wallet draw failed", err)
		}
		switch outcome {
		case PlanChangeWalletShort:
			return change, billing.PaymentRequired("no usable payment card on file and the credit wallet does not cover the upgrade")
		case PlanChangeWalletDecided:
			if drawn > 0 {
				s.observeWalletMutation(ctx, change.AccountID)
			}
		}
		latest, found, err := s.store.PlanChange(ctx, change.ID)
		if err != nil {
			return change, billing.Internal("plan change re-read failed", err)
		}
		if !found {
			return change, billing.Internal("plan change vanished while being settled", nil)
		}
		change = latest
		if change.Status == PlanChangeSettled {
			return change, nil
		}
	} else if change.WalletMicros > 0 && change.CardRef == "" && change.CardWindowStart.IsZero() {
		// The open drew the wallet in its own transaction; the standing push
		// happens here, once, on the first pass after it.
		s.observeWalletMutation(ctx, change.AccountID)
	}

	remainder := change.AmountMicros - change.WalletMicros
	if remainder < 0 {
		remainder = 0
	}
	sealMicros, err := collectableMicros(remainder)
	if err != nil {
		return change, billing.Internal("micros to collectable micros conversion failed", err)
	}
	cardRef := ""
	if sealMicros > 0 {
		if !cardOK {
			return change, billing.PaymentRequired("no usable payment card on file for the upgrade's remainder; it will be retried")
		}
		if s.proposer == nil {
			return change, billing.Internal(
				"a plan upgrade has no intent proposer installed and this leg holds no charge path of its own; "+
					"this deployment cannot bill an upgrade", nil)
		}
		// The window anchor: the request instant, unless the window that
		// would open there has already closed — then this seal instant.
		// Stored, so a retry seals the same digest; re-anchored only when
		// the stored window itself has closed (a failed seal's leftover, or a
		// document that is dead anyway), never to seal what cannot collect.
		anchor := change.RequestedAt
		if !now.Before(change.RequestedAt.Add(executionWindow)) {
			anchor = now
		}
		windowStart, err := s.store.EnsurePlanChangeCardWindow(ctx, change.ID, anchor, now.Add(-executionWindow))
		if err != nil {
			return change, billing.Internal("plan change card window anchor failed", err)
		}
		sealed, err := s.proposer.Propose(ctx, planChangeCharge(change, sealMicros, windowStart))
		if err != nil {
			return change, billing.Internal("propose plan change intent failed", err)
		}
		cardRef = "intent:" + sealed.Digest()
	}
	if _, err := s.store.SettlePlanChangeCard(ctx, change.ID, sealMicros, cardRef, now); err != nil {
		return change, billing.Internal("settle plan change failed", err)
	}
	latest, found, err := s.store.PlanChange(ctx, change.ID)
	if err != nil {
		return change, billing.Internal("plan change re-read failed", err)
	}
	if !found {
		return change, billing.Internal("plan change vanished while being settled", nil)
	}
	return latest, nil
}

// upgradeDeltaMicros is what a mid-period upgrade from → to costs at `at`:
// (to's base − from's base) × the remaining whole days of [periodStart,
// periodEnd), the change day inclusive at the new plan, rounded half-up once.
//
// 🔴 THE DIFFERENCE, NEVER THE NEW PRICE (owner 2026-09-13): the current
// period was already paid at `from`, so charging `to` in full would bill those
// days twice. Free → Pro at half period = $10, Pro → Business at half period =
// $15 — the pure helper is pinned on both so the second cannot regress while
// business is still refused at the RPC.
func upgradeDeltaMicros(from, to usage.Plan, at, periodStart, periodEnd time.Time) (int64, error) {
	delta := usage.TermsFor(to).BaseFeeMicros - usage.TermsFor(from).BaseFeeMicros
	if delta < 0 {
		return 0, fmt.Errorf("upgrade %s → %s has a negative base difference %d", from, to, delta)
	}
	return usage.ProratedSegmentMicros(delta, at, periodEnd, periodStart, periodEnd), nil
}

// planChangeCharge is the upgrade's card leg as one sealed charge: one line
// for the gross (wallet draw + whole-cent remainder) with the draw stated as
// the wallet allocation, so the provider remainder Seal derives is exactly
// sealMicros. Sealing the remainder alone would say the customer was charged
// less than they owed; sealing the raw derived micros would attest to a figure
// the card was never charged.
//
// 🔴 RENAME-STABLE, RETRY-STABLE. The line names the app by id only — a
// display name inside the digest made a rename between a crashed seal and its
// retry a second document for one charge — and the execution window opens at
// the anchor stored on the row, not at "now".
func planChangeCharge(change PlanChange, sealMicros int64, windowStart time.Time) proposer.Charge {
	return proposer.Charge{
		AccountID: change.AccountID.String(),
		Kind:      intent.KindPlatformBase,
		Currency:  chargeCurrency,
		Lines: proposer.SingleLine(
			fmt.Sprintf("MirrorStack plan upgrade %s → %s (prorated) — app %s", change.FromPlan, change.ToPlan, change.AppID),
			planChangeRef(change.ID),
			change.WalletMicros+sealMicros,
		),
		WalletAllocationMicros: change.WalletMicros,

		AuthorizationID:   "plan-change:" + change.AccountID.String(),
		TermsRevision:     proposedTermsRevision,
		PriceBookRevision: proposedPriceBookRevision,
		NoticePolicy:      proposedNoticePolicy,
		SelectedRail:      proposedRail,

		RoutingPolicyRevision: proposedRoutingPolicy,
		// Zero tax, resolved — the same honest state every other leg records
		// until docs/DESIGN.md §12's tax decisions change it.
		Tax: intent.TaxDetermination{
			Resolved:     true,
			Jurisdiction: "not-applicable",
			RuleRevision: proposedTaxRuleRevision,
			Verification: intent.TaxNotApplicable,
		},
		ExecuteNotBefore: windowStart,
		ExecuteNotAfter:  windowStart.Add(executionWindow),
	}
}

// planChangeRef ties the sealed line back to its ledger row.
func planChangeRef(id uuid.UUID) string { return "plan-change:" + id.String() }

// SweepPlanChangesResult tallies one SweepPendingPlanChanges batch.
type SweepPlanChangesResult struct {
	Pending int // upgrades whose money steps had not all committed
	Settled int // finished this sweep
	Skipped int // still pending (no usable card yet, or prepaid); retried next sweep
	Failed  int // per-change errors; retried next sweep
}

// SweepPendingPlanChanges is the reconciler: it finishes every upgrade whose
// money steps did not all commit — a crash between the open and the card
// seal, a card that vanished between the gate and the seal, a proposer
// outage, an account that went prepaid. Driven by cmd/billing-cycle after
// the other sweeps. Idempotent: a settled row drops out of the work list, and
// a row that still cannot be finished is counted and left for the next sweep.
func (s *Service) SweepPendingPlanChanges(ctx context.Context, at time.Time) (*SweepPlanChangesResult, error) {
	if at.IsZero() {
		return nil, billing.InvalidInput("sweep instant required")
	}
	pending, err := s.store.PendingPlanChanges(ctx, at.UTC())
	if err != nil {
		return nil, billing.Internal("list pending plan changes failed", err)
	}
	res := &SweepPlanChangesResult{Pending: len(pending)}
	for _, change := range pending {
		settled, err := s.settlePlanChange(ctx, change)
		switch {
		case err == nil && settled.Status == PlanChangeSettled:
			res.Settled++
		case err != nil && isPaymentRequired(err):
			res.Skipped++
		case err != nil:
			slog.ErrorContext(ctx, "plan change settlement failed",
				"plan_change_id", change.ID, "app_id", change.AppID, "error", err)
			res.Failed++
			continue
		default:
			res.Skipped++
		}
		slog.InfoContext(ctx, "plan change sweep",
			"plan_change_id", change.ID, "app_id", change.AppID, "from", change.FromPlan, "to", change.ToPlan,
			"status", string(settled.Status), "amount_micros", change.AmountMicros,
			"wallet_micros", settled.WalletMicros, "card_micros", settled.CardMicros, "card_ref", settled.CardRef)
	}
	return res, nil
}

// ApplyDuePlanChanges moves every scheduled downgrade whose boundary has
// arrived, on every account, onto its plan — the driver's global apply, run
// before the charge phase so the boundary reads the plan in force, and
// reaching the accounts the charge phase never does (an account whose only
// apps are in their creation grace has no boundary run). RunBillingCycle
// applies its own account's again as the belt.
func (s *Service) ApplyDuePlanChanges(ctx context.Context, at time.Time) (applied, cancelled int, err error) {
	if at.IsZero() {
		return 0, 0, billing.InvalidInput("apply instant required")
	}
	applied, cancelled, err = s.store.ApplyAllDuePlanChanges(ctx, at.UTC())
	if err != nil {
		return 0, 0, billing.Internal("apply due plan changes failed", err)
	}
	return applied, cancelled, nil
}

func isPaymentRequired(err error) bool {
	be, ok := err.(*billing.Error)
	return ok && be.Code == billing.CodePaymentRequired
}
