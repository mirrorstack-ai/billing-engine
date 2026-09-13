package cycle_test

// The fakeStore half of the plan-change ledger (migration 076) and the member
// history (migration 077). Mirrors plan_change_store.go's contracts — the
// open row winning over a stale derivation, the fold decided from the row's
// markers, the cap counting scheduled downgrades, the wallet decision inside
// the open with a lots-only, balance-capped draw, the apply's cap re-check —
// over the in-memory maps service_test.go owns.

import (
	"context"
	"errors"
	"sort"
	"time"

	"github.com/google/uuid"

	"github.com/mirrorstack-ai/billing-engine/internal/account/cycle"
	"github.com/mirrorstack-ai/billing-engine/internal/account/usage"
)

var errFakePlanChangeMissing = errors.New("fake store: no such plan change")

// fakeMemberCount is one app_member_counts row.
type fakeMemberCount struct {
	count int
	at    time.Time
}

// fakeEffectivePlan is the plan a fake roster row prices at: its column, or
// the migration-075 default for a row a test built without one.
func fakeEffectivePlan(app cycle.AppMirror) usage.Plan {
	if app.Plan == "" {
		return usage.DefaultPlan
	}
	return app.Plan
}

func (f *fakeStore) openPlanChangeFor(appID uuid.UUID) (cycle.PlanChange, bool) {
	for _, c := range f.planChanges {
		if c.AppID == appID && (c.Status == cycle.PlanChangePending || c.Status == cycle.PlanChangeScheduled) {
			return c, true
		}
	}
	return cycle.PlanChange{}, false
}

// planCommitments mirrors CountUser/OrgPlanCommitments: live apps on `plan`
// or with a downgrade scheduled to it, for the owner, excluding exceptAppID.
func (f *fakeStore) planCommitments(accountID, ownerOrgID uuid.UUID, plan usage.Plan, exceptAppID uuid.UUID) int {
	n := 0
	for _, app := range f.apps {
		if app.Deleted || app.AppID == exceptAppID {
			continue
		}
		if ownerOrgID != uuid.Nil {
			if app.OwnerOrgID != ownerOrgID {
				continue
			}
		} else if app.AccountID != accountID || app.OwnerOrgID != uuid.Nil {
			continue
		}
		committed := fakeEffectivePlan(app) == plan
		if !committed {
			if open, has := f.openPlanChangeFor(app.AppID); has && open.Status == cycle.PlanChangeScheduled && open.ToPlan == plan {
				committed = true
			}
		}
		if committed {
			n++
		}
	}
	return n
}

func (f *fakeStore) capReached(accountID, ownerOrgID uuid.UUID, plan usage.Plan, exceptAppID uuid.UUID) bool {
	limit := usage.TermsFor(plan).MaxAppsFor(ownerOrgID != uuid.Nil)
	if limit == usage.Unlimited {
		return false
	}
	return f.planCommitments(accountID, ownerOrgID, plan, exceptAppID) >= limit
}

// spendableLots mirrors WalletSpendableLots in the creation draw's tier order.
func (f *fakeStore) spendableLots() []*fakeWalletSource {
	sources := make([]*fakeWalletSource, 0, len(f.walletSources))
	for _, source := range f.walletSources {
		if source.remaining > 0 && (source.expiresAt.IsZero() || source.expiresAt.After(time.Now())) {
			sources = append(sources, source)
		}
	}
	sort.Slice(sources, func(i, j int) bool {
		a, b := sources[i], sources[j]
		tier := func(source *fakeWalletSource) int {
			switch {
			case source.typ == "grant" && !source.expiresAt.IsZero():
				return 0
			case source.typ == "grant", source.typ == "preallocation", source.typ == "refund", source.typ == "adjustment":
				return 1
			default:
				return 2
			}
		}
		if tier(a) != tier(b) {
			return tier(a) < tier(b)
		}
		return a.createdAt.Before(b.createdAt)
	})
	return sources
}

// drawPlanChangeLots mirrors the store's lots-only, balance-capped draw. The
// fake's posted balance is Σ remaining − walletUnallocated − walletNegativeAdjust;
// a short wallet with no remainder allowed draws nothing.
func (f *fakeStore) drawPlanChangeLots(changeID uuid.UUID, amount int64, allowRemainder bool) (drawn int64, short bool) {
	if f.walletMode != cycle.CreditBillingModeCredits {
		return 0, !allowRemainder
	}
	sources := f.spendableLots()
	var coverable int64
	for _, s := range sources {
		coverable += s.remaining
	}
	capMicros := coverable - f.walletUnallocated - f.walletNegativeAdjust
	if capMicros < 0 {
		capMicros = 0
	}
	if coverable > capMicros {
		coverable = capMicros
	}
	if coverable < amount && !allowRemainder {
		return 0, true
	}
	left := amount
	if left > coverable {
		left = coverable
	}
	for _, source := range sources {
		if left == 0 {
			break
		}
		consume := source.remaining
		if consume > left {
			consume = left
		}
		source.remaining -= consume
		left -= consume
		drawn += consume
	}
	f.planChangeDraws[changeID] += drawn
	return drawn, false
}

func (f *fakeStore) OpenPlanChange(_ context.Context, p cycle.OpenPlanChangeParams) (cycle.PlanChange, cycle.OpenPlanChangeOutcome, error) {
	if f.errOpenPlanChange != nil {
		return cycle.PlanChange{}, 0, f.errOpenPlanChange
	}
	if f.beforePlanChangeOpen != nil {
		hook := f.beforePlanChangeOpen
		f.beforePlanChangeOpen = nil
		hook(f, p.AppID)
	}
	app, ok := f.apps[p.AppID]
	if !ok {
		return cycle.PlanChange{}, cycle.PlanChangeAppStale, nil
	}
	if existing, has := f.openPlanChangeFor(p.AppID); has {
		return existing, cycle.PlanChangeExisting, nil // the open row wins over a stale derivation
	}
	if app.Deleted || fakeEffectivePlan(app) != p.FromPlan || app.AccountID != p.AccountID {
		return cycle.PlanChange{}, cycle.PlanChangeAppStale, nil
	}
	if f.capReached(p.AccountID, app.OwnerOrgID, p.ToPlan, p.AppID) {
		return cycle.PlanChange{}, cycle.PlanChangeCapReached, nil
	}
	shape := p.Downgrade
	folded := false
	status := cycle.PlanChangeScheduled
	if p.Kind == cycle.PlanChangeUpgrade {
		unbilled := app.ProrationInvoiceID == "" && !app.ProrationSkipped && !app.ProrationAttempted
		folded = unbilled && !p.FoldUntil.IsZero() && p.RequestedAt.Before(p.FoldUntil)
		if folded {
			shape = p.Folded
		} else {
			shape = p.Charged
		}
		status = cycle.PlanChangePending
		if shape.AmountMicros == 0 {
			status = cycle.PlanChangeSettled
		}
		if status == cycle.PlanChangePending && !p.ChargeAllowed {
			return cycle.PlanChange{}, cycle.PlanChangeChargeRefused, nil
		}
	}
	c := cycle.PlanChange{
		ID: uuid.New(), AppID: p.AppID, AccountID: p.AccountID,
		FromPlan: p.FromPlan, ToPlan: p.ToPlan, Kind: p.Kind,
		RequestedAt: p.RequestedAt.UTC(), EffectiveAt: shape.EffectiveAt.UTC(),
		PeriodStart: shape.PeriodStart.UTC(), PeriodEnd: shape.PeriodEnd.UTC(),
		FoldedIntoCreation: folded, AmountMicros: shape.AmountMicros, Status: status,
	}
	if status == cycle.PlanChangeSettled {
		c.WalletDecided, c.WalletDecidedAt, c.SettledAt = true, c.RequestedAt, c.RequestedAt
	}
	if status == cycle.PlanChangePending {
		var drawn int64
		if p.Wallet != nil && p.Wallet.Credits {
			var short bool
			drawn, short = f.drawPlanChangeLots(c.ID, c.AmountMicros, p.Wallet.AllowRemainder)
			if short {
				return cycle.PlanChange{}, cycle.PlanChangeOpenWalletShort, nil // rolled back: nothing written
			}
		}
		c.WalletMicros, c.WalletDecided, c.WalletDecidedAt = drawn, true, c.RequestedAt
		if drawn >= c.AmountMicros {
			c.Status, c.SettledAt = cycle.PlanChangeSettled, c.RequestedAt
		}
	}
	f.planChanges[c.ID] = c
	if p.Kind == cycle.PlanChangeUpgrade {
		app.Plan = p.ToPlan // the flip commits with the row
		f.apps[p.AppID] = app
	}
	return c, cycle.PlanChangeOpened, nil
}

func (f *fakeStore) OpenPlanChangeForApp(_ context.Context, appID uuid.UUID) (cycle.PlanChange, bool, error) {
	c, ok := f.openPlanChangeFor(appID)
	return c, ok, nil
}

func (f *fakeStore) PlanChange(_ context.Context, id uuid.UUID) (cycle.PlanChange, bool, error) {
	c, ok := f.planChanges[id]
	return c, ok, nil
}

func (f *fakeStore) PendingPlanChanges(_ context.Context, requestedBefore time.Time) ([]cycle.PlanChange, error) {
	var out []cycle.PlanChange
	for _, c := range f.planChanges {
		if c.Status == cycle.PlanChangePending && !c.RequestedAt.After(requestedBefore) {
			out = append(out, c)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].RequestedAt.Before(out[j].RequestedAt) })
	return out, nil
}

func (f *fakeStore) EffectivePlanChanges(_ context.Context, appID uuid.UUID) ([]cycle.PlanChange, error) {
	var out []cycle.PlanChange
	for _, c := range f.planChanges {
		if c.AppID == appID && (c.Status == cycle.PlanChangePending || c.Status == cycle.PlanChangeSettled || c.Status == cycle.PlanChangeApplied) {
			out = append(out, c)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].EffectiveAt.Before(out[j].EffectiveAt) })
	return out, nil
}

// DrawPlanChangeFromWallet takes the decision for a row written without one.
func (f *fakeStore) DrawPlanChangeFromWallet(_ context.Context, change cycle.PlanChange, allowRemainder bool, at time.Time) (cycle.PlanChangeWalletOutcome, int64, error) {
	if f.errPlanChangeDraw != nil {
		return 0, 0, f.errPlanChangeDraw
	}
	c, ok := f.planChanges[change.ID]
	if !ok || c.Status != cycle.PlanChangePending || c.WalletDecided || c.AmountMicros <= 0 {
		return cycle.PlanChangeWalletAlreadyDecided, 0, nil
	}
	drawn, short := f.drawPlanChangeLots(c.ID, c.AmountMicros, allowRemainder)
	if short {
		return cycle.PlanChangeWalletShort, 0, nil
	}
	c.WalletMicros, c.WalletDecided, c.WalletDecidedAt = drawn, true, at.UTC()
	if drawn >= c.AmountMicros {
		c.Status, c.SettledAt = cycle.PlanChangeSettled, at.UTC()
	}
	f.planChanges[c.ID] = c
	return cycle.PlanChangeWalletDecided, drawn, nil
}

func (f *fakeStore) EnsurePlanChangeCardWindow(_ context.Context, id uuid.UUID, windowStart, reanchorBefore time.Time) (time.Time, error) {
	c, ok := f.planChanges[id]
	if !ok {
		return time.Time{}, errFakePlanChangeMissing
	}
	if c.CardWindowStart.IsZero() || c.CardWindowStart.Before(reanchorBefore) {
		c.CardWindowStart = windowStart.UTC()
		f.planChanges[id] = c
	}
	return c.CardWindowStart, nil
}

func (f *fakeStore) SettlePlanChangeCard(_ context.Context, id uuid.UUID, cardMicros int64, cardRef string, at time.Time) (bool, error) {
	if f.errSettlePlanCard != nil {
		return false, f.errSettlePlanCard
	}
	c, ok := f.planChanges[id]
	if !ok || c.Status != cycle.PlanChangePending {
		return false, nil
	}
	c.CardMicros, c.CardRef, c.Status, c.SettledAt = cardMicros, cardRef, cycle.PlanChangeSettled, at.UTC()
	f.planChanges[id] = c
	return true, nil
}

func (f *fakeStore) CancelScheduledPlanChange(_ context.Context, appID uuid.UUID, at time.Time) (bool, error) {
	if f.beforeCancelScheduled != nil {
		hook := f.beforeCancelScheduled
		f.beforeCancelScheduled = nil
		hook(f, appID)
	}
	for id, c := range f.planChanges {
		if c.AppID == appID && c.Status == cycle.PlanChangeScheduled {
			c.Status, c.SettledAt = cycle.PlanChangeCancelled, at.UTC()
			f.planChanges[id] = c
			return true, nil
		}
	}
	return false, nil
}

// applyDue mirrors applyDueRows: a due downgrade whose destination cap is
// full at the boundary is cancelled, the rest flip and close.
func (f *fakeStore) applyDue(filter func(cycle.PlanChange) bool, dueAt time.Time) (applied, cancelled int) {
	ids := make([]uuid.UUID, 0, len(f.planChanges))
	for id, c := range f.planChanges {
		if c.Status == cycle.PlanChangeScheduled && !c.EffectiveAt.After(dueAt) && filter(c) {
			ids = append(ids, id)
		}
	}
	sort.Slice(ids, func(i, j int) bool {
		return f.planChanges[ids[i]].EffectiveAt.Before(f.planChanges[ids[j]].EffectiveAt)
	})
	for _, id := range ids {
		c := f.planChanges[id]
		app, ok := f.apps[c.AppID]
		// A deleted app's downgrade is cancelled, never applied (its plan is
		// frozen with the row); so is one whose destination cap is full.
		if !ok || app.Deleted || f.capReached(c.AccountID, app.OwnerOrgID, c.ToPlan, c.AppID) {
			c.Status, c.SettledAt = cycle.PlanChangeCancelled, dueAt.UTC()
			f.planChanges[id] = c
			cancelled++
			continue
		}
		app.Plan = c.ToPlan
		f.apps[c.AppID] = app
		c.Status, c.SettledAt = cycle.PlanChangeApplied, dueAt.UTC()
		f.planChanges[id] = c
		applied++
	}
	return applied, cancelled
}

func (f *fakeStore) ApplyDuePlanChanges(_ context.Context, accountID uuid.UUID, dueAt time.Time) (int, error) {
	if f.errApplyPlanChange != nil {
		return 0, f.errApplyPlanChange
	}
	applied, _ := f.applyDue(func(c cycle.PlanChange) bool { return c.AccountID == accountID }, dueAt)
	return applied, nil
}

func (f *fakeStore) ApplyAllDuePlanChanges(_ context.Context, dueAt time.Time) (int, int, error) {
	if f.errApplyPlanChange != nil {
		return 0, 0, f.errApplyPlanChange
	}
	applied, cancelled := f.applyDue(func(cycle.PlanChange) bool { return true }, dueAt)
	return applied, cancelled, nil
}

func (f *fakeStore) InsertFreeAppMirror(ctx context.Context, appID, accountID, ownerOrgID uuid.UUID, moduleCount, memberCount int, createdAt time.Time, name string) (bool, error) {
	if _, exists := f.apps[appID]; exists {
		return false, nil
	}
	if f.capReached(accountID, ownerOrgID, usage.PlanFree, appID) {
		return true, nil
	}
	return false, f.InsertAppMirror(ctx, appID, accountID, ownerOrgID, moduleCount, memberCount, createdAt, name, usage.PlanFree)
}

func (f *fakeStore) SetAppMemberCount(_ context.Context, appID uuid.UUID, memberCount int, at time.Time) error {
	if app, ok := f.apps[appID]; ok && !app.Deleted {
		app.MemberCount = memberCount // deleted rows are frozen (WHERE deleted_at IS NULL)
		f.apps[appID] = app
		f.memberHistory[appID] = append(f.memberHistory[appID], fakeMemberCount{count: memberCount, at: at.UTC()})
	}
	return nil
}

// planAtInstant mirrors the ledger derivation the SQL does: the plan an app
// was on just before `at` is the from_plan of the earliest effective change
// (pending/settled/applied) at or after `at` — strictly after when
// `exclusive` — else the row's plan.
func (f *fakeStore) planAtInstant(app cycle.AppMirror, at time.Time, exclusive bool) usage.Plan {
	plan := fakeEffectivePlan(app)
	var earliest *cycle.PlanChange
	for id := range f.planChanges {
		c := f.planChanges[id]
		if c.AppID != app.AppID {
			continue
		}
		switch c.Status {
		case cycle.PlanChangePending, cycle.PlanChangeSettled, cycle.PlanChangeApplied:
		default:
			continue
		}
		if c.EffectiveAt.Before(at) || (exclusive && c.EffectiveAt.Equal(at)) {
			continue
		}
		if earliest == nil || c.EffectiveAt.Before(earliest.EffectiveAt) {
			cc := c
			earliest = &cc
		}
	}
	if earliest != nil {
		plan = earliest.FromPlan
	}
	return plan
}

// MemberHighWater mirrors MemberHighWaterForAccount: per app that held
// members in the period (live, or deleted inside it; created before it
// ended), max(count in force at period start, max count recorded inside the
// period), with the plan in force during the period.
func (f *fakeStore) MemberHighWater(_ context.Context, accountID uuid.UUID, periodStart, periodEnd time.Time) ([]cycle.MemberHighWater, error) {
	var out []cycle.MemberHighWater
	ids := make([]uuid.UUID, 0, len(f.apps))
	for id := range f.apps {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i].String() < ids[j].String() })
	for _, id := range ids {
		app := f.apps[id]
		if app.AccountID != accountID || !app.CreatedAt.Before(periodEnd) || (app.Deleted && app.DeletedAt.Before(periodStart)) {
			continue
		}
		history := append([]fakeMemberCount(nil), f.memberHistory[app.AppID]...)
		sort.SliceStable(history, func(i, j int) bool { return history[i].at.Before(history[j].at) })
		inForce, known := 0, false
		inside := 0
		for _, h := range history {
			switch {
			case h.at.Before(periodStart):
				inForce, known = h.count, true
			case h.at.Before(periodEnd):
				if h.count > inside {
					inside = h.count
				}
			}
		}
		if !known {
			inForce = 0 // the app did not exist when the period opened
		}
		hwm := inForce
		if inside > hwm {
			hwm = inside
		}
		out = append(out, cycle.MemberHighWater{AppID: app.AppID, Plan: f.planAtInstant(app, periodEnd, false), Count: hwm})
	}
	return out, nil
}
