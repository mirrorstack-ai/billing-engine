package cycle_test

// The fakeStore half of the plan-change ledger (migration 076) and the
// member count (migration 077). Mirrors plan_change_store.go's contracts —
// the locked re-verification of OpenPlanChange, the once-only wallet decision,
// lots-only draws — over the in-memory maps service_test.go owns.

import (
	"context"
	"sort"
	"time"

	"github.com/google/uuid"

	"github.com/mirrorstack-ai/billing-engine/internal/account/cycle"
	"github.com/mirrorstack-ai/billing-engine/internal/account/usage"
)

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

func (f *fakeStore) OpenPlanChange(_ context.Context, p cycle.OpenPlanChangeParams) (cycle.PlanChange, cycle.OpenPlanChangeOutcome, error) {
	if f.errOpenPlanChange != nil {
		return cycle.PlanChange{}, 0, f.errOpenPlanChange
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
	c := cycle.PlanChange{
		ID: uuid.New(), AppID: p.AppID, AccountID: p.AccountID,
		FromPlan: p.FromPlan, ToPlan: p.ToPlan, Kind: p.Kind,
		RequestedAt: p.RequestedAt.UTC(), EffectiveAt: p.EffectiveAt.UTC(),
		PeriodStart: p.PeriodStart.UTC(), PeriodEnd: p.PeriodEnd.UTC(),
		FoldedIntoCreation: p.Folded, AmountMicros: p.AmountMicros, Status: p.Status,
	}
	if p.Status == cycle.PlanChangeSettled {
		c.WalletDecided, c.WalletDecidedAt, c.SettledAt = true, c.RequestedAt, c.RequestedAt
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

func (f *fakeStore) FoldedPlanChanges(_ context.Context, appID uuid.UUID) ([]cycle.PlanChange, error) {
	var out []cycle.PlanChange
	for _, c := range f.planChanges {
		if c.AppID == appID && c.FoldedIntoCreation {
			out = append(out, c)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].EffectiveAt.Before(out[j].EffectiveAt) })
	return out, nil
}

// DrawPlanChangeFromWallet mirrors the store: the decision is taken once;
// standard mode decides 0; credits mode draws from the spendable lots only
// (in the same tier order the creation draw uses) and never writes an
// unsecured remainder; a short wallet with no card remainder writes nothing.
func (f *fakeStore) DrawPlanChangeFromWallet(_ context.Context, change cycle.PlanChange, allowRemainder bool, at time.Time) (cycle.PlanChangeWalletOutcome, int64, error) {
	if f.errPlanChangeDraw != nil {
		return 0, 0, f.errPlanChangeDraw
	}
	c, ok := f.planChanges[change.ID]
	if !ok || c.Status != cycle.PlanChangePending || c.WalletDecided || c.AmountMicros <= 0 {
		return cycle.PlanChangeWalletAlreadyDecided, 0, nil
	}
	decide := func(drawn int64) (cycle.PlanChangeWalletOutcome, int64, error) {
		c.WalletMicros, c.WalletDecided, c.WalletDecidedAt = drawn, true, at.UTC()
		if drawn >= c.AmountMicros {
			c.Status, c.SettledAt = cycle.PlanChangeSettled, at.UTC()
		}
		f.planChanges[c.ID] = c
		return cycle.PlanChangeWalletDecided, drawn, nil
	}
	if f.walletMode != cycle.CreditBillingModeCredits {
		return decide(0)
	}
	sources := make([]*fakeWalletSource, 0, len(f.walletSources))
	var coverable int64
	for _, source := range f.walletSources {
		if source.remaining > 0 && (source.expiresAt.IsZero() || source.expiresAt.After(time.Now())) {
			sources = append(sources, source)
			coverable += source.remaining
		}
	}
	if coverable < c.AmountMicros && !allowRemainder {
		return cycle.PlanChangeWalletShort, 0, nil
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
	left := c.AmountMicros
	var drawn int64
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
	f.planChangeDraws[c.ID] += drawn
	return decide(drawn)
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
	for id, c := range f.planChanges {
		if c.AppID == appID && c.Status == cycle.PlanChangeScheduled {
			c.Status, c.SettledAt = cycle.PlanChangeCancelled, at.UTC()
			f.planChanges[id] = c
			return true, nil
		}
	}
	return false, nil
}

func (f *fakeStore) ApplyDuePlanChanges(_ context.Context, accountID uuid.UUID, dueAt time.Time) (int, error) {
	if f.errApplyPlanChange != nil {
		return 0, f.errApplyPlanChange
	}
	applied := 0
	for id, c := range f.planChanges {
		if c.AccountID != accountID || c.Status != cycle.PlanChangeScheduled || c.EffectiveAt.After(dueAt) {
			continue
		}
		if app, ok := f.apps[c.AppID]; ok && !app.Deleted {
			app.Plan = c.ToPlan
			f.apps[c.AppID] = app
		}
		c.Status, c.SettledAt = cycle.PlanChangeApplied, dueAt.UTC()
		f.planChanges[id] = c
		applied++
	}
	return applied, nil
}

func (f *fakeStore) CountLiveAppsOnPlan(_ context.Context, accountID, ownerOrgID uuid.UUID, plan usage.Plan, exceptAppID uuid.UUID) (int, error) {
	n := 0
	for _, app := range f.apps {
		if app.Deleted || app.AppID == exceptAppID || fakeEffectivePlan(app) != plan {
			continue
		}
		if ownerOrgID != uuid.Nil {
			if app.OwnerOrgID == ownerOrgID {
				n++
			}
		} else if app.AccountID == accountID && app.OwnerOrgID == uuid.Nil {
			n++
		}
	}
	return n, nil
}

func (f *fakeStore) SetAppMemberCount(_ context.Context, appID uuid.UUID, memberCount int) error {
	if app, ok := f.apps[appID]; ok && !app.Deleted {
		app.MemberCount = memberCount // deleted rows are frozen (WHERE deleted_at IS NULL)
		f.apps[appID] = app
	}
	return nil
}
