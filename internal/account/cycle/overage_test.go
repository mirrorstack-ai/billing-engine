package cycle_test

// Account-wide POOLED module overage (migration 030): the grace-timer recompute
// (RegisterApp / SyncAppModules), the mid-period grace sweep (ChargeAccountOverage
// + AccountsInOverageGrace), and the no-double-charge interaction with the
// boundary advance leg. Reuses the in-memory fakeStore (service_test.go) +
// fakeStripe (charge_test.go).

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/mirrorstack-ai/billing-engine/internal/account/cycle"
	"github.com/mirrorstack-ai/billing-engine/internal/account/usage"
)

func overageSvc(store *fakeStore, sc *fakeStripe, now time.Time) *cycle.Service {
	return cycle.NewService(store, sc).WithNow(func() time.Time { return now })
}

// --- recompute: the timer arms / clears on pool crossings -------------------

func TestRecompute_PoolCrossingFiveArmsTimer(t *testing.T) {
	// Two apps' installs INTERLEAVE and SUM to the account pool: app1=3 (pool 3,
	// under 5 → not armed), then app2=4 (pool 7, over 5 → arms overage_since at
	// the register instant). Proves the timer keys off the ACCOUNT-WIDE sum, not
	// either app alone.
	now := time.Date(2026, 6, 10, 12, 0, 0, 0, time.UTC)
	store := newFakeStore()
	user, acct := registeredAccount(store)
	svc := overageSvc(store, newFakeStripe(), now)

	_, err := svc.RegisterApp(context.Background(), cycle.RegisterAppRequest{
		OwnerUserID: user, AppID: uuid.New(), ModuleCount: 3, CreatedAt: now,
	})
	require.NoError(t, err)
	require.NotContains(t, store.overageSince, acct, "pool 3 is under 5 → timer not armed")

	_, err = svc.RegisterApp(context.Background(), cycle.RegisterAppRequest{
		OwnerUserID: user, AppID: uuid.New(), ModuleCount: 4, CreatedAt: now,
	})
	require.NoError(t, err)
	require.Contains(t, store.overageSince, acct, "pool 3+4=7 crosses 5 → timer armed")
	require.Equal(t, now, store.overageSince[acct], "armed at the crossing instant")
}

func TestRecompute_FirstCrossingWinsTimerNotMoved(t *testing.T) {
	// A LATER recompute that finds the pool still over must NOT move the anchor
	// (first-crossing-wins) — the grace window is measured from the FIRST cross.
	first := time.Date(2026, 6, 1, 9, 0, 0, 0, time.UTC)
	store := newFakeStore()
	user, acct := registeredAccount(store)
	appID := uuid.New()

	_, err := overageSvc(store, newFakeStripe(), first).RegisterApp(context.Background(), cycle.RegisterAppRequest{
		OwnerUserID: user, AppID: appID, ModuleCount: 6, CreatedAt: first,
	})
	require.NoError(t, err)
	require.Equal(t, first, store.overageSince[acct])

	// Later: bump the count further; the pool is still over, so the anchor stays.
	later := first.AddDate(0, 0, 2)
	_, err = overageSvc(store, newFakeStripe(), later).SyncAppModules(context.Background(), cycle.SyncAppModulesRequest{
		AppID: appID, ModuleCount: intPtr(9),
	})
	require.NoError(t, err)
	require.Equal(t, first, store.overageSince[acct], "still-over recompute must not move the first-cross anchor")
}

func TestRecompute_DroppingUnderFiveClearsTimer(t *testing.T) {
	// Pool over 5 arms the timer; a later uninstall that drops the pool back to
	// ≤5 CLEARS it (no charge — the grace never elapsed).
	now := time.Date(2026, 6, 10, 12, 0, 0, 0, time.UTC)
	store := newFakeStore()
	user, acct := registeredAccount(store)
	svc := overageSvc(store, newFakeStripe(), now)
	appID := uuid.New()

	_, err := svc.RegisterApp(context.Background(), cycle.RegisterAppRequest{
		OwnerUserID: user, AppID: appID, ModuleCount: 7, CreatedAt: now,
	})
	require.NoError(t, err)
	require.Contains(t, store.overageSince, acct)

	_, err = svc.SyncAppModules(context.Background(), cycle.SyncAppModulesRequest{AppID: appID, ModuleCount: intPtr(2)})
	require.NoError(t, err)
	require.NotContains(t, store.overageSince, acct, "dropping to pool 2 clears the grace timer")
}

func TestRecompute_DeleteDroppingUnderFiveClearsTimer(t *testing.T) {
	// Deleting an app (not just a count sync) also drops the pool and clears the
	// timer — deleted apps leave the live pool.
	now := time.Date(2026, 6, 10, 12, 0, 0, 0, time.UTC)
	store := newFakeStore()
	user, acct := registeredAccount(store)
	svc := overageSvc(store, newFakeStripe(), now)
	a1, a2 := uuid.New(), uuid.New()

	_, err := svc.RegisterApp(context.Background(), cycle.RegisterAppRequest{OwnerUserID: user, AppID: a1, ModuleCount: 4, CreatedAt: now})
	require.NoError(t, err)
	_, err = svc.RegisterApp(context.Background(), cycle.RegisterAppRequest{OwnerUserID: user, AppID: a2, ModuleCount: 4, CreatedAt: now})
	require.NoError(t, err)
	require.Contains(t, store.overageSince, acct, "pool 8 armed")

	_, err = svc.SyncAppModules(context.Background(), cycle.SyncAppModulesRequest{AppID: a2, Deleted: true})
	require.NoError(t, err)
	require.NotContains(t, store.overageSince, acct, "deleting one app drops the pool to 4 → timer cleared")
}

// --- grace window: holds for exactly the grace period -----------------------

func TestAccountsInOverageGrace_HoldsForExactlyThreeDays(t *testing.T) {
	// The sweep work list includes an account only once its grace timer has
	// elapsed: overage_since <= at − 3d. Pin the boundary at EXACTLY 3 days.
	store := newFakeStore()
	acct := uuid.New()
	since := time.Date(2026, 6, 1, 8, 0, 0, 0, time.UTC)
	store.overageSince[acct] = since
	store.activation[acct] = time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC)

	// 1s BEFORE the 3-day mark → still in grace, excluded.
	cands, err := cycle.NewService(store, newFakeStripe()).AccountsInOverageGrace(context.Background(), since.Add(3*24*time.Hour-time.Second))
	require.NoError(t, err)
	require.Empty(t, cands, "grace still holding just under 3 days")

	// EXACTLY 3 days → grace elapsed, included.
	cands, err = cycle.NewService(store, newFakeStripe()).AccountsInOverageGrace(context.Background(), since.Add(3*24*time.Hour))
	require.NoError(t, err)
	require.Len(t, cands, 1, "grace elapsed at exactly 3 days")
	require.Equal(t, acct, cands[0].ID)
	require.Equal(t, since, cands[0].OverageSince)
}

func TestAccountsInOverageGrace_ExcludesUnactivated(t *testing.T) {
	// An un-activated account (no card) is never charged, so it never enters the
	// sweep even past the grace window.
	store := newFakeStore()
	acct := uuid.New()
	store.overageSince[acct] = time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)
	// no activation row

	cands, err := cycle.NewService(store, newFakeStripe()).AccountsInOverageGrace(context.Background(), time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC))
	require.NoError(t, err)
	require.Empty(t, cands)
}

// --- mid-period grace charge ------------------------------------------------

// armedOverageAccount seeds a fully-chargeable account (activation anchor day 1,
// usable PM, Stripe customer) with `n` apps of `perApp` modules each, created
// before `createdBefore`, and its grace timer armed at `since`. Returns the
// account id.
func armedOverageAccount(store *fakeStore, n, perApp int, since, createdBefore time.Time) uuid.UUID {
	acct := uuid.New()
	store.activation[acct] = time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC) // anchor day 1 → calendar-month periods
	store.hasPM = true
	store.stripeCustomer = "cus_overage"
	store.overageSince[acct] = since
	for i := 0; i < n; i++ {
		seedAppCreated(store, acct, perApp, false, createdBefore.AddDate(0, 0, -1))
	}
	return acct
}

func TestChargeAccountOverage_ChargesFullWhenGraceEndedBeforePeriodStart(t *testing.T) {
	// Grace ended May 31 (before the current period [Jun 1, Jul 1)), so the
	// pooled overage is charged in FULL for the period. Pool = 2 apps × 4 = 8 →
	// 3 over → $9 → 900 cents. One invoice item with the deterministic
	// per-(account, period) idem key; one 'grace' snapshot frozen.
	at := time.Date(2026, 6, 15, 12, 0, 0, 0, time.UTC)
	since := time.Date(2026, 5, 28, 0, 0, 0, 0, time.UTC) // graceEnd May 31 < Jun 1
	store := newFakeStore()
	jun1 := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)
	acct := armedOverageAccount(store, 2, 4, since, jun1)
	sc := newFakeStripe()

	summary, err := overageSvc(store, sc, at).ChargeAccountOverage(context.Background(), cycle.OverageGraceCandidate{
		ID: acct, OverageSince: since, ActivatedAt: store.activation[acct],
	}, at)
	require.NoError(t, err)
	require.Equal(t, cycle.OverageCharged, summary.Status)
	require.Equal(t, 3, summary.OverCount)
	require.EqualValues(t, 900, summary.ChargedCents)

	require.Len(t, sc.itemCalls, 1)
	require.EqualValues(t, 900, sc.itemCalls[0].amountCfg)
	require.Contains(t, sc.itemCalls[0].idemKey, "acct-overage-ii-"+acct.String())
	require.Len(t, sc.invoiceCalls, 1)

	snap, ok := store.overageSnaps[acctSnapKey{acct, jun1}]
	require.True(t, ok, "the grace charge must freeze a snapshot for the period")
	require.Equal(t, "grace", snap.Source)
	require.Equal(t, 3, snap.OverCount)
	require.EqualValues(t, 9_000_000, snap.ChargedMicros)
}

func TestChargeAccountOverage_ProratesFromGraceEndMidPeriod(t *testing.T) {
	// Grace ends mid-period (Jun 4 → coverage [Jun 4, Jul 1) = 27 of 30 days).
	// Pool = 6 → 1 over → $3 → prorated 3e6 × 27/30 = 2_700_000 → 270 cents.
	at := time.Date(2026, 6, 20, 12, 0, 0, 0, time.UTC)
	since := time.Date(2026, 6, 1, 6, 0, 0, 0, time.UTC) // graceEnd Jun 4 (truncated day)
	store := newFakeStore()
	acct := armedOverageAccount(store, 1, 6, since, time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC))
	sc := newFakeStripe()

	summary, err := overageSvc(store, sc, at).ChargeAccountOverage(context.Background(), cycle.OverageGraceCandidate{
		ID: acct, OverageSince: since, ActivatedAt: store.activation[acct],
	}, at)
	require.NoError(t, err)
	require.Equal(t, cycle.OverageCharged, summary.Status)
	require.EqualValues(t, 270, summary.ChargedCents)
}

func TestChargeAccountOverage_FiresOnceAndExcludedFromBoundary(t *testing.T) {
	// THE double-charge invariant. The mid-period sweep charges the pooled
	// overage for [Jun 1, Jul 1); a SECOND sweep is a no-op (the snapshot guards
	// it); and the BOUNDARY that closes [Jun 1, Jul 1) must NOT charge the
	// overage again (it sees the snapshot) — it bills only the flat advance base.
	at := time.Date(2026, 6, 15, 12, 0, 0, 0, time.UTC)
	since := time.Date(2026, 5, 28, 0, 0, 0, 0, time.UTC) // full overage this period
	store := newFakeStore()
	store.chargedTotal = 0
	jun1 := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)
	jul1 := time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)
	acct := armedOverageAccount(store, 2, 4, since, jun1) // pool 8 → $9 overage
	sc := newFakeStripe()
	svc := overageSvc(store, sc, at)
	cand := cycle.OverageGraceCandidate{ID: acct, OverageSince: since, ActivatedAt: store.activation[acct]}

	// Sweep #1 charges.
	first, err := svc.ChargeAccountOverage(context.Background(), cand, at)
	require.NoError(t, err)
	require.Equal(t, cycle.OverageCharged, first.Status)
	require.Len(t, sc.invoiceCalls, 1)

	// Sweep #2 (same period) is a no-op — the snapshot guards it.
	second, err := svc.ChargeAccountOverage(context.Background(), cand, at.Add(time.Hour))
	require.NoError(t, err)
	require.Equal(t, cycle.OverageSkippedAlreadyCharged, second.Status)
	require.Len(t, sc.invoiceCalls, 1, "no second overage invoice")

	// The BOUNDARY closing [Jun 1, Jul 1): flat advance base (2 × $20 = 40e6),
	// and ZERO pooled overage — the mid-period snapshot excludes it.
	resp, err := svc.RunBillingCycle(context.Background(), acct, jun1, jul1, 0)
	require.NoError(t, err)
	require.Equal(t, cycle.RunStatusInvoiced, resp.Status)
	require.EqualValues(t, 40_000_000, resp.AdvanceBaseMicros)
	require.EqualValues(t, 0, resp.AccountOverageMicros, "the mid-period overage is NOT charged again at the boundary")
	require.EqualValues(t, 4_000, resp.ChargedCents) // base only

	// Exactly two invoices total: the grace overage + the boundary base.
	require.Len(t, sc.invoiceCalls, 2)
	// The 'grace' snapshot (not 'advance') still owns the period row.
	require.Equal(t, "grace", store.overageSnaps[acctSnapKey{acct, jun1}].Source)
}

func TestRunBillingCycle_BoundaryChargesFullPooledOverageWhenNoSweep(t *testing.T) {
	// When the grace sweep never billed a period (e.g. grace expired at cutover),
	// the boundary charges the FULL pooled overage for the closing period and
	// writes its own 'advance' snapshot. Pool = 8 → 3 over → $9 on top of the
	// flat base.
	store := newFakeStore()
	store.chargedTotal = 0
	store.hasPM = true
	store.stripeCustomer = "cus_bnd_overage"
	seedApp(store, chargeAccount, 4, false)
	seedApp(store, chargeAccount, 4, false)
	sc := newFakeStripe()

	resp, err := chargeSvc(store, sc).RunBillingCycle(context.Background(), chargeAccount, periodStart, periodEnd, 0)
	require.NoError(t, err)
	require.EqualValues(t, 40_000_000, resp.AdvanceBaseMicros)
	require.EqualValues(t, 9_000_000, resp.AccountOverageMicros)
	require.EqualValues(t, 4_900, resp.ChargedCents) // (40e6 + 9e6) / 10_000

	snap, ok := store.overageSnaps[acctSnapKey{chargeAccount, periodStart}]
	require.True(t, ok)
	require.Equal(t, "advance", snap.Source)
	require.Equal(t, 3, snap.OverCount)
	require.EqualValues(t, 9_000_000, snap.ChargedMicros)
}

func TestChargeAccountOverage_UninstallDoesNotRefund(t *testing.T) {
	// After the pooled overage was charged for a period, dropping back under the
	// pool clears the timer but NEVER refunds the charge already taken (D1e). A
	// later sweep in the SAME period finds the snapshot and no-ops; nothing
	// negative is ever invoiced.
	at := time.Date(2026, 6, 15, 12, 0, 0, 0, time.UTC)
	since := time.Date(2026, 5, 28, 0, 0, 0, 0, time.UTC)
	store := newFakeStore()
	jun1 := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)
	acct := armedOverageAccount(store, 2, 4, since, jun1)
	sc := newFakeStripe()
	svc := overageSvc(store, sc, at)
	cand := cycle.OverageGraceCandidate{ID: acct, OverageSince: since, ActivatedAt: store.activation[acct]}

	_, err := svc.ChargeAccountOverage(context.Background(), cand, at)
	require.NoError(t, err)
	require.Len(t, sc.invoiceCalls, 1)
	require.EqualValues(t, 9_000_000, store.overageSnaps[acctSnapKey{acct, jun1}].ChargedMicros)

	// Now every module is uninstalled down under the pool via SyncAppModules —
	// the recompute CLEARS the timer, but the already-charged snapshot (the money
	// taken) is untouched: no refund (D1e).
	for id, app := range store.apps {
		if app.AccountID == acct {
			_, err := svc.SyncAppModules(context.Background(), cycle.SyncAppModulesRequest{AppID: id, ModuleCount: intPtr(0)})
			require.NoError(t, err)
		}
	}
	require.NotContains(t, store.overageSince, acct, "dropping under the pool clears the timer")
	require.EqualValues(t, 9_000_000, store.overageSnaps[acctSnapKey{acct, jun1}].ChargedMicros,
		"the charged snapshot survives — uninstall never refunds")
	require.Len(t, sc.invoiceCalls, 1, "no refund / negative invoice on uninstall")

	// And a re-run of the sweep in the SAME period is a no-op (snapshot guard) —
	// definitely no refund/re-charge.
	again, err := svc.ChargeAccountOverage(context.Background(), cand, at.Add(time.Hour))
	require.NoError(t, err)
	require.Equal(t, cycle.OverageSkippedAlreadyCharged, again.Status)
	require.Len(t, sc.invoiceCalls, 1)
}

// Guard: the overage amount is exactly the pooled tier, never a per-app tier.
func TestAccountOverageMicros_IsPooledNotPerApp(t *testing.T) {
	// Two apps of 4 modules each: per-app each is UNDER the old 5 tier (would be
	// $0 overage each pre-030), but POOLED they are 8 → 3 over → $9. This is the
	// whole point of the reversal.
	require.EqualValues(t, 9_000_000, usage.AccountOverageMicros(4+4))
}
