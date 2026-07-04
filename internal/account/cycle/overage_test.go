package cycle_test

// Account-wide POOLED module overage (migration 030): the grace-timer recompute
// (RegisterApp / SyncAppModules), the mid-period grace sweep (ChargeAccountOverage
// + AccountsInOverageGrace), and the no-double-charge interaction with the
// boundary advance leg. Reuses the in-memory fakeStore (service_test.go) +
// fakeStripe (charge_test.go).

import (
	"context"
	"errors"
	"strings"
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

// --- PR #47 review fixes: regression tests ----------------------------------

// Finding #1 [CRITICAL] — cross-leg double charge. Before the fix,
// ChargeAccountOverage called Stripe BEFORE writing account_overage_snapshots;
// a crash between "Stripe succeeded" and "the row committed" left NO row for
// the boundary to see, so it independently charged the FULL pooled overage
// again under a disjoint Idempotency-Key namespace — a real double charge.
// The fix claims the period (a 'pending' row) BEFORE calling Stripe, so the
// claim survives any crash after that point. This test simulates the crash by
// injecting a failure at the LAST step (flipping 'pending' → 'charged') —
// AFTER Stripe already succeeded — and proves the boundary that runs next
// still sees the claim and does NOT charge the overage a second time.
func TestChargeAccountOverage_CrashAfterStripeSuccess_BoundaryDoesNotDoubleCharge(t *testing.T) {
	at := time.Date(2026, 6, 15, 12, 0, 0, 0, time.UTC)
	since := time.Date(2026, 5, 28, 0, 0, 0, 0, time.UTC) // graceEnd May 31 < Jun 1 → full overage this period
	store := newFakeStore()
	jun1 := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)
	jul1 := time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)
	acct := armedOverageAccount(store, 2, 4, since, jun1) // pool 8 → 3 over → $9.00 pooled overage
	sc := newFakeStripe()
	svc := overageSvc(store, sc, at)
	cand := cycle.OverageGraceCandidate{ID: acct, OverageSince: since, ActivatedAt: store.activation[acct]}

	// Simulate the crash: Stripe already confirmed the charge (item + invoice
	// calls below prove it), but the LAST write — flipping the claim row from
	// 'pending' to 'charged' — fails, exactly like a Lambda dying right there.
	store.errMarkOverageSnap = errors.New("boom: process died before the claim flipped to charged")
	_, err := svc.ChargeAccountOverage(context.Background(), cand, at)
	require.Error(t, err, "the crash surfaces as an error to the caller (the Lambda dies / retries)")

	// Stripe's charge already went through before the crash.
	require.Len(t, sc.invoiceCalls, 1, "the grace leg's Stripe call already succeeded before the simulated crash")
	require.EqualValues(t, 900, sc.itemCalls[0].amountCfg)

	// CRITICAL: the claim row survives the crash (it was written BEFORE Stripe,
	// not after) — this is the durable evidence the OTHER leg must respect.
	snap, ok := store.overageSnaps[acctSnapKey{acct, jun1}]
	require.True(t, ok, "the pending claim row must survive the crash")
	require.Equal(t, "grace", snap.Source)

	// The BOUNDARY now closes [Jun 1, Jul 1). WITHOUT the fix (no row would
	// exist at this point), it would independently charge the FULL $9.00
	// pooled overage AGAIN under its own ii-<run> Idempotency-Key — a real
	// double charge totaling $18.00 for a $9.00 debt. WITH the fix, it must see
	// the claim and charge ZERO overage.
	store.errMarkOverageSnap = nil // the injected fault was specific to the grace leg's crash
	resp, err := svc.RunBillingCycle(context.Background(), acct, jun1, jul1, 0)
	require.NoError(t, err)
	require.EqualValues(t, 0, resp.AccountOverageMicros,
		"the boundary must NOT independently charge the pooled overage the grace leg already claimed, crash or not")
	require.EqualValues(t, 40_000_000, resp.AdvanceBaseMicros, "the flat per-app base is unaffected")
	require.EqualValues(t, 4_000, resp.ChargedCents, "base only ($40.00) — NOT $49.00, which would be the double charge")

	// Exactly TWO invoices total: the grace leg's original $9.00 overage charge
	// + the boundary's base-only invoice. Never a second overage charge.
	require.Len(t, sc.invoiceCalls, 2)
}

// Finding #2 [HIGH] — boundary retry livelock. Before the fix, a reclaim of a
// 'pending' billing_run (after InsertAccountOverageSnapshot succeeded but
// MarkBillingRun crashed) recomputed advanceOverage FRESH from
// snapshot-presence, collapsing it from a real amount to $0 — a DIFFERENT
// combined total reusing the SAME deterministic Stripe Idempotency-Key, which
// a real Stripe would reject (a mismatched-parameter idempotency-key reuse),
// permanently stuck. The fix freezes the overage amount into the
// account_overage_snapshots row at the FIRST attempt and REUSES it verbatim on
// every reclaim of the same run.
func TestRunBillingCycle_ReclaimAfterMarkBillingRunFailure_KeepsStableOverageAmount(t *testing.T) {
	store := newFakeStore()
	store.chargedTotal = 0
	store.hasPM = true
	store.stripeCustomer = "cus_reclaim_overage"
	seedApp(store, chargeAccount, 4, false)
	seedApp(store, chargeAccount, 4, false) // pool 8 → 3 over → $9.00 pooled overage
	sc := newFakeStripe()
	svc := chargeSvc(store, sc)

	// Attempt #1: the combined charge (base $40.00 + overage $9.00 = $49.00 =
	// 4_900 cents) succeeds at Stripe and the overage snapshot is claimed +
	// marked 'charged' — but MarkBillingRun then fails/crashes, so the run row
	// stays 'pending' (non-terminal).
	store.errMarkRun = errors.New("boom: process died before the run's terminal write landed")
	_, err := svc.RunBillingCycle(context.Background(), chargeAccount, periodStart, periodEnd, 0)
	require.Error(t, err)
	require.Len(t, sc.invoiceCalls, 1)
	require.EqualValues(t, 4_900, sc.itemCalls[0].amountCfg)

	snap, ok := store.overageSnaps[acctSnapKey{chargeAccount, periodStart}]
	require.True(t, ok)
	require.Equal(t, "advance", snap.Source)
	require.Equal(t, cycle.OverageSnapshotCharged, snap.Status)
	require.EqualValues(t, 9_000_000, snap.ChargedMicros)

	// Attempt #2: RECLAIM — InsertBillingRun reuses the SAME run id (the row is
	// still 'pending'). WITHOUT the fix, advanceOverage recomputes to $0 (the
	// snapshot "looks already billed" from the boundary's own prior write),
	// giving a DIFFERENT combined total ($40.00 = 4_000 cents) reusing the SAME
	// Idempotency-Key — a real Stripe rejects this and the run is stuck
	// forever. WITH the fix, the overage amount is FROZEN and reused, so
	// attempt #2 computes the IDENTICAL $49.00 = 4_900 cents.
	store.errMarkRun = nil
	resp, err := svc.RunBillingCycle(context.Background(), chargeAccount, periodStart, periodEnd, 0)
	require.NoError(t, err)
	require.True(t, resp.FirstRun, "the pending run is reclaimed for a fresh attempt")
	require.Equal(t, cycle.RunStatusInvoiced, resp.Status)
	require.EqualValues(t, 9_000_000, resp.AccountOverageMicros,
		"the overage amount must stay stable across the reclaim retry, never recompute to 0")
	require.EqualValues(t, 4_900, resp.ChargedCents, "the SAME combined total as attempt #1 — never 4_000 (base-only)")
	require.Len(t, sc.invoiceCalls, 2, "the reclaim re-calls Stripe with the SAME idem key (safe/idempotent for the real client)")
	require.EqualValues(t, 4_900, sc.itemCalls[1].amountCfg, "attempt #2's item amount matches attempt #1's exactly")
	require.Equal(t, sc.itemCalls[0].idemKey, sc.itemCalls[1].idemKey, "the SAME deterministic ii-<run> key is reused across the reclaim")
	require.Len(t, store.insertedRuns, 1, "reclaim reuses the same run row, never a second one")
}

// Finding #3 [MEDIUM, judgment call] — pooled-overage growth after the
// period's grace charge went permanently unbilled (recomputeAccountOverage
// only arms/clears the timer; it never re-evaluated an already-charged
// period). The fix charges an INCREMENTAL top-up, conservatively prorated from
// the sweep's own instant to the period end (never retroactively).
func TestChargeAccountOverage_PoolGrowthMidPeriodChargesIncrementalTopUp(t *testing.T) {
	// First charge: pool 8 (2 apps × 4) → 3 over → $9.00 (900 cents), the exact
	// fixture TestChargeAccountOverage_ChargesFullWhenGraceEndedBeforePeriodStart
	// uses. Then a THIRD app (4 modules) installs mid-period, growing the pool
	// to 12 → 7 over. A later sweep pass (Jun 20 — 11 days left of the 30-day
	// [Jun 1, Jul 1) period) must charge the INCREMENTAL 4-module delta
	// prorated for the remaining 11 days: 4 × $3.00 × 11/30 = $4.40 (440
	// cents) — never $0 (the pre-fix behavior, permanently unbilled) and never
	// the full $12.00 (never retroactive for time before the sweep noticed).
	at1 := time.Date(2026, 6, 15, 12, 0, 0, 0, time.UTC)
	since := time.Date(2026, 5, 28, 0, 0, 0, 0, time.UTC)
	store := newFakeStore()
	jun1 := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)
	acct := armedOverageAccount(store, 2, 4, since, jun1)
	sc := newFakeStripe()
	cand := cycle.OverageGraceCandidate{ID: acct, OverageSince: since, ActivatedAt: store.activation[acct]}

	first, err := overageSvc(store, sc, at1).ChargeAccountOverage(context.Background(), cand, at1)
	require.NoError(t, err)
	require.Equal(t, cycle.OverageCharged, first.Status)
	require.EqualValues(t, 900, first.ChargedCents)

	// Pool grows mid-period: a third app (4 modules) installs, 8 → 12.
	seedAppCreated(store, acct, 4, false, jun1.AddDate(0, 0, -1))

	at2 := time.Date(2026, 6, 20, 9, 0, 0, 0, time.UTC)
	second, err := overageSvc(store, sc, at2).ChargeAccountOverage(context.Background(), cand, at2)
	require.NoError(t, err)
	require.Equal(t, cycle.OverageToppedUp, second.Status)
	require.Equal(t, 7, second.OverCount, "pool 12 − included 5 = 7 over, the NEW cumulative over-count")
	require.EqualValues(t, 440, second.ChargedCents, "4 incremental modules × $3.00 × 11/30 remaining days = $4.40 — never $0, never $12.00")

	require.Len(t, sc.invoiceCalls, 2, "one invoice for the original charge, one for the top-up")
	require.NotEqual(t, sc.itemCalls[0].idemKey, sc.itemCalls[1].idemKey, "the top-up uses its OWN Idempotency-Key, distinct from the original charge")

	snap := store.overageSnaps[acctSnapKey{acct, jun1}]
	require.Equal(t, 7, snap.OverCount)
	require.EqualValues(t, 13_400_000, snap.ChargedMicros,
		"cumulative: the original $9.00 + the $4.40 top-up = $13.40 total billed for the period — the display reads this exact figure")
	require.Equal(t, cycle.OverageSnapshotCharged, snap.Status)

	// A third pass with NO further pool growth is a clean no-op (no third
	// charge, D1e — never re-bill what is already covered).
	third, err := overageSvc(store, sc, at2.Add(time.Hour)).ChargeAccountOverage(context.Background(), cand, at2.Add(time.Hour))
	require.NoError(t, err)
	require.Equal(t, cycle.OverageSkippedAlreadyCharged, third.Status)
	require.Len(t, sc.invoiceCalls, 2, "no third invoice when the pool hasn't grown further")
}

// Finding #4 [MEDIUM] — the boundary's account_overage_snapshots row
// (source='advance') stored the literal Idempotency-Key STRING ("ii-<runID>")
// as InvoiceItemID instead of the genuine Stripe invoice item id, because
// s.charge() discarded CreateInvoiceItem's return value. The fix threads the
// real item back from s.charge() and stores IT.
func TestRunBillingCycle_AdvanceOverageSnapshotStoresGenuineStripeItemID(t *testing.T) {
	store := newFakeStore()
	store.chargedTotal = 0
	store.hasPM = true
	store.stripeCustomer = "cus_item_id"
	seedApp(store, chargeAccount, 4, false)
	seedApp(store, chargeAccount, 4, false) // pool 8 → 3 over → $9.00 pooled overage
	sc := newFakeStripe()

	resp, err := chargeSvc(store, sc).RunBillingCycle(context.Background(), chargeAccount, periodStart, periodEnd, 0)
	require.NoError(t, err)
	require.EqualValues(t, 9_000_000, resp.AccountOverageMicros)
	require.Len(t, sc.itemCalls, 1)

	snap, ok := store.overageSnaps[acctSnapKey{chargeAccount, periodStart}]
	require.True(t, ok)

	idemKey := sc.itemCalls[0].idemKey
	require.True(t, strings.HasPrefix(idemKey, "ii-"), "sanity: the item was created under the ii-<run> idempotency key")
	require.NotEqual(t, idemKey, snap.InvoiceItemID,
		"the stored id must be the GENUINE Stripe invoice item id, never the ii-<run> idempotency-key string")
	require.True(t, strings.HasPrefix(snap.InvoiceItemID, "ii_test_"),
		"must be the REAL Stripe invoice item id the fake client generated for this call")
}
