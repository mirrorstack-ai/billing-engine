package cycle_test

// End-to-end scenario regression suite for the per-module overage timer model
// (DESIGN.md "Base fee — v2: creation grace + per-module overage timers",
// scenarios 1–6). Each test drives the SAME code paths cmd/billing-cycle drives
// (RegisterApp → SweepCreationProrations → SweepModuleOverage → RunBillingCycle)
// and asserts the exact dollar amounts + the exact Stripe invoice-count the spec
// calls out. Reuses the in-memory fakeStore (service_test.go) + fakeStripe
// (charge_test.go) + the registeredAccount / appsSvc / seedApp / seedTimer /
// seedIncluded helpers.
//
// Fixture: registeredAccount activates 2026-05-04 → anchor day 4, so the anchored
// period CONTAINING a mid-June instant is [2026-06-04, 2026-07-04) = 30 days. An
// app created 2026-06-19 has remain_days = whole UTC days in [Jun 19, Jul 4) = 15,
// exactly HALF the period, so each prorated amount is a clean half:
//   * base   $20 × 15/30 = $10.00 → 1000¢
//   * overage $3 × 15/30 =  $1.50 →  150¢

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/mirrorstack-ai/billing-engine/internal/account/cycle"
	"github.com/mirrorstack-ai/billing-engine/internal/account/usage"
)

func ptrI64(v int64) *int64 { return &v }

var scenarioCreatedAt = timeUTC(2026, 6, 19, 12) // mid-period create (anchor 4)

// scenarioSweepAt is past scenarioCreatedAt + GraceDays (Jun 22), so both the
// app's creation grace and its co-created modules' grace have elapsed.
var scenarioSweepAt = timeUTC(2026, 6, 25, 9)

func timeUTC(y int, m, d, h int) time.Time {
	return time.Date(y, time.Month(m), d, h, 0, 0, 0, time.UTC)
}

// --- Scenario 1: app just created → no charge; deleted in grace → never charged -

func TestScenario1_CreatedThenDeletedInGraceNeverCharged(t *testing.T) {
	store := newFakeStore()
	user, _ := registeredAccount(store)
	sc := newFakeStripe()
	svc := appsSvc(store, sc)
	ctx := context.Background()
	appID := uuid.New()

	// Create with 3 modules — no charge fires at creation (creation grace).
	registerMirror(t, svc, user, appID, scenarioCreatedAt, 3)
	require.Empty(t, sc.itemCalls, "no charge at creation (scenario 1)")
	require.Equal(t, 3, liveTimerCount(store, appID))

	// Deleted WITHIN grace (day 1) → the app + all its install timers drop out.
	_, err := svc.SyncAppModules(ctx, cycle.SyncAppModulesRequest{AppID: appID, Deleted: true})
	require.NoError(t, err)
	require.Equal(t, 0, liveTimerCount(store, appID), "delete soft-removes all timers")

	// Both grace sweeps, run long past grace, charge NOTHING — a deleted app is out
	// of the proration work-list and its timers are out of the overage work-list.
	pro, err := svc.SweepCreationProrations(ctx, scenarioSweepAt)
	require.NoError(t, err)
	require.Equal(t, 0, pro.Charged)
	over, err := svc.SweepModuleOverage(ctx, scenarioSweepAt)
	require.NoError(t, err)
	require.Equal(t, 0, over.Charged)

	require.Empty(t, sc.invoiceCalls, "an app deleted in grace is NEVER charged (scenario 1)")
	require.Empty(t, store.invoices)
}

// --- Scenario 2: survives grace, pool ≤ 5 → base-only prorated charge ----------

func TestScenario2_SurvivesGracePoolWithinIncludedBaseOnly(t *testing.T) {
	store := newFakeStore()
	user, _ := registeredAccount(store)
	sc := newFakeStripe()
	svc := appsSvc(store, sc)
	ctx := context.Background()
	appID := uuid.New()

	// 3 co-created modules → pool 3 ≤ IncludedModules(5): all "included", no overage.
	registerMirror(t, svc, user, appID, scenarioCreatedAt, 3)

	pro, err := svc.SweepCreationProrations(ctx, scenarioSweepAt)
	require.NoError(t, err)
	require.Equal(t, 1, pro.Charged)
	over, err := svc.SweepModuleOverage(ctx, scenarioSweepAt)
	require.NoError(t, err)
	require.Equal(t, 0, over.Charged, "3 modules are all included — no overage")

	// EXACTLY one invoice, ONE line item: the prorated FLAT base only.
	require.Len(t, sc.invoiceCalls, 1, "base-only creation invoice (scenario 2)")
	require.Len(t, sc.itemCalls, 1)
	require.EqualValues(t, 1000, sc.itemCalls[0].amountCfg, "$20 × 15/30 = $10.00")
	require.Equal(t, "app-ii-"+appID.String(), sc.itemCalls[0].idemKey)

	// The 3 co-created timers all resolved as included, none charged.
	for _, tm := range store.timers {
		require.True(t, tm.graceResolved)
		require.False(t, tm.graceCharged)
	}
}

// --- Scenario 3: pool > 5 from day 0 → ONE combined invoice (base + overage) ---

func TestScenario3_PoolOverFromDayZeroOneCombinedInvoice(t *testing.T) {
	store := newFakeStore()
	user, _ := registeredAccount(store)
	sc := newFakeStripe()
	svc := appsSvc(store, sc)
	ctx := context.Background()
	appID := uuid.New()

	// 7 co-created modules → pool 7 > IncludedModules(5): 2 are "over" from day 0
	// (installed AT created_at). Their grace elapses at the SAME instant as the
	// app's creation grace, so they ride ONE combined invoice with the base.
	registerMirror(t, svc, user, appID, scenarioCreatedAt, 7)

	pro, err := svc.SweepCreationProrations(ctx, scenarioSweepAt)
	require.NoError(t, err)
	require.Equal(t, 1, pro.Charged)
	// The overage sweep AFTER proration finds the 2 over-modules already resolved
	// (charged on the combined invoice) and the 5 included ones resolvable with no
	// charge — it adds NO second invoice.
	over, err := svc.SweepModuleOverage(ctx, scenarioSweepAt)
	require.NoError(t, err)
	require.Equal(t, 0, over.Charged, "co-created over-modules already billed on the combined invoice")
	require.Equal(t, 5, over.Included, "the 5 included modules resolve with no charge")

	// EXACTLY ONE Stripe invoice (not two), with THREE line items: base + 2 overage.
	require.Len(t, sc.invoiceCalls, 1, "scenario 3 is ONE combined invoice, never two")
	require.Len(t, sc.itemCalls, 3)
	require.EqualValues(t, 1000, sc.itemCalls[0].amountCfg, "base: $20 × 15/30 = $10.00")
	require.Equal(t, "app-ii-"+appID.String(), sc.itemCalls[0].idemKey)
	require.EqualValues(t, 150, sc.itemCalls[1].amountCfg, "overage: $3 × 15/30 = $1.50")
	require.EqualValues(t, 150, sc.itemCalls[2].amountCfg)
	// Overage line items use the SAME per-timer idem keys Leg 1 would use, so a
	// racing sweep can never double-charge them.
	require.Contains(t, sc.itemCalls[1].idemKey, "mod-overage-ii-")
	require.Contains(t, sc.itemCalls[2].idemKey, "mod-overage-ii-")

	// Exactly 2 timers marked charged (the over ones); all 7 resolved.
	charged, resolved := 0, 0
	for _, tm := range store.timers {
		if tm.graceCharged {
			charged++
			require.Contains(t, tm.graceInvoiceItemID, "ii_test_", "the REAL Stripe item id, not the idem key")
		}
		if tm.graceResolved {
			resolved++
		}
	}
	require.Equal(t, 2, charged, "exactly the 2 over-modules were charged")
	require.Equal(t, 7, resolved, "all 7 co-created timers reached a terminal verdict")
}

// --- Scenario 4: pool crosses 5 later → two independent prorated charges -------

func TestScenario4_PoolCrossesFiveLaterPerModuleTimers(t *testing.T) {
	// Two modules installed a day apart, each pushing the account-wide pool over 5,
	// get their OWN independently-anchored 3-day grace and two DIFFERENT prorated
	// charges on two DIFFERENT days (install-anchored to [install, period end)):
	//   * module A installed Jun 10 → grace ends Jun 13 → $3 × 24/30 = $2.40 (240¢)
	//   * module B installed Jun 11 → grace ends Jun 14 → $3 × 23/30 = $2.30 (230¢)
	store := newFakeStore()
	_, acct := registeredAccount(store)
	sc := newFakeStripe()
	svc := cycle.NewService(store, sc)
	ctx := context.Background()

	// 5 pre-existing included modules → the two newcomers land in the "over" bucket.
	seedIncluded(store, acct, uuid.New(), timeUTC(2026, 5, 4, 0), 5)
	app := uuid.New()
	timerA := seedTimer(store, acct, app, timeUTC(2026, 6, 10, 0))
	timerB := seedTimer(store, acct, app, timeUTC(2026, 6, 11, 0))

	// Sweep Jun 13: only A is past its own grace → one charge, $2.40.
	resA, err := svc.SweepModuleOverage(ctx, timeUTC(2026, 6, 13, 9))
	require.NoError(t, err)
	require.Equal(t, 1, resA.Charged)
	require.Len(t, sc.itemCalls, 1)
	require.EqualValues(t, 240, sc.itemCalls[0].amountCfg, "A: $3 × 24/30 = $2.40")
	require.Equal(t, "mod-overage-ii-"+timerA.String(), sc.itemCalls[0].idemKey)
	require.False(t, store.timers[timerB].graceResolved, "B is still in its own grace")

	// Sweep Jun 14: B's grace now elapsed → a DIFFERENT amount on a DIFFERENT day;
	// A is already resolved and never re-charged.
	resB, err := svc.SweepModuleOverage(ctx, timeUTC(2026, 6, 14, 9))
	require.NoError(t, err)
	require.Equal(t, 1, resB.Charged)
	require.Len(t, sc.itemCalls, 2, "A must not be charged again")
	require.EqualValues(t, 230, sc.itemCalls[1].amountCfg, "B: $3 × 23/30 = $2.30")
	require.Equal(t, "mod-overage-ii-"+timerB.String(), sc.itemCalls[1].idemKey)
	require.True(t, store.timers[timerA].graceCharged)
	require.True(t, store.timers[timerB].graceCharged)
}

// --- Scenario 5: the shared auto-collect helper fires at EVERY charge site -----

func TestScenario5_LargeAutoCollectFlagAtEveryChargeSite(t *testing.T) {
	// The SAME flagLargeAutoCollect helper (migration 034) sets is_large_auto_collect
	// on the mirror row of EVERY off-session charge — the creation/combined leg, the
	// per-module grace leg (Leg 1), and the boundary leg (Leg 2) — resolved AT CHARGE
	// TIME against the account's threshold. A per-account override BELOW the charged
	// amount flags it; the default $100 (nil override) does not, at every site.
	onlyMirror := func(store *fakeStore) cycle.InvoiceMirror {
		require.Len(t, store.invoices, 1)
		for _, m := range store.invoices {
			return m
		}
		return cycle.InvoiceMirror{}
	}

	t.Run("creation/combined leg", func(t *testing.T) {
		run := func(threshold *int64) cycle.InvoiceMirror {
			store := newFakeStore()
			user, _ := registeredAccount(store)
			store.collection.AutoCollectThresholdMicros = threshold
			sc := newFakeStripe()
			svc := appsSvc(store, sc)
			appID := uuid.New()
			registerMirror(t, svc, user, appID, scenarioCreatedAt, 0) // $10 base charge
			_, err := svc.SweepCreationProrations(context.Background(), scenarioSweepAt)
			require.NoError(t, err)
			return onlyMirror(store)
		}
		require.True(t, run(ptrI64(5_000_000)).IsLargeAutoCollect, "$10 base > $5 threshold → flagged")
		require.False(t, run(nil).IsLargeAutoCollect, "$10 base < $100 default → not flagged")
	})

	t.Run("per-module grace leg (Leg 1)", func(t *testing.T) {
		run := func(threshold *int64) cycle.InvoiceMirror {
			store := newFakeStore()
			_, acct := registeredAccount(store)
			store.collection.AutoCollectThresholdMicros = threshold
			sc := newFakeStripe()
			svc := cycle.NewService(store, sc)
			seedIncluded(store, acct, uuid.New(), timeUTC(2026, 5, 4, 0), 5)
			seedTimer(store, acct, uuid.New(), timeUTC(2026, 6, 19, 0)) // over, $1.50
			_, err := svc.SweepModuleOverage(context.Background(), timeUTC(2026, 6, 25, 9))
			require.NoError(t, err)
			return onlyMirror(store)
		}
		require.True(t, run(ptrI64(1_000_000)).IsLargeAutoCollect, "$1.50 overage > $1 threshold → flagged")
		require.False(t, run(nil).IsLargeAutoCollect, "$1.50 overage < $100 default → not flagged")
	})

	t.Run("boundary leg (Leg 2)", func(t *testing.T) {
		run := func(threshold *int64) cycle.InvoiceMirror {
			store := newFakeStore()
			store.hasPM = true
			store.stripeCustomer = "cus_s5"
			store.collection.AutoCollectThresholdMicros = threshold
			seedApp(store, chargeAccount, 0, false) // $20 advance base
			sc := newFakeStripe()
			_, err := chargeSvc(store, sc).RunBillingCycle(context.Background(), chargeAccount, periodStart, periodEnd, 0)
			require.NoError(t, err)
			return onlyMirror(store)
		}
		require.True(t, run(ptrI64(5_000_000)).IsLargeAutoCollect, "$20 boundary > $5 threshold → flagged")
		require.False(t, run(nil).IsLargeAutoCollect, "$20 boundary < $100 default → not flagged")
	})
}

// --- Scenario 6: boundary = arrears + base + ongoing-over-module overage -------

func TestScenario6_BoundaryPrechargesOngoingOverModulesOnly(t *testing.T) {
	// At the period boundary the ONE invoice = closed period's usage arrears +
	// the new period's FLAT base (per live pre-existing app) + a FULL $3 precharge
	// for every ONGOING over-module (a live "over" timer already charged at least
	// once). A timer still inside its OWN grace (never charged) is NOT double-counted.
	store := newFakeStore()
	store.chargedTotal = 1_000_000 // $1 usage arrears
	store.hasPM = true
	store.stripeCustomer = "cus_s6"
	app := seedApp(store, chargeAccount, 0, false) // one live pre-existing app → $20 base

	// 5 included (ranks 0-4) + two ONGOING over-modules already charged in an
	// earlier period (ranks 5-6) + one over-module STILL in its own grace (rank 7,
	// never charged). Only the two ongoing ones are precharged for the new period.
	seedIncluded(store, chargeAccount, app, timeUTC(2026, 5, 1, 0), 5)
	ongoing1 := seedTimer(store, chargeAccount, app, timeUTC(2026, 5, 10, 0))
	ongoing2 := seedTimer(store, chargeAccount, app, timeUTC(2026, 5, 11, 0))
	// One over-module STILL inside its own grace (never charged) — excluded below.
	seedTimer(store, chargeAccount, app, timeUTC(2026, 6, 28, 0))
	for _, id := range []uuid.UUID{ongoing1, ongoing2} {
		store.timers[id].graceResolved = true
		store.timers[id].graceCharged = true // charged in a prior period → ongoing
	}

	sc := newFakeStripe()
	resp, err := chargeSvc(store, sc).RunBillingCycle(context.Background(), chargeAccount, periodStart, periodEnd, 0)
	require.NoError(t, err)
	require.Equal(t, cycle.RunStatusInvoiced, resp.Status)
	require.EqualValues(t, 1_000_000, resp.ArrearsMicros)
	require.EqualValues(t, usage.BaseFeeMicros, resp.AdvanceBaseMicros, "one live app → $20 base")
	require.EqualValues(t, 2*usage.ModuleOverageFeeMicros, resp.AdvanceOverageMicros,
		"only the 2 ONGOING over-modules are precharged; the in-grace one is excluded")

	// One invoice, ONE pooled line: $1 arrears + $20 base + 2 × $3 overage = $27 → 2700¢.
	require.Len(t, sc.invoiceCalls, 1)
	require.Len(t, sc.itemCalls, 1, "arrears + base + overage pool into ONE line")
	require.EqualValues(t, 2_700, resp.ChargedCents)
	require.EqualValues(t, 2_700, sc.itemCalls[0].amountCfg)
}
