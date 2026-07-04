package cycle_test

// ChargeCreationProration + SweepCreationProrations (creation grace, owner spec
// 2026-07-05). Reuses the in-memory fakeStore (service_test.go) + fakeStripe
// (charge_test.go) and the appsNow / registeredAccount helpers (apps_test.go).

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/mirrorstack-ai/billing-engine/internal/account/billing"
	"github.com/mirrorstack-ai/billing-engine/internal/account/cycle"
	"github.com/mirrorstack-ai/billing-engine/internal/account/usage"
)

// registerMirror seeds a roster row through RegisterApp (which only mirrors — no
// charge) so the app is owned by the fully-chargeable registeredAccount.
func registerMirror(t *testing.T, svc *cycle.Service, user, appID uuid.UUID, created time.Time, moduleCount int) {
	t.Helper()
	_, err := svc.RegisterApp(context.Background(), cycle.RegisterAppRequest{
		OwnerUserID: user, AppID: appID, ModuleCount: moduleCount, CreatedAt: created,
	})
	require.NoError(t, err)
}

// --- (a) grace holds: an app charged before GraceDays elapse is impossible ---

func TestSweep_SkipsAppsWithinGrace(t *testing.T) {
	store := newFakeStore()
	user, _ := registeredAccount(store)
	sc := newFakeStripe()
	svc := appsSvc(store, sc)
	appID := uuid.New()
	registerMirror(t, svc, user, appID, time.Date(2026, 7, 1, 8, 0, 0, 0, time.UTC), 0)

	// Sweep only 2 days later (< GraceDays=3): the app is INSIDE the grace window
	// (created_at Jul 1 > cutoff Jul 3 − 3d = Jun 30), so it is not even a
	// candidate — no charge, nothing pending.
	res, err := svc.SweepCreationProrations(context.Background(), time.Date(2026, 7, 3, 9, 0, 0, 0, time.UTC))
	require.NoError(t, err)
	require.Equal(t, 0, res.Pending, "an app younger than GraceDays is not swept")
	require.Equal(t, 0, res.Charged)
	require.Empty(t, sc.itemCalls, "charging within grace is impossible")
	require.Empty(t, store.apps[appID].ProrationInvoiceID)
}

// --- (b) an app deleted within grace is NEVER charged, even by a later sweep --

func TestSweep_NeverChargesAppDeletedWithinGrace(t *testing.T) {
	store := newFakeStore()
	user, _ := registeredAccount(store)
	sc := newFakeStripe()
	svc := appsSvc(store, sc)
	appID := uuid.New()
	registerMirror(t, svc, user, appID, time.Date(2026, 7, 1, 8, 0, 0, 0, time.UTC), 0)

	// Deleted on day 0–1 (well within grace).
	_, err := svc.SyncAppModules(context.Background(), cycle.SyncAppModulesRequest{AppID: appID, Deleted: true})
	require.NoError(t, err)
	require.True(t, store.apps[appID].Deleted)

	// A sweep 9 days later (long past grace) must still NEVER charge it: the
	// deleted row is excluded from the work list.
	res, err := svc.SweepCreationProrations(context.Background(), time.Date(2026, 7, 10, 0, 0, 0, 0, time.UTC))
	require.NoError(t, err)
	require.Equal(t, 0, res.Pending, "a deleted app is excluded from the sweep")
	require.Empty(t, sc.itemCalls, "an app deleted within grace is never charged")
	require.Empty(t, store.apps[appID].ProrationInvoiceID)
}

// --- (c) a survivor is charged EXACTLY ONCE even if the sweep runs twice ------

func TestSweep_ChargesSurvivorExactlyOnceAcrossReruns(t *testing.T) {
	store := newFakeStore()
	user, _ := registeredAccount(store)
	sc := newFakeStripe()
	svc := appsSvc(store, sc)
	appID := uuid.New()
	registerMirror(t, svc, user, appID, time.Date(2026, 7, 1, 8, 0, 0, 0, time.UTC), 0)

	// First sweep past grace → charges the creation proration once.
	first, err := svc.SweepCreationProrations(context.Background(), time.Date(2026, 7, 5, 0, 0, 0, 0, time.UTC))
	require.NoError(t, err)
	require.Equal(t, 1, first.Pending)
	require.Equal(t, 1, first.Charged)
	require.Len(t, sc.itemCalls, 1)
	require.EqualValues(t, 200, sc.itemCalls[0].amountCfg) // 3 of 30 days of $20
	armed := store.apps[appID].ProrationInvoiceID
	require.NotEmpty(t, armed)

	// Second sweep (a re-fire the next day): the guard is armed, so the app is no
	// longer pending and no second invoice is created.
	second, err := svc.SweepCreationProrations(context.Background(), time.Date(2026, 7, 6, 0, 0, 0, 0, time.UTC))
	require.NoError(t, err)
	require.Equal(t, 0, second.Pending, "an already-charged app drops out of the work list")
	require.Equal(t, 0, second.Charged)
	require.Len(t, sc.itemCalls, 1, "the re-fire must never charge twice")
	require.Equal(t, armed, store.apps[appID].ProrationInvoiceID)
}

// --- (d) the proration $ amount is unchanged from the pre-grace charge --------

func TestChargeCreationProration_AmountMatchesLegacyProration(t *testing.T) {
	// The SAME numbers the pre-grace RegisterApp charge produced, now via the
	// delayed charge leg: 20e6 × 3/30 = 2_000_000 micros → 200 cents, mirrored
	// with the PARTIAL window [creation day, period end), snapshot frozen at the
	// FULL anchored period start with the prorated amount.
	store := newFakeStore()
	user, acct := registeredAccount(store)
	sc := newFakeStripe()
	svc := appsSvc(store, sc)
	appID := uuid.New()
	registerMirror(t, svc, user, appID, time.Date(2026, 7, 1, 8, 0, 0, 0, time.UTC), 0)

	resp, err := svc.ChargeCreationProration(context.Background(), appID)
	require.NoError(t, err)
	require.Equal(t, cycle.ProrationStatusCharged, resp.Status)
	require.EqualValues(t, 200, resp.ProrationCents)

	require.Len(t, sc.itemCalls, 1)
	require.Len(t, sc.invoiceCalls, 1)
	require.EqualValues(t, 200, sc.itemCalls[0].amountCfg)
	require.Equal(t, "cus_apps_1", sc.itemCalls[0].custID)
	require.Equal(t, "app-ii-"+appID.String(), sc.itemCalls[0].idemKey)
	require.Equal(t, "app-inv-"+appID.String(), sc.invoiceCalls[0].idemKey)
	require.True(t, sc.invoiceCalls[0].autoAdvance)

	require.Equal(t, sc.invoiceID, resp.ProrationInvoiceID)
	mirror := store.invoices[sc.invoiceID]
	require.Equal(t, acct, mirror.AccountID)
	require.Equal(t, time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC), mirror.PeriodStart) // partial coverage start
	require.Equal(t, time.Date(2026, 7, 4, 0, 0, 0, 0, time.UTC), mirror.PeriodEnd)
	require.Equal(t, sc.invoiceID, store.apps[appID].ProrationInvoiceID)

	snap, ok := store.baseSnapshots[snapKey{appID, time.Date(2026, 6, 4, 0, 0, 0, 0, time.UTC)}]
	require.True(t, ok, "the charge freezes its base keyed on the FULL anchored period start")
	require.Equal(t, "proration", snap.source)
	require.EqualValues(t, 2_000_000, snap.snap.BaseMicros)
	require.Equal(t, time.Date(2026, 7, 4, 0, 0, 0, 0, time.UTC), snap.snap.PeriodEnd)
	require.Equal(t, 0, snap.snap.ModuleCount)
}

func TestChargeCreationProration_IncludesModuleOverage(t *testing.T) {
	// module_count 7 → base 20e6 + 2×3e6 = 26e6; 15 of 30 remaining days →
	// 13e6 micros → 1300 cents. Identical to the pre-grace math.
	store := newFakeStore()
	user, _ := registeredAccount(store)
	sc := newFakeStripe()
	svc := appsSvc(store, sc)
	appID := uuid.New()
	registerMirror(t, svc, user, appID, time.Date(2026, 6, 19, 12, 0, 0, 0, time.UTC), 7)

	resp, err := svc.ChargeCreationProration(context.Background(), appID)
	require.NoError(t, err)
	require.EqualValues(t, 1300, resp.ProrationCents)
	require.Equal(t, 7, store.baseSnapshots[snapKey{appID, time.Date(2026, 6, 4, 0, 0, 0, 0, time.UTC)}].snap.ModuleCount)
}

func TestChargeCreationProration_CreatedOnBoundaryChargesFullNewPeriodBase(t *testing.T) {
	// Created exactly ON an anchor boundary (Jul 4 00:00): half-open windows put
	// it at the START of the NEW period [Jul 4, Aug 4) → the FULL base ($20 →
	// 2000 cents), snapshot keyed on Jul 4. Unchanged from the pre-grace charge.
	store := newFakeStore()
	user, _ := registeredAccount(store)
	sc := newFakeStripe()
	svc := appsSvc(store, sc)
	appID := uuid.New()
	registerMirror(t, svc, user, appID, time.Date(2026, 7, 4, 0, 0, 0, 0, time.UTC), 0)

	resp, err := svc.ChargeCreationProration(context.Background(), appID)
	require.NoError(t, err)
	require.EqualValues(t, 2_000, resp.ProrationCents, "creation-day == period start → full base")
	mirror := store.invoices[sc.invoiceID]
	require.Equal(t, time.Date(2026, 7, 4, 0, 0, 0, 0, time.UTC), mirror.PeriodStart)
	require.Equal(t, time.Date(2026, 8, 4, 0, 0, 0, 0, time.UTC), mirror.PeriodEnd)
	snap := store.baseSnapshots[snapKey{appID, time.Date(2026, 7, 4, 0, 0, 0, 0, time.UTC)}]
	require.Equal(t, "proration", snap.source)
	require.EqualValues(t, usage.BaseFeeMicros, snap.snap.BaseMicros)
}

func TestChargeCreationProration_DelayedPastPeriodEndStillCharges(t *testing.T) {
	// Delayed billing (grace point 5): the charge is anchored to the TRUE
	// created_at, NOT now, so even when grace pushes the charge past the creation
	// period's end, the creation-period proration STILL fires — that period is
	// billed by NO other leg (the boundary advance leg only ever bills an app's
	// SUBSEQUENT periods), so charging it is correct and never double-bills.
	// App created May 20 → period [May 4, Jun 4), long closed by the sweep in
	// July: 15 of 31 days of $20 = round_half_up(9_677_419.8) → 968 cents.
	store := newFakeStore()
	user, _ := registeredAccount(store)
	sc := newFakeStripe()
	svc := appsSvc(store, sc)
	appID := uuid.New()
	registerMirror(t, svc, user, appID, time.Date(2026, 5, 20, 9, 0, 0, 0, time.UTC), 0)

	res, err := svc.SweepCreationProrations(context.Background(), time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC))
	require.NoError(t, err)
	require.Equal(t, 1, res.Charged)
	require.Len(t, sc.itemCalls, 1)
	require.EqualValues(t, 968, sc.itemCalls[0].amountCfg)
	snap := store.baseSnapshots[snapKey{appID, time.Date(2026, 5, 4, 0, 0, 0, 0, time.UTC)}]
	require.Equal(t, "proration", snap.source)
}

// --- ChargeCreationProration: idempotency + gates ----------------------------

func TestChargeCreationProration_IdempotentGuard(t *testing.T) {
	store := newFakeStore()
	user, _ := registeredAccount(store)
	sc := newFakeStripe()
	svc := appsSvc(store, sc)
	appID := uuid.New()
	registerMirror(t, svc, user, appID, time.Date(2026, 7, 1, 8, 0, 0, 0, time.UTC), 0)

	first, err := svc.ChargeCreationProration(context.Background(), appID)
	require.NoError(t, err)
	require.Equal(t, cycle.ProrationStatusCharged, first.Status)

	second, err := svc.ChargeCreationProration(context.Background(), appID)
	require.NoError(t, err)
	require.Equal(t, cycle.ProrationStatusAlreadyCharged, second.Status)
	require.Equal(t, first.ProrationInvoiceID, second.ProrationInvoiceID)
	require.Len(t, sc.itemCalls, 1, "the one-shot guard prevents a second charge")
}

func TestChargeCreationProration_SkipsDeleted(t *testing.T) {
	store := newFakeStore()
	user, _ := registeredAccount(store)
	sc := newFakeStripe()
	svc := appsSvc(store, sc)
	appID := uuid.New()
	registerMirror(t, svc, user, appID, time.Date(2026, 7, 1, 8, 0, 0, 0, time.UTC), 0)
	_, err := svc.SyncAppModules(context.Background(), cycle.SyncAppModulesRequest{AppID: appID, Deleted: true})
	require.NoError(t, err)

	resp, err := svc.ChargeCreationProration(context.Background(), appID)
	require.NoError(t, err)
	require.Equal(t, cycle.ProrationStatusDeleted, resp.Status)
	require.Empty(t, sc.itemCalls)
}

func TestChargeCreationProration_SkipsUnactivatedAndNoPM(t *testing.T) {
	// Unactivated account → skipped_unactivated (D1d, no retroactive catch-up).
	store := newFakeStore()
	user, acct := registeredAccount(store)
	delete(store.activation, acct)
	sc := newFakeStripe()
	svc := appsSvc(store, sc)
	appID := uuid.New()
	registerMirror(t, svc, user, appID, time.Date(2026, 7, 1, 8, 0, 0, 0, time.UTC), 0)

	resp, err := svc.ChargeCreationProration(context.Background(), appID)
	require.NoError(t, err)
	require.Equal(t, cycle.ProrationStatusUnactivated, resp.Status)
	require.Empty(t, sc.itemCalls)

	// Activated but no usable PM → skipped_no_pm (re-attempted next sweep).
	store.activation[acct] = time.Date(2026, 5, 4, 9, 0, 0, 0, time.UTC)
	store.hasPM = false
	resp, err = svc.ChargeCreationProration(context.Background(), appID)
	require.NoError(t, err)
	require.Equal(t, cycle.ProrationStatusNoPM, resp.Status)
	require.Empty(t, sc.itemCalls)
	require.Empty(t, store.apps[appID].ProrationInvoiceID)
}

func TestChargeCreationProration_NotFound(t *testing.T) {
	svc := appsSvc(newFakeStore(), newFakeStripe())
	resp, err := svc.ChargeCreationProration(context.Background(), uuid.New())
	require.NoError(t, err)
	require.Equal(t, cycle.ProrationStatusNotFound, resp.Status)
}

func TestChargeCreationProration_Validation(t *testing.T) {
	svc := appsSvc(newFakeStore(), newFakeStripe())
	_, err := svc.ChargeCreationProration(context.Background(), uuid.Nil)
	requireCode(t, err, billing.CodeInvalidInput)
}

func TestSweepCreationProrations_Validation(t *testing.T) {
	svc := appsSvc(newFakeStore(), newFakeStripe())
	_, err := svc.SweepCreationProrations(context.Background(), time.Time{})
	requireCode(t, err, billing.CodeInvalidInput)
}

// --- Sweep: multiple apps, mixed states --------------------------------------

func TestSweep_ChargesOnlyPastGraceLiveUnchargedApps(t *testing.T) {
	store := newFakeStore()
	user, _ := registeredAccount(store)
	sc := newFakeStripe()
	svc := appsSvc(store, sc)

	past := uuid.New()  // past grace, live, uncharged → charged
	young := uuid.New() // within grace → skipped
	gone := uuid.New()  // past grace but deleted → skipped
	registerMirror(t, svc, user, past, time.Date(2026, 6, 20, 8, 0, 0, 0, time.UTC), 0)
	registerMirror(t, svc, user, young, time.Date(2026, 6, 29, 8, 0, 0, 0, time.UTC), 0)
	registerMirror(t, svc, user, gone, time.Date(2026, 6, 20, 8, 0, 0, 0, time.UTC), 0)
	_, err := svc.SyncAppModules(context.Background(), cycle.SyncAppModulesRequest{AppID: gone, Deleted: true})
	require.NoError(t, err)

	// Sweep as of Jun 30 → cutoff Jun 27: past (Jun 20) qualifies; young (Jun 29)
	// is within grace; gone is deleted.
	res, err := svc.SweepCreationProrations(context.Background(), time.Date(2026, 6, 30, 0, 0, 0, 0, time.UTC))
	require.NoError(t, err)
	require.Equal(t, 1, res.Pending)
	require.Equal(t, 1, res.Charged)
	require.NotEmpty(t, store.apps[past].ProrationInvoiceID)
	require.Empty(t, store.apps[young].ProrationInvoiceID)
	require.Empty(t, store.apps[gone].ProrationInvoiceID)
}
