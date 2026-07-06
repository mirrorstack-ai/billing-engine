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
	billingstripe "github.com/mirrorstack-ai/billing-engine/internal/shared/stripe"
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

// Regression (review 2026-07-06, H5): a creation-proration retry past Stripe's
// ~24h idempotency-key window reconciles by the app's ms_charge_ref anchor —
// a crashed attempt's finalized combined invoice is adopted (guard armed with
// ITS id, timers marked against it) with no new Stripe objects.
func TestChargeCreationProration_LateRetryAdoptsFoundInvoice(t *testing.T) {
	store := newFakeStore()
	user, _ := registeredAccount(store)
	sc := newFakeStripe()
	svc := appsSvc(store, sc)
	appID := uuid.New()
	registerMirror(t, svc, user, appID, time.Date(2026, 6, 19, 12, 0, 0, 0, time.UTC), 0)

	// A prior attempt reached its Stripe section (marker set), finalized the
	// invoice, and crashed before persisting.
	app := store.apps[appID]
	app.ProrationAttempted = true
	store.apps[appID] = app
	sc.findByRef = &billingstripe.Invoice{ID: "in_prior_combined", Status: "paid", AmountDue: 1000, AmountPaid: 1000, Currency: "usd"}

	resp, err := svc.ChargeCreationProration(context.Background(), appID)
	require.NoError(t, err)
	require.Equal(t, cycle.ProrationStatusCharged, resp.Status)
	require.Equal(t, "in_prior_combined", resp.ProrationInvoiceID)
	require.Equal(t, "in_prior_combined", store.apps[appID].ProrationInvoiceID, "the guard arms with the recovered invoice")
	require.Empty(t, sc.invoiceCalls, "no second draft")
	require.Empty(t, sc.itemCalls, "no re-attached lines")
	require.Empty(t, sc.finalizeCalls, "no second finalize — the money moved once")
}

// Regression (review 2026-07-06, H10): a PREPAID account is never auto-charged
// off-session — the boundary spine always gated on this, but the creation-
// proration leg bypassed it. Transient skip (guard unarmed); a relax back to
// arrears charges through the same keys.
func TestChargeCreationProration_PrepaidAccountSkippedNotCharged(t *testing.T) {
	store := newFakeStore()
	user, _ := registeredAccount(store)
	store.collection.Mode = cycle.BillingModePrepaid
	sc := newFakeStripe()
	svc := appsSvc(store, sc)
	appID := uuid.New()
	registerMirror(t, svc, user, appID, time.Date(2026, 6, 19, 12, 0, 0, 0, time.UTC), 0)

	resp, err := svc.ChargeCreationProration(context.Background(), appID)
	require.NoError(t, err)
	require.Equal(t, cycle.ProrationStatusPrepaid, resp.Status)
	require.Empty(t, sc.invoiceCalls, "a prepaid account is never auto-charged by the creation leg")
	require.Empty(t, store.apps[appID].ProrationInvoiceID, "transient skip — the guard stays unarmed")

	// Relax → the deferred creation charge fires normally.
	store.collection.Mode = cycle.BillingModeArrears
	resp, err = svc.ChargeCreationProration(context.Background(), appID)
	require.NoError(t, err)
	require.Equal(t, cycle.ProrationStatusCharged, resp.Status)
	require.NotEmpty(t, store.apps[appID].ProrationInvoiceID)
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
	// 3 of 30 days of $20 ($2) + the straddled [Jul 4, Aug 4) period in full
	// ($20) — created Jul 1 08:00, so the grace crosses the Jul 4 boundary and
	// the advance leg excludes the app there (coverage contract, H2).
	require.EqualValues(t, 2200, sc.itemCalls[0].amountCfg)
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

// --- (d) the proration $ amount, mirror window, and snapshots -----------------

func TestChargeCreationProration_AmountMatchesLegacyProration(t *testing.T) {
	// The creation-period part is the SAME number the pre-grace RegisterApp
	// charge produced: 20e6 × 3/30 = 2_000_000 micros. Created Jul 1 08:00, the
	// grace crosses the Jul 4 boundary (coverage contract, H2), so the charge
	// ALSO covers the straddled [Jul 4, Aug 4) period in full: 22e6 micros →
	// 2200 cents, mirrored with the window [creation day, straddled period end),
	// TWO snapshots frozen — the creation period's prorated amount and the
	// straddled period's full base.
	store := newFakeStore()
	user, acct := registeredAccount(store)
	sc := newFakeStripe()
	svc := appsSvc(store, sc)
	appID := uuid.New()
	registerMirror(t, svc, user, appID, time.Date(2026, 7, 1, 8, 0, 0, 0, time.UTC), 0)

	resp, err := svc.ChargeCreationProration(context.Background(), appID)
	require.NoError(t, err)
	require.Equal(t, cycle.ProrationStatusCharged, resp.Status)
	require.EqualValues(t, 2200, resp.ProrationCents)

	require.Len(t, sc.itemCalls, 1)
	require.Len(t, sc.invoiceCalls, 1)
	require.EqualValues(t, 2200, sc.itemCalls[0].amountCfg)
	require.Equal(t, "cus_apps_1", sc.itemCalls[0].custID)
	require.Equal(t, "app-ii-"+appID.String(), sc.itemCalls[0].idemKey)
	require.Equal(t, "app-inv-"+appID.String(), sc.invoiceCalls[0].idemKey)
	require.Len(t, sc.finalizeCalls, 1, "the draft is finalized (auto_advance) — the money-moving step")

	require.Equal(t, sc.invoiceID, resp.ProrationInvoiceID)
	mirror := store.invoices[sc.invoiceID]
	require.Equal(t, acct, mirror.AccountID)
	require.Equal(t, time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC), mirror.PeriodStart) // partial coverage start
	require.Equal(t, time.Date(2026, 8, 4, 0, 0, 0, 0, time.UTC), mirror.PeriodEnd, "coverage runs through the straddled period's end")
	require.Equal(t, sc.invoiceID, store.apps[appID].ProrationInvoiceID)

	snap, ok := store.baseSnapshots[snapKey{appID, time.Date(2026, 6, 4, 0, 0, 0, 0, time.UTC)}]
	require.True(t, ok, "the charge freezes its base keyed on the FULL anchored period start")
	require.Equal(t, "proration", snap.source)
	require.EqualValues(t, 2_000_000, snap.snap.BaseMicros, "the creation-period snapshot carries only the prorated part")
	require.Equal(t, time.Date(2026, 7, 4, 0, 0, 0, 0, time.UTC), snap.snap.PeriodEnd)
	require.Equal(t, 0, snap.snap.ModuleCount)

	straddleSnap, ok := store.baseSnapshots[snapKey{appID, time.Date(2026, 7, 4, 0, 0, 0, 0, time.UTC)}]
	require.True(t, ok, "the straddled period billed in full gets its own snapshot (the boundary leg writes nothing for it)")
	require.Equal(t, "proration", straddleSnap.source)
	require.EqualValues(t, 20_000_000, straddleSnap.snap.BaseMicros)
	require.Equal(t, time.Date(2026, 8, 4, 0, 0, 0, 0, time.UTC), straddleSnap.snap.PeriodEnd)
}

func TestChargeCreationProration_ChargesFlatBaseNotFoldedOverage(t *testing.T) {
	// Migration 032: module overage is NO LONGER folded into the per-app base —
	// the creation proration is the FLAT $20 base regardless of module_count (a
	// 7-module app prorates EXACTLY like a 0-module app). 15 of 30 remaining days
	// → 20e6 × 15/30 = 10e6 micros → 1000 cents (NOT the pre-032 26e6 → 1300).
	// Overage for the modules co-created with the app is a SEPARATE line, added
	// by the module-overage grace leg (see overage tests).
	store := newFakeStore()
	user, _ := registeredAccount(store)
	sc := newFakeStripe()
	svc := appsSvc(store, sc)
	appID := uuid.New()
	registerMirror(t, svc, user, appID, time.Date(2026, 6, 19, 12, 0, 0, 0, time.UTC), 7)

	resp, err := svc.ChargeCreationProration(context.Background(), appID)
	require.NoError(t, err)
	require.EqualValues(t, 1000, resp.ProrationCents, "flat base only — overage is billed per module instance, not folded here")
	// The frozen created_module_count (7) is still recorded on the snapshot for
	// display, even though it no longer moves the base amount.
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
	// created_at, NOT now, so even when grace + ordinary sweep cadence pushes the
	// charge attempt past the creation period's end, the creation-period
	// proration STILL fires — that period is billed by NO other leg (the
	// boundary advance leg only ever bills an app's SUBSEQUENT periods), so
	// charging it is correct and never double-bills. This is NOT the D1d
	// retroactive-catch-up case (see TestChargeCreationProration_
	// SkipsPermanentlyWhenActivatedAfterPeriodClosed below): the account here was
	// ALREADY activated (May 4, registeredAccount) well before the app's period
	// even opened — the period-closed check in ChargeCreationProration compares
	// against activatedAt, not "now", so a healthy already-activated account is
	// never penalized for a sweep that simply runs late.
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

// --- FINDING 1: the creation-proration charge must price off the module count
// FROZEN at RegisterApp time, never whatever module_count SyncAppModules moves
// it to during (or after) the grace window ----------------------------------

func TestChargeCreationProration_PricesFrozenCountNotLiveCountAfterMidGraceInstall(t *testing.T) {
	// Reproduces the exact failure scenario: an app registers with 0 modules,
	// the customer installs 7 MORE modules (via SyncAppModules) DURING the
	// mandatory grace window — before the sweep ever charges — and the sweep
	// fires after grace elapses. Pre-fix, the charge priced off the module_count
	// read FRESH at sweep time (7, live) → 20e6 + 2×3e6 = 26e6 base, 3 of 30 days
	// → 2_600_000 micros: a HIGHER tier retroactively applied to days that never
	// had 7 modules installed. Fixed: the charge prices off created_module_count
	// (frozen at registration, 0) → 20e6 base, 3 of 30 days → 2_000_000 micros,
	// plus the straddled [Jul 4, Aug 4) period's full base (created Jul 1 08:00 —
	// the grace crosses the boundary) → 22e6 → 2200 cents — identical to
	// TestChargeCreationProration_AmountMatchesLegacyProration's un-synced case.
	store := newFakeStore()
	user, _ := registeredAccount(store)
	sc := newFakeStripe()
	svc := appsSvc(store, sc)
	appID := uuid.New()
	registerMirror(t, svc, user, appID, time.Date(2026, 7, 1, 8, 0, 0, 0, time.UTC), 0)

	// Mid-grace install: the live count jumps to 7 BEFORE the sweep ever runs.
	_, err := svc.SyncAppModules(context.Background(), cycle.SyncAppModulesRequest{
		AppID: appID, ModuleCount: intPtr(7),
	})
	require.NoError(t, err)
	require.Equal(t, 7, store.apps[appID].ModuleCount, "the live count DID move")
	require.Equal(t, 0, store.apps[appID].CreatedModuleCount, "the frozen count must NOT move")

	resp, err := svc.ChargeCreationProration(context.Background(), appID)
	require.NoError(t, err)
	require.Equal(t, cycle.ProrationStatusCharged, resp.Status)
	require.EqualValues(t, 2200, resp.ProrationCents,
		"must price off the FROZEN count (0 modules → $20 base), not the live count (7 → $26 base)")
	require.Len(t, sc.itemCalls, 1)
	require.EqualValues(t, 2200, sc.itemCalls[0].amountCfg)

	// The migration-028 snapshot must also record the FROZEN count/amount — the
	// display must never show a tier that never applied to those days either.
	snap := store.baseSnapshots[snapKey{appID, time.Date(2026, 6, 4, 0, 0, 0, 0, time.UTC)}]
	require.Equal(t, 0, snap.snap.ModuleCount)
	require.EqualValues(t, 2_000_000, snap.snap.BaseMicros)

	// The LIVE count survives untouched for the boundary advance leg's future
	// periods — only the historical creation-period charge is frozen.
	require.Equal(t, 7, store.apps[appID].ModuleCount)
}

func TestChargeCreationProration_FlatBaseUnaffectedByMidGraceUninstall(t *testing.T) {
	// Migration 032: the creation base is FLAT, so a mid-grace uninstall (7 → 0
	// modules) cannot move it — it prorates the flat $20 either way: 15 of 30
	// remaining days → 10e6 micros → 1000 cents. The frozen created_module_count
	// (7) is still preserved for display (the snapshot ModuleCount), it just no
	// longer drives the base amount now that overage is a separate per-module leg.
	store := newFakeStore()
	user, _ := registeredAccount(store)
	sc := newFakeStripe()
	svc := appsSvc(store, sc)
	appID := uuid.New()
	registerMirror(t, svc, user, appID, time.Date(2026, 6, 19, 12, 0, 0, 0, time.UTC), 7)

	_, err := svc.SyncAppModules(context.Background(), cycle.SyncAppModulesRequest{
		AppID: appID, ModuleCount: intPtr(0),
	})
	require.NoError(t, err)
	require.Equal(t, 0, store.apps[appID].ModuleCount)
	require.Equal(t, 7, store.apps[appID].CreatedModuleCount, "the frozen count must NOT move")

	resp, err := svc.ChargeCreationProration(context.Background(), appID)
	require.NoError(t, err)
	require.EqualValues(t, 1_000, resp.ProrationCents, "flat base — unaffected by the module count or its mid-grace change")
	// The frozen count is still recorded on the snapshot for display.
	require.Equal(t, 7, store.baseSnapshots[snapKey{appID, time.Date(2026, 6, 4, 0, 0, 0, 0, time.UTC)}].snap.ModuleCount)
}

// --- FINDING 2: no retroactive catch-up (D1d) when an account only activates
// after the app's anchored creation period already closed -------------------

func TestChargeCreationProration_SkipsPermanentlyWhenActivatedAfterPeriodClosed(t *testing.T) {
	// Reproduces the exact failure scenario: an app is created while its account
	// is unactivated. Every sweep correctly leaves it unbilled (skipped_
	// unactivated). MONTHS later the owner finally binds a card — with anchor
	// day 1 (activated Apr 1), the app's anchored creation period is
	// [Jan 1, Feb 1), long closed by Apr 1. Pre-fix, the very next charge
	// attempt would retroactively bill that period in FULL (2000 cents on 0
	// modules, since Jan 1 == the period start): exactly the retroactive
	// catch-up D1d forbids. Fixed: the charge is PERMANENTLY skipped — no
	// Stripe call, ever, and the app never resurfaces on a later sweep (it
	// would otherwise sit pending forever, since proration_invoice_id never
	// gets set for a skipped charge).
	store := newFakeStore()
	user, acct := registeredAccount(store)
	delete(store.activation, acct) // unactivated at creation
	sc := newFakeStripe()
	svc := appsSvc(store, sc)
	appID := uuid.New()
	created := time.Date(2026, 1, 1, 8, 0, 0, 0, time.UTC)
	registerMirror(t, svc, user, appID, created, 0)

	// Past grace, still unactivated → correctly pending, no charge (D1d's
	// existing unactivated gate — unchanged by this fix).
	first, err := svc.ChargeCreationProration(context.Background(), appID)
	require.NoError(t, err)
	require.Equal(t, cycle.ProrationStatusUnactivated, first.Status)
	require.Empty(t, sc.itemCalls)

	// MONTHS later: the owner binds a card (anchor day 1) and a PM.
	store.activation[acct] = time.Date(2026, 4, 1, 9, 0, 0, 0, time.UTC)
	store.hasPM = true

	second, err := svc.ChargeCreationProration(context.Background(), appID)
	require.NoError(t, err)
	require.Equal(t, cycle.ProrationStatusPeriodClosed, second.Status)
	require.Empty(t, sc.itemCalls, "no Stripe call — a retroactive catch-up charge must never fire")
	require.Empty(t, store.apps[appID].ProrationInvoiceID)
	require.True(t, store.apps[appID].ProrationSkipped, "permanently marked so it is never re-evaluated")

	// A cheap re-evaluation (e.g. a retried RPC) short-circuits on the marker
	// without even re-reading account activation — still no Stripe call.
	third, err := svc.ChargeCreationProration(context.Background(), appID)
	require.NoError(t, err)
	require.Equal(t, cycle.ProrationStatusPeriodClosed, third.Status)
	require.Empty(t, sc.itemCalls)

	// A LATER sweep must never resurface it — it would otherwise sit pending
	// forever (proration_invoice_id stays NULL for a permanently-skipped app).
	res, err := svc.SweepCreationProrations(context.Background(), time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC))
	require.NoError(t, err)
	require.Equal(t, 0, res.Pending, "a permanently-skipped app never resurfaces on a later sweep")
	require.Empty(t, sc.itemCalls)
}

func TestChargeCreationProration_ActivatedBeforePeriodClosesStillCharges(t *testing.T) {
	// Guard against an over-broad fix: an account that activates BEFORE the
	// app's anchored creation period closes must charge normally — D1d only
	// blocks a retroactive catch-up when the account was unactivated for the
	// app's ENTIRE creation period. The anchor day is DERIVED from activatedAt
	// itself (billingperiod.AnchorDay), so activating the SAME calendar day the
	// app was created (the common "sign up, create an app, add a card" onboarding
	// flow) anchors the period at that same day-of-month — putting its END a
	// full month out, safely after activation.
	store := newFakeStore()
	user, acct := registeredAccount(store)
	delete(store.activation, acct)
	sc := newFakeStripe()
	svc := appsSvc(store, sc)
	appID := uuid.New()
	registerMirror(t, svc, user, appID, time.Date(2026, 1, 10, 6, 0, 0, 0, time.UTC), 0)

	first, err := svc.ChargeCreationProration(context.Background(), appID)
	require.NoError(t, err)
	require.Equal(t, cycle.ProrationStatusUnactivated, first.Status)

	// Activates a few hours later the SAME day (anchor day 10) → period
	// [Jan 10, Feb 10) — still wide open — and binds a PM.
	store.activation[acct] = time.Date(2026, 1, 10, 9, 0, 0, 0, time.UTC)
	store.hasPM = true

	resp, err := svc.ChargeCreationProration(context.Background(), appID)
	require.NoError(t, err)
	require.Equal(t, cycle.ProrationStatusCharged, resp.Status)
	require.EqualValues(t, 2_000, resp.ProrationCents, "created on/after the period start → full base")
	require.Len(t, sc.itemCalls, 1)
	require.False(t, store.apps[appID].ProrationSkipped)
}
