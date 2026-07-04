package cycle_test

// RegisterApp / SyncAppModules + the boundary advance-base leg (base-fee v1,
// owner spec 2026-07-05). Reuses the in-memory fakeStore (service_test.go) and
// fakeStripe (charge_test.go) — no new harness.

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

// Fixed clock: 2026-07-01 10:00 UTC. With an anchor-4 activation the current
// anchored period is [2026-06-04, 2026-07-04) — 30 days — so an app created
// "on the 1st" has exactly the owner's worked 3 remaining days.
var appsNow = time.Date(2026, 7, 1, 10, 0, 0, 0, time.UTC)

// registeredAccount seeds the fake with a user → account mapping plus the
// chargeable state (activation anchor, usable PM, Stripe customer) and returns
// (user, account). Tests weaken the gates from this fully-chargeable baseline.
func registeredAccount(store *fakeStore) (uuid.UUID, uuid.UUID) {
	user, acct := uuid.New(), uuid.New()
	store.accountsByUser[user] = acct
	store.activation[acct] = time.Date(2026, 5, 4, 9, 0, 0, 0, time.UTC) // anchor day 4
	store.hasPM = true
	store.stripeCustomer = "cus_apps_1"
	return user, acct
}

func appsSvc(store *fakeStore, sc *fakeStripe) *cycle.Service {
	return cycle.NewService(store, sc).WithNow(func() time.Time { return appsNow })
}

// --- RegisterApp: proration happy path --------------------------------------

func TestRegisterApp_ChargesCreationProration(t *testing.T) {
	store := newFakeStore()
	user, acct := registeredAccount(store)
	sc := newFakeStripe()
	appID := uuid.New()

	resp, err := appsSvc(store, sc).RegisterApp(context.Background(), cycle.RegisterAppRequest{
		OwnerUserID: user,
		AppID:       appID,
		CreatedAt:   time.Date(2026, 7, 1, 8, 0, 0, 0, time.UTC), // the 1st → days 1–3 of [Jun 4, Jul 4)
	})
	require.NoError(t, err)
	require.Equal(t, acct, resp.AccountID)

	// 20e6 × 3/30 = 2_000_000 micros → 200 cents, one item + one invoice with
	// the app-scoped deterministic idem keys.
	require.EqualValues(t, 200, resp.ProrationCents)
	require.Len(t, sc.itemCalls, 1)
	require.Len(t, sc.invoiceCalls, 1)
	require.EqualValues(t, 200, sc.itemCalls[0].amountCfg)
	require.Equal(t, "cus_apps_1", sc.itemCalls[0].custID)
	require.Equal(t, "app-ii-"+appID.String(), sc.itemCalls[0].idemKey)
	require.Equal(t, "app-inv-"+appID.String(), sc.invoiceCalls[0].idemKey)
	require.True(t, sc.invoiceCalls[0].autoAdvance)

	// Mirrored with the PARTIAL window [creation day, period end) and the
	// one-shot guard armed with the invoice id.
	require.Equal(t, resp.ProrationInvoiceID, sc.invoiceID)
	mirror := store.invoices[sc.invoiceID]
	require.Equal(t, acct, mirror.AccountID)
	require.Equal(t, time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC), mirror.PeriodStart)
	require.Equal(t, time.Date(2026, 7, 4, 0, 0, 0, 0, time.UTC), mirror.PeriodEnd)
	require.Equal(t, sc.invoiceID, store.apps[appID].ProrationInvoiceID)

	// And the migration-028 snapshot froze EXACTLY what was billed, keyed by
	// the FULL anchored period start (the display identity), with the
	// prorated partial-window amount.
	snap, ok := store.baseSnapshots[snapKey{appID, time.Date(2026, 6, 4, 0, 0, 0, 0, time.UTC)}]
	require.True(t, ok, "the proration leg must freeze its charged base")
	require.Equal(t, "proration", snap.source)
	require.EqualValues(t, 2_000_000, snap.snap.BaseMicros)
	require.Equal(t, time.Date(2026, 7, 4, 0, 0, 0, 0, time.UTC), snap.snap.PeriodEnd)
	require.Equal(t, 0, snap.snap.ModuleCount)
}

func TestRegisterApp_ProrationIncludesModuleOverage(t *testing.T) {
	// module_count 7 at creation → base_at_creation = 20e6 + 2×3e6 = 26e6;
	// 15 of 30 remaining days → 13e6 micros → 1300 cents.
	store := newFakeStore()
	user, _ := registeredAccount(store)
	sc := newFakeStripe()

	resp, err := appsSvc(store, sc).RegisterApp(context.Background(), cycle.RegisterAppRequest{
		OwnerUserID: user,
		AppID:       uuid.New(),
		ModuleCount: 7,
		CreatedAt:   time.Date(2026, 6, 19, 12, 0, 0, 0, time.UTC),
	})
	require.NoError(t, err)
	require.EqualValues(t, 1300, resp.ProrationCents)
}

func TestRegisterApp_DefaultsCreatedAtToNow(t *testing.T) {
	// Zero CreatedAt → the server's now (appsNow, Jul 1) → the same 3-day
	// proration as the explicit-timestamp case.
	store := newFakeStore()
	user, _ := registeredAccount(store)
	sc := newFakeStripe()

	resp, err := appsSvc(store, sc).RegisterApp(context.Background(), cycle.RegisterAppRequest{
		OwnerUserID: user, AppID: uuid.New(),
	})
	require.NoError(t, err)
	require.EqualValues(t, 200, resp.ProrationCents)
}

// --- RegisterApp: idempotency ------------------------------------------------

func TestRegisterApp_RetryNeverChargesTwice(t *testing.T) {
	store := newFakeStore()
	user, _ := registeredAccount(store)
	sc := newFakeStripe()
	svc := appsSvc(store, sc)
	req := cycle.RegisterAppRequest{
		OwnerUserID: user,
		AppID:       uuid.New(),
		CreatedAt:   time.Date(2026, 7, 1, 8, 0, 0, 0, time.UTC),
	}

	first, err := svc.RegisterApp(context.Background(), req)
	require.NoError(t, err)
	second, err := svc.RegisterApp(context.Background(), req)
	require.NoError(t, err)

	// The one-shot guard short-circuits the retry: SAME invoice id back, ONE
	// Stripe item + invoice total, ONE roster row.
	require.Equal(t, first.ProrationInvoiceID, second.ProrationInvoiceID)
	require.Len(t, sc.itemCalls, 1, "a retry must never create a second charge")
	require.Len(t, sc.invoiceCalls, 1)
	require.Len(t, store.apps, 1)
}

func TestRegisterApp_RetryKeepsFirstRegistrationsAnchor(t *testing.T) {
	// A retry with a DIFFERENT created_at / module_count must not move the
	// stored anchor (ON CONFLICT DO NOTHING): the row keeps the first values.
	store := newFakeStore()
	user, _ := registeredAccount(store)
	sc := newFakeStripe()
	svc := appsSvc(store, sc)
	appID := uuid.New()
	created := time.Date(2026, 7, 1, 8, 0, 0, 0, time.UTC)

	_, err := svc.RegisterApp(context.Background(), cycle.RegisterAppRequest{
		OwnerUserID: user, AppID: appID, ModuleCount: 2, CreatedAt: created,
	})
	require.NoError(t, err)
	_, err = svc.RegisterApp(context.Background(), cycle.RegisterAppRequest{
		OwnerUserID: user, AppID: appID, ModuleCount: 9, CreatedAt: created.AddDate(0, 0, 1),
	})
	require.NoError(t, err)

	require.Equal(t, 2, store.apps[appID].ModuleCount)
	require.Equal(t, created, store.apps[appID].CreatedAt)
}

// --- RegisterApp: activation gate (D1d) ---------------------------------------

func TestRegisterApp_UnactivatedAccountRowButNoInvoice(t *testing.T) {
	store := newFakeStore()
	user, acct := registeredAccount(store)
	delete(store.activation, acct) // never bound a card
	sc := newFakeStripe()
	appID := uuid.New()

	resp, err := appsSvc(store, sc).RegisterApp(context.Background(), cycle.RegisterAppRequest{
		OwnerUserID: user, AppID: appID,
	})
	require.NoError(t, err)

	// The mirror row IS recorded (the boundary leg needs it once the account
	// activates) but NO invoice exists and the guard stays unarmed.
	require.Contains(t, store.apps, appID)
	require.Empty(t, resp.ProrationInvoiceID)
	require.Empty(t, sc.itemCalls)
	require.Empty(t, sc.invoiceCalls)
	require.Empty(t, store.apps[appID].ProrationInvoiceID)
}

func TestRegisterApp_NoUsablePMRowButNoInvoice(t *testing.T) {
	store := newFakeStore()
	user, _ := registeredAccount(store)
	store.hasPM = false
	sc := newFakeStripe()
	appID := uuid.New()

	resp, err := appsSvc(store, sc).RegisterApp(context.Background(), cycle.RegisterAppRequest{
		OwnerUserID: user, AppID: appID,
	})
	require.NoError(t, err)
	require.Contains(t, store.apps, appID)
	require.Empty(t, resp.ProrationInvoiceID)
	require.Empty(t, sc.itemCalls)
}

func TestRegisterApp_CreatedOnBoundaryChargesNewPeriodFullBase(t *testing.T) {
	// Created exactly ON the anchor boundary instant (Jul 4 00:00): half-open
	// windows put the creation in the NEW period [Jul 4, Aug 4), so the
	// proration leg charges the FULL new-period base ($20 → 2000 cents). This
	// is the fix direction of the charge-leg overlap review: the Jul-4
	// boundary's advance leg EXCLUDES apps with created_at ≥ Jul 4, so if
	// RegisterApp skipped here the app's first period would never be
	// base-charged (a revenue leak); charging it here bills it exactly once.
	// (Pre-fix this recomputed 0 remaining days against the OLD window and
	// charged nothing, leaving the period to the advance leg's roster scan.)
	store := newFakeStore()
	user, _ := registeredAccount(store)
	sc := newFakeStripe()
	appID := uuid.New()

	resp, err := appsSvc(store, sc).RegisterApp(context.Background(), cycle.RegisterAppRequest{
		OwnerUserID: user,
		AppID:       appID,
		CreatedAt:   time.Date(2026, 7, 4, 0, 0, 0, 0, time.UTC),
	})
	require.NoError(t, err)
	require.EqualValues(t, 2_000, resp.ProrationCents, "creation-day == period start → full base")
	require.Len(t, sc.itemCalls, 1)

	// Mirrored with the FULL new window (partial start == period start) and
	// the snapshot frozen for [Jul 4, Aug 4).
	mirror := store.invoices[sc.invoiceID]
	require.Equal(t, time.Date(2026, 7, 4, 0, 0, 0, 0, time.UTC), mirror.PeriodStart)
	require.Equal(t, time.Date(2026, 8, 4, 0, 0, 0, 0, time.UTC), mirror.PeriodEnd)
	snap, ok := store.baseSnapshots[snapKey{appID, time.Date(2026, 7, 4, 0, 0, 0, 0, time.UTC)}]
	require.True(t, ok, "the proration leg must freeze the charged base")
	require.Equal(t, "proration", snap.source)
	require.EqualValues(t, usage.BaseFeeMicros, snap.snap.BaseMicros)
}

func TestRegisterApp_RetryAfterCreationPeriodClosedNeverCharges(t *testing.T) {
	// FINDING-1 pin: a RegisterApp retry that lands AFTER the creation period
	// closed must NOT charge — the proration window is derived from the mirror
	// row's created_at (May 20 → [May 4, Jun 4)), that window ended before now
	// (Jul 1), and every later period belongs to the boundary advance leg
	// (which already billed [Jun 4, Jul 4) for pre-existing apps). Pre-fix the
	// window was recomputed from NOW, ProratedBaseMicros saw creation-day ≤
	// period start, and the retry double-charged the FULL current-period base.
	store := newFakeStore()
	user, _ := registeredAccount(store)
	sc := newFakeStripe()
	svc := appsSvc(store, sc)
	appID := uuid.New()
	req := cycle.RegisterAppRequest{
		OwnerUserID: user,
		AppID:       appID,
		CreatedAt:   time.Date(2026, 5, 20, 9, 0, 0, 0, time.UTC), // period [May 4, Jun 4) — long closed at appsNow (Jul 1)
	}

	resp, err := svc.RegisterApp(context.Background(), req)
	require.NoError(t, err)

	// Row recorded, NO charge, guard UNARMED, no snapshot.
	require.Contains(t, store.apps, appID)
	require.Empty(t, resp.ProrationInvoiceID)
	require.Empty(t, sc.itemCalls, "a closed creation period must never be prorated late")
	require.Empty(t, sc.invoiceCalls)
	require.Empty(t, store.apps[appID].ProrationInvoiceID, "guard stays unarmed")
	require.Empty(t, store.baseSnapshots)

	// Deterministic: every further retry skips identically (never "eventually
	// charges").
	resp, err = svc.RegisterApp(context.Background(), req)
	require.NoError(t, err)
	require.Empty(t, resp.ProrationInvoiceID)
	require.Empty(t, sc.itemCalls)
	require.Empty(t, store.apps[appID].ProrationInvoiceID)
}

func TestRegisterApp_CurrentPeriodProrationUnchangedByWindowFix(t *testing.T) {
	// FINDING-1 regression guard: for an app created in a period that is
	// still CURRENT, deriving the window from created_at instead of now must
	// change NOTHING — same window, same 3-day proration, snapshot frozen at
	// the anchored period start with the prorated amount.
	store := newFakeStore()
	user, _ := registeredAccount(store)
	sc := newFakeStripe()

	resp, err := appsSvc(store, sc).RegisterApp(context.Background(), cycle.RegisterAppRequest{
		OwnerUserID: user,
		AppID:       uuid.New(),
		CreatedAt:   time.Date(2026, 7, 1, 8, 0, 0, 0, time.UTC), // inside the current [Jun 4, Jul 4)
	})
	require.NoError(t, err)
	require.EqualValues(t, 200, resp.ProrationCents, "3 of 30 days of $20 → 200 cents, exactly as before the fix")
	require.Len(t, sc.itemCalls, 1)
}

// --- RegisterApp: account resolution + validation -----------------------------

func TestRegisterApp_CreatesMissingAccount(t *testing.T) {
	// A lazy owner (never visited billing) still gets the mirror row: the
	// account is get-or-created via the SAME EnsureAccount path billing.Ensure
	// uses. Fresh account → unactivated → no charge.
	store := newFakeStore()
	sc := newFakeStripe()
	user, appID := uuid.New(), uuid.New()

	resp, err := appsSvc(store, sc).RegisterApp(context.Background(), cycle.RegisterAppRequest{
		OwnerUserID: user, AppID: appID,
	})
	require.NoError(t, err)
	require.NotEqual(t, uuid.Nil, resp.AccountID)
	require.Equal(t, resp.AccountID, store.accountsByUser[user])
	require.Equal(t, resp.AccountID, store.apps[appID].AccountID)
	require.Empty(t, sc.itemCalls)
}

func TestRegisterApp_Validation(t *testing.T) {
	svc := appsSvc(newFakeStore(), newFakeStripe())
	for _, tc := range []struct {
		name string
		req  cycle.RegisterAppRequest
	}{
		{"no owner", cycle.RegisterAppRequest{AppID: uuid.New()}},
		{"both owners", cycle.RegisterAppRequest{OwnerUserID: uuid.New(), OwnerOrgID: uuid.New(), AppID: uuid.New()}},
		{"org owner (v1 user-only)", cycle.RegisterAppRequest{OwnerOrgID: uuid.New(), AppID: uuid.New()}},
		{"nil app", cycle.RegisterAppRequest{OwnerUserID: uuid.New()}},
		{"negative module count", cycle.RegisterAppRequest{OwnerUserID: uuid.New(), AppID: uuid.New(), ModuleCount: -1}},
		// FINDING-4 pin: a count past the cap must be rejected loudly, never
		// silently truncated at the int32 store boundary.
		{"module count over cap", cycle.RegisterAppRequest{OwnerUserID: uuid.New(), AppID: uuid.New(), ModuleCount: 100_001}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := svc.RegisterApp(context.Background(), tc.req)
			requireCode(t, err, billing.CodeInvalidInput)
		})
	}
}

// --- SyncAppModules -----------------------------------------------------------

func intPtr(v int) *int { return &v }

func TestSyncAppModules_UpdatesCount(t *testing.T) {
	store := newFakeStore()
	user, _ := registeredAccount(store)
	svc := appsSvc(store, newFakeStripe())
	appID := uuid.New()
	_, err := svc.RegisterApp(context.Background(), cycle.RegisterAppRequest{OwnerUserID: user, AppID: appID})
	require.NoError(t, err)

	resp, err := svc.SyncAppModules(context.Background(), cycle.SyncAppModulesRequest{
		AppID: appID, ModuleCount: intPtr(8),
	})
	require.NoError(t, err)
	require.Equal(t, 8, resp.ModuleCount)
	require.False(t, resp.Deleted)
	require.Equal(t, 8, store.apps[appID].ModuleCount)
}

func TestSyncAppModules_DeleteIsIdempotent(t *testing.T) {
	store := newFakeStore()
	user, _ := registeredAccount(store)
	svc := appsSvc(store, newFakeStripe())
	appID := uuid.New()
	_, err := svc.RegisterApp(context.Background(), cycle.RegisterAppRequest{OwnerUserID: user, AppID: appID})
	require.NoError(t, err)

	resp, err := svc.SyncAppModules(context.Background(), cycle.SyncAppModulesRequest{AppID: appID, Deleted: true})
	require.NoError(t, err)
	require.True(t, resp.Deleted)
	firstDeletedAt := store.apps[appID].DeletedAt

	// Re-fire: still deleted, the FIRST deletion instant survives.
	resp, err = svc.SyncAppModules(context.Background(), cycle.SyncAppModulesRequest{AppID: appID, Deleted: true})
	require.NoError(t, err)
	require.True(t, resp.Deleted)
	require.Equal(t, firstDeletedAt, store.apps[appID].DeletedAt)
}

func TestSyncAppModules_CountUpdateOnDeletedAppIsNoOp(t *testing.T) {
	store := newFakeStore()
	user, _ := registeredAccount(store)
	svc := appsSvc(store, newFakeStripe())
	appID := uuid.New()
	_, err := svc.RegisterApp(context.Background(), cycle.RegisterAppRequest{OwnerUserID: user, AppID: appID, ModuleCount: 3})
	require.NoError(t, err)
	_, err = svc.SyncAppModules(context.Background(), cycle.SyncAppModulesRequest{AppID: appID, Deleted: true})
	require.NoError(t, err)

	// A later count sync must not thaw the frozen tier (D1e).
	resp, err := svc.SyncAppModules(context.Background(), cycle.SyncAppModulesRequest{AppID: appID, ModuleCount: intPtr(9)})
	require.NoError(t, err)
	require.Equal(t, 3, resp.ModuleCount, "deleted app's count is frozen")
	require.Equal(t, 3, store.apps[appID].ModuleCount)

	// Deletion + count in ONE call: deletion wins, count ignored.
	app2 := uuid.New()
	_, err = svc.RegisterApp(context.Background(), cycle.RegisterAppRequest{OwnerUserID: user, AppID: app2, ModuleCount: 1})
	require.NoError(t, err)
	resp, err = svc.SyncAppModules(context.Background(), cycle.SyncAppModulesRequest{AppID: app2, ModuleCount: intPtr(6), Deleted: true})
	require.NoError(t, err)
	require.True(t, resp.Deleted)
	require.Equal(t, 1, resp.ModuleCount)
}

func TestSyncAppModules_UnknownAppNotFound(t *testing.T) {
	svc := appsSvc(newFakeStore(), newFakeStripe())
	_, err := svc.SyncAppModules(context.Background(), cycle.SyncAppModulesRequest{AppID: uuid.New(), Deleted: true})
	requireCode(t, err, billing.CodeNotFound)

	_, err = svc.SyncAppModules(context.Background(), cycle.SyncAppModulesRequest{AppID: uuid.Nil})
	requireCode(t, err, billing.CodeInvalidInput)

	_, err = svc.SyncAppModules(context.Background(), cycle.SyncAppModulesRequest{AppID: uuid.New(), ModuleCount: intPtr(-2)})
	requireCode(t, err, billing.CodeInvalidInput)

	// FINDING-4 pin (both RPCs): module_count 100001 → invalid_input, never a
	// silent int32 truncation.
	_, err = svc.SyncAppModules(context.Background(), cycle.SyncAppModulesRequest{AppID: uuid.New(), ModuleCount: intPtr(100_001)})
	requireCode(t, err, billing.CodeInvalidInput)
}

// --- RunBillingCycle: boundary invoice = usage arrears + advance base ---------

// seedApp inserts a roster row directly (the boundary leg reads the roster
// regardless of how it was written), created well before the test periods so
// it always counts as pre-existing at the [Jun 1, Jul 1) boundary.
func seedApp(store *fakeStore, accountID uuid.UUID, moduleCount int, deleted bool) uuid.UUID {
	return seedAppCreated(store, accountID, moduleCount, deleted, time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC))
}

// seedAppCreated is seedApp with an explicit created_at — the advance leg's
// created-before-the-new-period cutoff pivots on it.
func seedAppCreated(store *fakeStore, accountID uuid.UUID, moduleCount int, deleted bool, createdAt time.Time) uuid.UUID {
	id := uuid.New()
	app := cycle.AppMirror{AppID: id, AccountID: accountID, ModuleCount: moduleCount, CreatedAt: createdAt}
	if deleted {
		app.Deleted = true
		app.DeletedAt = time.Date(2026, 6, 15, 0, 0, 0, 0, time.UTC)
	}
	store.apps[id] = app
	return id
}

func TestRunBillingCycle_InvoicesUsagePlusAdvanceBase(t *testing.T) {
	// arrears 1e6 (usage) + base (20e6 flat + [20e6 + 1×3e6] for a 6-module
	// app) = 44e6 total → 4400 cents on ONE invoice.
	store := newFakeStore()
	store.chargedTotal = 1_000_000
	store.hasPM = true
	store.stripeCustomer = "cus_base_1"
	flat := seedApp(store, chargeAccount, 0, false)
	tiered := seedApp(store, chargeAccount, 6, false)
	sc := newFakeStripe()

	resp, err := chargeSvc(store, sc).RunBillingCycle(context.Background(), chargeAccount, periodStart, periodEnd, 0)
	require.NoError(t, err)
	require.Equal(t, cycle.RunStatusInvoiced, resp.Status)
	require.EqualValues(t, 1_000_000, resp.ArrearsMicros)
	require.EqualValues(t, 43_000_000, resp.AdvanceBaseMicros) // 20e6 + 23e6
	require.EqualValues(t, 4_400, resp.ChargedCents)
	require.Len(t, sc.itemCalls, 1, "usage + base pool into ONE line on ONE invoice")
	require.EqualValues(t, 4_400, sc.itemCalls[0].amountCfg)

	// The advance leg froze one migration-028 snapshot per billed app for the
	// NEW window [Jul 1, Aug 1) — count + base as invoiced, so the display can
	// never drift after later syncs.
	fs, ok := store.baseSnapshots[snapKey{flat, periodEnd}]
	require.True(t, ok)
	require.Equal(t, "advance", fs.source)
	require.EqualValues(t, usage.BaseFeeMicros, fs.snap.BaseMicros)
	require.Equal(t, periodEnd.AddDate(0, 1, 0), fs.snap.PeriodEnd)
	ts, ok := store.baseSnapshots[snapKey{tiered, periodEnd}]
	require.True(t, ok)
	require.EqualValues(t, 23_000_000, ts.snap.BaseMicros)
	require.Equal(t, 6, ts.snap.ModuleCount)
}

func TestRunBillingCycle_BaseOnlyInvoiceWhenNoUsage(t *testing.T) {
	// Zero usage arrears but a live app → the boundary still invoices the NEW
	// period's advance base (the zero-skip needs BOTH legs zero).
	store := newFakeStore()
	store.chargedTotal = 0
	store.hasPM = true
	store.stripeCustomer = "cus_base_2"
	seedApp(store, chargeAccount, 0, false)
	sc := newFakeStripe()

	resp, err := chargeSvc(store, sc).RunBillingCycle(context.Background(), chargeAccount, periodStart, periodEnd, 0)
	require.NoError(t, err)
	require.Equal(t, cycle.RunStatusInvoiced, resp.Status)
	require.EqualValues(t, 0, resp.ArrearsMicros)
	require.EqualValues(t, usage.BaseFeeMicros, resp.AdvanceBaseMicros)
	require.EqualValues(t, 2_000, resp.ChargedCents) // $20 → 2000 cents
	require.Len(t, sc.invoiceCalls, 1)
}

func TestRunBillingCycle_BothZeroSkipsStripe(t *testing.T) {
	// No usage AND no live apps (pre-backfill) → invoiced with NO Stripe call
	// — exactly the pre-027 zero-arrears behavior.
	store := newFakeStore()
	store.chargedTotal = 0
	store.hasPM = true
	sc := newFakeStripe()

	resp, err := chargeSvc(store, sc).RunBillingCycle(context.Background(), chargeAccount, periodStart, periodEnd, 0)
	require.NoError(t, err)
	require.Equal(t, cycle.RunStatusInvoiced, resp.Status)
	require.EqualValues(t, 0, resp.AdvanceBaseMicros)
	require.Empty(t, sc.itemCalls)
	require.Empty(t, sc.invoiceCalls)
}

func TestRunBillingCycle_DeletedAppsExcludedFromBaseButUsageStillBills(t *testing.T) {
	// One live app + one deleted app: the deleted app contributes NO base
	// (D1e) but the period's usage arrears (which include whatever the deleted
	// app metered) still bill in full.
	store := newFakeStore()
	store.chargedTotal = 5_000_000 // includes the deleted app's usage
	store.hasPM = true
	store.stripeCustomer = "cus_base_3"
	seedApp(store, chargeAccount, 0, false)
	seedApp(store, chargeAccount, 9, true) // deleted — its 9-module tier must NOT bill
	sc := newFakeStripe()

	resp, err := chargeSvc(store, sc).RunBillingCycle(context.Background(), chargeAccount, periodStart, periodEnd, 0)
	require.NoError(t, err)
	require.EqualValues(t, 5_000_000, resp.ArrearsMicros)
	require.EqualValues(t, usage.BaseFeeMicros, resp.AdvanceBaseMicros, "only the live app's base")
	require.EqualValues(t, 2_500, resp.ChargedCents) // (5e6 + 20e6) / 10_000
}

func TestRunBillingCycle_OtherAccountsAppsDoNotBill(t *testing.T) {
	// Roster rows belong to accounts; another account's app must not leak into
	// this account's advance base.
	store := newFakeStore()
	store.chargedTotal = 0
	store.hasPM = true
	seedApp(store, uuid.New(), 3, false) // someone else's app
	sc := newFakeStripe()

	resp, err := chargeSvc(store, sc).RunBillingCycle(context.Background(), chargeAccount, periodStart, periodEnd, 0)
	require.NoError(t, err)
	require.EqualValues(t, 0, resp.AdvanceBaseMicros)
	require.Empty(t, sc.invoiceCalls)
}

func TestRunBillingCycle_ExcludesAppCreatedInsideNewPeriod(t *testing.T) {
	// FINDING-2 pin (same-day race): the boundary closing [Jun 1, Jul 1) bills
	// the NEW period [Jul 1, Aug 1) in advance — but ONLY for apps that
	// existed before Jul 1. An app created Jul 1 10:00 (inside the new period)
	// already had its new-period base charged by RegisterApp's proration leg,
	// so the advance leg must NOT add it again; it joins at the NEXT boundary.
	store := newFakeStore()
	store.chargedTotal = 0
	store.hasPM = true
	store.stripeCustomer = "cus_cutoff_1"
	seedApp(store, chargeAccount, 0, false)                                               // pre-existing → counts
	newApp := seedAppCreated(store, chargeAccount, 6, false, periodEnd.Add(10*time.Hour)) // inside the new period → excluded
	sc := newFakeStripe()
	svc := chargeSvc(store, sc)

	resp, err := svc.RunBillingCycle(context.Background(), chargeAccount, periodStart, periodEnd, 0)
	require.NoError(t, err)
	require.EqualValues(t, usage.BaseFeeMicros, resp.AdvanceBaseMicros,
		"only the pre-existing app's base — the new app's new-period base is the proration leg's")
	require.EqualValues(t, 2_000, resp.ChargedCents)

	// And no advance snapshot was minted for the excluded app (nothing was
	// billed for it at this boundary).
	_, ok := store.baseSnapshots[snapKey{newApp, periodEnd}]
	require.False(t, ok)

	// NEXT boundary (closing [Jul 1, Aug 1)): the app now pre-exists the newer
	// period and joins the advance leg — billed exactly once, never twice.
	resp, err = svc.RunBillingCycle(context.Background(), chargeAccount, periodEnd, periodEnd.AddDate(0, 1, 0), 0)
	require.NoError(t, err)
	require.EqualValues(t, usage.BaseFeeMicros+usage.AppBaseFeeMicros(usage.BaseFeeMicros, 6), resp.AdvanceBaseMicros,
		"the new app joins the advance leg at the NEXT boundary")
}

func TestRunBillingCycle_ReclaimedRunNoDoubleBase(t *testing.T) {
	// FINDING-2 pin (deterministic reclaim path): boundary run skipped_no_pm →
	// the owner binds a PM → an app is created MID-NEW-PERIOD and RegisterApp
	// charges its proration → the skipped run is RECLAIMED. The reclaimed
	// advance leg must count ONLY the pre-existing app: pre-fix it summed the
	// whole live roster and re-billed the new app's period on top of the
	// proration — a guaranteed double charge.
	store := newFakeStore()
	store.chargedTotal = 1_000_000
	store.hasPM = false // cycle 1: no PM → skipped_no_pm
	store.stripeCustomer = "cus_reclaim_base"
	user, acct := uuid.New(), uuid.New()
	store.accountsByUser[user] = acct
	store.activation[acct] = time.Date(2026, 5, 1, 9, 0, 0, 0, time.UTC) // anchor day 1 → [Jun 1, Jul 1), [Jul 1, Aug 1)
	preApp := seedAppCreated(store, acct, 0, false, time.Date(2026, 5, 10, 0, 0, 0, 0, time.UTC))
	sc := newFakeStripe()
	svc := cycle.NewService(store, sc).WithNow(func() time.Time {
		return time.Date(2026, 7, 2, 13, 0, 0, 0, time.UTC)
	})

	first, err := svc.RunBillingCycle(context.Background(), acct, periodStart, periodEnd, 0)
	require.NoError(t, err)
	require.Equal(t, cycle.RunStatusSkippedNoPM, first.Status)
	require.Empty(t, sc.invoiceCalls)

	// PM bound; an app is created Jul 2 (inside the new period [Jul 1, Aug 1))
	// and RegisterApp prorates it: 30 of 31 days of $20 → 19_354_839 micros.
	store.hasPM = true
	newApp := uuid.New()
	reg, err := svc.RegisterApp(context.Background(), cycle.RegisterAppRequest{
		OwnerUserID: user,
		AppID:       newApp,
		CreatedAt:   time.Date(2026, 7, 2, 10, 0, 0, 0, time.UTC),
	})
	require.NoError(t, err)
	require.EqualValues(t, 1_935, reg.ProrationCents)
	require.Len(t, sc.invoiceCalls, 1)

	// Reclaim: the advance leg bills ONLY the pre-existing app's base.
	second, err := svc.RunBillingCycle(context.Background(), acct, periodStart, periodEnd, 0)
	require.NoError(t, err)
	require.True(t, second.FirstRun, "skipped_no_pm is reclaimed")
	require.Equal(t, cycle.RunStatusInvoiced, second.Status)
	require.EqualValues(t, usage.BaseFeeMicros, second.AdvanceBaseMicros,
		"the reclaimed run must NOT re-bill the prorated app's period")
	require.EqualValues(t, 2_100, second.ChargedCents) // 1e6 arrears + 20e6 base

	// Exactly ONE proration invoice + ONE boundary invoice — never a third.
	require.Len(t, sc.invoiceCalls, 2)

	// Snapshot ledger for the new period [Jul 1, Aug 1): the proration row for
	// the new app + the advance row for the pre-existing app — one row per
	// app-period, each recording exactly what its leg billed.
	jul1 := time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)
	pro, ok := store.baseSnapshots[snapKey{newApp, jul1}]
	require.True(t, ok)
	require.Equal(t, "proration", pro.source)
	require.EqualValues(t, 19_354_839, pro.snap.BaseMicros)
	adv, ok := store.baseSnapshots[snapKey{preApp, jul1}]
	require.True(t, ok)
	require.Equal(t, "advance", adv.source)
	require.EqualValues(t, usage.BaseFeeMicros, adv.snap.BaseMicros)
	require.Len(t, store.baseSnapshots, 2)
}

func TestAccountHasLiveApps(t *testing.T) {
	store := newFakeStore()
	svc := cycle.NewService(store, nil)
	newPeriodStart := periodEnd // the gate's cutoff is the NEW period's start

	has, err := svc.AccountHasLiveApps(context.Background(), chargeAccount, newPeriodStart)
	require.NoError(t, err)
	require.False(t, has, "empty roster (pre-backfill) → no boundary run for a no-usage period")

	appID := seedApp(store, chargeAccount, 0, false)
	has, err = svc.AccountHasLiveApps(context.Background(), chargeAccount, newPeriodStart)
	require.NoError(t, err)
	require.True(t, has)

	// Deleting the only app flips it back off.
	app := store.apps[appID]
	app.Deleted = true
	store.apps[appID] = app
	has, err = svc.AccountHasLiveApps(context.Background(), chargeAccount, newPeriodStart)
	require.NoError(t, err)
	require.False(t, has)

	// An app created INSIDE the new period does not arm the gate either: its
	// new-period base is the RegisterApp proration leg's, so a no-usage
	// account with only such apps keeps the historical skip.
	seedAppCreated(store, chargeAccount, 0, false, periodEnd.Add(6*time.Hour))
	has, err = svc.AccountHasLiveApps(context.Background(), chargeAccount, newPeriodStart)
	require.NoError(t, err)
	require.False(t, has, "apps created in the new period join at the NEXT boundary")
}
