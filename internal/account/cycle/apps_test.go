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

func TestRegisterApp_ZeroRemainingDaysNoInvoice(t *testing.T) {
	// Created ON the period end boundary → 0 remaining days → row, no invoice.
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
	require.Contains(t, store.apps, appID)
	require.Empty(t, resp.ProrationInvoiceID)
	require.Empty(t, sc.itemCalls)
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
}

// --- RunBillingCycle: boundary invoice = usage arrears + advance base ---------

// seedApp inserts a roster row directly (the boundary leg reads the roster
// regardless of how it was written).
func seedApp(store *fakeStore, accountID uuid.UUID, moduleCount int, deleted bool) uuid.UUID {
	id := uuid.New()
	app := cycle.AppMirror{AppID: id, AccountID: accountID, ModuleCount: moduleCount,
		CreatedAt: time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC)}
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
	seedApp(store, chargeAccount, 0, false)
	seedApp(store, chargeAccount, 6, false)
	sc := newFakeStripe()

	resp, err := chargeSvc(store, sc).RunBillingCycle(context.Background(), chargeAccount, periodStart, periodEnd, 0)
	require.NoError(t, err)
	require.Equal(t, cycle.RunStatusInvoiced, resp.Status)
	require.EqualValues(t, 1_000_000, resp.ArrearsMicros)
	require.EqualValues(t, 43_000_000, resp.AdvanceBaseMicros) // 20e6 + 23e6
	require.EqualValues(t, 4_400, resp.ChargedCents)
	require.Len(t, sc.itemCalls, 1, "usage + base pool into ONE line on ONE invoice")
	require.EqualValues(t, 4_400, sc.itemCalls[0].amountCfg)
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

func TestAccountHasLiveApps(t *testing.T) {
	store := newFakeStore()
	svc := cycle.NewService(store, nil)

	has, err := svc.AccountHasLiveApps(context.Background(), chargeAccount)
	require.NoError(t, err)
	require.False(t, has, "empty roster (pre-backfill) → no boundary run for a no-usage period")

	appID := seedApp(store, chargeAccount, 0, false)
	has, err = svc.AccountHasLiveApps(context.Background(), chargeAccount)
	require.NoError(t, err)
	require.True(t, has)

	// Deleting the only app flips it back off.
	app := store.apps[appID]
	app.Deleted = true
	store.apps[appID] = app
	has, err = svc.AccountHasLiveApps(context.Background(), chargeAccount)
	require.NoError(t, err)
	require.False(t, has)
}
