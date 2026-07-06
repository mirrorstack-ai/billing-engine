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

// --- RegisterApp: mirror-only (creation grace — RegisterApp never charges) ---

// Migration 037: RegisterApp freezes the app name; SyncAppModules renames it
// while live; a rename AFTER deletion is a no-op (the last-known name survives
// so a deleted app's bill still shows it).
func TestRegisterApp_FreezesNameThatSurvivesDeletion(t *testing.T) {
	store := newFakeStore()
	user, _ := registeredAccount(store)
	sc := newFakeStripe()
	svc := appsSvc(store, sc)
	ctx := context.Background()
	appID := uuid.New()

	_, err := svc.RegisterApp(ctx, cycle.RegisterAppRequest{
		OwnerUserID: user, AppID: appID, CreatedAt: time.Date(2026, 7, 1, 8, 0, 0, 0, time.UTC),
		Name: "影音教育平台",
	})
	require.NoError(t, err)
	require.Equal(t, "影音教育平台", store.apps[appID].Name, "name frozen on registration")

	// Rename while live → mirror tracks it.
	newName := "影音學習平台"
	syncResp, err := svc.SyncAppModules(ctx, cycle.SyncAppModulesRequest{AppID: appID, Name: &newName})
	require.NoError(t, err)
	require.Equal(t, newName, syncResp.Name)
	require.Equal(t, newName, store.apps[appID].Name, "live rename updates the frozen name")

	// Delete, then attempt a rename → frozen (no-op), last-known name kept.
	_, err = svc.SyncAppModules(ctx, cycle.SyncAppModulesRequest{AppID: appID, Deleted: true})
	require.NoError(t, err)
	frozen := "should-not-apply"
	_, err = svc.SyncAppModules(ctx, cycle.SyncAppModulesRequest{AppID: appID, Name: &frozen})
	require.NoError(t, err)
	require.Equal(t, newName, store.apps[appID].Name, "a rename after deletion is a no-op — the last-known name is frozen for the bill")
}

func TestRegisterApp_MirrorsRowWithoutCharging(t *testing.T) {
	// Creation grace: even a FULLY chargeable account (activated + PM + Stripe
	// customer) is NOT charged at registration — RegisterApp only mirrors the
	// row. The creation-period base is the sweep's job (see proration_test.go),
	// so an app charged before it survives grace is impossible.
	store := newFakeStore()
	user, acct := registeredAccount(store)
	sc := newFakeStripe()
	appID := uuid.New()

	resp, err := appsSvc(store, sc).RegisterApp(context.Background(), cycle.RegisterAppRequest{
		OwnerUserID: user,
		AppID:       appID,
		ModuleCount: 3,
		CreatedAt:   time.Date(2026, 7, 1, 8, 0, 0, 0, time.UTC),
	})
	require.NoError(t, err)
	require.Equal(t, acct, resp.AccountID)

	// NO charge of any kind, guard unarmed, no snapshot, no invoice.
	require.Empty(t, resp.ProrationInvoiceID)
	require.Zero(t, resp.ProrationCents)
	require.Empty(t, sc.itemCalls)
	require.Empty(t, sc.invoiceCalls)
	require.Empty(t, store.invoices)
	require.Empty(t, store.baseSnapshots)

	// But the roster row IS recorded verbatim (created_at / module_count / account)
	// — the stable anchor the sweep later prices from.
	app := store.apps[appID]
	require.Equal(t, acct, app.AccountID)
	require.Equal(t, 3, app.ModuleCount)
	require.Equal(t, time.Date(2026, 7, 1, 8, 0, 0, 0, time.UTC), app.CreatedAt)
	require.Empty(t, app.ProrationInvoiceID)

	// 3 modules → 3 per-module install timers synthesized at created_at, all live,
	// none charged (each on its own independent grace timer).
	require.Equal(t, 3, liveTimerCount(store, appID))
	require.Empty(t, sc.itemCalls)
}

func TestRegisterApp_SynthesizesPerModuleTimersButNeverCharges(t *testing.T) {
	// Creation grace + per-module timers (migration 033): a create with K modules
	// synthesizes K install timers anchored at created_at (each with its own
	// 3-day grace) but charges NOTHING at registration — the flat base is the
	// grace sweep's job and each module's overage is Leg 1's, once it survives.
	store := newFakeStore()
	user, _ := registeredAccount(store)
	sc := newFakeStripe()
	appID := uuid.New()
	created := time.Date(2026, 6, 19, 12, 0, 0, 0, time.UTC)

	resp, err := appsSvc(store, sc).RegisterApp(context.Background(), cycle.RegisterAppRequest{
		OwnerUserID: user,
		AppID:       appID,
		ModuleCount: 7,
		CreatedAt:   created,
	})
	require.NoError(t, err)

	// No charge of any kind at registration (creation grace).
	require.Empty(t, resp.ProrationInvoiceID)
	require.Zero(t, resp.ProrationCents)
	require.Empty(t, sc.itemCalls)
	require.Empty(t, sc.invoiceCalls)

	// 7 timers synthesized, all anchored at created_at with grace = created + 3d.
	require.Equal(t, 7, liveTimerCount(store, appID))
	for _, tm := range store.timers {
		require.Equal(t, created, tm.installedAt)
		require.Equal(t, created.AddDate(0, 0, 3), tm.graceExpiresAt)
		require.False(t, tm.removed)
		require.False(t, tm.graceResolved)
	}
}

// Regression (review 2026-07-06, H4): a late RegisterApp retry — or a
// billing-backfill re-registration — that lands AFTER the app was deleted must
// not resurrect live timers. Deletion freezes module_count (it is not zeroed)
// and soft-removes all timers, so the pre-fix reconcile saw live=0 against the
// frozen count and re-inserted K phantom timers for a DELETED app, which then
// occupied FIFO slots and charged at every boundary with no removal path.
func TestRegisterApp_RetryAfterDeletionDoesNotResurrectTimers(t *testing.T) {
	store := newFakeStore()
	user, _ := registeredAccount(store)
	sc := newFakeStripe()
	svc := appsSvc(store, sc)
	appID := uuid.New()
	created := time.Date(2026, 6, 19, 12, 0, 0, 0, time.UTC)

	_, err := svc.RegisterApp(context.Background(), cycle.RegisterAppRequest{
		OwnerUserID: user, AppID: appID, ModuleCount: 4, CreatedAt: created,
	})
	require.NoError(t, err)
	require.Equal(t, 4, liveTimerCount(store, appID))

	// The app is deleted (timers soft-removed, module_count frozen at 4).
	_, err = svc.SyncAppModules(context.Background(), cycle.SyncAppModulesRequest{AppID: appID, Deleted: true})
	require.NoError(t, err)
	require.Zero(t, liveTimerCount(store, appID))

	// The fire-and-forget RegisterApp RETRY lands late.
	_, err = svc.RegisterApp(context.Background(), cycle.RegisterAppRequest{
		OwnerUserID: user, AppID: appID, ModuleCount: 4, CreatedAt: created,
	})
	require.NoError(t, err)
	require.Zero(t, liveTimerCount(store, appID),
		"a deleted app's timers must never be resurrected by a late retry")
}

// Regression (review 2026-07-06, H4): SyncAppModules deletion is two
// non-transactional writes (MarkAppDeleted, then the timer soft-remove). A
// crash between them leaves the app deleted with live orphaned timers — and
// the pre-fix retry guard (`req.Deleted && !app.Deleted`) skipped the
// soft-remove forever on the re-fire, leaving the orphans in the account FIFO
// (demoting other apps' modules to "over") and chargeable. The delete signal
// now re-fires the idempotent soft-remove every time.
func TestSyncAppModules_DeleteRetrySelfHealsOrphanedTimers(t *testing.T) {
	store := newFakeStore()
	user, _ := registeredAccount(store)
	sc := newFakeStripe()
	svc := appsSvc(store, sc)
	appID := uuid.New()

	_, err := svc.RegisterApp(context.Background(), cycle.RegisterAppRequest{
		OwnerUserID: user, AppID: appID, ModuleCount: 3,
		CreatedAt: time.Date(2026, 6, 19, 12, 0, 0, 0, time.UTC),
	})
	require.NoError(t, err)
	require.Equal(t, 3, liveTimerCount(store, appID))

	// Simulate the crash window: the app row was marked deleted, but the timer
	// soft-remove never committed.
	app := store.apps[appID]
	app.Deleted = true
	store.apps[appID] = app
	require.Equal(t, 3, liveTimerCount(store, appID), "orphaned live timers of a deleted app")

	// The fire-and-forget delete RETRY must self-heal the orphans.
	_, err = svc.SyncAppModules(context.Background(), cycle.SyncAppModulesRequest{AppID: appID, Deleted: true})
	require.NoError(t, err)
	require.Zero(t, liveTimerCount(store, appID),
		"the delete re-fire soft-removes the orphaned timers instead of skipping on !app.Deleted")
}

func TestRegisterApp_DefaultsCreatedAtToNow(t *testing.T) {
	// Zero CreatedAt → the server's now (appsNow, Jul 1) is stamped on the mirror
	// row (the anchor the later sweep prorates from). Still no charge here.
	store := newFakeStore()
	user, _ := registeredAccount(store)
	sc := newFakeStripe()
	appID := uuid.New()

	resp, err := appsSvc(store, sc).RegisterApp(context.Background(), cycle.RegisterAppRequest{
		OwnerUserID: user, AppID: appID,
	})
	require.NoError(t, err)
	require.Empty(t, resp.ProrationInvoiceID)
	require.Empty(t, sc.itemCalls)
	require.Equal(t, appsNow, store.apps[appID].CreatedAt)
}

func TestRegisterApp_EchoesArmedGuardOnRetry(t *testing.T) {
	// A RegisterApp retry that lands AFTER the sweep already charged echoes the
	// armed guard's invoice id (idempotent visibility) and still never charges.
	store := newFakeStore()
	user, _ := registeredAccount(store)
	sc := newFakeStripe()
	svc := appsSvc(store, sc)
	appID := uuid.New()
	req := cycle.RegisterAppRequest{OwnerUserID: user, AppID: appID, CreatedAt: time.Date(2026, 7, 1, 8, 0, 0, 0, time.UTC)}

	_, err := svc.RegisterApp(context.Background(), req)
	require.NoError(t, err)
	// The sweep charges it (grace elapsed).
	_, err = svc.SweepCreationProrations(context.Background(), time.Date(2026, 7, 5, 0, 0, 0, 0, time.UTC))
	require.NoError(t, err)
	armed := store.apps[appID].ProrationInvoiceID
	require.NotEmpty(t, armed)

	resp, err := svc.RegisterApp(context.Background(), req)
	require.NoError(t, err)
	require.Equal(t, armed, resp.ProrationInvoiceID)
	require.Len(t, sc.itemCalls, 1, "the retry must not add a second charge")
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

func TestRegisterApp_RecordsRowRegardlessOfAccountState(t *testing.T) {
	// RegisterApp no longer gates on activation / PM (that moved to the charge
	// sweep). It records the roster row unconditionally — even for an
	// unactivated account with no PM — so the sweep can price it once the
	// account becomes chargeable. No charge, ever, in this RPC.
	store := newFakeStore()
	user, acct := registeredAccount(store)
	delete(store.activation, acct) // never bound a card
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
	require.Empty(t, sc.invoiceCalls)
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
	// Under the per-module-instance overage model (migration 033) the boundary
	// bills ONLY usage arrears + the FLAT advance base — module overage is NO
	// longer charged here (it rides per-module grace timers, Leg 1). arrears 1e6
	// (usage) + 2 × flat $20 (40e6) = 41e6 → 4100 cents on ONE invoice.
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
	require.EqualValues(t, 40_000_000, resp.AdvanceBaseMicros) // 2 × flat $20
	require.EqualValues(t, 4_100, resp.ChargedCents)
	require.Len(t, sc.itemCalls, 1, "usage + base pool into ONE line on ONE invoice")
	require.EqualValues(t, 4_100, sc.itemCalls[0].amountCfg)

	// The advance leg froze one migration-028 base snapshot per billed app for
	// the NEW window [Jul 1, Aug 1) — the FLAT base (overage is not billed here).
	fs, ok := store.baseSnapshots[snapKey{flat, periodEnd}]
	require.True(t, ok)
	require.Equal(t, "advance", fs.source)
	require.EqualValues(t, usage.BaseFeeMicros, fs.snap.BaseMicros)
	require.Equal(t, periodEnd.AddDate(0, 1, 0), fs.snap.PeriodEnd)
	ts, ok := store.baseSnapshots[snapKey{tiered, periodEnd}]
	require.True(t, ok)
	require.EqualValues(t, usage.BaseFeeMicros, ts.snap.BaseMicros, "per-app base is flat")
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
	// period and joins the advance leg — billed exactly once, never twice. Both
	// apps contribute the FLAT $20 base (2 × 20e6). Module overage is NOT billed
	// at the boundary anymore (it rides per-module grace timers, Leg 1).
	resp, err = svc.RunBillingCycle(context.Background(), chargeAccount, periodEnd, periodEnd.AddDate(0, 1, 0), 0)
	require.NoError(t, err)
	require.EqualValues(t, 2*usage.BaseFeeMicros, resp.AdvanceBaseMicros,
		"the new app joins the advance leg at the NEXT boundary (flat base)")
	require.EqualValues(t, 4_000, resp.ChargedCents, "2 × flat $20, no boundary overage")
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

	// PM bound; an app is created Jul 2 (inside the new period [Jul 1, Aug 1)).
	// RegisterApp only mirrors it; the creation-proration charge (the leg the
	// grace sweep drives) prorates 30 of 31 days of $20 → 19_354_839 micros.
	store.hasPM = true
	newApp := uuid.New()
	_, err = svc.RegisterApp(context.Background(), cycle.RegisterAppRequest{
		OwnerUserID: user,
		AppID:       newApp,
		CreatedAt:   time.Date(2026, 7, 2, 10, 0, 0, 0, time.UTC),
	})
	require.NoError(t, err)
	reg, err := svc.ChargeCreationProration(context.Background(), newApp)
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
