package usage_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/mirrorstack-ai/billing-engine/internal/account/billing"
	"github.com/mirrorstack-ai/billing-engine/internal/account/usage"
)

// seedSettledApp wires an app CREATED at createdAt whose creation-proration leg
// has already fired: the roster row, the armed proration_invoice_id guard, and
// the joined invoice mirror row. Returns the invoice's mirror UUID (the
// invoice_id fallback when the invoice carries no customer-facing Number).
func seedSettledApp(store *fakeStore, appID uuid.UUID, createdAt time.Time, stripeID, number, status string, amountMicros int64, invoiceCreatedAt time.Time) uuid.UUID {
	invUUID := uuid.New()
	store.appMirrors[appID] = usage.AppMirrorInfo{CreatedAt: createdAt}
	store.newAppProrationInvoiceID[appID] = stripeID
	store.newAppInvoices[stripeID] = fakeInvoice{
		id: invUUID, number: number, status: status, amountMicros: amountMicros, createdAt: invoiceCreatedAt,
	}
	return invUUID
}

func TestListNewCreationCharges_RequiresOwner(t *testing.T) {
	_, err := newService(newFakeStore()).ListNewCreationCharges(context.Background(), usage.ListNewCreationChargesRequest{})
	requireCode(t, err, billing.CodeInvalidInput)
}

func TestListNewCreationCharges_RejectsBothOwners(t *testing.T) {
	_, err := newService(newFakeStore()).ListNewCreationCharges(context.Background(), usage.ListNewCreationChargesRequest{
		OwnerUserID: uuid.New(), OwnerOrgID: uuid.New(),
	})
	requireCode(t, err, billing.CodeInvalidInput)
}

func TestListNewCreationCharges_MalformedPeriodRejected(t *testing.T) {
	store := newFakeStore()
	owner := seedOwnerAccount(store)
	_, err := newService(store).ListNewCreationCharges(context.Background(), usage.ListNewCreationChargesRequest{
		OwnerUserID: owner, PeriodID: "not-a-period-id",
	})
	requireCode(t, err, billing.CodeInvalidInput)
}

// TestListNewCreationCharges_NoAccountReturnsEmpty: a lazy owner (no billing account
// yet) can have charged no app — an EMPTY slice (never nil), not an error.
func TestListNewCreationCharges_NoAccountReturnsEmpty(t *testing.T) {
	resp, err := newService(newFakeStore()).ListNewCreationCharges(context.Background(), usage.ListNewCreationChargesRequest{
		OwnerUserID: uuid.New(),
	})
	require.NoError(t, err)
	require.NotNil(t, resp.Charges)
	require.Empty(t, resp.Charges)
}

// TestListNewCreationCharges_SettledDerivation: a settled row carries the invoice's
// ACTUAL amount_due, the invoice Number as invoice_id, and the invoice
// created_at as recorded_at — resolved over a frozen historical window.
func TestListNewCreationCharges_SettledDerivation(t *testing.T) {
	store := newFakeStore()
	owner := uuid.New()
	store.accounts[owner] = uuid.New()
	pid := mirrorPeriod(store) // frozen [May 1, Jun 1)

	created := time.Date(2026, 5, 10, 0, 0, 0, 0, time.UTC)
	invAt := time.Date(2026, 5, 13, 0, 0, 0, 0, time.UTC)
	app := uuid.New()
	seedSettledApp(store, app, created, "in_abc", "INV-100", "paid", 22_000_000, invAt)

	resp, err := newService(store).ListNewCreationCharges(context.Background(), usage.ListNewCreationChargesRequest{
		OwnerUserID: owner, PeriodID: pid.String(),
	})
	require.NoError(t, err)
	require.Len(t, resp.Charges, 1)
	c := resp.Charges[0]
	require.Equal(t, app, c.AppID)
	require.Equal(t, usage.NewCreationChargeStatusSettled, c.Status)
	require.EqualValues(t, 22_000_000, c.AmountMicros) // the invoice total, not a base snapshot
	require.Equal(t, "INV-100", c.InvoiceID)
	require.NotNil(t, c.RecordedAt)
	require.True(t, c.RecordedAt.Equal(invAt))
	require.Nil(t, c.ChargeETA) // settled rows carry no ETA
}

// TestListNewCreationCharges_SettledBreakdown: a settled 7-module app splits its
// invoice total into base (the 'proration' snapshot) + add-ons, surfaces the app
// name, and reports addon_module_count = max(0, 7 − IncludedModules) = 2 — with
// base + addon == amount (the contract invariant).
func TestListNewCreationCharges_SettledBreakdown(t *testing.T) {
	store := newFakeStore()
	owner := uuid.New()
	store.accounts[owner] = uuid.New()
	pid := mirrorPeriod(store)

	created := time.Date(2026, 5, 10, 0, 0, 0, 0, time.UTC)
	invAt := time.Date(2026, 5, 13, 0, 0, 0, 0, time.UTC)
	app := uuid.New()
	seedSettledApp(store, app, created, "in_brk", "INV-BRK", "paid", 22_000_000, invAt)
	// 7 installed modules (2 over the bundled allowance) + a $16 base snapshot →
	// the remaining $6 is the co-created over-module add-on component.
	m := store.appMirrors[app]
	m.Name = "Marketing Site"
	m.ModuleCount = 7 // the fake maps this to created_module_count
	store.appMirrors[app] = m
	store.newAppProrationBase[app] = 16_000_000

	resp, err := newService(store).ListNewCreationCharges(context.Background(), usage.ListNewCreationChargesRequest{
		OwnerUserID: owner, PeriodID: pid.String(),
	})
	require.NoError(t, err)
	require.Len(t, resp.Charges, 1)
	c := resp.Charges[0]
	require.Equal(t, "Marketing Site", c.Name)
	require.EqualValues(t, 22_000_000, c.AmountMicros)
	require.EqualValues(t, 16_000_000, c.BaseFeeMicros)
	require.EqualValues(t, 6_000_000, c.AddonMicros)
	require.Equal(t, 2, c.AddonModuleCount, "7 − IncludedModules(5) = 2 add-on modules")
	require.EqualValues(t, c.AmountMicros, c.BaseFeeMicros+c.AddonMicros, "base + addon == amount")
}

// TestListNewCreationCharges_SettledNoAddons: a <=5-module app has no add-on
// modules — addon_module_count 0, addon_micros 0, and the base equals the whole
// invoice total.
func TestListNewCreationCharges_SettledNoAddons(t *testing.T) {
	store := newFakeStore()
	owner := uuid.New()
	store.accounts[owner] = uuid.New()
	pid := mirrorPeriod(store)

	created := time.Date(2026, 5, 10, 0, 0, 0, 0, time.UTC)
	app := uuid.New()
	seedSettledApp(store, app, created, "in_noadd", "INV-NA", "paid", 12_000_000, created)
	m := store.appMirrors[app]
	m.Name = "Tiny App"
	m.ModuleCount = 5 // exactly the allowance → no add-ons
	store.appMirrors[app] = m
	store.newAppProrationBase[app] = 12_000_000

	resp, err := newService(store).ListNewCreationCharges(context.Background(), usage.ListNewCreationChargesRequest{
		OwnerUserID: owner, PeriodID: pid.String(),
	})
	require.NoError(t, err)
	require.Len(t, resp.Charges, 1)
	c := resp.Charges[0]
	require.Equal(t, "Tiny App", c.Name)
	require.Zero(t, c.AddonModuleCount)
	require.Zero(t, c.AddonMicros)
	require.EqualValues(t, 12_000_000, c.BaseFeeMicros)
}

// TestListNewCreationCharges_PendingBreakdown: a pending (in-grace) app previews
// the exact creation base the sweep will charge, reports no add-on money, and
// still surfaces its name and frozen registration-time add-on count.
func TestListNewCreationCharges_PendingBreakdown(t *testing.T) {
	store := newFakeStore()
	owner := uuid.New()
	accountID := uuid.New()
	store.accounts[owner] = accountID
	store.anchorDays[accountID] = 11
	now := time.Date(2026, 7, 18, 12, 0, 0, 0, time.UTC)
	periodStart := time.Date(2026, 7, 11, 0, 0, 0, 0, time.UTC)
	periodEnd := time.Date(2026, 8, 11, 0, 0, 0, 0, time.UTC)
	createdAt := time.Date(2026, 7, 18, 9, 0, 0, 0, time.UTC)

	app := uuid.New()
	store.appMirrors[app] = usage.AppMirrorInfo{CreatedAt: createdAt, Name: "Draft App", ModuleCount: 8}

	resp, err := newService(store).WithNow(func() time.Time { return now }).ListNewCreationCharges(context.Background(), usage.ListNewCreationChargesRequest{
		OwnerUserID: owner, // current window
	})
	require.NoError(t, err)
	require.Len(t, resp.Charges, 1)
	c := resp.Charges[0]
	expected := usage.CreationChargeBaseMicros(createdAt, periodStart, periodEnd)
	require.Equal(t, usage.NewCreationChargeStatusPending, c.Status)
	require.Equal(t, "Draft App", c.Name)
	require.EqualValues(t, expected, c.BaseFeeMicros, "pending previews the sweep's exact base amount")
	require.EqualValues(t, expected, c.AmountMicros, "amount == exact base preview for pending")
	require.NotEqualValues(t, usage.BaseFeeMicros, c.AmountMicros, "mid-period fixture must discriminate from the old flat preview")
	require.Zero(t, c.AddonMicros, "pending projects base only, not add-on overage")
	require.Equal(t, 3, c.AddonModuleCount, "8 − IncludedModules(5) = 3, known even while uncharged")
}

// TestListNewCreationCharges_SettledInvoiceIDFallsBackToUUID: an invoice not yet
// number-enriched (Number "") surfaces the mirror UUID as invoice_id.
func TestListNewCreationCharges_SettledInvoiceIDFallsBackToUUID(t *testing.T) {
	store := newFakeStore()
	owner := uuid.New()
	store.accounts[owner] = uuid.New()
	pid := mirrorPeriod(store)

	created := time.Date(2026, 5, 10, 0, 0, 0, 0, time.UTC)
	app := uuid.New()
	invUUID := seedSettledApp(store, app, created, "in_xyz", "", "open", 20_000_000, created)

	resp, err := newService(store).ListNewCreationCharges(context.Background(), usage.ListNewCreationChargesRequest{
		OwnerUserID: owner, PeriodID: pid.String(),
	})
	require.NoError(t, err)
	require.Len(t, resp.Charges, 1)
	require.Equal(t, invUUID.String(), resp.Charges[0].InvoiceID)
}

// TestListNewCreationCharges_CurrentWindowSettledAndPending exercises the whole
// derivation on the CURRENT live window: a valid settled row + a valid pending
// row survive, while a $0 invoice, a voided invoice, a permanently-skipped app,
// and a soft-deleted app are all excluded.
func TestListNewCreationCharges_CurrentWindowSettledAndPending(t *testing.T) {
	store := newFakeStore()
	owner := uuid.New()
	accountID := uuid.New()
	store.accounts[owner] = accountID
	store.anchorDays[accountID] = 11
	now := time.Date(2026, 7, 18, 12, 0, 0, 0, time.UTC)
	periodStart := time.Date(2026, 7, 11, 0, 0, 0, 0, time.UTC)
	periodEnd := time.Date(2026, 8, 11, 0, 0, 0, 0, time.UTC)
	pendingCreatedAt := time.Date(2026, 7, 17, 12, 34, 0, 0, time.UTC)

	appValid := uuid.New()
	seedSettledApp(store, appValid, now, "in_valid", "INV-1", "paid", 22_000_000, now)

	appZero := uuid.New() // settled guard but a $0 invoice → excluded
	seedSettledApp(store, appZero, now, "in_zero", "INV-0", "paid", 0, now)

	appVoid := uuid.New() // settled guard but a voided invoice → excluded
	seedSettledApp(store, appVoid, now, "in_void", "INV-V", "void", 22_000_000, now)

	appPending := uuid.New() // uncharged, live, in grace → pending
	store.appMirrors[appPending] = usage.AppMirrorInfo{CreatedAt: pendingCreatedAt}

	appSkipped := uuid.New() // permanently skipped → excluded from pending
	store.appMirrors[appSkipped] = usage.AppMirrorInfo{CreatedAt: now}
	store.newAppProrationSkipped[appSkipped] = true

	appDeleted := uuid.New() // soft-deleted → excluded from pending
	store.appMirrors[appDeleted] = usage.AppMirrorInfo{CreatedAt: now, Deleted: true, DeletedAt: now}

	resp, err := newService(store).WithNow(func() time.Time { return now }).ListNewCreationCharges(context.Background(), usage.ListNewCreationChargesRequest{
		OwnerUserID: owner, // current window (no PeriodID)
	})
	require.NoError(t, err)
	require.Len(t, resp.Charges, 2, "only the valid settled + valid pending survive")

	// Settled first (the sole settled row), then pending.
	require.Equal(t, appValid, resp.Charges[0].AppID)
	require.Equal(t, usage.NewCreationChargeStatusSettled, resp.Charges[0].Status)
	require.EqualValues(t, 22_000_000, resp.Charges[0].AmountMicros)

	require.Equal(t, appPending, resp.Charges[1].AppID)
	require.Equal(t, usage.NewCreationChargeStatusPending, resp.Charges[1].Status)
	expected := usage.CreationChargeBaseMicros(pendingCreatedAt, periodStart, periodEnd)
	require.EqualValues(t, expected, resp.Charges[1].AmountMicros, "pending previews the sweep's exact base amount")
	require.EqualValues(t, expected, resp.Charges[1].BaseFeeMicros)
	require.NotEqualValues(t, usage.BaseFeeMicros, resp.Charges[1].AmountMicros, "mid-period fixture must discriminate from the old flat preview")
	require.Nil(t, resp.Charges[1].RecordedAt)
	require.Empty(t, resp.Charges[1].InvoiceID)
	require.NotNil(t, resp.Charges[1].ChargeETA)
	require.True(t, resp.Charges[1].ChargeETA.Equal(usage.GraceExpiry(pendingCreatedAt)),
		"charge_eta == created_at + GraceDays")

	// The service resolved graceCutoff = now − GraceDays and passed it through.
	require.Equal(t, now.AddDate(0, 0, -usage.GraceDays), store.gotPendingGraceCutoff)
}

// TestListNewCreationCharges_PendingPreviewEqualsSweepCharge covers the
// reporter's anchored 7/11→8/11 window: a 7/17 creation previews the exact
// 25/31 creation-period amount the sweep will charge, not the flat $20 base.
func TestListNewCreationCharges_PendingPreviewEqualsSweepCharge(t *testing.T) {
	store := newFakeStore()
	owner := uuid.New()
	accountID := uuid.New()
	store.accounts[owner] = accountID
	store.anchorDays[accountID] = 11

	periodStart := time.Date(2026, 7, 11, 0, 0, 0, 0, time.UTC)
	periodEnd := time.Date(2026, 8, 11, 0, 0, 0, 0, time.UTC)
	createdAt := time.Date(2026, 7, 17, 12, 34, 0, 0, time.UTC)
	now := time.Date(2026, 7, 18, 12, 0, 0, 0, time.UTC)
	appID := uuid.New()
	store.appMirrors[appID] = usage.AppMirrorInfo{CreatedAt: createdAt, Name: "Reporter App"}

	resp, err := newService(store).WithNow(func() time.Time { return now }).ListNewCreationCharges(context.Background(), usage.ListNewCreationChargesRequest{
		OwnerUserID: owner,
	})
	require.NoError(t, err)
	require.Len(t, resp.Charges, 1)

	expected := usage.CreationChargeBaseMicros(createdAt, periodStart, periodEnd)
	require.EqualValues(t, 16_129_032, expected, "25/31 of $20, rounded half-up in micros")
	require.Equal(t, appID, resp.Charges[0].AppID)
	require.EqualValues(t, expected, resp.Charges[0].AmountMicros)
	require.EqualValues(t, expected, resp.Charges[0].BaseFeeMicros)
	require.NotEqualValues(t, usage.BaseFeeMicros, resp.Charges[0].AmountMicros)
}

// TestListNewCreationCharges_PendingOnlyInCurrentWindow: a past period holds no
// still-in-grace apps, so the service NEVER issues the pending query for a
// historical period (proven by the untouched gotPendingGraceCutoff), while the
// current window DOES.
func TestListNewCreationCharges_PendingOnlyInCurrentWindow(t *testing.T) {
	// Historical period: pending query is gated OFF.
	histStore := newFakeStore()
	owner := uuid.New()
	histStore.accounts[owner] = uuid.New()
	pid := mirrorPeriod(histStore)
	// An in-grace-looking app created in the historical window (guard null, live).
	histStore.appMirrors[uuid.New()] = usage.AppMirrorInfo{CreatedAt: time.Date(2026, 5, 20, 0, 0, 0, 0, time.UTC)}

	histResp, err := newService(histStore).ListNewCreationCharges(context.Background(), usage.ListNewCreationChargesRequest{
		OwnerUserID: owner, PeriodID: pid.String(),
	})
	require.NoError(t, err)
	require.Empty(t, histResp.Charges)
	require.True(t, histStore.gotPendingGraceCutoff.IsZero(), "pending query must NOT run for a historical period")

	// Current window: the pending query runs and returns the in-grace app.
	curStore := newFakeStore()
	curStore.accounts[owner] = uuid.New()
	pendingApp := uuid.New()
	curStore.appMirrors[pendingApp] = usage.AppMirrorInfo{CreatedAt: time.Now().UTC()}

	curResp, err := newService(curStore).ListNewCreationCharges(context.Background(), usage.ListNewCreationChargesRequest{
		OwnerUserID: owner, // current window
	})
	require.NoError(t, err)
	require.Len(t, curResp.Charges, 1)
	require.Equal(t, pendingApp, curResp.Charges[0].AppID)
	require.Equal(t, usage.NewCreationChargeStatusPending, curResp.Charges[0].Status)
	require.False(t, curStore.gotPendingGraceCutoff.IsZero(), "pending query must run for the current window")
}

// TestListNewCreationCharges_Empty: an account with no new apps in the window yields
// an empty (never nil) slice.
func TestListNewCreationCharges_Empty(t *testing.T) {
	store := newFakeStore()
	owner := seedOwnerAccount(store)
	resp, err := newService(store).ListNewCreationCharges(context.Background(), usage.ListNewCreationChargesRequest{
		OwnerUserID: owner,
	})
	require.NoError(t, err)
	require.NotNil(t, resp.Charges)
	require.Empty(t, resp.Charges)
}

// TestListNewCreationCharges_Ordering: settled rows come newest-first (by the invoice
// recorded_at), then every pending row.
func TestListNewCreationCharges_Ordering(t *testing.T) {
	store := newFakeStore()
	owner := uuid.New()
	store.accounts[owner] = uuid.New()
	now := time.Now().UTC()

	older := uuid.New()
	seedSettledApp(store, older, now, "in_old", "INV-OLD", "paid", 20_000_000, now.Add(-2*time.Hour))
	newer := uuid.New()
	seedSettledApp(store, newer, now, "in_new", "INV-NEW", "paid", 20_000_000, now.Add(-1*time.Hour))

	pending := uuid.New()
	store.appMirrors[pending] = usage.AppMirrorInfo{CreatedAt: now}

	resp, err := newService(store).ListNewCreationCharges(context.Background(), usage.ListNewCreationChargesRequest{
		OwnerUserID: owner,
	})
	require.NoError(t, err)
	require.Len(t, resp.Charges, 3)
	require.Equal(t, newer, resp.Charges[0].AppID, "newest settled first")
	require.Equal(t, older, resp.Charges[1].AppID)
	require.Equal(t, pending, resp.Charges[2].AppID, "pending after every settled row")
}

// TestListNewCreationCharges_PendingAddonRows: post-creation over-module installs
// surface as per-app pending ADD-ON rows — base 0, amount = flat surcharge ×
// count, ETA = earliest timer expiry — merged soonest-first with the creation
// pendings.
func TestListNewCreationCharges_PendingAddonRows(t *testing.T) {
	store := newFakeStore()
	owner := uuid.New()
	store.accounts[owner] = uuid.New()
	now := time.Now().UTC()

	// A creation pending whose ETA lands AFTER the addon row's (created now →
	// eta now+3d; the addon timer expires tomorrow) — the merge must interleave.
	creationPending := uuid.New()
	store.appMirrors[creationPending] = usage.AppMirrorInfo{CreatedAt: now, Name: "Draft App"}

	addonApp := uuid.New()
	store.pendingAddonCharges = []usage.PendingAddonChargeRaw{
		{AppID: addonApp, Name: "老 App", AddonCount: 2, ChargeETA: now.Add(24 * time.Hour)},
	}

	resp, err := newService(store).ListNewCreationCharges(context.Background(), usage.ListNewCreationChargesRequest{
		OwnerUserID: owner, // current window
	})
	require.NoError(t, err)
	require.Len(t, resp.Charges, 2)

	addon := resp.Charges[0]
	require.Equal(t, addonApp, addon.AppID, "soonest ETA first across BOTH pending sources")
	require.Equal(t, usage.NewCreationChargeStatusPending, addon.Status)
	require.Equal(t, "老 App", addon.Name)
	require.Zero(t, addon.BaseFeeMicros, "an add-on row carries no base fee")
	require.Equal(t, 2, addon.AddonModuleCount)
	require.EqualValues(t, 2*usage.ModuleOverageFeeMicros, addon.AddonMicros)
	require.EqualValues(t, 2*usage.ModuleOverageFeeMicros, addon.AmountMicros, "amount == projected flat surcharge × count")
	require.NotNil(t, addon.ChargeETA)
	require.True(t, addon.ChargeETA.Equal(now.Add(24*time.Hour)), "ETA = the earliest timer expiry")

	require.Equal(t, creationPending, resp.Charges[1].AppID, "creation pending (eta +3d) sorts after")
	require.True(t, store.gotPendingAddonNow.After(now.Add(-time.Minute)), "service passed its own now to the timer read")
}

// TestListNewCreationCharges_PendingAddonSkippedOnPastPeriod: like the creation
// pendings, add-on pendings exist only in the CURRENT live window — resolving a
// frozen billing_periods id must not even issue the timer read.
func TestListNewCreationCharges_PendingAddonSkippedOnPastPeriod(t *testing.T) {
	store := newFakeStore()
	owner := uuid.New()
	store.accounts[owner] = uuid.New()
	pid := mirrorPeriod(store)
	store.pendingAddonCharges = []usage.PendingAddonChargeRaw{
		{AppID: uuid.New(), Name: "X", AddonCount: 1, ChargeETA: time.Now().UTC()},
	}

	resp, err := newService(store).ListNewCreationCharges(context.Background(), usage.ListNewCreationChargesRequest{
		OwnerUserID: owner, PeriodID: pid.String(),
	})
	require.NoError(t, err)
	require.Empty(t, resp.Charges)
	require.True(t, store.gotPendingAddonNow.IsZero(), "past period: the timer read is never issued")
}
