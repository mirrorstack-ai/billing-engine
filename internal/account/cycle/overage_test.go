package cycle_test

// Per-module-instance overage — Leg 1 (migration 033): the per-module grace
// charge sweep (SweepModuleOverage / ChargeModuleOverage), the LIVE FIFO
// included-vs-over determination, the install-anchored proration, and the FIFO
// monotonicity / permanent-inclusion property. Reuses the in-memory fakeStore
// (service_test.go) + fakeStripe (charge_test.go) + the registeredAccount /
// registerMirror / appsSvc helpers (apps_test.go / proration_test.go).

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/mirrorstack-ai/billing-engine/internal/account/cycle"
)

// seedTimer inserts one live, unresolved install timer directly into the fake
// (bypassing RegisterApp so a test can pin exact install dates), grace expiring
// at install + 3 days, and returns its id.
func seedTimer(store *fakeStore, accountID, appID uuid.UUID, installedAt time.Time) uuid.UUID {
	id := uuid.New()
	store.timers[id] = &fakeTimer{
		id:             id,
		accountID:      accountID,
		appID:          appID,
		installedAt:    installedAt,
		graceExpiresAt: installedAt.AddDate(0, 0, 3),
	}
	return id
}

// liveTimerCount counts an app's currently-live (not-removed) install timers.
func liveTimerCount(store *fakeStore, appID uuid.UUID) int {
	n := 0
	for _, t := range store.timers {
		if t.appID == appID && !t.removed {
			n++
		}
	}
	return n
}

// seedIncluded seeds n live install timers already resolved-as-included at the
// SAME (earliest) install instant, so they occupy the included FIFO slots and
// stay out of the sweep's work list.
func seedIncluded(store *fakeStore, accountID, appID uuid.UUID, installedAt time.Time, n int) {
	for i := 0; i < n; i++ {
		id := seedTimer(store, accountID, appID, installedAt)
		store.timers[id].graceResolved = true
	}
}

// Scenario 4 (pool crosses 5 later, one module at a time → two independent
// prorated charges on different days) lives in the end-to-end scenario suite
// (scenarios_test.go, TestScenario4_PoolCrossesFiveLaterPerModuleTimers). This
// file keeps the Leg-1 PROPERTY tests (FIFO monotonicity, over→included flips,
// removed-in-grace, no-PM retry, unactivated) the scenario suite doesn't repeat.

// --- FIFO monotonicity: an included module is a PERMANENT verdict -------------

func TestModuleOverage_IncludedIsPermanentNeverReEvaluated(t *testing.T) {
	// Once a grace-check finds a module "included", that verdict is permanent
	// (grace_resolved) — it is never re-checked and never charged, even after the
	// pool later grows well past the included 5. Monotonicity: a new install
	// always gets the latest installed_at, so an existing row's rank can only
	// improve (over→included), never regress (included→over).
	store := newFakeStore()
	_, acct := registeredAccount(store)
	sc := newFakeStripe()
	svc := cycle.NewService(store, sc)
	ctx := context.Background()
	app := uuid.New()

	// 5 early installs — all "included" (ranks 0-4), initially UNRESOLVED.
	early := time.Date(2026, 5, 4, 0, 0, 0, 0, time.UTC)
	var earlyIDs []uuid.UUID
	for i := 0; i < 5; i++ {
		earlyIDs = append(earlyIDs, seedTimer(store, acct, app, early))
	}

	// First sweep past their grace → all 5 resolved as included, none charged.
	res, err := svc.SweepModuleOverage(ctx, early.AddDate(0, 0, 4))
	require.NoError(t, err)
	require.Equal(t, 5, res.Pending)
	require.Equal(t, 5, res.Included)
	require.Equal(t, 0, res.Charged)
	require.Empty(t, sc.itemCalls, "included modules are never charged")
	for _, id := range earlyIDs {
		require.True(t, store.timers[id].graceResolved)
		require.False(t, store.timers[id].graceCharged)
	}

	// Now install 10 MORE modules a month later — the pool jumps to 15, but the 5
	// early installs keep ranks 0-4 (monotonicity); the newcomers are "over".
	late := time.Date(2026, 6, 4, 0, 0, 0, 0, time.UTC)
	for i := 0; i < 10; i++ {
		seedTimer(store, acct, app, late)
	}

	// Second sweep → only the 10 unresolved newcomers are candidates and charged;
	// the 5 already-included ones are NOT re-evaluated and stay uncharged forever.
	res2, err := svc.SweepModuleOverage(ctx, late.AddDate(0, 0, 4))
	require.NoError(t, err)
	require.Equal(t, 10, res2.Pending, "only the unresolved newcomers are candidates")
	require.Equal(t, 10, res2.Charged)
	require.Len(t, sc.itemCalls, 10)
	for _, id := range earlyIDs {
		require.False(t, store.timers[id].graceCharged,
			"an included module is never charged even after the pool grows past 5")
		require.True(t, store.timers[id].graceResolved)
	}
}

// --- rank flips over→included before grace elapses → NOT charged --------------

func TestModuleOverage_FlipsToIncludedWhenEarlierRemovedBeforeGrace(t *testing.T) {
	// A module whose rank flips from "over" to "included" before its own grace
	// elapses (because an earlier module was removed) must NOT be charged overage
	// — the determination is LIVE at the grace-check.
	store := newFakeStore()
	_, acct := registeredAccount(store)
	sc := newFakeStripe()
	svc := cycle.NewService(store, sc)
	ctx := context.Background()
	app := uuid.New()

	// 5 earlier installs (ranks 0-4) + X installed a day later (rank 5 = over).
	early := time.Date(2026, 5, 4, 0, 0, 0, 0, time.UTC)
	var earlyIDs []uuid.UUID
	for i := 0; i < 5; i++ {
		earlyIDs = append(earlyIDs, seedTimer(store, acct, app, early))
	}
	x := seedTimer(store, acct, app, time.Date(2026, 5, 5, 0, 0, 0, 0, time.UTC))

	// One earlier module is removed BEFORE X's grace elapses → X's live rank
	// improves to 4 → included.
	store.timers[earlyIDs[0]].removed = true
	store.timers[earlyIDs[0]].removedAt = time.Date(2026, 5, 6, 0, 0, 0, 0, time.UTC)

	// Sweep past all remaining graces → X (and the 4 remaining early installs) are
	// all resolved as included; nothing is charged.
	res, err := svc.SweepModuleOverage(ctx, time.Date(2026, 5, 10, 0, 0, 0, 0, time.UTC))
	require.NoError(t, err)
	require.Equal(t, 5, res.Pending, "the removed module is out of the work list")
	require.Equal(t, 5, res.Included)
	require.Equal(t, 0, res.Charged)
	require.Empty(t, sc.itemCalls)
	require.True(t, store.timers[x].graceResolved)
	require.False(t, store.timers[x].graceCharged, "an over→included flip is never charged")
}

// --- a module removed within its own grace is never charged -------------------

func TestModuleOverage_RemovedWithinGraceNeverCharged(t *testing.T) {
	store := newFakeStore()
	_, acct := registeredAccount(store)
	sc := newFakeStripe()
	svc := cycle.NewService(store, sc)
	ctx := context.Background()

	seedIncluded(store, acct, uuid.New(), time.Date(2026, 5, 4, 0, 0, 0, 0, time.UTC), 5)
	app := uuid.New()
	over := seedTimer(store, acct, app, time.Date(2026, 6, 10, 0, 0, 0, 0, time.UTC))
	// Removed on day 1 (well within its own 3-day grace).
	store.timers[over].removed = true
	store.timers[over].removedAt = time.Date(2026, 6, 11, 0, 0, 0, 0, time.UTC)

	// A sweep long past grace must still never charge it (removed rows are out of
	// the work list).
	res, err := svc.SweepModuleOverage(ctx, time.Date(2026, 6, 20, 0, 0, 0, 0, time.UTC))
	require.NoError(t, err)
	require.Equal(t, 0, res.Pending, "a removed timer is excluded from the sweep")
	require.Empty(t, sc.itemCalls)
	require.False(t, store.timers[over].graceCharged)
}

// --- over module with no usable PM is skipped and retried (not resolved) ------

func TestModuleOverage_NoPMSkipsAndRetries(t *testing.T) {
	store := newFakeStore()
	_, acct := registeredAccount(store)
	store.hasPM = false // account activated but no usable default PM
	sc := newFakeStripe()
	svc := cycle.NewService(store, sc)
	ctx := context.Background()

	seedIncluded(store, acct, uuid.New(), time.Date(2026, 5, 4, 0, 0, 0, 0, time.UTC), 5)
	app := uuid.New()
	over := seedTimer(store, acct, app, time.Date(2026, 6, 10, 0, 0, 0, 0, time.UTC))

	res, err := svc.SweepModuleOverage(ctx, time.Date(2026, 6, 14, 0, 0, 0, 0, time.UTC))
	require.NoError(t, err)
	require.Equal(t, 1, res.Pending)
	require.Equal(t, 0, res.Charged)
	require.Equal(t, 1, res.Skipped)
	require.Empty(t, sc.itemCalls, "no PM → no Stripe call")
	// NOT resolved — it stays a candidate for the next sweep once a PM is added.
	require.False(t, store.timers[over].graceResolved)

	// Add a PM → the next sweep charges it (idempotent per-timer idem keys).
	store.hasPM = true
	res2, err := svc.SweepModuleOverage(ctx, time.Date(2026, 6, 15, 0, 0, 0, 0, time.UTC))
	require.NoError(t, err)
	require.Equal(t, 1, res2.Charged)
	require.Len(t, sc.itemCalls, 1)
	require.True(t, store.timers[over].graceCharged)
}

// --- unactivated accounts are never swept -------------------------------------

func TestModuleOverage_UnactivatedAccountNeverSwept(t *testing.T) {
	store := newFakeStore()
	_, acct := registeredAccount(store)
	delete(store.activation, acct) // never bound a card
	sc := newFakeStripe()
	svc := cycle.NewService(store, sc)
	ctx := context.Background()

	seedIncluded(store, acct, uuid.New(), time.Date(2026, 5, 4, 0, 0, 0, 0, time.UTC), 5)
	over := seedTimer(store, acct, uuid.New(), time.Date(2026, 6, 10, 0, 0, 0, 0, time.UTC))

	res, err := svc.SweepModuleOverage(ctx, time.Date(2026, 6, 20, 0, 0, 0, 0, time.UTC))
	require.NoError(t, err)
	require.Equal(t, 0, res.Pending, "unactivated accounts are excluded from the work list")
	require.Empty(t, sc.itemCalls)
	require.False(t, store.timers[over].graceResolved)
}

// --- SyncAppModules timer synthesis (grow + LIFO shrink + delete) -------------

func TestSyncAppModules_GrowsAndLIFOShrinksTimers(t *testing.T) {
	store := newFakeStore()
	user, _ := registeredAccount(store)
	sc := newFakeStripe()
	svc := appsSvc(store, sc)
	ctx := context.Background()
	appID := uuid.New()

	// Register with 2 modules → 2 timers at created_at.
	registerMirror(t, svc, user, appID, time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC), 2)
	require.Equal(t, 2, liveTimerCount(store, appID))

	// Grow 2 → 5: inserts 3 new timers anchored at now (appsNow).
	five := 5
	_, err := svc.SyncAppModules(ctx, cycle.SyncAppModulesRequest{AppID: appID, ModuleCount: &five})
	require.NoError(t, err)
	require.Equal(t, 5, liveTimerCount(store, appID))

	// Shrink 5 → 3: LIFO-removes the 2 NEWEST (the appsNow installs), leaving the
	// 2 original created_at timers + 1 of the appsNow ones.
	three := 3
	_, err = svc.SyncAppModules(ctx, cycle.SyncAppModulesRequest{AppID: appID, ModuleCount: &three})
	require.NoError(t, err)
	require.Equal(t, 3, liveTimerCount(store, appID))
	// Both original created_at timers survive (they are the OLDEST — LIFO removes
	// newest first).
	created := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)
	origLive := 0
	for _, tm := range store.timers {
		if tm.appID == appID && !tm.removed && tm.installedAt.Equal(created) {
			origLive++
		}
	}
	require.Equal(t, 2, origLive, "LIFO removal keeps the oldest installs")

	// Delete the app → all remaining live timers soft-removed.
	_, err = svc.SyncAppModules(ctx, cycle.SyncAppModulesRequest{AppID: appID, Deleted: true})
	require.NoError(t, err)
	require.Equal(t, 0, liveTimerCount(store, appID))
}
