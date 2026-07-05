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

// --- Scenario 4: two modules a day apart → two independent prorated charges ---

func TestModuleOverage_Scenario4_TwoModulesTwoDaysTwoAmounts(t *testing.T) {
	// registeredAccount activates on the 4th → anchor day 4 → the period
	// CONTAINING a mid-June install is [June 4, July 4) = 30 days. The account
	// already has 5 included modules, so two NEW modules installed a day apart are
	// both "over" and each gets its OWN independently-anchored grace charge,
	// prorated from ITS OWN install date to the period end:
	//   * module A installed June 10 → grace ends June 13 → charge $3 × 24/30 =
	//     $2.40 (240¢); remain_days = whole UTC days in [June 10, July 4) = 24.
	//   * module B installed June 11 → grace ends June 14 → charge $3 × 23/30 =
	//     $2.30 (230¢); remain_days = [June 11, July 4) = 23.
	store := newFakeStore()
	_, acct := registeredAccount(store)
	sc := newFakeStripe()
	svc := cycle.NewService(store, sc)
	ctx := context.Background()

	// 5 pre-existing included modules (installed back on the anchor day), so the
	// two newcomers land in the "over" bucket.
	seedIncluded(store, acct, uuid.New(), time.Date(2026, 5, 4, 0, 0, 0, 0, time.UTC), 5)

	appAB := uuid.New()
	timerA := seedTimer(store, acct, appAB, time.Date(2026, 6, 10, 0, 0, 0, 0, time.UTC))
	timerB := seedTimer(store, acct, appAB, time.Date(2026, 6, 11, 0, 0, 0, 0, time.UTC))

	// Sweep on June 13 (A's grace elapsed, B's has not) → charges A only.
	resA, err := svc.SweepModuleOverage(ctx, time.Date(2026, 6, 13, 9, 0, 0, 0, time.UTC))
	require.NoError(t, err)
	require.Equal(t, 1, resA.Pending, "only A is past grace on June 13")
	require.Equal(t, 1, resA.Charged)
	require.Equal(t, 0, resA.Failed)
	require.Len(t, sc.itemCalls, 1)
	require.EqualValues(t, 240, sc.itemCalls[0].amountCfg, "A: $3 × 24/30 = $2.40")
	require.Equal(t, "mod-overage-ii-"+timerA.String(), sc.itemCalls[0].idemKey)
	require.Equal(t, "mod-overage-inv-"+timerA.String(), sc.invoiceCalls[0].idemKey)

	ta := store.timers[timerA]
	require.True(t, ta.graceResolved)
	require.True(t, ta.graceCharged)
	require.Equal(t, time.Date(2026, 6, 13, 9, 0, 0, 0, time.UTC), ta.graceChargedAt)
	// The stored item id is the GENUINE Stripe object id, NOT the idempotency-key
	// string (a prior-PR bug this guards against).
	require.Contains(t, ta.graceInvoiceItemID, "ii_test_")
	require.NotEqual(t, "mod-overage-ii-"+timerA.String(), ta.graceInvoiceItemID)
	require.Contains(t, ta.graceInvoiceID, "in_test_")

	// B untouched so far.
	require.False(t, store.timers[timerB].graceResolved)

	// Sweep on June 14 (B's grace now elapsed) → charges B, a DIFFERENT amount on
	// a DIFFERENT day; A is already resolved and never re-charged.
	resB, err := svc.SweepModuleOverage(ctx, time.Date(2026, 6, 14, 9, 0, 0, 0, time.UTC))
	require.NoError(t, err)
	require.Equal(t, 1, resB.Pending, "only B remains past grace on June 14 (A resolved)")
	require.Equal(t, 1, resB.Charged)
	require.Len(t, sc.itemCalls, 2, "A must not be charged a second time")
	require.EqualValues(t, 230, sc.itemCalls[1].amountCfg, "B: $3 × 23/30 = $2.30")
	require.Equal(t, "mod-overage-ii-"+timerB.String(), sc.itemCalls[1].idemKey)

	tb := store.timers[timerB]
	require.True(t, tb.graceCharged)
	require.Contains(t, tb.graceInvoiceItemID, "ii_test_")
}

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
