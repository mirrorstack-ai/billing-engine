package cycle_test

// Per-module-instance overage — Leg 1 (migration 033): the per-module grace
// charge sweep (SweepModuleOverage / ChargeModuleOverage), the LIVE FIFO
// included-vs-over determination, the install-anchored proration, and the FIFO
// monotonicity / permanent-inclusion property. Reuses the in-memory fakeStore
// (service_test.go) + fakeStripe (charge_test.go) + the registeredAccount /
// registerMirror / appsSvc helpers (apps_test.go / proration_test.go).

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/mirrorstack-ai/billing-engine/internal/account/cycle"
	billingstripe "github.com/mirrorstack-ai/billing-engine/internal/shared/stripe"
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

// --- grace straddling a period boundary: Leg 1 covers the straddled period ----

// Regression (review 2026-07-06, M1): a grace window that straddles a period
// boundary used to leave the straddled period billed by NO leg — Leg 1's comment
// deferred it to the boundary precharge, but the boundary ran BEFORE the grace
// elapsed and (correctly) excluded the unresolved timer. Under the coverage
// contract Leg 1 now charges install day → the END of the period the grace
// elapses into: the install period prorated + the straddled period in full.
func TestModuleOverage_GraceStraddlingBoundaryCoversStraddledPeriod(t *testing.T) {
	store := newFakeStore()
	_, acct := registeredAccount(store) // activated 2026-05-04 → anchor day 4
	sc := newFakeStripe()
	svc := cycle.NewService(store, sc)
	ctx := context.Background()
	app := uuid.New()

	seedIncluded(store, acct, uuid.New(), time.Date(2026, 5, 4, 0, 0, 0, 0, time.UTC), 5)
	// Installed Jul 2 — 2 days before the [Jun 4, Jul 4) period closes — so its
	// grace elapses Jul 5, INSIDE the next period [Jul 4, Aug 4).
	over := seedTimer(store, acct, app, time.Date(2026, 7, 2, 0, 0, 0, 0, time.UTC))

	res, err := svc.SweepModuleOverage(ctx, time.Date(2026, 7, 6, 0, 0, 0, 0, time.UTC))
	require.NoError(t, err)
	require.Equal(t, 1, res.Charged)
	require.True(t, store.timers[over].graceCharged)

	// $3 × 2/30 days (Jul 2 → Jul 4 of the 30-day install period, round-half-up
	// = $0.20) + the FULL $3 for the straddled [Jul 4, Aug 4) period = $3.20.
	require.Len(t, sc.itemCalls, 1)
	require.EqualValues(t, 320, sc.itemCalls[0].amountCfg,
		"install-period proration + the full straddled period")

	// The mirrored window agrees with the amount: coverage runs through the END
	// of the period the grace elapsed into.
	require.Len(t, store.invoices, 1)
	for _, inv := range store.invoices {
		require.Equal(t, time.Date(2026, 7, 2, 0, 0, 0, 0, time.UTC), inv.PeriodStart)
		require.Equal(t, time.Date(2026, 8, 4, 0, 0, 0, 0, time.UTC), inv.PeriodEnd)
	}
}

// The complement: a grace that elapses INSIDE the install period keeps the
// familiar install-anchored proration — no straddle surcharge.
func TestModuleOverage_GraceInsidePeriodChargesInstallPeriodOnly(t *testing.T) {
	store := newFakeStore()
	_, acct := registeredAccount(store) // anchor day 4
	sc := newFakeStripe()
	svc := cycle.NewService(store, sc)
	ctx := context.Background()

	seedIncluded(store, acct, uuid.New(), time.Date(2026, 5, 4, 0, 0, 0, 0, time.UTC), 5)
	// Installed Jun 10; grace elapses Jun 13, well inside [Jun 4, Jul 4).
	over := seedTimer(store, acct, uuid.New(), time.Date(2026, 6, 10, 0, 0, 0, 0, time.UTC))

	res, err := svc.SweepModuleOverage(ctx, time.Date(2026, 6, 14, 0, 0, 0, 0, time.UTC))
	require.NoError(t, err)
	require.Equal(t, 1, res.Charged)
	require.True(t, store.timers[over].graceCharged)

	// $3 × 24/30 days (Jun 10 → Jul 4) = $2.40 — and nothing more.
	require.Len(t, sc.itemCalls, 1)
	require.EqualValues(t, 240, sc.itemCalls[0].amountCfg)
	require.Len(t, store.invoices, 1)
	for _, inv := range store.invoices {
		require.Equal(t, time.Date(2026, 7, 4, 0, 0, 0, 0, time.UTC), inv.PeriodEnd,
			"no straddle → coverage ends at the install period's end")
	}
}

// Regression (review 2026-07-06, H9): a crash between the Stripe charge and
// MarkModuleTimerCharged, followed by an earlier module's removal, used to let
// the retry recompute the timer's rank as "included" and resolve it uncharged —
// real money moved with no invoice mirror, no disclosure, no mark. With the
// migration-036 attempt marker the retry reconciles against Stripe FIRST: the
// finalized invoice is found by its ms_charge_ref, mirrored, and marked, and
// the (now-improved) live rank never gets to orphan it.
func TestModuleOverage_RetryAfterCrashRecoversChargeEvenWhenRankFlipped(t *testing.T) {
	store := newFakeStore()
	_, acct := registeredAccount(store)
	sc := newFakeStripe()
	svc := cycle.NewService(store, sc)
	ctx := context.Background()

	seedIncluded(store, acct, uuid.New(), time.Date(2026, 5, 4, 0, 0, 0, 0, time.UTC), 5)
	x := seedTimer(store, acct, uuid.New(), time.Date(2026, 6, 10, 0, 0, 0, 0, time.UTC))
	// Attempt 1 reached the Stripe section (marker set), charged, and crashed
	// before the mark. Its finalized invoice sits on Stripe under the ref.
	store.timers[x].chargeAttemptedAt = time.Date(2026, 6, 14, 0, 0, 0, 0, time.UTC)
	sc.findByRef = &billingstripe.Invoice{ID: "in_crashed", Status: "paid", AmountDue: 240, AmountPaid: 240, Currency: "usd"}

	// Between crash and retry an EARLIER module is removed — x's live rank
	// improves to 4 ("included").
	for id, tm := range store.timers {
		if tm.installedAt.Equal(time.Date(2026, 5, 4, 0, 0, 0, 0, time.UTC)) {
			store.timers[id].removed = true
			store.timers[id].removedAt = time.Date(2026, 6, 15, 0, 0, 0, 0, time.UTC)
			break
		}
	}

	res, err := svc.SweepModuleOverage(ctx, time.Date(2026, 6, 16, 0, 0, 0, 0, time.UTC))
	require.NoError(t, err)
	require.Equal(t, 1, res.Charged, "the moved money is recovered, not orphaned by the rank flip")
	require.True(t, store.timers[x].graceCharged)
	require.Equal(t, "in_crashed", store.timers[x].graceInvoiceID)
	inv, ok := store.invoices["in_crashed"]
	require.True(t, ok, "the crashed attempt's invoice is mirrored")
	require.EqualValues(t, 240, inv.AmountDueCents)
	// H5: no NEW Stripe objects were minted on recovery.
	require.Empty(t, sc.invoiceCalls, "no second draft")
	require.Empty(t, sc.finalizeCalls, "no second finalize")
}

// H5, the draft-completion arm: the crashed attempt created its draft but died
// before attaching the line / finalizing. The retry completes THAT draft (the
// deterministic line attached to the FOUND invoice id, then finalized) instead
// of minting a second one — safe even after Stripe pruned the idem keys.
func TestModuleOverage_RetryCompletesCrashedDraftInsteadOfMintingSecond(t *testing.T) {
	store := newFakeStore()
	_, acct := registeredAccount(store)
	sc := newFakeStripe()
	svc := cycle.NewService(store, sc)
	ctx := context.Background()

	seedIncluded(store, acct, uuid.New(), time.Date(2026, 5, 4, 0, 0, 0, 0, time.UTC), 5)
	x := seedTimer(store, acct, uuid.New(), time.Date(2026, 6, 10, 0, 0, 0, 0, time.UTC))
	store.timers[x].chargeAttemptedAt = time.Date(2026, 6, 14, 0, 0, 0, 0, time.UTC)
	sc.findByRef = &billingstripe.Invoice{ID: "in_orphan_draft", Status: "draft", AmountDue: 0, Currency: "usd"}

	res, err := svc.SweepModuleOverage(ctx, time.Date(2026, 6, 16, 0, 0, 0, 0, time.UTC))
	require.NoError(t, err)
	require.Equal(t, 1, res.Charged)
	require.Empty(t, sc.invoiceCalls, "the found draft is completed — never a second CreateDraftInvoice")
	require.Len(t, sc.itemCalls, 1)
	require.Equal(t, "in_orphan_draft", sc.itemCalls[0].invoiceID, "the line lands on the crashed attempt's own draft")
	require.EqualValues(t, 240, sc.itemCalls[0].amountCfg)
	require.Len(t, sc.finalizeCalls, 1)
	require.Equal(t, "in_orphan_draft", sc.finalizeCalls[0].invoiceID)
	require.True(t, store.timers[x].graceCharged)
}

// The recovery no-op arm: the marker is set but Stripe has NOTHING under the
// ref — the crashed attempt never created its invoice. The retry charges fresh.
func TestModuleOverage_AttemptedButNothingOnStripeChargesFresh(t *testing.T) {
	store := newFakeStore()
	_, acct := registeredAccount(store)
	sc := newFakeStripe()
	svc := cycle.NewService(store, sc)
	ctx := context.Background()

	seedIncluded(store, acct, uuid.New(), time.Date(2026, 5, 4, 0, 0, 0, 0, time.UTC), 5)
	x := seedTimer(store, acct, uuid.New(), time.Date(2026, 6, 10, 0, 0, 0, 0, time.UTC))
	store.timers[x].chargeAttemptedAt = time.Date(2026, 6, 14, 0, 0, 0, 0, time.UTC)
	// sc.findByRef stays nil — not found.

	res, err := svc.SweepModuleOverage(ctx, time.Date(2026, 6, 16, 0, 0, 0, 0, time.UTC))
	require.NoError(t, err)
	require.Equal(t, 1, res.Charged)
	require.NotEmpty(t, sc.findByRefCalls, "the recovery lookup ran")
	require.Len(t, sc.invoiceCalls, 1, "nothing to recover → fresh draft→item→finalize")
	require.Len(t, sc.finalizeCalls, 1)
	require.True(t, store.timers[x].graceCharged)
}

// Regression (review 2026-07-06, M2): a candidate that went stale between the
// work-list read and its turn — removed, or resolved by a concurrent sweep —
// is re-verified at charge time and never charged.
func TestModuleOverage_StaleCandidateNotCharged(t *testing.T) {
	store := newFakeStore()
	_, acct := registeredAccount(store)
	sc := newFakeStripe()
	svc := cycle.NewService(store, sc)
	ctx := context.Background()

	seedIncluded(store, acct, uuid.New(), time.Date(2026, 5, 4, 0, 0, 0, 0, time.UTC), 5)
	x := seedTimer(store, acct, uuid.New(), time.Date(2026, 6, 10, 0, 0, 0, 0, time.UTC))

	// The candidate as the work list saw it...
	cand := cycle.ModuleOverageCandidate{
		ID: x, AccountID: acct, AppID: store.timers[x].appID,
		InstalledAt: store.timers[x].installedAt, GraceExpiresAt: store.timers[x].graceExpiresAt,
		ActivatedAt: time.Date(2026, 5, 4, 9, 0, 0, 0, time.UTC),
	}
	// ...then the module is uninstalled before its turn in the batch.
	store.timers[x].removed = true
	store.timers[x].removedAt = time.Date(2026, 6, 14, 0, 0, 0, 0, time.UTC)

	res, err := svc.ChargeModuleOverage(ctx, cand, time.Date(2026, 6, 14, 1, 0, 0, 0, time.UTC))
	require.NoError(t, err)
	require.Equal(t, cycle.ModuleOverageSkippedStale, res.Status)
	require.Empty(t, sc.invoiceCalls, "a stale candidate never reaches Stripe")
	require.False(t, store.timers[x].graceCharged)
}

// Regression (review 2026-07-06, H10): a PREPAID account is never auto-charged
// off-session — the boundary spine always gated on this, but the per-module
// grace leg bypassed it entirely. The skip is transient: nothing is resolved,
// and a relax back to arrears charges through the same keys.
func TestModuleOverage_PrepaidAccountSkippedNotCharged(t *testing.T) {
	store := newFakeStore()
	_, acct := registeredAccount(store)
	store.collection.Mode = cycle.BillingModePrepaid
	sc := newFakeStripe()
	svc := cycle.NewService(store, sc)
	ctx := context.Background()

	seedIncluded(store, acct, uuid.New(), time.Date(2026, 5, 4, 0, 0, 0, 0, time.UTC), 5)
	over := seedTimer(store, acct, uuid.New(), time.Date(2026, 6, 10, 0, 0, 0, 0, time.UTC))
	sweepAt := time.Date(2026, 6, 14, 0, 0, 0, 0, time.UTC)

	res, err := svc.SweepModuleOverage(ctx, sweepAt)
	require.NoError(t, err)
	require.Equal(t, 1, res.Skipped)
	require.Zero(t, res.Charged)
	require.Empty(t, sc.invoiceCalls, "a prepaid account is never auto-charged by Leg 1")
	require.False(t, store.timers[over].graceResolved, "transient skip — nothing resolved")

	// The account relaxes back to arrears → the deferred charge fires.
	store.collection.Mode = cycle.BillingModeArrears
	res, err = svc.SweepModuleOverage(ctx, sweepAt)
	require.NoError(t, err)
	require.Equal(t, 1, res.Charged)
	require.True(t, store.timers[over].graceCharged)
}

// --- C2: items are pinned to their own draft; Stripe failures stay retryable --

// Regression (review 2026-07-06, C2): every Leg-1 line item is PINNED to the
// timer's own draft invoice (created first, pending_invoice_items_behavior=
// exclude) and money moves only at finalize — a crash at any step leaves an
// inert draft that no other charge leg's invoice can sweep up. Also the first
// Stripe-failure injection on this leg: a failed item or finalize leaves the
// timer unresolved, and the retry replays the SAME deterministic idem keys.
func TestModuleOverage_StripeFailureLeavesTimerRetryableWithSameKeys(t *testing.T) {
	store := newFakeStore()
	_, acct := registeredAccount(store)
	sc := newFakeStripe()
	svc := cycle.NewService(store, sc)
	ctx := context.Background()

	seedIncluded(store, acct, uuid.New(), time.Date(2026, 5, 4, 0, 0, 0, 0, time.UTC), 5)
	over := seedTimer(store, acct, uuid.New(), time.Date(2026, 6, 10, 0, 0, 0, 0, time.UTC))
	sweepAt := time.Date(2026, 6, 14, 0, 0, 0, 0, time.UTC)

	// Attempt 1: the pinned-item create fails AFTER the draft exists.
	sc.errItem = errors.New("stripe: item boom")
	res, err := svc.SweepModuleOverage(ctx, sweepAt)
	require.NoError(t, err, "a per-timer failure never aborts the batch")
	require.Equal(t, 1, res.Failed)
	require.Empty(t, sc.finalizeCalls, "no finalize → no money moved")
	require.False(t, store.timers[over].graceResolved, "left unresolved for the retry")
	require.Empty(t, store.invoices, "nothing mirrored")

	// Attempt 2: the finalize (money-moving step) fails.
	sc.errItem = nil
	sc.errInvoice = errors.New("stripe: finalize boom")
	res, err = svc.SweepModuleOverage(ctx, sweepAt)
	require.NoError(t, err)
	require.Equal(t, 1, res.Failed)
	require.False(t, store.timers[over].graceResolved)
	require.Empty(t, store.invoices)

	// Attempt 3 succeeds — through the SAME deterministic keys as both failures.
	sc.errInvoice = nil
	res, err = svc.SweepModuleOverage(ctx, sweepAt)
	require.NoError(t, err)
	require.Equal(t, 1, res.Charged)
	require.True(t, store.timers[over].graceCharged)

	require.GreaterOrEqual(t, len(sc.invoiceCalls), 3)
	require.GreaterOrEqual(t, len(sc.itemCalls), 2)
	for _, dc := range sc.invoiceCalls {
		require.Equal(t, "mod-overage-inv-"+over.String(), dc.idemKey, "every attempt reuses the same draft idem key")
		require.Equal(t, "timer:"+over.String(), dc.ref, "the draft carries the charge-ref metadata anchor")
	}
	for _, ic := range sc.itemCalls {
		require.Equal(t, "mod-overage-ii-"+over.String(), ic.idemKey)
		require.NotEmpty(t, ic.invoiceID, "the line item is PINNED to the timer's own draft, never a floating pending item")
	}
	require.Equal(t, "mod-overage-fin-"+over.String(), sc.finalizeCalls[len(sc.finalizeCalls)-1].idemKey)
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

// --- FINDING 1: no retroactive catch-up (D1d) when an account only activates
// after an over-module's anchored install period already closed ---------------

func TestModuleOverage_NoRetroactiveCatchUpWhenActivatedAfterPeriodClosed(t *testing.T) {
	// Reproduces the exact failure scenario. An account installs 8 modules on an
	// app while UNACTIVATED (RegisterApp synthesizes the timers regardless of
	// activation). They sit installed + past-grace for months. Then the owner
	// finally binds a card — with anchor day 1 (activated Apr 1), the timers'
	// install-anchored period [Jan 1, Feb 1) is long closed. Pre-fix, the very next
	// sweep charged the 3 "over" timers (ranks 5-7) a REAL Stripe invoice for that
	// historical, never-chargeable January period — exactly the retroactive
	// catch-up D1d forbids. Fixed: the over timers are resolved terminally WITH NO
	// charge (period_closed), never resurface, and Stripe is never called; the 5
	// included ones resolve as included as usual.
	store := newFakeStore()
	_, acct := registeredAccount(store)
	delete(store.activation, acct) // unactivated at install time
	sc := newFakeStripe()
	svc := cycle.NewService(store, sc)
	ctx := context.Background()
	app := uuid.New()

	installed := time.Date(2026, 1, 1, 8, 0, 0, 0, time.UTC)
	var ids []uuid.UUID
	for i := 0; i < 8; i++ {
		ids = append(ids, seedTimer(store, acct, app, installed))
	}

	// While unactivated the sweep never even lists them (activated_at IS NULL gate).
	res0, err := svc.SweepModuleOverage(ctx, time.Date(2026, 1, 10, 0, 0, 0, 0, time.UTC))
	require.NoError(t, err)
	require.Equal(t, 0, res0.Pending, "unactivated → excluded from the work list")

	// MONTHS later: the owner binds a card (anchor day 1, Apr 1) + a PM. The 8
	// timers' install period [Jan 1, Feb 1) closed long before Apr 1.
	store.activation[acct] = time.Date(2026, 4, 1, 9, 0, 0, 0, time.UTC)

	res, err := svc.SweepModuleOverage(ctx, time.Date(2026, 4, 2, 0, 0, 0, 0, time.UTC))
	require.NoError(t, err)
	require.Equal(t, 8, res.Pending, "now activated, all 8 past-grace timers are listed")
	require.Equal(t, 0, res.Charged, "no retroactive catch-up charge for a closed period (D1d)")
	require.Equal(t, 5, res.Included, "the 5 earliest installs resolve as included")
	require.Equal(t, 3, res.Skipped, "the 3 over installs resolve period_closed (counted as skipped)")
	require.Empty(t, sc.itemCalls, "no Stripe call for a period the account was never chargeable in")
	for _, id := range ids {
		require.True(t, store.timers[id].graceResolved, "every timer reached a terminal verdict")
		require.False(t, store.timers[id].graceCharged, "none charged")
	}

	// A later sweep never resurfaces them (permanently resolved, never re-swept).
	res2, err := svc.SweepModuleOverage(ctx, time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC))
	require.NoError(t, err)
	require.Equal(t, 0, res2.Pending, "resolved timers never resurface")
	require.Empty(t, sc.itemCalls)
}

func TestModuleOverage_ActivatedBeforePeriodClosesStillCharges(t *testing.T) {
	// Guard against an over-broad fix: an over-module whose account activated
	// BEFORE its install-anchored period closes must still charge normally. The
	// period-closed check compares against ActivatedAt (not the sweep instant), so
	// an account activated well before the install (registeredAccount: May 4) that
	// is swept a few days late is NOT treated as a retroactive catch-up.
	store := newFakeStore()
	_, acct := registeredAccount(store) // activated 2026-05-04, anchor day 4
	sc := newFakeStripe()
	svc := cycle.NewService(store, sc)
	ctx := context.Background()

	// 5 included + one over-module installed Jun 10 → period [Jun 4, Jul 4), which
	// opened AFTER activation, so the account was chargeable for the whole window.
	seedIncluded(store, acct, uuid.New(), time.Date(2026, 5, 4, 0, 0, 0, 0, time.UTC), 5)
	over := seedTimer(store, acct, uuid.New(), time.Date(2026, 6, 10, 0, 0, 0, 0, time.UTC))

	res, err := svc.SweepModuleOverage(ctx, time.Date(2026, 6, 14, 9, 0, 0, 0, time.UTC))
	require.NoError(t, err)
	require.Equal(t, 1, res.Charged, "an over-module whose period opened after activation charges normally")
	require.Len(t, sc.itemCalls, 1)
	require.True(t, store.timers[over].graceCharged)
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
