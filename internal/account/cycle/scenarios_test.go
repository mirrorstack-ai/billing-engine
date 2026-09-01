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
//   * overage $1 × 15/30 =  $0.50 →   50¢   (per-module one-time stub rate)

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/mirrorstack-ai/billing-engine/internal/account/cycle"
	"github.com/mirrorstack-ai/billing-engine/internal/account/usage"
	billingstripe "github.com/mirrorstack-ai/billing-engine/internal/shared/stripe"
)

func ptrI64(v int64) *int64 { return &v }

func onlyInvoiceMirror(t *testing.T, store *fakeStore) cycle.InvoiceMirror {
	t.Helper()
	require.Len(t, store.invoices, 1)
	for _, mirror := range store.invoices {
		return mirror
	}
	return cycle.InvoiceMirror{}
}

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

	// Deleted WITHIN grace (day 1 — the delete must be clocked inside the
	// grace window: D11 makes a post-grace delete chargeable) → the app + all
	// its install timers drop out.
	svcDay1 := cycle.NewService(store, sc).WithNow(func() time.Time { return scenarioCreatedAt.AddDate(0, 0, 1) })
	_, err := svcDay1.SyncAppModules(ctx, cycle.SyncAppModulesRequest{AppID: appID, Deleted: true})
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
	// ONE proposer stands behind BOTH sweeps: since the cutover the creation leg
	// and Leg 1 each SEAL their charge instead of collecting it, so "what this
	// app was billed" is read off p.charges where it used to be read off the
	// invoice items. The figures below are unchanged — sealed micros are the
	// cents a collection takes, x 10_000.
	svc, p := prorationSvc(store, sc)
	ctx := context.Background()
	appID := uuid.New()

	// 3 co-created modules → pool 3 ≤ IncludedModules(5): all "included", no overage.
	registerMirror(t, svc, user, appID, scenarioCreatedAt, 3)

	pro, err := svc.SweepCreationProrations(ctx, scenarioSweepAt)
	require.NoError(t, err)
	require.Equal(t, 1, pro.Proposed, "the creation charge is sealed, never collected")
	require.Zero(t, pro.Failed)
	over, err := svc.SweepModuleOverage(ctx, scenarioSweepAt)
	require.NoError(t, err)
	require.Equal(t, 3, over.Pending, "all 3 co-created timers are past grace and were evaluated")
	require.Equal(t, 3, over.Included, "3 modules are all included — no overage")

	// EXACTLY one sealed charge, ONE line: the prorated FLAT base only — the same
	// single-item invoice, in the artifact that replaced it.
	require.Len(t, p.charges, 1, "base-only creation charge (scenario 2)")
	require.Len(t, p.charges[0].Lines, 1)
	require.EqualValues(t, 1000, sealedProrationCents(t, p), "$20 × 15/30 = $10.00")
	require.Equal(t, "base", p.charges[0].Lines[0].SourceRef)
	require.Contains(t, p.charges[0].Lines[0].Description, appID.String(),
		"the line still names the app whose creation period it bills")
	require.Empty(t, sc.invoiceCalls, "nothing reaches the provider")
	require.Empty(t, sc.itemCalls)
	require.Empty(t, store.invoices, "a sealed charge mirrors no invoice")

	// The window billed is unchanged: [Jun 19, Jul 4). It rides the frozen
	// attempt the charge is derived from, and the intent seals that window's END
	// as the instant a collection may run — where the deleted invoice line's
	// period carried it.
	attempt, frozen := store.combinedProrationAttempts[appID]
	require.True(t, frozen)
	require.True(t, attempt.Shape.CoverageStart.Equal(timeUTC(2026, 6, 19, 0)),
		"coverage starts on the creation day, got %s", attempt.Shape.CoverageStart)
	require.True(t, attempt.Shape.CoverageEnd.Equal(timeUTC(2026, 7, 4, 0)),
		"coverage ends at the anchored period end, got %s", attempt.Shape.CoverageEnd)
	require.True(t, p.charges[0].ExecuteNotBefore.Equal(timeUTC(2026, 7, 4, 0)),
		"a collection may run once the coverage it bills has ended")

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
	svc, p := prorationSvc(store, sc)
	ctx := context.Background()
	appID := uuid.New()

	// 7 co-created modules → pool 7 > IncludedModules(5): 2 are "over" from day 0
	// (installed AT created_at). Their grace elapses at the SAME instant as the
	// app's creation grace, so they ride ONE combined charge with the base.
	registerMirror(t, svc, user, appID, scenarioCreatedAt, 7)

	pro, err := svc.SweepCreationProrations(ctx, scenarioSweepAt)
	require.NoError(t, err)
	require.Equal(t, 1, pro.Proposed)
	// The overage sweep AFTER the proposal finds the 5 included modules
	// resolvable with no charge and the 2 over ones already folded into the
	// combined charge — it bills NOTHING on top.
	over, err := svc.SweepModuleOverage(ctx, scenarioSweepAt)
	require.NoError(t, err)
	require.Equal(t, 7, over.Pending, "every co-created timer was evaluated")
	require.Equal(t, 0, over.Charged, "co-created over-modules already ride the combined charge")
	require.Equal(t, 5, over.Included, "the 5 included modules resolve with no charge")
	require.Equal(t, 2, over.Skipped, "the 2 over ones defer to the combined charge")

	// EXACTLY ONE sealed charge (never two), carrying THREE lines: base + 2
	// overage = $10.00 + $0.50 + $0.50 = $11.00 — the same money the one
	// combined invoice carried across its three items.
	require.Len(t, p.charges, 1, "scenario 3 is ONE combined charge, never two")
	lines := p.charges[0].Lines
	require.Len(t, lines, 3)
	require.EqualValues(t, 1000, lines[0].AmountMicros/10_000, "base: $20 × 15/30 = $10.00")
	require.EqualValues(t, 50, lines[1].AmountMicros/10_000, "overage: $1 × 15/30 = $0.50")
	require.EqualValues(t, 50, lines[2].AmountMicros/10_000)
	require.EqualValues(t, 1100, sealedProrationCents(t, p), "$10.00 base + 2 × $0.50 overage")
	require.Empty(t, sc.invoiceCalls, "no invoice is minted, so there can never be two")
	require.Empty(t, sc.itemCalls)
	require.Empty(t, store.invoices)

	// The overage lines carry the SAME per-timer refs Leg 1 seals under
	// (moduleOverageChargeRef == combinedProrationTimerItemKey), so a racing
	// sweep's charge for one of these timers is recognisable as a duplicate of a
	// line this charge already carries — the property the shared per-timer idem
	// keys used to provide.
	attempt, frozen := store.combinedProrationAttempts[appID]
	require.True(t, frozen)
	require.Len(t, attempt.TimerIDs, 2, "exactly the 2 over-modules were folded in")
	require.Equal(t, "base", lines[0].SourceRef)
	for i, timerID := range attempt.TimerIDs {
		require.Equal(t, "timer:"+timerID.String(), lines[i+1].SourceRef)
	}

	// 🔴 The 2 over timers are NOT stamped charged here, and that is a deliberate
	// refusal rather than a dropped assertion: sealing an intent moves no money,
	// so MarkCombinedProrationProposed resolves the attempt HEADER only —
	// stamping grace_charged_at would claim a debit that has not happened. They
	// stay unresolved AND owned by this charge, which is exactly what keeps Leg 1
	// off them (over.Skipped above). The 5 included ones still reach their
	// terminal verdict, because "included" is a verdict, not a charge.
	included, unresolved := 0, 0
	for _, tm := range store.timers {
		if tm.graceResolved {
			included++
			require.False(t, tm.graceCharged, "an included module is never charged")
			continue
		}
		unresolved++
	}
	require.Equal(t, 5, included)
	require.Equal(t, 2, unresolved, "the 2 over-modules ride the sealed combined charge")
}

// --- FINDING 2: a combined-invoice Phase-3 failure must not let Leg 1 mint a
// SECOND overage invoice for the co-created over-modules ------------------------

func TestScenario3_ProrationPhase3FailureDoesNotMintSecondOverageInvoice(t *testing.T) {
	// Reproduces the exact failure scenario, at the step that replaced the one it
	// was written against. App A is created with 7 co-created modules (5 included
	// + 2 over). The combined creation charge's MONEY STEP succeeds — pre-cutover
	// that was base + 2 overage items on ONE finalized Stripe invoice, now it is
	// ONE sealed intent carrying the same three lines — and the terminal write
	// after it (resolve the attempt, arm the guard) FAILS (deadlock / transient tx
	// error). cmd/billing-cycle then runs the overage sweep in the SAME process.
	// Pre-fix, Leg 1 found the 2 still-unresolved over timers and billed them
	// AGAIN on an artifact of its own, mis-attributing overage the combined charge
	// had already pooled. Fixed: Leg 1 DEFERS them (they belong to the combined
	// charge), so the proration sweep's next retry converges on the SAME charge —
	// exactly ONE billing artifact for that overage, ever.
	store := newFakeStore()
	user, _ := registeredAccount(store)
	sc := newFakeStripe()
	svc, p := prorationSvc(store, sc)
	ctx := context.Background()
	appID := uuid.New()
	registerMirror(t, svc, user, appID, scenarioCreatedAt, 7)

	// The combined charge seals; its terminal write fails after it.
	store.errMarkProposed = errors.New("serialization failure (deadlock)")
	pro, err := svc.SweepCreationProrations(ctx, scenarioSweepAt)
	require.NoError(t, err, "a per-app charge failure never aborts the sweep")
	require.Equal(t, 1, pro.Failed, "App A's combined charge failed at its terminal write")
	require.Equal(t, 0, pro.Proposed)
	require.Empty(t, store.apps[appID].ProrationInvoiceID, "guard unarmed after the terminal write failed")

	// The charge DID seal: base + 2 co-created overage lines, $11.00, ONE document.
	require.Len(t, p.charges, 1)
	require.Len(t, p.charges[0].Lines, 3, "base + 2 co-created overage lines")
	require.EqualValues(t, 1100, sealedProrationCents(t, p))
	attempt, frozen := store.combinedProrationAttempts[appID]
	require.True(t, frozen, "the exact ownership header survives the failure")
	require.Len(t, attempt.TimerIDs, 2)
	require.Empty(t, attempt.ResolvedInvoiceID, "the attempt is unresolved — resolving it is what failed")
	// Every timer is still unresolved — the terminal write is what marks them.
	for _, tm := range store.timers {
		require.False(t, tm.graceResolved, "no timer resolved — the terminal write failed")
	}

	// The SAME-process overage sweep must NOT bill the co-created over-modules a
	// second time. It resolves the 5 included (harmless) and DEFERS the 2 over
	// ones back to the combined-charge path.
	over, err := svc.SweepModuleOverage(ctx, scenarioSweepAt)
	require.NoError(t, err)
	require.Equal(t, 7, over.Pending, "all 7 reached the leg — the deferral is a decision, not a miss")
	require.Equal(t, 0, over.Charged, "co-created over-modules are NOT re-charged by Leg 1")
	require.Equal(t, 5, over.Included, "the 5 included co-created timers resolve with no charge")
	require.Equal(t, 2, over.Skipped, "the 2 over co-created timers are deferred to the combined charge")
	require.Len(t, p.charges, 1,
		"Leg 1 sealed nothing: a second charge for a co-created over-module bills that overage twice")
	require.Empty(t, sc.invoiceCalls, "and it reaches no provider either")
	// The 2 over timers stay unresolved, awaiting the proration retry.
	unresolvedOver := 0
	for _, tm := range store.timers {
		if !tm.graceResolved {
			unresolvedOver++
		}
	}
	require.Equal(t, 2, unresolvedOver, "the 2 over timers wait for the combined-charge retry")

	// Clear the transient failure: the proration sweep's retry re-derives the
	// SAME charge from the SAME frozen attempt and finally resolves it.
	store.errMarkProposed = nil
	pro2, err := svc.SweepCreationProrations(ctx, scenarioSweepAt)
	require.NoError(t, err)
	require.Equal(t, 1, pro2.Proposed, "the retry finally lands the combined charge")
	armed := store.apps[appID].ProrationInvoiceID
	require.NotEmpty(t, armed)
	require.Contains(t, armed, "intent:",
		"the guard arms with the intent that owns the charge; a digest is not an invoice id")
	require.Len(t, p.charges, 2, "only the combined charge was ever sealed — once, then re-derived")
	require.Equal(t, p.charges[0], p.charges[1],
		"the retry seals the IDENTICAL charge — same three lines, same timers, same window, "+
			"so the same digest — the overage landed on the combined charge, never on a stray second one")
	require.Empty(t, store.invoices, "exactly ZERO invoices ever — this leg mints none")
}

// --- Scenario 4: pool crosses 5 later → two independent prorated charges -------

func TestScenario4_PoolCrossesFiveLaterPerModuleTimers(t *testing.T) {
	// Two modules installed a day apart, each pushing the account-wide pool over 5,
	// get their OWN independently-anchored 3-day grace and two DIFFERENT prorated
	// charges on two DIFFERENT days (install-anchored to [install, period end)):
	//   * module A installed Jun 10 → grace ends Jun 13 → $1 × 24/30 = $0.80
	//   * module B installed Jun 11 → grace ends Jun 14 → $1 × 23/30 = $0.77
	//
	// Leg 1 no longer collects, so the two amounts are read off the two sealed
	// intents rather than off two Stripe line items. The figures are unchanged —
	// sealed micros are the cents a collection would take, ×10_000.
	store := newFakeStore()
	_, acct := registeredAccount(store)
	sc := newFakeStripe()
	p := &capturingProposer{}
	svc := cycle.NewService(store, sc).WithIntentProposer(p)
	ctx := context.Background()

	// 5 pre-existing included modules → the two newcomers land in the "over" bucket.
	seedIncluded(store, acct, uuid.New(), timeUTC(2026, 5, 4, 0), 5)
	app := uuid.New()
	timerA := seedTimer(store, acct, app, timeUTC(2026, 6, 10, 0))
	timerB := seedTimer(store, acct, app, timeUTC(2026, 6, 11, 0))

	// Sweep Jun 13: only A is past its own grace → one seal, $0.80.
	_, err := svc.SweepModuleOverage(ctx, timeUTC(2026, 6, 13, 9))
	require.NoError(t, err)
	require.Len(t, p.charges, 1)
	require.EqualValues(t, 800_000, p.charges[0].TotalMicros(), "A: $1 × 24/30 = $0.80")
	require.False(t, store.timers[timerB].graceResolved, "B is still in its own grace")

	// Sweep Jun 14: B's grace now elapsed → a DIFFERENT amount on a DIFFERENT day;
	// A is already resolved and never re-sealed.
	_, err = svc.SweepModuleOverage(ctx, timeUTC(2026, 6, 14, 9))
	require.NoError(t, err)
	require.Len(t, p.charges, 2, "A must not be billed again")
	require.EqualValues(t, 770_000, p.charges[1].TotalMicros(), "B: $1 × 23/30 = $0.77")
	require.Empty(t, sc.itemCalls, "neither charge reaches the provider")
	require.True(t, store.timers[timerA].graceCharged)
	require.True(t, store.timers[timerB].graceCharged)
	require.NotEqual(t, timerA, timerB)
}

// --- Scenario 5: the shared auto-collect helper fires at EVERY charge site -----

func TestScenario5_LargeAutoCollectFlagAtEveryChargeSite(t *testing.T) {
	// The SAME flagLargeAutoCollect helper (migration 034) sets is_large_auto_collect
	// on the mirror row of an off-session charge, resolved AT CHARGE TIME against
	// the account's threshold: a per-account override BELOW the charged amount
	// flags it; the default $100 (nil override) does not.
	//
	// 🔴 The list of sites shrank with the cutover, and for the same reason at each
	// one: a leg that SEALS its charge collects nothing and mirrors no invoice, so
	// there is no is_large_auto_collect on it to resolve. The disclosure decision
	// travels with the collection, into the intent's notice policy — it is not a
	// property a proposing leg can still be asked about.
	//
	// What is left of the helper in this package fires where an invoice still
	// exists: when a crashed legacy attempt's FINALIZED invoice is adopted. That
	// site is asserted below, at the same $10 charge and against the same two
	// thresholds the fresh path was asserted at.
	t.Run("creation/combined leg, adopting a crashed attempt's invoice", func(t *testing.T) {
		run := func(threshold *int64) cycle.InvoiceMirror {
			store := newFakeStore()
			user, _ := registeredAccount(store)
			store.collection.AutoCollectThresholdMicros = threshold
			sc := newFakeStripe()
			svc, p := prorationSvc(store, sc)
			appID := uuid.New()
			registerMirror(t, svc, user, appID, scenarioCreatedAt, 0) // $10 base charge

			// A legacy attempt froze this app's ownership and finalized its
			// invoice before dying — the one case this leg still mirrors.
			attempt := freezeCombinedAttemptThenCrash(t, svc, store, sc, p, appID)
			require.EqualValues(t, 1000, attempt.Shape.BaseChargeCents, "$20 × 15/30 = $10.00")
			require.Empty(t, attempt.TimerIDs)
			sc.setFindByRef("app-proration:"+appID.String(), billingstripe.Invoice{
				ID:         "in_scenario5",
				CustomerID: store.stripeCustomer,
				Status:     "paid",
				AmountDue:  attempt.Shape.BaseChargeCents,
				AmountPaid: attempt.Shape.BaseChargeCents,
				Currency:   "usd",
			})
			seedCombinedAttemptItems(t, sc, "in_scenario5", attempt, nil)

			resp, err := svc.ChargeCreationProration(context.Background(), appID)
			require.NoError(t, err)
			require.Equal(t, cycle.ProrationStatusCharged, resp.Status)
			require.Empty(t, p.charges, "an adopted charge is not sealed a second time")
			return onlyInvoiceMirror(t, store)
		}
		require.True(t, run(ptrI64(5_000_000)).IsLargeAutoCollect, "$10 charged > $5 threshold → flagged")
		require.False(t, run(nil).IsLargeAutoCollect, "$10 charged < $100 default → not flagged")
	})

	// 🔴 The per-module grace leg (Leg 1) is NOT in this list any more. On its
	// fresh path it mirrors no invoice: it seals an intent and collects nothing,
	// so there is no is_large_auto_collect on it to resolve, and asserting one
	// here would have meant re-mirroring an invoice that no longer exists. What
	// remains of the helper at that site (overage.go) fires only when a crashed
	// attempt's invoice is ADOPTED — a charge that was disclosed, if at all, by
	// the attempt that made it.

	// 🔴 The boundary leg (Leg 2) has left this list for the same reason, and it
	// is the site the disclosure mattered most at: it collects the largest
	// amounts. Its collector is deleted, so a boundary mirrors no invoice, and
	// what survives there (charge.go) likewise fires only on adoption.
}

// The mirror a charge leg writes must never latch ever_failed by itself. The
// latch belongs to the webhook router, which ORs it on a failure event; a leg
// that inferred "failed" from a non-paid status would mark every open invoice as
// having failed a charge that is simply still settling.
//
// 🔴 The list of sites this ran over is down to one, and not because the property
// weakened: a leg that seals a charge writes NO mirror row at all, so it has no
// ever_failed to latch. The creation/combined leg's surviving mirror is the one
// it writes when a crashed legacy attempt's finalized invoice is ADOPTED, so that
// is where the latch is asserted — over the same three finalized statuses.
//
// The per-module grace leg is gone from this list for the same reason. Its
// surviving mirror is the crash-recovery one, and
// TestModuleOverage_RetryCompletesCrashedDraftInsteadOfMintingSecond asserts the
// same non-latching property on it directly. The boundary leg finalizes nothing
// either; its surviving mirror is the one written when a crashed attempt's
// invoice is adopted, covered by
// TestRunBillingCycle_LateReclaimAdoptsFoundInvoiceWithoutNewObjects.
func TestAdoptedProrationMirrorNeverLatchesEverFailed(t *testing.T) {
	for _, status := range []string{"paid", "open", "uncollectible"} {
		t.Run(status, func(t *testing.T) {
			store := newFakeStore()
			user, _ := registeredAccount(store)
			sc := newFakeStripe()
			svc, p := prorationSvc(store, sc)
			appID := uuid.New()
			registerMirror(t, svc, user, appID, scenarioCreatedAt, 0)

			attempt := freezeCombinedAttemptThenCrash(t, svc, store, sc, p, appID)
			paid := int64(0)
			if status == "paid" {
				paid = attempt.Shape.BaseChargeCents
			}
			sc.setFindByRef("app-proration:"+appID.String(), billingstripe.Invoice{
				ID:         "in_adopted",
				CustomerID: store.stripeCustomer,
				Status:     status,
				AmountDue:  attempt.Shape.BaseChargeCents,
				AmountPaid: paid,
				Currency:   "usd",
			})
			seedCombinedAttemptItems(t, sc, "in_adopted", attempt, nil)

			resp, err := svc.ChargeCreationProration(context.Background(), appID)
			require.NoError(t, err)
			require.Equal(t, cycle.ProrationStatusCharged, resp.Status)

			mirror := onlyInvoiceMirror(t, store)
			require.Equal(t, status, mirror.Status)
			require.False(t, mirror.EverFailed,
				"a mirror written at charge time must never latch ever_failed")
		})
	}
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
	svc, p := chargeSvcProposing(store, sc)
	resp, err := svc.RunBillingCycle(context.Background(), chargeAccount, periodStart, periodEnd, 0)
	require.NoError(t, err)
	require.Equal(t, cycle.RunStatusProposed, resp.Status)
	require.EqualValues(t, 1_000_000, resp.ArrearsMicros)
	require.EqualValues(t, usage.BaseFeeMicros, resp.AdvanceBaseMicros, "one live app → $20 base")
	require.EqualValues(t, usage.ModuleBlockFeeMicros, resp.AdvanceOverageMicros,
		"only the 2 ONGOING over-modules are precharged (1 block); the in-grace one is excluded")

	// ONE group, ONE collection: $1 arrears + $20 base + 1 block ($5) = $26. The
	// three components no longer pool into one Stripe line — the closed period's
	// usage and the next period's subscription are separate §6 kinds — but they
	// are proposed together, so what settles is still one collection with one
	// rounding.
	require.Empty(t, sc.invoiceCalls)
	require.EqualValues(t, 26_000_000, proposedMicros(t, p))
	require.Len(t, p.groups[0], 2, "arrears and the subscription half are one group of two")
}

// Regression (review 2026-07-06, H1): a module installed INSIDE the new period
// whose own grace already elapsed and was charged by Leg 1 (install-anchored,
// covering that same new period) must NOT be precharged again by a late/reclaimed
// boundary run — the advance-overage leg needs the same installed_at < periodEnd
// cutoff the advance-base leg has always had.
func TestScenario6_ReclaimedBoundaryNeverPrechargesInsidePeriodModule(t *testing.T) {
	store := newFakeStore()
	store.chargedTotal = 1_000_000
	store.hasPM = true
	store.stripeCustomer = "cus_h1"
	app := seedApp(store, chargeAccount, 0, false)

	seedIncluded(store, chargeAccount, app, timeUTC(2026, 5, 1, 0), 5)
	// Installed Jul 2 — INSIDE the new period [Jul 1, Aug 1) — grace elapsed Jul 5
	// and Leg 1 charged it (prorated Jul 2 → Aug 1, i.e. covering the new period).
	inside := seedTimer(store, chargeAccount, app, timeUTC(2026, 7, 2, 0))
	store.timers[inside].graceResolved = true
	store.timers[inside].graceCharged = true

	// The delayed/reclaimed [Jun 1, Jul 1) boundary run executes on Jul 6.
	sc := newFakeStripe()
	resp, err := chargeSvc(store, sc).RunBillingCycle(context.Background(), chargeAccount, periodStart, periodEnd, 0)
	require.NoError(t, err)
	require.Zero(t, resp.AdvanceOverageMicros,
		"a module installed inside the new period was already covered by its own grace charge — precharging it again double-bills the period")
}

// Regression (wave 2, D1): the precharge must NOT depend on the mutable
// grace_resolved flag. cmd/billing-cycle runs the boundary spine before the
// grace sweeps, so a timer whose grace expired in the ~24h before the boundary
// is still UNRESOLVED when the boundary run executes — keying on resolution
// excluded it, and (Leg 1's coverage being derived from immutable timestamps
// and stopping at the boundary) its post-boundary period was then billed by
// nobody. The predicate now uses immutable cutoffs only.
func TestScenario6_ExpiredButUnresolvedTimerStillPrecharged(t *testing.T) {
	store := newFakeStore()
	store.hasPM = true
	store.stripeCustomer = "cus_d1"
	app := seedApp(store, chargeAccount, 0, false)

	seedIncluded(store, chargeAccount, app, timeUTC(2026, 5, 1, 0), 5)
	// Installed Jun 25 → grace expired Jun 28, BEFORE the Jul 1 boundary — but
	// no sweep has resolved it yet (the boundary runs first in the beat).
	seedTimer(store, chargeAccount, app, timeUTC(2026, 6, 25, 0))

	sc := newFakeStripe()
	resp, err := chargeSvc(store, sc).RunBillingCycle(context.Background(), chargeAccount, periodStart, periodEnd, 0)
	require.NoError(t, err)
	require.EqualValues(t, usage.ModuleBlockFeeMicros, resp.AdvanceOverageMicros,
		"an expired-but-unresolved over-module is ongoing — its own Leg 1 charge stops at the boundary, so skipping it here gaps the new period")
}

// Regression (review 2026-07-06, C1): an over-module resolved WITHOUT charge
// under the D1d period-closed posture (installed pre-activation, so its own
// install period is forgiven) still owes overage for every post-activation
// period. The old grace_charged_at IS NOT NULL predicate exempted such modules
// from ALL boundary precharges, forever.
func TestScenario6_D1dResolvedUnchargedOverModuleStillPrecharged(t *testing.T) {
	store := newFakeStore()
	store.hasPM = true
	store.stripeCustomer = "cus_c1"
	app := seedApp(store, chargeAccount, 0, false)

	seedIncluded(store, chargeAccount, app, timeUTC(2026, 5, 1, 0), 5)
	// Over-rank timer resolved terminally with NO charge (the D1d posture):
	// grace_resolved = true, grace_charged_at never set.
	d1d := seedTimer(store, chargeAccount, app, timeUTC(2026, 5, 10, 0))
	store.timers[d1d].graceResolved = true

	sc := newFakeStripe()
	resp, err := chargeSvc(store, sc).RunBillingCycle(context.Background(), chargeAccount, periodStart, periodEnd, 0)
	require.NoError(t, err)
	require.EqualValues(t, usage.ModuleBlockFeeMicros, resp.AdvanceOverageMicros,
		"a D1d resolved-uncharged over-module is ongoing — only its pre-activation install period is forgiven, not every period after")
}

// Regression (review 2026-07-06, H2): an app created within GraceDays of the
// boundary is still IN GRACE when the boundary runs — it must NOT be precharged
// the new period's base. It can still be deleted for free (scenario 1: deleted
// within grace is NEVER charged); an app that survives has the straddled period
// billed by its own creation charge, and joins the advance leg at the NEXT
// boundary. Pre-fix the boundary billed it a full month while still deletable.
func TestScenario6_AppStillInGraceAtBoundaryNotPrechargedBase(t *testing.T) {
	store := newFakeStore()
	store.chargedTotal = 1_000_000 // keep the invoice non-zero without any base
	store.hasPM = true
	store.stripeCustomer = "cus_h2"
	// Created Jun 29 — inside period A [Jun 1, Jul 1) but within GraceDays of
	// the Jul 1 boundary, so its grace (expires Jul 2) straddles it.
	seedAppCreated(store, chargeAccount, 0, false, timeUTC(2026, 6, 29, 0))

	sc := newFakeStripe()
	resp, err := chargeSvc(store, sc).RunBillingCycle(context.Background(), chargeAccount, periodStart, periodEnd, 0)
	require.NoError(t, err)
	require.Zero(t, resp.AdvanceBaseMicros,
		"an app still inside its creation grace at the boundary is not precharged — deleted-in-grace must stay free, and a survivor's creation charge covers the straddled period")
	require.EqualValues(t, 1_000_000, resp.ArrearsMicros)
}

// Regression (review 2026-07-06, M1 boundary side): a charged over-module whose
// grace STRADDLES the boundary is excluded from the precharge — its own Leg 1
// charge covers through the END of the period its grace elapses into, so the
// precharge counting it would double-bill, and Leg 1's coverage means skipping
// it leaves no gap.
func TestScenario6_StraddlingGraceExcludedFromPrecharge(t *testing.T) {
	store := newFakeStore()
	store.hasPM = true
	store.stripeCustomer = "cus_m1"
	app := seedApp(store, chargeAccount, 0, false)

	seedIncluded(store, chargeAccount, app, timeUTC(2026, 5, 1, 0), 5)
	// Installed Jun 29 → grace expires Jul 2, past the Jul 1 boundary. Already
	// charged by (a delayed) Leg 1 covering install → end of the period the grace
	// elapsed into.
	straddle := seedTimer(store, chargeAccount, app, timeUTC(2026, 6, 29, 0))
	store.timers[straddle].graceResolved = true
	store.timers[straddle].graceCharged = true

	sc := newFakeStripe()
	resp, err := chargeSvc(store, sc).RunBillingCycle(context.Background(), chargeAccount, periodStart, periodEnd, 0)
	require.NoError(t, err)
	require.Zero(t, resp.AdvanceOverageMicros,
		"a boundary-straddling grace is Leg 1's coverage — the precharge must not double-bill the new period")
}
