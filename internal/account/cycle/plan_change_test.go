package cycle_test

// Plan changes (core-v2#1412, billing-engine#202 PR-2b): the owner's rules of
// 2026-09-12/13 as scenarios over the in-memory fakes. Every amount below is
// written out, never read back from usage.TermsFor, so a price move or a
// rounding change fails a test instead of passing through it.
//
// Fixture: registeredAccount anchors on day 4 (activated May 4), so the period
// every scenario runs in is [Jun 4, Jul 4) — 30 whole days. The change instant
// is Jun 19 15:00, which leaves 15 days (the 19th inclusive) at the new plan:
// Free → Pro = 20 × 15/30 = $10.00, the owner's own example.

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/mirrorstack-ai/billing-engine/internal/account/billing"
	"github.com/mirrorstack-ai/billing-engine/internal/account/cycle"
	"github.com/mirrorstack-ai/billing-engine/internal/account/usage"
)

var (
	pcCreated  = time.Date(2026, 6, 10, 9, 0, 0, 0, time.UTC)
	pcChangeAt = time.Date(2026, 6, 19, 15, 0, 0, 0, time.UTC)
	pcPeriodSt = time.Date(2026, 6, 4, 0, 0, 0, 0, time.UTC)
	pcPeriodEd = time.Date(2026, 7, 4, 0, 0, 0, 0, time.UTC)
)

// planSvc is appsSvc clocked at the change instant, with the proposer every
// charging leg now requires.
func planSvc(store *fakeStore, at time.Time) (*cycle.Service, *capturingProposer) {
	p := &capturingProposer{}
	return cycle.NewService(store, newFakeStripe()).WithNow(func() time.Time { return at }).WithIntentProposer(p), p
}

// registerOnPlan registers an app created at pcCreated on the given plan.
func registerOnPlan(t *testing.T, svc *cycle.Service, user uuid.UUID, plan usage.Plan, memberCount int) uuid.UUID {
	t.Helper()
	appID := uuid.New()
	_, err := svc.RegisterApp(context.Background(), cycle.RegisterAppRequest{
		OwnerUserID: user, AppID: appID, CreatedAt: pcCreated, Plan: string(plan), MemberCount: memberCount, Name: "示範應用",
	})
	require.NoError(t, err)
	return appID
}

// armCreationCharge marks the app's creation period as billed, so a later
// upgrade owes its own delta instead of folding into the creation charge.
func armCreationCharge(store *fakeStore, appID uuid.UUID) {
	app := store.apps[appID]
	app.ProrationInvoiceID = "intent:creation-" + appID.String()
	store.apps[appID] = app
}

func onlyPlanChange(t *testing.T, store *fakeStore, appID uuid.UUID) cycle.PlanChange {
	t.Helper()
	var found []cycle.PlanChange
	for _, c := range store.planChanges {
		if c.AppID == appID {
			found = append(found, c)
		}
	}
	require.Len(t, found, 1, "expected exactly one ledger row for the app")
	return found[0]
}

// --- RegisterApp on a plan ---------------------------------------------------

func TestRegisterApp_OnFreeAppliesTheFreeRules(t *testing.T) {
	store := newFakeStore()
	user, _ := registeredAccount(store)
	svc, _ := planSvc(store, pcChangeAt)

	appID := registerOnPlan(t, svc, user, usage.PlanFree, 2)
	require.Equal(t, usage.PlanFree, store.apps[appID].Plan, "created ON free, no fold needed")
	require.Equal(t, 2, store.apps[appID].MemberCount)

	// Unknown and business plans are refused at creation like at a change.
	_, err := svc.RegisterApp(context.Background(), cycle.RegisterAppRequest{OwnerUserID: user, AppID: uuid.New(), Plan: "hobby"})
	requirePlanErrCode(t, err, billing.CodeInvalidInput)
	_, err = svc.RegisterApp(context.Background(), cycle.RegisterAppRequest{OwnerUserID: user, AppID: uuid.New(), Plan: "business"})
	requirePlanErrCode(t, err, billing.CodePlanNotAvailable)

	// The personal cap: 3 Free apps per personal account (owner 2026-09-13).
	registerOnPlan(t, svc, user, usage.PlanFree, 0)
	registerOnPlan(t, svc, user, usage.PlanFree, 0)
	_, err = svc.RegisterApp(context.Background(), cycle.RegisterAppRequest{OwnerUserID: user, AppID: uuid.New(), Plan: "free"})
	requirePlanErrCode(t, err, billing.CodePlanLimit)
	// A fourth on Pro is fine, and can then NOT move to Free.
	fourth := registerOnPlan(t, svc, user, usage.PlanPro, 0)
	_, err = svc.SetAppPlan(context.Background(), cycle.SetAppPlanRequest{AppID: fourth, Plan: "free"})
	requirePlanErrCode(t, err, billing.CodePlanLimit)
	require.Equal(t, usage.PlanPro, store.apps[fourth].Plan)
	// A retry of a REGISTERED Free app is idempotent and never re-gated.
	_, err = svc.RegisterApp(context.Background(), cycle.RegisterAppRequest{OwnerUserID: user, AppID: appID, Plan: "free"})
	require.NoError(t, err)
}

func TestSetAppPlan_FreeNeedsACardAndHonoursTheOrgCap(t *testing.T) {
	store := newFakeStore()
	user, acct := registeredAccount(store)
	svc, _ := planSvc(store, pcChangeAt)
	appID := registerOnPlan(t, svc, user, usage.PlanPro, 0)
	armCreationCharge(store, appID)

	// No usable card → Free is refused (Free is cheaper to start, not
	// card-less).
	store.cardCount[acct] = 0
	_, err := svc.SetAppPlan(context.Background(), cycle.SetAppPlanRequest{AppID: appID, Plan: "free"})
	requirePlanErrCode(t, err, billing.CodePaymentRequired)
	require.Equal(t, usage.PlanPro, store.apps[appID].Plan)
	delete(store.cardCount, acct)

	// The org cap: 1 Free app per org, keyed by owner_org_id, not the payer.
	org := uuid.New()
	first, second := uuid.New(), uuid.New()
	store.apps[first] = cycle.AppMirror{AppID: first, AccountID: acct, OwnerOrgID: org, CreatedAt: pcCreated, Plan: usage.PlanFree, ProrationInvoiceID: "in_1"}
	store.apps[second] = cycle.AppMirror{AppID: second, AccountID: acct, OwnerOrgID: org, CreatedAt: pcCreated, Plan: usage.PlanPro, ProrationInvoiceID: "in_2"}
	_, err = svc.SetAppPlan(context.Background(), cycle.SetAppPlanRequest{AppID: second, Plan: "free"})
	requirePlanErrCode(t, err, billing.CodePlanLimit)
	require.Equal(t, usage.PlanPro, store.apps[second].Plan)
	// The org's Free apps do not count against the personal cap and vice
	// versa: the user's own app can still go Free.
	resp, err := svc.SetAppPlan(context.Background(), cycle.SetAppPlanRequest{AppID: appID, Plan: "free"})
	require.NoError(t, err)
	require.NotNil(t, resp.Change)
	require.Equal(t, cycle.PlanChangeDowngrade, resp.Change.Kind, "pro → free is a downgrade, scheduled for the boundary")
}

// --- Upgrades ----------------------------------------------------------------

// The owner's example: Free → Pro with half the period left is $10.00, not
// $20.00, charged at once, sealed as one intent on the card.
func TestSetAppPlan_UpgradeChargesTheDifferenceProratedAtOnce(t *testing.T) {
	store := newFakeStore()
	user, acct := registeredAccount(store)
	svc, p := planSvc(store, pcChangeAt)
	appID := registerOnPlan(t, svc, user, usage.PlanFree, 0)
	armCreationCharge(store, appID)

	resp, err := svc.SetAppPlan(context.Background(), cycle.SetAppPlanRequest{AppID: appID, Plan: "pro"})
	require.NoError(t, err)
	require.Equal(t, usage.PlanPro, store.apps[appID].Plan, "an upgrade takes effect at once")
	require.Equal(t, usage.TermsFor(usage.PlanPro), resp.Terms)

	change := onlyPlanChange(t, store, appID)
	require.Equal(t, cycle.PlanChangeUpgrade, change.Kind)
	require.Equal(t, cycle.PlanChangeSettled, change.Status)
	require.False(t, change.FoldedIntoCreation)
	require.EqualValues(t, 10_000_000, change.AmountMicros, "(20 − 0) × 15/30")
	require.Zero(t, change.WalletMicros, "a standard account has no wallet leg mid-period")
	require.EqualValues(t, 10_000_000, change.CardMicros)
	require.True(t, change.WalletDecided)
	require.Equal(t, pcPeriodSt, change.PeriodStart)
	require.Equal(t, pcPeriodEd, change.PeriodEnd)
	require.Equal(t, pcChangeAt, change.EffectiveAt)

	require.Len(t, p.charges, 1, "one intent for the delta")
	c := p.charges[0]
	require.EqualValues(t, 10_000_000, c.TotalMicros())
	require.Zero(t, c.WalletAllocationMicros)
	require.Equal(t, acct.String(), c.AccountID)
	require.Equal(t, "plan-change:"+change.ID.String(), c.Lines[0].SourceRef)
	require.Contains(t, c.Lines[0].Description, "free → pro")
	require.Contains(t, c.Lines[0].Description, "app "+appID.String(), "the line names the app by id")
	require.NotContains(t, c.Lines[0].Description, "示範應用", "never the display name: a rename must not change the digest")
	require.Equal(t, pcChangeAt, c.ExecuteNotBefore, "the window opens at the request, from the row")
	require.NotEmpty(t, change.IntentDigest())
	require.Equal(t, "intent:"+change.IntentDigest(), change.CardRef)
	require.Equal(t, change.IntentDigest(), resp.Change.IntentDigest)

	// Idempotent: the same request again is a no-op — no second row, no
	// second intent.
	resp, err = svc.SetAppPlan(context.Background(), cycle.SetAppPlanRequest{AppID: appID, Plan: "pro"})
	require.NoError(t, err)
	require.Nil(t, resp.Change)
	require.Len(t, p.charges, 1)
	require.Len(t, store.planChanges, 1)
}

// An upgrade on the last day of the period, when the prorated delta is under
// half a cent … is not reachable with whole-dollar bases (1/30 of $20 is 66.7¢),
// so pin the rounding at the smallest real remainder instead: one day left
// → 20e6 × 1/30 = 666_667 micros, sealed at 67 cents.
func TestSetAppPlan_UpgradeOnTheLastDaySealsWholeCents(t *testing.T) {
	store := newFakeStore()
	user, _ := registeredAccount(store)
	lastDay := pcPeriodEd.AddDate(0, 0, -1).Add(23 * time.Hour)
	svc, p := planSvc(store, lastDay)
	appID := registerOnPlan(t, svc, user, usage.PlanFree, 0)
	armCreationCharge(store, appID)

	_, err := svc.SetAppPlan(context.Background(), cycle.SetAppPlanRequest{AppID: appID, Plan: "pro"})
	require.NoError(t, err)
	change := onlyPlanChange(t, store, appID)
	require.EqualValues(t, 666_667, change.AmountMicros, "20e6 × 1/30, half-up")
	require.EqualValues(t, 670_000, change.CardMicros, "sealed at whole cents: 67¢")
	require.EqualValues(t, 670_000, p.charges[0].TotalMicros())
}

// Inside the creation window — the creation charge has not run — the upgrade
// charges nothing itself: the creation charge prices the window split by day.
func TestSetAppPlan_UpgradeBeforeTheCreationChargeFoldsIntoIt(t *testing.T) {
	store := newFakeStore()
	user, _ := registeredAccount(store)
	svc, p := planSvc(store, pcChangeAt)
	appID := registerOnPlan(t, svc, user, usage.PlanFree, 0)

	resp, err := svc.SetAppPlan(context.Background(), cycle.SetAppPlanRequest{AppID: appID, Plan: "pro"})
	require.NoError(t, err)
	require.Equal(t, usage.PlanPro, store.apps[appID].Plan, "the plan flips immediately")
	require.Empty(t, p.charges, "nothing is sealed by the change itself")
	change := onlyPlanChange(t, store, appID)
	require.True(t, change.FoldedIntoCreation)
	require.Equal(t, cycle.PlanChangeSettled, change.Status)
	require.Zero(t, change.AmountMicros)
	require.True(t, resp.Change.FoldedIntoCreation)

	// The creation charge, when the sweep reaches it: days 10–18 at Free ($0),
	// days 19–3 at Pro → 20e6 × 15/30 = $10.00 = 1000 cents, ONE charge.
	sweep := cycle.NewService(store, newFakeStripe()).
		WithNow(func() time.Time { return pcChangeAt.Add(24 * time.Hour) }).
		WithIntentProposer(p)
	res, err := sweep.ChargeCreationProration(context.Background(), appID)
	require.NoError(t, err)
	require.Equal(t, cycle.ProrationStatusProposed, res.Status)
	require.EqualValues(t, 1000, sealedProrationCents(t, p))
	// (The Stripe rail's proposed path freezes no display snapshot — whatever
	// settles the intent has to; the wallet rail below pins the snapshot.)

	// Control: the same app that never changed plan seals the whole window at
	// Free — nothing. The fold is what made the 1000 cents.
	store2 := newFakeStore()
	user2, _ := registeredAccount(store2)
	svc2, p2 := planSvc(store2, pcChangeAt.Add(24*time.Hour))
	app2 := registerOnPlan(t, svc2, user2, usage.PlanFree, 0)
	res, err = svc2.ChargeCreationProration(context.Background(), app2)
	require.NoError(t, err)
	require.Equal(t, cycle.ProrationStatusNoCharge, res.Status)
	require.Empty(t, p2.charges)
}

// Two folded changes chain: Free → Pro on the 19th, and since business is
// refused today, the second segment is exercised through the pure helper in
// usage (TestSegmentedProrationSplitsByDay). What this pins is the wallet
// rail pricing the SAME split — the two rails must agree to the micro.
func TestChargeCreationProration_WalletRailPricesTheFoldedSplit(t *testing.T) {
	store := newFakeStore()
	user, acct := registeredAccount(store)
	store.walletMode = cycle.CreditBillingModeCredits
	seedWalletSource(store, "grant", 50_000_000, time.Time{}, timeUTC(2026, 5, 1, 0))
	svc, _ := planSvc(store, pcChangeAt)
	svc = svc.WithCreditWallet(true)
	appID := registerOnPlan(t, svc, user, usage.PlanFree, 0)

	_, err := svc.SetAppPlan(context.Background(), cycle.SetAppPlanRequest{AppID: appID, Plan: "pro"})
	require.NoError(t, err)
	require.True(t, onlyPlanChange(t, store, appID).FoldedIntoCreation)

	sweep := cycle.NewService(store, newFakeStripe()).
		WithNow(func() time.Time { return pcChangeAt.Add(24 * time.Hour) }).
		WithCreditWallet(true)
	res, err := sweep.ChargeCreationProration(context.Background(), appID)
	require.NoError(t, err)
	require.Equal(t, cycle.ProrationStatusWalletCharged, res.Status)
	require.EqualValues(t, 10_000_000, store.creationDrawn[appID], "the wallet draws the split window's base")
	snap, ok := store.baseSnapshots[snapKey{appID, billingPeriodStart(store, acct, pcCreated)}]
	require.True(t, ok)
	require.EqualValues(t, 10_000_000, snap.snap.BaseMicros, "the display snapshot freezes the split window")
}

// Credits mode: draw what the wallet holds, seal the remainder for the card.
func TestSetAppPlan_CreditsModeDrawsTheWalletThenTheCard(t *testing.T) {
	store := newFakeStore()
	user, _ := registeredAccount(store)
	store.walletMode = cycle.CreditBillingModeCredits
	grant := seedWalletSource(store, "grant", 3_000_000, time.Time{}, timeUTC(2026, 5, 1, 0))
	svc, p := planSvc(store, pcChangeAt)
	svc = svc.WithCreditWallet(true)
	appID := registerOnPlan(t, svc, user, usage.PlanFree, 0)
	armCreationCharge(store, appID)

	_, err := svc.SetAppPlan(context.Background(), cycle.SetAppPlanRequest{AppID: appID, Plan: "pro"})
	require.NoError(t, err)
	change := onlyPlanChange(t, store, appID)
	require.Equal(t, cycle.PlanChangeSettled, change.Status)
	require.EqualValues(t, 10_000_000, change.AmountMicros)
	require.EqualValues(t, 3_000_000, change.WalletMicros, "the wallet is drawn for what it holds")
	require.EqualValues(t, 7_000_000, change.CardMicros, "the remainder goes to the card")
	require.Zero(t, store.walletSources[grant].remaining)
	require.Zero(t, store.walletUnallocated, "never an unsecured remainder for an upgrade")

	require.Len(t, p.charges, 1)
	c := p.charges[0]
	require.EqualValues(t, 10_000_000, c.TotalMicros(), "the intent states the gross")
	require.EqualValues(t, 3_000_000, c.WalletAllocationMicros, "and the draw, so the provider remainder is 7_000_000")

	// A wallet that covers the whole delta settles without a card leg.
	store2 := newFakeStore()
	user2, _ := registeredAccount(store2)
	store2.walletMode = cycle.CreditBillingModeCredits
	seedWalletSource(store2, "grant", 50_000_000, time.Time{}, timeUTC(2026, 5, 1, 0))
	svc2, p2 := planSvc(store2, pcChangeAt)
	svc2 = svc2.WithCreditWallet(true)
	app2 := registerOnPlan(t, svc2, user2, usage.PlanFree, 0)
	armCreationCharge(store2, app2)
	_, err = svc2.SetAppPlan(context.Background(), cycle.SetAppPlanRequest{AppID: app2, Plan: "pro"})
	require.NoError(t, err)
	change = onlyPlanChange(t, store2, app2)
	require.Equal(t, cycle.PlanChangeSettled, change.Status)
	require.EqualValues(t, 10_000_000, change.WalletMicros)
	require.Zero(t, change.CardMicros)
	require.Empty(t, change.CardRef)
	require.Empty(t, p2.charges, "fully wallet-settled: nothing for the card")
}

// Refused only with no usable card either — and then NOTHING is written.
func TestSetAppPlan_UpgradeRefusedOnlyWithNoCardEither(t *testing.T) {
	// Standard account, no card.
	store := newFakeStore()
	user, _ := registeredAccount(store)
	svc, p := planSvc(store, pcChangeAt)
	appID := registerOnPlan(t, svc, user, usage.PlanFree, 0)
	armCreationCharge(store, appID)
	store.hasPM = false // the card went away after creation
	_, err := svc.SetAppPlan(context.Background(), cycle.SetAppPlanRequest{AppID: appID, Plan: "pro"})
	requirePlanErrCode(t, err, billing.CodePaymentRequired)
	require.Equal(t, usage.PlanFree, store.apps[appID].Plan, "a refused upgrade moves nothing")
	require.Empty(t, store.planChanges)
	require.Empty(t, p.charges)

	// Credits account, no card, wallet SHORT → refused, nothing drawn.
	store2 := newFakeStore()
	user2, _ := registeredAccount(store2)
	store2.walletMode = cycle.CreditBillingModeCredits
	grant := seedWalletSource(store2, "grant", 3_000_000, time.Time{}, timeUTC(2026, 5, 1, 0))
	svc2, _ := planSvc(store2, pcChangeAt)
	svc2 = svc2.WithCreditWallet(true)
	app2 := registerOnPlan(t, svc2, user2, usage.PlanFree, 0)
	armCreationCharge(store2, app2)
	store2.hasPM = false
	_, err = svc2.SetAppPlan(context.Background(), cycle.SetAppPlanRequest{AppID: app2, Plan: "pro"})
	requirePlanErrCode(t, err, billing.CodePaymentRequired)
	require.Equal(t, usage.PlanFree, store2.apps[app2].Plan)
	require.Empty(t, store2.planChanges)
	require.EqualValues(t, 3_000_000, store2.walletSources[grant].remaining, "nothing drawn on a refusal")

	// Credits account, no card, wallet COVERS → allowed, wallet-settled.
	store3 := newFakeStore()
	user3, _ := registeredAccount(store3)
	store3.walletMode = cycle.CreditBillingModeCredits
	seedWalletSource(store3, "grant", 30_000_000, time.Time{}, timeUTC(2026, 5, 1, 0))
	svc3, p3 := planSvc(store3, pcChangeAt)
	svc3 = svc3.WithCreditWallet(true)
	app3 := registerOnPlan(t, svc3, user3, usage.PlanFree, 0)
	armCreationCharge(store3, app3)
	store3.hasPM = false
	_, err = svc3.SetAppPlan(context.Background(), cycle.SetAppPlanRequest{AppID: app3, Plan: "pro"})
	require.NoError(t, err)
	require.Equal(t, usage.PlanPro, store3.apps[app3].Plan)
	require.EqualValues(t, 10_000_000, onlyPlanChange(t, store3, app3).WalletMicros)
	require.Empty(t, p3.charges)
}

// H10: a prepaid account is never charged off-session by any leg.
func TestSetAppPlan_UpgradeRefusedForPrepaidCollectionMode(t *testing.T) {
	store := newFakeStore()
	user, _ := registeredAccount(store)
	store.collection = cycle.AccountCollection{Mode: cycle.BillingModePrepaid}
	svc, _ := planSvc(store, pcChangeAt)
	appID := registerOnPlan(t, svc, user, usage.PlanFree, 0)
	armCreationCharge(store, appID)

	_, err := svc.SetAppPlan(context.Background(), cycle.SetAppPlanRequest{AppID: appID, Plan: "pro"})
	requirePlanErrCode(t, err, billing.CodePaymentRequired)
	require.Equal(t, usage.PlanFree, store.apps[appID].Plan)
	require.Empty(t, store.planChanges)
}

// A crash between the row and the seal leaves a pending row and the plan in
// force; a retry — the RPC's own, or the reconciler's — finishes it with ONE
// intent, never two.
func TestSetAppPlan_RetryAndSweepFinishAPendingUpgrade(t *testing.T) {
	store := newFakeStore()
	user, _ := registeredAccount(store)
	svc, p := planSvc(store, pcChangeAt)
	appID := registerOnPlan(t, svc, user, usage.PlanFree, 0)
	armCreationCharge(store, appID)

	p.err = errors.New("intent store unavailable")
	_, err := svc.SetAppPlan(context.Background(), cycle.SetAppPlanRequest{AppID: appID, Plan: "pro"})
	requirePlanErrCode(t, err, billing.CodeInternal)
	require.Equal(t, usage.PlanPro, store.apps[appID].Plan, "the plan is in force; the row says the money is pending")
	change := onlyPlanChange(t, store, appID)
	require.Equal(t, cycle.PlanChangePending, change.Status)
	require.True(t, change.WalletDecided, "the wallet decision was taken (0) before the seal failed")
	require.Empty(t, p.charges)

	// A DIFFERENT request while the row is pending is refused; the same one
	// resumes it.
	p.err = nil
	_, err = svc.SetAppPlan(context.Background(), cycle.SetAppPlanRequest{AppID: appID, Plan: "free"})
	requirePlanErrCode(t, err, billing.CodeConflict)
	resp, err := svc.SetAppPlan(context.Background(), cycle.SetAppPlanRequest{AppID: appID, Plan: "pro"})
	require.NoError(t, err)
	require.Equal(t, cycle.PlanChangeSettled, resp.Change.Status)
	require.Len(t, p.charges, 1)
	require.Len(t, store.planChanges, 1)
	require.Equal(t, cycle.PlanChangeSettled, onlyPlanChange(t, store, appID).Status)

	// The reconciler path: a second app, the same crash, finished by the sweep.
	app2 := registerOnPlan(t, svc, user, usage.PlanFree, 0)
	armCreationCharge(store, app2)
	p.err = errors.New("intent store unavailable")
	_, err = svc.SetAppPlan(context.Background(), cycle.SetAppPlanRequest{AppID: app2, Plan: "pro"})
	requirePlanErrCode(t, err, billing.CodeInternal)
	p.err = nil
	sweep, err := svc.SweepPendingPlanChanges(context.Background(), pcChangeAt.Add(time.Hour))
	require.NoError(t, err)
	require.Equal(t, &cycle.SweepPlanChangesResult{Pending: 1, Settled: 1}, sweep)
	require.Len(t, p.charges, 2, "one intent per upgrade, across the crash")
	require.Equal(t, cycle.PlanChangeSettled, onlyPlanChange(t, store, app2).Status)

	// Nothing pending: the sweep is a no-op.
	sweep, err = svc.SweepPendingPlanChanges(context.Background(), pcChangeAt.Add(2*time.Hour))
	require.NoError(t, err)
	require.Equal(t, &cycle.SweepPlanChangesResult{}, sweep)
}

// --- Downgrades --------------------------------------------------------------

func TestSetAppPlan_DowngradeWaitsForTheBoundaryAndCanBeCancelled(t *testing.T) {
	store := newFakeStore()
	user, acct := registeredAccount(store)
	svc, p := planSvc(store, pcChangeAt)
	appID := registerOnPlan(t, svc, user, usage.PlanPro, 0)
	armCreationCharge(store, appID)

	resp, err := svc.SetAppPlan(context.Background(), cycle.SetAppPlanRequest{AppID: appID, Plan: "free"})
	require.NoError(t, err)
	require.Equal(t, usage.PlanPro, store.apps[appID].Plan, "a downgrade does not take effect yet")
	require.Equal(t, usage.TermsFor(usage.PlanPro), resp.Terms, "the terms in force are still Pro's")
	require.NotNil(t, resp.Change)
	require.Equal(t, cycle.PlanChangeScheduled, resp.Change.Status)
	require.Equal(t, pcPeriodEd, resp.Change.EffectiveAt, "effective at the period boundary")
	require.Zero(t, resp.Change.AmountMicros, "no refund, no charge")
	require.NotNil(t, resp.PendingChange)
	require.Empty(t, p.charges)

	// GetAppPlan reports it; a repeat schedules nothing new.
	got, err := svc.GetAppPlan(context.Background(), cycle.GetAppPlanRequest{AppID: appID})
	require.NoError(t, err)
	require.NotNil(t, got.PendingChange)
	require.Equal(t, resp.Change.ID, got.PendingChange.ID)
	again, err := svc.SetAppPlan(context.Background(), cycle.SetAppPlanRequest{AppID: appID, Plan: "free"})
	require.NoError(t, err)
	require.Equal(t, resp.Change.ID, again.Change.ID)
	require.Len(t, store.planChanges, 1)

	// Asking for the current plan WITHOUT naming the scheduled change is a
	// no-op (a stale retry of an earlier "pro" request must not undo a newer
	// downgrade): still scheduled. Naming it cancels it, free of charge.
	noop, err := svc.SetAppPlan(context.Background(), cycle.SetAppPlanRequest{AppID: appID, Plan: "pro"})
	require.NoError(t, err)
	require.Nil(t, noop.Change)
	require.NotNil(t, noop.PendingChange, "the downgrade survives an unnamed request for the current plan")
	require.Equal(t, cycle.PlanChangeScheduled, store.planChanges[resp.Change.ID].Status)
	back, err := svc.SetAppPlan(context.Background(), cycle.SetAppPlanRequest{AppID: appID, Plan: "pro", CancelChangeID: resp.Change.ID})
	require.NoError(t, err)
	require.Equal(t, cycle.PlanChangeCancelled, back.Change.Status)
	require.Nil(t, back.PendingChange)
	require.Empty(t, p.charges)
	require.Equal(t, usage.PlanPro, store.apps[appID].Plan)

	// Schedule again, then reach the boundary: the boundary run applies it
	// FIRST, so the advance base it bills is Free's $0, and the ledger row
	// closes as applied.
	_, err = svc.SetAppPlan(context.Background(), cycle.SetAppPlanRequest{AppID: appID, Plan: "free"})
	require.NoError(t, err)
	boundary, bp := chargeSvcProposing(store, newFakeStripe())
	sum, err := boundary.RunBillingCycle(context.Background(), acct, pcPeriodSt, pcPeriodEd, 0)
	require.NoError(t, err)
	require.Equal(t, cycle.RunStatusInvoiced, sum.Status, "nothing to bill: the app is Free from this boundary")
	require.Zero(t, sum.AdvanceBaseMicros)
	require.Empty(t, bp.groups)
	require.Equal(t, usage.PlanFree, store.apps[appID].Plan)
	var applied int
	for _, c := range store.planChanges {
		if c.Status == cycle.PlanChangeApplied {
			applied++
		}
	}
	require.Equal(t, 1, applied)
	snap, ok := store.baseSnapshots[snapKey{appID, pcPeriodEd}]
	require.True(t, ok, "an all-Free roster still freezes its $0 base for the new period")
	require.Zero(t, snap.snap.BaseMicros)

	// Control: a boundary WITHOUT the downgrade bills Pro's $20.
	store2 := newFakeStore()
	user2, acct2 := registeredAccount(store2)
	svc2, _ := planSvc(store2, pcChangeAt)
	app2 := registerOnPlan(t, svc2, user2, usage.PlanPro, 0)
	armCreationCharge(store2, app2)
	boundary2, _ := chargeSvcProposing(store2, newFakeStripe())
	sum, err = boundary2.RunBillingCycle(context.Background(), acct2, pcPeriodSt, pcPeriodEd, 0)
	require.NoError(t, err)
	require.EqualValues(t, 20_000_000, sum.AdvanceBaseMicros)
	require.Equal(t, usage.PlanPro, store2.apps[app2].Plan)
}

// --- Extra members ------------------------------------------------------------

func TestRunBillingCycle_BillsMembersAboveThePlansIncludedCount(t *testing.T) {
	store := newFakeStore()
	user, acct := registeredAccount(store)
	svc, _ := planSvc(store, pcChangeAt)
	// Pro includes 10: 12 members → 2 × $2.00 = $4.00 for the new period.
	appID := registerOnPlan(t, svc, user, usage.PlanPro, 12)
	armCreationCharge(store, appID)
	// Free includes 3: 3 members → nothing.
	free := registerOnPlan(t, svc, user, usage.PlanFree, 3)
	armCreationCharge(store, free)

	boundary, p := chargeSvcProposing(store, newFakeStripe())
	sum, err := boundary.RunBillingCycle(context.Background(), acct, pcPeriodSt, pcPeriodEd, 0)
	require.NoError(t, err)
	require.Equal(t, cycle.RunStatusProposed, sum.Status)
	require.EqualValues(t, 20_000_000, sum.AdvanceBaseMicros)
	require.EqualValues(t, 4_000_000, sum.MembersMicros)
	require.Len(t, p.groups, 1)
	var members int64
	for _, c := range p.groups[0] {
		for _, l := range c.Lines {
			if l.SourceRef == "members:closed" {
				members += l.AmountMicros
				require.Contains(t, l.Description, "closed period", "the line names the period it is for")
			}
		}
	}
	require.EqualValues(t, 4_000_000, members, "the forward intent carries the members line")
	require.EqualValues(t, 24_000_000, proposedMicros(t, p), "$20 base + $4 members")

	// SyncAppModules moves the count; a deleted app's count is frozen.
	n := 15
	resp, err := svc.SyncAppModules(context.Background(), cycle.SyncAppModulesRequest{AppID: appID, MemberCount: &n})
	require.NoError(t, err)
	require.Equal(t, 15, resp.MemberCount)
	require.Equal(t, 15, store.apps[appID].MemberCount)
	_, err = svc.SyncAppModules(context.Background(), cycle.SyncAppModulesRequest{AppID: appID, Deleted: true})
	require.NoError(t, err)
	n = 1
	resp, err = svc.SyncAppModules(context.Background(), cycle.SyncAppModulesRequest{AppID: appID, MemberCount: &n})
	require.NoError(t, err)
	require.Equal(t, 15, resp.MemberCount, "frozen once deleted")

	// Bounds.
	neg := -1
	_, err = svc.SyncAppModules(context.Background(), cycle.SyncAppModulesRequest{AppID: free, MemberCount: &neg})
	requirePlanErrCode(t, err, billing.CodeInvalidInput)
	_, err = svc.RegisterApp(context.Background(), cycle.RegisterAppRequest{OwnerUserID: user, AppID: uuid.New(), MemberCount: -1})
	requirePlanErrCode(t, err, billing.CodeInvalidInput)
}

// --- The $0 creation guard (carried from the #207 review) --------------------

func TestChargeCreationProration_NothingToBillIsTerminalOnBothRails(t *testing.T) {
	for _, credits := range []bool{false, true} {
		t.Run(map[bool]string{false: "stripe", true: "wallet"}[credits], func(t *testing.T) {
			store := newFakeStore()
			user, _ := registeredAccount(store)
			if credits {
				store.walletMode = cycle.CreditBillingModeCredits
				seedWalletSource(store, "grant", 50_000_000, time.Time{}, timeUTC(2026, 5, 1, 0))
			}
			svc, p := planSvc(store, pcChangeAt)
			if credits {
				svc = svc.WithCreditWallet(true)
			}
			appID := registerOnPlan(t, svc, user, usage.PlanFree, 0)

			before, err := store.AppsPendingProration(context.Background(), pcChangeAt)
			require.NoError(t, err)
			require.Contains(t, before, appID, "listed until the sweep judges it")

			res, err := svc.ChargeCreationProration(context.Background(), appID)
			require.NoError(t, err)
			require.Equal(t, cycle.ProrationStatusNoCharge, res.Status)
			require.Empty(t, p.charges)
			// TERMINAL on both rails, BEFORE any gate: a $0 base with no
			// co-created over-module timer can never bill, so the permanent
			// skip is armed ahead of the prepaid / no-PM gates that would
			// otherwise skip it transiently forever.
			app := store.apps[appID]
			require.True(t, app.ProrationSkipped, "a $0 window is TERMINAL: the skip marker is armed")
			require.Empty(t, app.ProrationInvoiceID)

			after, err := store.AppsPendingProration(context.Background(), pcChangeAt)
			require.NoError(t, err)
			require.NotContains(t, after, appID, "never re-swept")

			// And a second call is the terminal short-circuit.
			res, err = svc.ChargeCreationProration(context.Background(), appID)
			require.NoError(t, err)
			require.Equal(t, cycle.ProrationStatusPeriodClosed, res.Status)
			require.Empty(t, p.charges, "still nothing sealed")
		})
	}
}

// A Free app WITH a co-created over-module timer: the $0 base is TERMINAL on
// both rails before any gate — the skip marker is armed, nothing is sealed by
// the creation leg — and the marker is what hands the timer to the standalone
// overage sweep (Leg 1), which prices it on its own intent. Before round 3 the
// Stripe rail routed this shape into a freeze that refuses a $0 base and the
// wallet rail returned unarmed, so the timer was deferred forever (round-2
// review, #1 and #3).
func TestChargeCreationProration_FreeAppWithOverModulesArmsTheSkipAndLeg1BillsTheTimer(t *testing.T) {
	for _, credits := range []bool{false, true} {
		t.Run(map[bool]string{false: "stripe", true: "wallet"}[credits], func(t *testing.T) {
			store := newFakeStore()
			user, acct := registeredAccount(store)
			if credits {
				store.walletMode = cycle.CreditBillingModeCredits
				seedWalletSource(store, "grant", 50_000_000, time.Time{}, timeUTC(2026, 5, 1, 0))
			}
			svc, p := planSvc(store, pcChangeAt)
			if credits {
				svc = svc.WithCreditWallet(true)
			}
			appID := uuid.New()
			_, err := svc.RegisterApp(context.Background(), cycle.RegisterAppRequest{
				OwnerUserID: user, AppID: appID, CreatedAt: pcCreated, Plan: "free", ModuleCount: 6,
			})
			require.NoError(t, err)
			over, err := store.CoCreatedOverModuleTimers(context.Background(), acct, appID, pcCreated, usage.IncludedModules)
			require.NoError(t, err)
			require.Len(t, over, 1, "the sixth co-created module is over the pool of 5")
			timer := store.timers[over[0]]
			cand := cycle.ModuleOverageCandidate{
				ID: over[0], AccountID: acct, AppID: appID, InstalledAt: timer.installedAt,
				GraceExpiresAt: timer.graceExpiresAt, ActivatedAt: store.activation[acct],
			}

			// While the creation leg has not judged the window, Leg 1 defers.
			res1, err := svc.ChargeModuleOverage(context.Background(), cand, pcChangeAt)
			require.NoError(t, err)
			require.Equal(t, cycle.ModuleOverageDeferredToCombined, res1.Status)

			res, err := svc.ChargeCreationProration(context.Background(), appID)
			require.NoError(t, err)
			require.Equal(t, cycle.ProrationStatusNoCharge, res.Status)
			require.Empty(t, p.charges, "the creation leg seals nothing for a $0 base")
			require.True(t, store.apps[appID].ProrationSkipped, "the skip marker is armed on the "+map[bool]string{false: "stripe", true: "wallet"}[credits]+" rail")
			require.False(t, store.apps[appID].ProrationAttempted, "never frozen")
			require.Empty(t, store.apps[appID].ProrationInvoiceID)

			// Now Leg 1 owns the timer and bills it on its own.
			res1, err = svc.ChargeModuleOverage(context.Background(), cand, pcChangeAt)
			require.NoError(t, err)
			require.NotEqual(t, cycle.ModuleOverageDeferredToCombined, res1.Status, "the marker released the timer to Leg 1")
			if credits {
				require.Equal(t, cycle.ModuleOverageWalletCharged, res1.Status)
			} else {
				require.Equal(t, cycle.ModuleOverageProposed, res1.Status)
				require.Len(t, p.charges, 1, "one timer line sealed by Leg 1")
			}
			require.Positive(t, res1.ChargedCents)

			// A later upgrade is CHARGED (the marker says the window is judged),
			// never folded to $0.
			svc.WithNow(func() time.Time { return pcChangeAt.Add(48 * time.Hour) })
			resp, err := svc.SetAppPlan(context.Background(), cycle.SetAppPlanRequest{AppID: appID, Plan: "pro"})
			require.NoError(t, err)
			require.False(t, resp.Change.FoldedIntoCreation)
			require.Positive(t, resp.Change.AmountMicros)
		})
	}
}

// --- Round 2: the #208 review findings, each pinned -------------------------

// (a) The creation window is priced from the plan the app was CREATED on,
// through every effective change — never from apps.plan as it stands when a
// late sweep reaches the app. A downgrade applied at the boundary before the
// creation sweep must not reprice the Pro days at Free, nor wedge the app.
func TestChargeCreationProration_PricesTheCreatedPlanAfterABoundaryDowngrade(t *testing.T) {
	store := newFakeStore()
	user, acct := registeredAccount(store)
	svc, _ := planSvc(store, pcChangeAt)
	appID := registerOnPlan(t, svc, user, usage.PlanPro, 0)

	// Scheduled on the 19th, applied at the Jul 4 boundary, all before the
	// creation sweep ever runs.
	_, err := svc.SetAppPlan(context.Background(), cycle.SetAppPlanRequest{AppID: appID, Plan: "free"})
	require.NoError(t, err)
	applied, cancelled, err := svc.ApplyDuePlanChanges(context.Background(), pcPeriodEd)
	require.NoError(t, err)
	require.Equal(t, 1, applied)
	require.Zero(t, cancelled)
	require.Equal(t, usage.PlanFree, store.apps[appID].Plan)
	require.Equal(t, usage.PlanPro, store.apps[appID].CreatedPlan, "the created plan never moves")

	// The late sweep (Jul 5): days 10–3 of the creation period at PRO —
	// 20e6 × 24/30 = $16.00 — then nothing from the boundary on.
	late := cycle.NewService(store, newFakeStripe()).
		WithNow(func() time.Time { return pcPeriodEd.Add(24 * time.Hour) })
	p := &capturingProposer{}
	late = late.WithIntentProposer(p)
	res, err := late.ChargeCreationProration(context.Background(), appID)
	require.NoError(t, err)
	require.Equal(t, cycle.ProrationStatusProposed, res.Status, "priced from the created plan: no $0, no wedge")
	require.EqualValues(t, 1600, sealedProrationCents(t, p))
	require.EqualValues(t, 1600, sealedProrationCents(t, p))
	_ = acct
}

// (b) A downgrade lands at its boundary even on an account the charge phase
// never reaches: the driver's global apply moves it, no boundary run needed.
func TestApplyDuePlanChanges_ReachesAccountsWithNoBoundaryRun(t *testing.T) {
	store := newFakeStore()
	user, _ := registeredAccount(store)
	svc, _ := planSvc(store, pcChangeAt)
	appID := registerOnPlan(t, svc, user, usage.PlanPro, 0) // still in its creation window: no advance run
	_, err := svc.SetAppPlan(context.Background(), cycle.SetAppPlanRequest{AppID: appID, Plan: "free"})
	require.NoError(t, err)

	applied, _, err := svc.ApplyDuePlanChanges(context.Background(), pcPeriodEd.Add(-time.Second))
	require.NoError(t, err)
	require.Zero(t, applied, "not due yet")
	applied, _, err = svc.ApplyDuePlanChanges(context.Background(), pcPeriodEd)
	require.NoError(t, err)
	require.Equal(t, 1, applied)
	require.Equal(t, usage.PlanFree, store.apps[appID].Plan)
	applied, _, err = svc.ApplyDuePlanChanges(context.Background(), pcPeriodEd)
	require.NoError(t, err)
	require.Zero(t, applied, "applied once")
}

// (c) The Free cap counts apps already Free AND downgrades scheduled to Free,
// at open and at apply; RegisterApp on Free counts under the same rule.
func TestFreeCap_CountsScheduledDowngradesAndRechecksAtApply(t *testing.T) {
	store := newFakeStore()
	user, _ := registeredAccount(store)
	svc, _ := planSvc(store, pcChangeAt)
	free1 := registerOnPlan(t, svc, user, usage.PlanFree, 0)
	free2 := registerOnPlan(t, svc, user, usage.PlanFree, 0)
	pro1 := registerOnPlan(t, svc, user, usage.PlanPro, 0)
	pro2 := registerOnPlan(t, svc, user, usage.PlanPro, 0)
	for _, id := range []uuid.UUID{free1, free2, pro1, pro2} {
		armCreationCharge(store, id)
	}

	// 2 Free + this downgrade = 3: allowed.
	_, err := svc.SetAppPlan(context.Background(), cycle.SetAppPlanRequest{AppID: pro1, Plan: "free"})
	require.NoError(t, err)
	// 2 Free + 1 scheduled = the cap: a second downgrade AND a new Free app
	// are both refused, though only two apps are Free right now.
	_, err = svc.SetAppPlan(context.Background(), cycle.SetAppPlanRequest{AppID: pro2, Plan: "free"})
	requirePlanErrCode(t, err, billing.CodePlanLimit)
	_, err = svc.RegisterApp(context.Background(), cycle.RegisterAppRequest{OwnerUserID: user, AppID: uuid.New(), Plan: "free"})
	requirePlanErrCode(t, err, billing.CodePlanLimit)

	// At apply: a slot taken meanwhile (a transfer, a race) cancels the due
	// downgrade instead of breaching the cap.
	slipped := store.apps[pro2]
	slipped.Plan = usage.PlanFree
	store.apps[pro2] = slipped
	applied, cancelled, err := svc.ApplyDuePlanChanges(context.Background(), pcPeriodEd)
	require.NoError(t, err)
	require.Zero(t, applied)
	require.Equal(t, 1, cancelled)
	require.Equal(t, usage.PlanPro, store.apps[pro1].Plan, "not moved: the cap was full at the boundary")
	require.Equal(t, cycle.PlanChangeCancelled, onlyPlanChange(t, store, pro1).Status)
}

// The org first-Free-app path through RegisterApp: cap 1 per org, keyed by
// the org, counted under the org's lock.
func TestRegisterApp_OrgFreeCapIsOnePerOrg(t *testing.T) {
	store := newFakeStore()
	org, acct := uuid.New(), uuid.New()
	store.accountsByOrg[org] = acct
	store.orgDesignations[org] = cycle.OrgDesignation{OrgID: org, Funding: cycle.OrgFundingOrg}
	store.activation[acct] = time.Date(2026, 5, 4, 9, 0, 0, 0, time.UTC)
	store.cardCount[acct] = 1
	svc, _ := planSvc(store, pcChangeAt)

	first := uuid.New()
	_, err := svc.RegisterApp(context.Background(), cycle.RegisterAppRequest{OwnerOrgID: org, AppID: first, Plan: "free", CreatedAt: pcCreated})
	require.NoError(t, err)
	require.Equal(t, usage.PlanFree, store.apps[first].Plan)
	require.Equal(t, org, store.apps[first].OwnerOrgID)
	_, err = svc.RegisterApp(context.Background(), cycle.RegisterAppRequest{OwnerOrgID: org, AppID: uuid.New(), Plan: "free", CreatedAt: pcCreated})
	requirePlanErrCode(t, err, billing.CodePlanLimit)
	// A second org is its own cap.
	org2, acct2 := uuid.New(), uuid.New()
	store.accountsByOrg[org2] = acct2
	store.orgDesignations[org2] = cycle.OrgDesignation{OrgID: org2, Funding: cycle.OrgFundingOrg}
	store.activation[acct2] = store.activation[acct]
	store.cardCount[acct2] = 1
	_, err = svc.RegisterApp(context.Background(), cycle.RegisterAppRequest{OwnerOrgID: org2, AppID: uuid.New(), Plan: "free", CreatedAt: pcCreated})
	require.NoError(t, err)
}

// (d) The fold is decided under the lock, from the row: when the caller's
// unlocked read expected a fold but the locked row says the creation charge
// is armed — and the caller's gates did not pass for a charge — nothing is
// written. And H10 runs on every resume: a pending row on an account that
// went prepaid stays pending, skipped by the reconciler, until it relaxes.
func TestSetAppPlan_FoldDecidedUnderTheLockAndH10OnResume(t *testing.T) {
	store := newFakeStore()
	user, _ := registeredAccount(store)
	svc, p := planSvc(store, pcChangeAt)
	appID := registerOnPlan(t, svc, user, usage.PlanFree, 0)                                   // unbilled creation: the caller expects a fold
	store.hasPM = false                                                                        // and could not charge
	store.beforePlanChangeOpen = func(f *fakeStore, id uuid.UUID) { armCreationCharge(f, id) } // but the sweep armed the guard first

	_, err := svc.SetAppPlan(context.Background(), cycle.SetAppPlanRequest{AppID: appID, Plan: "pro"})
	requirePlanErrCode(t, err, billing.CodePaymentRequired)
	require.Equal(t, usage.PlanFree, store.apps[appID].Plan, "refused traceless: no flip")
	require.Empty(t, store.planChanges)
	require.Empty(t, p.charges)

	// H10 on resume.
	store.hasPM = true
	p.err = errors.New("intent store unavailable")
	_, err = svc.SetAppPlan(context.Background(), cycle.SetAppPlanRequest{AppID: appID, Plan: "pro"})
	requirePlanErrCode(t, err, billing.CodeInternal)
	require.Equal(t, cycle.PlanChangePending, onlyPlanChange(t, store, appID).Status)
	p.err = nil
	store.collection = cycle.AccountCollection{Mode: cycle.BillingModePrepaid}
	sweep, err := svc.SweepPendingPlanChanges(context.Background(), pcChangeAt.Add(time.Hour))
	require.NoError(t, err)
	require.Equal(t, &cycle.SweepPlanChangesResult{Pending: 1, Skipped: 1}, sweep, "prepaid: skipped, not sealed")
	require.Empty(t, p.charges)
	store.collection = cycle.AccountCollection{Mode: cycle.BillingModeArrears}
	sweep, err = svc.SweepPendingPlanChanges(context.Background(), pcChangeAt.Add(2*time.Hour))
	require.NoError(t, err)
	require.Equal(t, &cycle.SweepPlanChangesResult{Pending: 1, Settled: 1}, sweep)
	require.Len(t, p.charges, 1)
}

// (f)/(h1) The wallet decision is inside the open: a card-remainder failure
// AFTER the wallet draw leaves a pending row whose resume seals ONE intent
// with the SAME allocation; and the draw never exceeds the posted balance.
func TestSetAppPlan_CardFailureAfterTheWalletDrawResumesWithTheSameAllocation(t *testing.T) {
	store := newFakeStore()
	user, _ := registeredAccount(store)
	store.walletMode = cycle.CreditBillingModeCredits
	seedWalletSource(store, "grant", 3_000_000, time.Time{}, timeUTC(2026, 5, 1, 0))
	svc, p := planSvc(store, pcChangeAt)
	svc = svc.WithCreditWallet(true)
	appID := registerOnPlan(t, svc, user, usage.PlanFree, 0)
	armCreationCharge(store, appID)

	p.err = errors.New("intent store unavailable")
	_, err := svc.SetAppPlan(context.Background(), cycle.SetAppPlanRequest{AppID: appID, Plan: "pro"})
	requirePlanErrCode(t, err, billing.CodeInternal)
	change := onlyPlanChange(t, store, appID)
	require.Equal(t, cycle.PlanChangePending, change.Status)
	require.EqualValues(t, 3_000_000, change.WalletMicros, "the draw committed with the open")
	require.True(t, change.WalletDecided)

	p.err = nil
	resp, err := svc.SetAppPlan(context.Background(), cycle.SetAppPlanRequest{AppID: appID, Plan: "pro"})
	require.NoError(t, err)
	require.Equal(t, cycle.PlanChangeSettled, resp.Change.Status)
	require.Len(t, p.charges, 1)
	require.EqualValues(t, 3_000_000, p.charges[0].WalletAllocationMicros, "the same allocation, not a second draw")
	require.EqualValues(t, 10_000_000, p.charges[0].TotalMicros())
	require.EqualValues(t, 3_000_000, store.planChangeDraws[change.ID], "drawn once")

	// (h1) A posted balance below the lots (a settled negative adjustment)
	// caps the draw: 5e6 in lots, balance 2e6 → 2e6 drawn.
	store2 := newFakeStore()
	user2, _ := registeredAccount(store2)
	store2.walletMode = cycle.CreditBillingModeCredits
	seedWalletSource(store2, "grant", 5_000_000, time.Time{}, timeUTC(2026, 5, 1, 0))
	store2.walletNegativeAdjust = 3_000_000
	svc2, p2 := planSvc(store2, pcChangeAt)
	svc2 = svc2.WithCreditWallet(true)
	app2 := registerOnPlan(t, svc2, user2, usage.PlanFree, 0)
	armCreationCharge(store2, app2)
	_, err = svc2.SetAppPlan(context.Background(), cycle.SetAppPlanRequest{AppID: app2, Plan: "pro"})
	require.NoError(t, err)
	require.EqualValues(t, 2_000_000, onlyPlanChange(t, store2, app2).WalletMicros, "capped at the posted balance")
	require.EqualValues(t, 2_000_000, p2.charges[0].WalletAllocationMicros)
}

// (g) The digest is rename-stable: the line names the app by id, so a rename
// between a crashed seal and its retry seals the same document.
func TestSetAppPlan_RenameBetweenCrashAndRetryKeepsTheDigest(t *testing.T) {
	store := newFakeStore()
	user, _ := registeredAccount(store)
	svc, p := planSvc(store, pcChangeAt)
	appID := registerOnPlan(t, svc, user, usage.PlanFree, 0)
	armCreationCharge(store, appID)

	p.err = errors.New("intent store unavailable")
	_, err := svc.SetAppPlan(context.Background(), cycle.SetAppPlanRequest{AppID: appID, Plan: "pro"})
	requirePlanErrCode(t, err, billing.CodeInternal)
	name := "改名後的應用"
	_, err = svc.SyncAppModules(context.Background(), cycle.SyncAppModulesRequest{AppID: appID, Name: &name})
	require.NoError(t, err)
	p.err = nil
	resp, err := svc.SetAppPlan(context.Background(), cycle.SetAppPlanRequest{AppID: appID, Plan: "pro"})
	require.NoError(t, err)
	require.NotContains(t, p.charges[0].Lines[0].Description, name)
	require.Contains(t, p.charges[0].Lines[0].Description, appID.String())

	// The same change on an identically-shaped app with another name seals
	// the same description — the name is not in the document.
	store2 := newFakeStore()
	user2, _ := registeredAccount(store2)
	svc2, p2 := planSvc(store2, pcChangeAt)
	app2 := uuid.New()
	_, err = svc2.RegisterApp(context.Background(), cycle.RegisterAppRequest{OwnerUserID: user2, AppID: app2, CreatedAt: pcCreated, Plan: "free", Name: "別的名字"})
	require.NoError(t, err)
	armCreationCharge(store2, app2)
	_, err = svc2.SetAppPlan(context.Background(), cycle.SetAppPlanRequest{AppID: app2, Plan: "pro"})
	require.NoError(t, err)
	require.Equal(t,
		strings.ReplaceAll(p.charges[0].Lines[0].Description, appID.String(), "<id>"),
		strings.ReplaceAll(p2.charges[0].Lines[0].Description, app2.String(), "<id>"))
	_ = resp
}

// (h2) A pending upgrade sealed after its request-anchored window would have
// closed opens its window at the first seal instant — stored on the row, so
// a retry seals the same digest — never a dead document.
func TestSetAppPlan_LateSealAnchorsTheWindowOnceAtTheSealInstant(t *testing.T) {
	store := newFakeStore()
	user, _ := registeredAccount(store)
	svc, p := planSvc(store, pcChangeAt)
	appID := registerOnPlan(t, svc, user, usage.PlanFree, 0)
	armCreationCharge(store, appID)
	p.err = errors.New("intent store unavailable")
	_, err := svc.SetAppPlan(context.Background(), cycle.SetAppPlanRequest{AppID: appID, Plan: "pro"})
	requirePlanErrCode(t, err, billing.CodeInternal)

	// 40 days later the reconciler seals it.
	late := pcChangeAt.Add(40 * 24 * time.Hour)
	p.err = nil
	lateSvc := cycle.NewService(store, newFakeStripe()).WithNow(func() time.Time { return late }).WithIntentProposer(p)
	sweep, err := lateSvc.SweepPendingPlanChanges(context.Background(), late)
	require.NoError(t, err)
	require.Equal(t, 1, sweep.Settled)
	require.Equal(t, late, p.charges[0].ExecuteNotBefore, "the window opens at the seal, not 40 days ago")
	require.True(t, p.charges[0].ExecuteNotAfter.After(late))
	require.Equal(t, late, onlyPlanChange(t, store, appID).CardWindowStart, "stored on the row")

	// A prompt seal keeps the request instant as the anchor.
	store2 := newFakeStore()
	user2, _ := registeredAccount(store2)
	svc2, p2 := planSvc(store2, pcChangeAt)
	app2 := registerOnPlan(t, svc2, user2, usage.PlanFree, 0)
	armCreationCharge(store2, app2)
	_, err = svc2.SetAppPlan(context.Background(), cycle.SetAppPlanRequest{AppID: app2, Plan: "pro"})
	require.NoError(t, err)
	require.Equal(t, pcChangeAt, p2.charges[0].ExecuteNotBefore)
	require.Equal(t, pcChangeAt, onlyPlanChange(t, store2, app2).CardWindowStart)
}

// Members bill on the HIGH-WATER MARK of the period (owner 2026-09-13):
// 12 → 15 → 11 inside the period on Pro (10 included) is 5 extras = $10.00;
// a rise after the boundary bills nothing for the closed period.
func TestRunBillingCycle_BillsMembersOnThePeriodsHighWaterMark(t *testing.T) {
	store := newFakeStore()
	user, acct := registeredAccount(store)
	svc, _ := planSvc(store, pcChangeAt)
	appID := registerOnPlan(t, svc, user, usage.PlanPro, 12)
	armCreationCharge(store, appID)
	set := func(at time.Time, n int) {
		s := cycle.NewService(store, newFakeStripe()).WithNow(func() time.Time { return at })
		_, err := s.SyncAppModules(context.Background(), cycle.SyncAppModulesRequest{AppID: appID, MemberCount: &n})
		require.NoError(t, err)
	}
	set(time.Date(2026, 6, 20, 0, 0, 0, 0, time.UTC), 15)
	set(time.Date(2026, 6, 25, 0, 0, 0, 0, time.UTC), 11)
	set(time.Date(2026, 7, 5, 0, 0, 0, 0, time.UTC), 20) // after the boundary: next period's mark
	require.Equal(t, 20, store.apps[appID].MemberCount)

	boundary, _ := chargeSvcProposing(store, newFakeStripe())
	sum, err := boundary.RunBillingCycle(context.Background(), acct, pcPeriodSt, pcPeriodEd, 0)
	require.NoError(t, err)
	require.EqualValues(t, 10_000_000, sum.MembersMicros, "(15 − 10) × $2, the mark inside the period")

	// The count in force when the period opened counts even with no change
	// inside it.
	store2 := newFakeStore()
	user2, acct2 := registeredAccount(store2)
	svc2, _ := planSvc(store2, pcChangeAt)
	app2 := registerOnPlan(t, svc2, user2, usage.PlanPro, 13)
	armCreationCharge(store2, app2)
	boundary2, _ := chargeSvcProposing(store2, newFakeStripe())
	sum, err = boundary2.RunBillingCycle(context.Background(), acct2, pcPeriodSt, pcPeriodEd, 0)
	require.NoError(t, err)
	require.EqualValues(t, 6_000_000, sum.MembersMicros, "(13 − 10) × $2 from the count in force")
}

// M15: the store refuses a short wallet with no card remainder, writing
// nothing; with a card it draws what it can.
func TestOpenPlanChange_ShortWalletWithNoCardWritesNothing(t *testing.T) {
	store := newFakeStore()
	user, acct := registeredAccount(store)
	store.walletMode = cycle.CreditBillingModeCredits
	grant := seedWalletSource(store, "grant", 3_000_000, time.Time{}, timeUTC(2026, 5, 1, 0))
	svc, _ := planSvc(store, pcChangeAt)
	appID := registerOnPlan(t, svc, user, usage.PlanFree, 0)
	armCreationCharge(store, appID)
	params := cycle.OpenPlanChangeParams{
		AppID: appID, AccountID: acct, FromPlan: usage.PlanFree, ToPlan: usage.PlanPro, Kind: cycle.PlanChangeUpgrade,
		RequestedAt:   pcChangeAt,
		Folded:        &cycle.PlanChangeShape{EffectiveAt: pcChangeAt, PeriodStart: pcPeriodSt, PeriodEnd: pcPeriodEd},
		Charged:       &cycle.PlanChangeShape{EffectiveAt: pcChangeAt, PeriodStart: pcPeriodSt, PeriodEnd: pcPeriodEd, AmountMicros: 10_000_000},
		Wallet:        &cycle.PlanChangeWalletParams{Credits: true, AllowRemainder: false},
		ChargeAllowed: true,
	}
	_, outcome, err := store.OpenPlanChange(context.Background(), params)
	require.NoError(t, err)
	require.Equal(t, cycle.PlanChangeOpenWalletShort, outcome)
	require.Empty(t, store.planChanges)
	require.Equal(t, usage.PlanFree, store.apps[appID].Plan)
	require.EqualValues(t, 3_000_000, store.walletSources[grant].remaining, "nothing drawn")

	params.Wallet.AllowRemainder = true
	change, outcome, err := store.OpenPlanChange(context.Background(), params)
	require.NoError(t, err)
	require.Equal(t, cycle.PlanChangeOpened, outcome)
	require.EqualValues(t, 3_000_000, change.WalletMicros)
	require.Equal(t, cycle.PlanChangePending, change.Status)
	require.Equal(t, usage.PlanPro, store.apps[appID].Plan)
}

// M19: the freeze refuses a shape priced at a plan the locked row no longer
// carries; the attempt is not frozen at the stale price, and the next sweep
// re-derives from the ledger.
func TestChargeCreationProration_StalePlanUnderTheFreezeIsNotFrozen(t *testing.T) {
	store := newFakeStore()
	user, _ := registeredAccount(store)
	svc, p := planSvc(store, pcChangeAt.Add(24*time.Hour))
	appID := registerOnPlan(t, svc, user, usage.PlanPro, 0)
	// Between the sweep's derivation (Pro) and its freeze, a downgrade the
	// boundary applied moves the row: the shape prices a plan the locked row
	// no longer carries.
	store.beforeCombinedFreeze = func(f *fakeStore, id uuid.UUID) {
		app := f.apps[id]
		app.Plan = usage.PlanFree
		f.apps[id] = app
		f.planChanges[uuid.New()] = cycle.PlanChange{
			ID: uuid.New(), AppID: id, AccountID: app.AccountID, FromPlan: usage.PlanPro, ToPlan: usage.PlanFree,
			Kind: cycle.PlanChangeDowngrade, RequestedAt: pcChangeAt, EffectiveAt: pcPeriodEd,
			PeriodStart: pcPeriodSt, PeriodEnd: pcPeriodEd, Status: cycle.PlanChangeApplied,
		}
	}
	res, err := svc.ChargeCreationProration(context.Background(), appID)
	require.NoError(t, err)
	require.NotEqual(t, cycle.ProrationStatusProposed, res.Status, "the stale shape must not seal")
	require.Empty(t, p.charges)
	require.False(t, store.apps[appID].ProrationAttempted, "not frozen at the stale price")

	// The next sweep prices the ledger: the creation period at Pro from the
	// 10th, 20e6 × 24/30 = $16.00, and nothing after the boundary.
	res, err = svc.ChargeCreationProration(context.Background(), appID)
	require.NoError(t, err)
	require.Equal(t, cycle.ProrationStatusProposed, res.Status)
	require.Len(t, p.charges, 1)
	var base int64
	for _, l := range p.charges[0].Lines {
		if l.SourceRef == "base" {
			base = l.AmountMicros
		}
	}
	require.EqualValues(t, 16_000_000, base)
}

// --- Round 3: the second review's findings, each pinned ---------------------

// (#2) An upgrade requested in the gap between a period end and the cron
// that closes it charges (new − old) for the new period on its own row, and
// the boundary run prices its advance base at the plan IN FORCE AT THE
// BOUNDARY (from the ledger) — the old one — so the new base is never billed
// twice. The same derivation on a reclaim gives the same figure.
func TestRunBillingCycle_AdvanceBaseIsThePlanAtTheBoundaryNotTheLivePlan(t *testing.T) {
	store := newFakeStore()
	user, acct := registeredAccount(store)
	svc, p := planSvc(store, pcChangeAt)
	appID := registerOnPlan(t, svc, user, usage.PlanFree, 0)
	armCreationCharge(store, appID)

	gap := pcPeriodEd.Add(time.Hour) // Jul 4 01:00: the boundary passed, the cron has not run
	svc.WithNow(func() time.Time { return gap })
	resp, err := svc.SetAppPlan(context.Background(), cycle.SetAppPlanRequest{AppID: appID, Plan: "pro"})
	require.NoError(t, err)
	require.Equal(t, cycle.PlanChangeSettled, resp.Change.Status)
	require.EqualValues(t, 20_000_000, resp.Change.AmountMicros, "(20 − 0) × 31/31: the whole new period, on this row")
	require.Len(t, p.charges, 1)
	require.Equal(t, usage.PlanPro, store.apps[appID].Plan)

	boundary, bp := chargeSvcProposing(store, newFakeStripe())
	sum, err := boundary.RunBillingCycle(context.Background(), acct, pcPeriodSt, pcPeriodEd, 0)
	require.NoError(t, err)
	require.True(t, sum.FirstRun)
	require.Zero(t, sum.AdvanceBaseMicros, "at the boundary the app was on Free; the upgrade row priced the new base")
	var forward int64
	for _, g := range bp.groups {
		for _, c := range g {
			forward += c.TotalMicros()
		}
	}
	require.Zero(t, forward, "nothing else sealed for the new period")
	require.EqualValues(t, 20_000_000, p.charges[0].TotalMicros(), "$20 for the new period, once")
}

// (#5) The fold is decided by the WINDOW, not the markers alone: an upgrade
// requested after the window the creation charge covers is a priced delta
// even while that charge is still unbilled — and the creation charge then
// prices its own window (at Free: $0, the terminal skip), never a wedge and
// never an unpriced delta.
func TestSetAppPlan_UpgradeAfterTheCoverageWindowIsChargedWhileTheCreationChargeIsUnbilled(t *testing.T) {
	store := newFakeStore()
	user, _ := registeredAccount(store)
	late := pcPeriodEd.Add(24 * time.Hour) // Jul 5: the creation window (Jun 4–Jul 4) has closed
	svc, p := planSvc(store, late)
	appID := registerOnPlan(t, svc, user, usage.PlanFree, 0) // created Jun 10, grace to Jun 13: no straddle
	require.Empty(t, store.apps[appID].ProrationInvoiceID, "the creation charge has not run")

	resp, err := svc.SetAppPlan(context.Background(), cycle.SetAppPlanRequest{AppID: appID, Plan: "pro"})
	require.NoError(t, err)
	require.False(t, resp.Change.FoldedIntoCreation, "outside the coverage window: charged")
	nextEnd := pcPeriodEd.AddDate(0, 1, 0)
	want := usage.ProratedSegmentMicros(usage.TermsFor(usage.PlanPro).BaseFeeMicros, late, nextEnd, pcPeriodEd, nextEnd)
	require.EqualValues(t, want, resp.Change.AmountMicros, "(20 − 0) × the remaining days of the CURRENT period")
	require.Equal(t, cycle.PlanChangeSettled, resp.Change.Status)
	require.Len(t, p.charges, 1)

	// The creation sweep prices the window from the created plan: Free, $0,
	// terminal — the upgrade's days are not its to price.
	res, err := svc.ChargeCreationProration(context.Background(), appID)
	require.NoError(t, err)
	require.Equal(t, cycle.ProrationStatusNoCharge, res.Status)
	require.True(t, store.apps[appID].ProrationSkipped)
	require.Len(t, p.charges, 1, "nothing more sealed")
}

// (#4, #8, #13, #20) The members fee is the CLOSED period's, on its
// high-water count, at the plan in force DURING the period: a Free app one
// over its 3 pays $2 (a DefaultPlan mutant pays $0); a Pro app whose
// downgrade applies at this boundary is priced at Pro's 10 for the period it
// ran on Pro, not Free's 3; an app deleted inside the period still owes it.
func TestRunBillingCycle_MembersArePricedOnTheClosedPeriodsPlanAndCount(t *testing.T) {
	store := newFakeStore()
	user, acct := registeredAccount(store)
	svc, _ := planSvc(store, pcChangeAt)
	free := registerOnPlan(t, svc, user, usage.PlanFree, 4)
	armCreationCharge(store, free)
	pro := registerOnPlan(t, svc, user, usage.PlanPro, 12)
	armCreationCharge(store, pro)
	_, err := svc.SetAppPlan(context.Background(), cycle.SetAppPlanRequest{AppID: pro, Plan: "free"})
	require.NoError(t, err)
	gone := registerOnPlan(t, svc, user, usage.PlanPro, 20)
	armCreationCharge(store, gone)
	_, err = svc.SyncAppModules(context.Background(), cycle.SyncAppModulesRequest{AppID: gone, Deleted: true})
	require.NoError(t, err)

	boundary, bp := chargeSvcProposing(store, newFakeStripe())
	sum, err := boundary.RunBillingCycle(context.Background(), acct, pcPeriodSt, pcPeriodEd, 0)
	require.NoError(t, err)
	require.Equal(t, usage.PlanFree, store.apps[pro].Plan, "the downgrade applied at the boundary")
	require.EqualValues(t, 2_000_000+4_000_000+20_000_000, sum.MembersMicros,
		"Free 4 → (4−3)×$2; Pro 12 for the closed period → (12−10)×$2; deleted Pro 20 → (20−10)×$2")
	require.Zero(t, sum.AdvanceBaseMicros, "the new period: Free, Free (applied), and the deleted app is off the roster")
	var line int64
	for _, c := range bp.groups[0] {
		for _, l := range c.Lines {
			if l.SourceRef == "members:closed" {
				line += l.AmountMicros
			}
		}
	}
	require.EqualValues(t, sum.MembersMicros, line)

	// A boundary whose ONLY charge is the members fee still seals: the fee
	// is part of the boundary total the zero-skip and the wallet draw see,
	// not only of the disclosure lines.
	store2 := newFakeStore()
	user2, acct2 := registeredAccount(store2)
	svc2, _ := planSvc(store2, pcChangeAt)
	only := registerOnPlan(t, svc2, user2, usage.PlanFree, 4)
	armCreationCharge(store2, only)
	boundary2, bp2 := chargeSvcProposing(store2, newFakeStripe())
	sum, err = boundary2.RunBillingCycle(context.Background(), acct2, pcPeriodSt, pcPeriodEd, 0)
	require.NoError(t, err)
	require.Equal(t, cycle.RunStatusProposed, sum.Status, "$2 of members is a charge, not a zero boundary")
	require.EqualValues(t, 2_000_000, proposedMicros(t, bp2))
}

// (#10/#12) A deleted app's due downgrade is cancelled, never applied: the
// row's plan stays what the app was on, the ledger chain stays consistent,
// and the creation charge an app deleted AFTER its grace still owes prices
// its Pro days instead of erroring on every sweep.
func TestApplyDuePlanChanges_CancelsADeletedAppsDowngradeInsteadOfApplyingIt(t *testing.T) {
	store := newFakeStore()
	user, _ := registeredAccount(store)
	svc, _ := planSvc(store, pcChangeAt)
	appID := registerOnPlan(t, svc, user, usage.PlanPro, 0)
	_, err := svc.SetAppPlan(context.Background(), cycle.SetAppPlanRequest{AppID: appID, Plan: "free"})
	require.NoError(t, err)
	_, err = svc.SyncAppModules(context.Background(), cycle.SyncAppModulesRequest{AppID: appID, Deleted: true}) // Jun 19: after the grace (Jun 13)
	require.NoError(t, err)

	applied, cancelled, err := svc.ApplyDuePlanChanges(context.Background(), pcPeriodEd)
	require.NoError(t, err)
	require.Zero(t, applied)
	require.Equal(t, 1, cancelled)
	require.Equal(t, cycle.PlanChangeCancelled, onlyPlanChange(t, store, appID).Status)
	require.Equal(t, usage.PlanPro, store.apps[appID].Plan, "frozen with the row")

	late := cycle.NewService(store, newFakeStripe()).WithNow(func() time.Time { return pcPeriodEd.Add(24 * time.Hour) })
	p := &capturingProposer{}
	late = late.WithIntentProposer(p)
	res, err := late.ChargeCreationProration(context.Background(), appID)
	require.NoError(t, err)
	require.Equal(t, cycle.ProrationStatusProposed, res.Status, "the owed Pro days are priced; no chain error")
	require.EqualValues(t, 1600, sealedProrationCents(t, p), "20e6 × 24/30")
}

// crashedAfterSeal opens a Free → Pro upgrade whose card intent sealed but
// whose settle failed — the row is pending with no card_ref while the sealed
// document lives on. Returns the first document's digest.
func crashedAfterSeal(t *testing.T) (*fakeStore, *cycle.Service, *capturingProposer, uuid.UUID, string) {
	t.Helper()
	store := newFakeStore()
	user, _ := registeredAccount(store)
	svc, p := planSvc(store, pcChangeAt)
	appID := registerOnPlan(t, svc, user, usage.PlanFree, 0)
	armCreationCharge(store, appID)
	store.errSettlePlanCard = errors.New("connection dropped after the seal")
	_, err := svc.SetAppPlan(context.Background(), cycle.SetAppPlanRequest{AppID: appID, Plan: "pro"})
	requirePlanErrCode(t, err, billing.CodeInternal)
	change := onlyPlanChange(t, store, appID)
	require.Equal(t, cycle.PlanChangePending, change.Status)
	require.Empty(t, change.CardRef)
	require.False(t, change.CardWindowStart.IsZero(), "the window anchor was stored before the seal")
	require.Len(t, p.charges, 1)
	first, err := p.Digest(context.Background(), p.charges[0])
	require.NoError(t, err)
	store.errSettlePlanCard = nil
	return store, svc, p, appID, first
}

// (#15) Seal succeeded, settle failed: the next sweep re-proposes the SAME
// document — identical lines, allocation and window, so the same digest —
// and the row settles with it.
func TestSweepPendingPlanChanges_SealSucceededSettleFailedReplaysTheSameDigest(t *testing.T) {
	store, svc, p, appID, first := crashedAfterSeal(t)

	sweep, err := svc.SweepPendingPlanChanges(context.Background(), pcChangeAt.Add(time.Hour))
	require.NoError(t, err)
	require.Equal(t, &cycle.SweepPlanChangesResult{Pending: 1, Settled: 1}, sweep)
	require.Len(t, p.charges, 2, "proposed again …")
	require.Equal(t, p.charges[0], p.charges[1], "… as the same charge: lines, wallet allocation, execution window")
	second, err := p.Digest(context.Background(), p.charges[1])
	require.NoError(t, err)
	require.Equal(t, first, second, "one digest for one upgrade")
	change := onlyPlanChange(t, store, appID)
	require.Equal(t, cycle.PlanChangeSettled, change.Status)
	require.Equal(t, "intent:"+first, change.CardRef)
}

// (#7) A stored window that has closed is NOT re-anchored while the document
// sealed under it may have been collected: collected → the row settles with
// it, no second seal; still in flight → wait; absent or dead → re-anchor and
// seal afresh.
func TestSettlePlanChange_NeverReSealsAfterTheFirstDocumentWasCollected(t *testing.T) {
	late := pcChangeAt.AddDate(0, 0, 40) // the 30-day window anchored at the request has closed

	t.Run("collected: settle with the first document", func(t *testing.T) {
		store, svc, p, appID, first := crashedAfterSeal(t)
		p.states = map[string]string{first: "succeeded"}
		svc.WithNow(func() time.Time { return late })
		sweep, err := svc.SweepPendingPlanChanges(context.Background(), late)
		require.NoError(t, err)
		require.Equal(t, &cycle.SweepPlanChangesResult{Pending: 1, Settled: 1}, sweep)
		require.Len(t, p.charges, 1, "no second document for a collected charge")
		change := onlyPlanChange(t, store, appID)
		require.Equal(t, cycle.PlanChangeSettled, change.Status)
		require.Equal(t, "intent:"+first, change.CardRef)
	})

	t.Run("in flight: wait", func(t *testing.T) {
		store, svc, p, appID, first := crashedAfterSeal(t)
		p.states = map[string]string{first: "provider_in_progress"}
		svc.WithNow(func() time.Time { return late })
		sweep, err := svc.SweepPendingPlanChanges(context.Background(), late)
		require.NoError(t, err)
		require.Equal(t, &cycle.SweepPlanChangesResult{Pending: 1, Skipped: 1}, sweep)
		require.Len(t, p.charges, 1)
		require.Equal(t, cycle.PlanChangePending, onlyPlanChange(t, store, appID).Status)
	})

	for state, label := range map[string]string{"": "absent", "expired": "dead"} {
		t.Run(label+": re-anchor and seal afresh", func(t *testing.T) {
			store, svc, p, appID, first := crashedAfterSeal(t)
			if state != "" {
				p.states = map[string]string{first: state}
			}
			svc.WithNow(func() time.Time { return late })
			sweep, err := svc.SweepPendingPlanChanges(context.Background(), late)
			require.NoError(t, err)
			require.Equal(t, &cycle.SweepPlanChangesResult{Pending: 1, Settled: 1}, sweep)
			require.Len(t, p.charges, 2)
			require.True(t, p.charges[1].ExecuteNotBefore.Equal(late), "re-anchored at the seal instant")
			second, err := p.Digest(context.Background(), p.charges[1])
			require.NoError(t, err)
			require.NotEqual(t, first, second)
			change := onlyPlanChange(t, store, appID)
			require.Equal(t, cycle.PlanChangeSettled, change.Status)
			require.Equal(t, "intent:"+second, change.CardRef)
		})
	}
}

// (#16) The card vanished after the wallet draw: the row stays pending with
// the draw retained (never reversed), the sweep counts it skipped, the
// direct retry says PaymentRequired, and once a card is back the remainder
// seals with the SAME allocation.
func TestSetAppPlan_CardVanishedAfterTheWalletDrawStaysPendingAndKeepsTheDraw(t *testing.T) {
	store := newFakeStore()
	user, _ := registeredAccount(store)
	store.walletMode = cycle.CreditBillingModeCredits
	seedWalletSource(store, "grant", 3_000_000, time.Time{}, timeUTC(2026, 5, 1, 0))
	svc, p := planSvc(store, pcChangeAt)
	svc = svc.WithCreditWallet(true)
	appID := registerOnPlan(t, svc, user, usage.PlanFree, 0)
	armCreationCharge(store, appID)

	p.err = errors.New("intent store unavailable")
	_, err := svc.SetAppPlan(context.Background(), cycle.SetAppPlanRequest{AppID: appID, Plan: "pro"})
	requirePlanErrCode(t, err, billing.CodeInternal)
	change := onlyPlanChange(t, store, appID)
	require.EqualValues(t, 3_000_000, change.WalletMicros)
	p.err = nil
	store.hasPM = false // the card is gone before the remainder seals

	sweep, err := svc.SweepPendingPlanChanges(context.Background(), pcChangeAt.Add(time.Hour))
	require.NoError(t, err)
	require.Equal(t, &cycle.SweepPlanChangesResult{Pending: 1, Skipped: 1}, sweep)
	require.Empty(t, p.charges)
	change = onlyPlanChange(t, store, appID)
	require.Equal(t, cycle.PlanChangePending, change.Status)
	require.EqualValues(t, 3_000_000, change.WalletMicros, "the draw is retained, never reversed")
	require.EqualValues(t, 3_000_000, store.planChangeDraws[change.ID])
	_, err = svc.SetAppPlan(context.Background(), cycle.SetAppPlanRequest{AppID: appID, Plan: "pro"})
	requirePlanErrCode(t, err, billing.CodePaymentRequired)
	require.Equal(t, usage.PlanPro, store.apps[appID].Plan, "the plan is in force throughout")

	store.hasPM = true
	sweep, err = svc.SweepPendingPlanChanges(context.Background(), pcChangeAt.Add(2*time.Hour))
	require.NoError(t, err)
	require.Equal(t, &cycle.SweepPlanChangesResult{Pending: 1, Settled: 1}, sweep)
	require.Len(t, p.charges, 1)
	require.EqualValues(t, 3_000_000, p.charges[0].WalletAllocationMicros, "the same allocation")
	require.EqualValues(t, 10_000_000, p.charges[0].TotalMicros())
	require.EqualValues(t, 3_000_000, store.planChangeDraws[change.ID], "drawn once")
}

// (#22) Withdrawing a downgrade the boundary applied between the read and
// the cancel is a visible Conflict, not a stale "you are on Pro, nothing
// pending" — a retry of which would become a charged upgrade.
func TestSetAppPlan_CancelRacingTheBoundaryApplyIsAConflict(t *testing.T) {
	store := newFakeStore()
	user, _ := registeredAccount(store)
	svc, p := planSvc(store, pcChangeAt)
	appID := registerOnPlan(t, svc, user, usage.PlanPro, 0)
	armCreationCharge(store, appID)
	resp, err := svc.SetAppPlan(context.Background(), cycle.SetAppPlanRequest{AppID: appID, Plan: "free"})
	require.NoError(t, err)
	store.beforeCancelScheduled = func(f *fakeStore, id uuid.UUID) {
		f.applyDue(func(c cycle.PlanChange) bool { return c.AppID == id }, pcPeriodEd)
	}
	_, err = svc.SetAppPlan(context.Background(), cycle.SetAppPlanRequest{AppID: appID, Plan: "pro", CancelChangeID: resp.Change.ID})
	requirePlanErrCode(t, err, billing.CodeConflict)
	require.Equal(t, usage.PlanFree, store.apps[appID].Plan)
	require.Equal(t, cycle.PlanChangeApplied, onlyPlanChange(t, store, appID).Status)
	require.Empty(t, p.charges)
}
