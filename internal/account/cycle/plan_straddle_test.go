package cycle_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/mirrorstack-ai/billing-engine/internal/account/cycle"
	"github.com/mirrorstack-ai/billing-engine/internal/account/usage"
	"github.com/mirrorstack-ai/billing-engine/internal/billingperiod"
)

// 🔴 EVERY STRADDLE TEST BEFORE THIS ONE RAN AT THE DEFAULT PLAN.
//
// The boundary-straddle sites price the FULL period base — proration.go:800
// (baseMicros = planBase), :845 and :853 (the straddle snapshots) on the Stripe
// rail, and :1119/:1147/:1155 on the wallet rail — plus basefee.go's straddle
// top-up and the forecast in accountbill.go. Each now reads
// usage.TermsFor(app.Plan).BaseFeeMicros instead of the flat usage.BaseFeeMicros.
//
// At the default plan those two are the SAME NUMBER ($20), so every existing
// straddle assertion passes either way: a mutant reverting any of those sites to
// the flat constant survives the whole suite. These tests run a BUSINESS app
// ($50) through the same paths, where the two differ, so the mutant dies.
//
// Mutation-proved 2026-09-13: reverting each site to usage.BaseFeeMicros one at
// a time makes the named test below fail, and restoring it makes it pass.
//
// businessBaseMicros is the figure a reverted site cannot produce.
const businessBaseMicros = 50_000_000

// onBusinessPlan puts an already-registered app on Business.
//
// SetAppPlan deliberately refuses anything but Pro until PR-2b lifts it
// (cycle/apps.go), so the plan is set on the mirror directly — this is a test
// for the PRICING sites, not for the setter's admission rules.
func onBusinessPlan(t *testing.T, store *fakeStore, appID uuid.UUID) {
	t.Helper()
	app, ok := store.apps[appID]
	require.True(t, ok, "app must be registered before its plan is set")
	app.Plan = usage.PlanBusiness
	app.CreatedPlan = usage.PlanBusiness // as an app CREATED on Business: no ledger row to chain through
	store.apps[appID] = app
	require.EqualValues(t, businessBaseMicros, usage.TermsFor(app.Plan).BaseFeeMicros,
		"the fixture must differ from the flat base, or it cannot discriminate")
}

// A Business app created inside its 3-day grace window right before the period
// boundary: the creation period is forgiven and the STRADDLED period is billed
// at the full base. On Business that is $50, not the flat $20.
//
// Kills a revert at proration.go:800 (baseMicros = planBase) and at :845/:853
// (the straddle snapshots).
func TestCreationStraddle_StripeRail_BillsTheBusinessBase(t *testing.T) {
	store := newFakeStore()
	user, _ := registeredAccount(store) // activated 2026-05-04 → anchor day 4
	sc := newFakeStripe()
	svc, p := prorationSvc(store, sc)
	appID := uuid.New()
	// Created May 2: the creation period [Apr 4, May 4) closes at activation and
	// the grace expires May 5, inside [May 4, Jun 4) — the straddle.
	registerMirror(t, svc, user, appID, time.Date(2026, 5, 2, 0, 0, 0, 0, time.UTC), 0)
	onBusinessPlan(t, store, appID)

	resp, err := svc.ChargeCreationProration(context.Background(), appID)
	require.NoError(t, err)
	require.Equal(t, cycle.ProrationStatusProposed, resp.Status)

	// 🔴 THE DISCRIMINATING ASSERTION. $50.00, not $20.00 — a site reverted to
	// the flat usage.BaseFeeMicros seals 2000 here.
	require.EqualValues(t, 5000, sealedProrationCents(t, p),
		"the straddled period is billed at the APP'S plan base, not the flat base")
	require.Equal(t, time.Date(2026, 6, 4, 0, 0, 0, 0, time.UTC), p.charges[0].ExecuteNotBefore,
		"the seal still covers the straddled period, not the forgiven creation period")
}

// The OTHER straddle branch. proration.go has two: the creation period already
// CLOSED at activation (:845), and the creation period still open while the
// grace crosses the boundary (:853). The first fixture only reaches :845, so a
// revert of :853 survived the mutation run until this existed.
//
// Created Jun 2 against anchor day 4: its period is [May 4, Jun 4), activation
// (May 4) is before that period's end so the creation period is OPEN, and the
// grace expires Jun 5 — inside [Jun 4, Jul 4).
func TestCreationStraddle_OpenCreationPeriod_SnapshotCarriesTheBusinessBase(t *testing.T) {
	store := newFakeStore()
	user, acct := registeredAccount(store)
	store.walletMode = cycle.CreditBillingModeCredits
	seedWalletSource(store, "grant", 200_000_000, time.Time{}, timeUTC(2026, 5, 1, 0))
	sc := newFakeStripe()
	svc := appsSvc(store, sc).WithCreditWallet(true)

	created := time.Date(2026, 6, 2, 0, 0, 0, 0, time.UTC)
	appID := uuid.New()
	registerMirror(t, svc, user, appID, created, 0)
	onBusinessPlan(t, store, appID)

	_, err := svc.ChargeCreationProration(context.Background(), appID)
	require.NoError(t, err)

	// 🔴 THE PRORATED SLICE ITSELF MUST USE THE PLAN BASE. With the creation
	// period still open it is CreationChargeBaseMicros(planBase, …), not the
	// full base.
	//
	// Computed AT the Business base rather than by scaling the flat result:
	// proration ROUNDS, so 2.5× a rounded $20 slice is a micro off a rounded
	// $50 slice — which this assertion caught on its first run.
	ps, pe := billingperiod.AnchoredPeriodWindow(created.UTC(), billingperiod.AnchorDay(store.activation[acct]))
	wantDrawn := usage.CreationChargeBaseMicros(businessBaseMicros, created, ps, pe)
	require.Positive(t, wantDrawn, "the fixture must bill something, or it cannot discriminate")
	require.NotEqualValues(t, creationBaseMicros(store, acct, created), wantDrawn,
		"the Business slice must differ from the flat slice, or this cannot discriminate")
	require.EqualValues(t, wantDrawn, store.creationDrawn[appID],
		"the open-period slice must be priced at the app's plan base")

	var mine []cycle.AppBaseSnapshot
	for key, row := range store.baseSnapshots {
		if key.app == appID {
			mine = append(mine, row.snap)
		}
	}
	require.NotEmpty(t, mine, "an open-creation-period straddle must still freeze a snapshot")
	found := false
	for _, snap := range mine {
		if snap.BaseMicros == businessBaseMicros {
			found = true
		}
	}
	require.True(t, found,
		"the straddled period's snapshot must carry the plan base %d; got %+v", businessBaseMicros, mine)
}

// The same straddle on the CREDIT rail. It prices identically by contract
// (proration.go's wallet leg mirrors the Stripe callback), so it needs its own
// discriminating fixture — the two legs compute the base in different code.
//
// Kills a revert at proration.go:1119/:1147/:1155.
func TestCreationStraddle_WalletRail_DrawsTheBusinessBase(t *testing.T) {
	store := newFakeStore()
	user, _ := registeredAccount(store)
	store.walletMode = cycle.CreditBillingModeCredits
	grant := seedWalletSource(store, "grant", 200_000_000, time.Time{}, timeUTC(2026, 5, 1, 0))
	sc := newFakeStripe()
	svc := appsSvc(store, sc).WithCreditWallet(true)

	appID := uuid.New()
	registerMirror(t, svc, user, appID, time.Date(2026, 5, 2, 0, 0, 0, 0, time.UTC), 0)
	onBusinessPlan(t, store, appID)

	resp, err := svc.ChargeCreationProration(context.Background(), appID)
	require.NoError(t, err)
	require.Equal(t, cycle.ProrationStatusWalletCharged, resp.Status)

	// 🔴 $50 drawn, and the grant reduced by exactly that — a reverted site
	// draws 20_000_000.
	require.EqualValues(t, businessBaseMicros, store.creationDrawn[appID],
		"the wallet leg draws the APP'S plan base for the straddled period")
	require.EqualValues(t, 200_000_000-businessBaseMicros, store.walletSources[grant].remaining)
	require.Empty(t, sc.invoiceCalls, "credit mode never creates a Stripe invoice")

	// 🔴 AND THE DISPLAY MUST AGREE WITH THE MONEY. The console renders from
	// these rows, so a correct $50 draw beside a $20 snapshot would bill one
	// number and show another. Covers proration.go:1147/:1155.
	var mine []cycle.AppBaseSnapshot
	for key, row := range store.baseSnapshots {
		if key.app == appID {
			mine = append(mine, row.snap)
		}
	}
	require.NotEmpty(t, mine, "the wallet straddle must freeze a display snapshot")
	found := false
	for _, snap := range mine {
		if snap.BaseMicros == businessBaseMicros {
			found = true
		}
		require.NotEqualValues(t, usage.BaseFeeMicros, snap.BaseMicros,
			"a snapshot at the flat base means a pricing site was reverted")
	}
	require.True(t, found, "no snapshot carried the Business base %d; got %+v", businessBaseMicros, mine)
}
