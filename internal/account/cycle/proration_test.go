package cycle_test

// ChargeCreationProration + SweepCreationProrations (creation grace, owner spec
// 2026-07-05). Reuses the in-memory fakeStore (service_test.go) + fakeStripe
// (charge_test.go) and the appsNow / registeredAccount helpers (apps_test.go).

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/mirrorstack-ai/billing-engine/internal/account/billing"
	"github.com/mirrorstack-ai/billing-engine/internal/account/cycle"
	"github.com/mirrorstack-ai/billing-engine/internal/account/usage"
	billingstripe "github.com/mirrorstack-ai/billing-engine/internal/shared/stripe"
)

// registerMirror seeds a roster row through RegisterApp (which only mirrors — no
// charge) so the app is owned by the fully-chargeable registeredAccount.
func registerMirror(t *testing.T, svc *cycle.Service, user, appID uuid.UUID, created time.Time, moduleCount int) {
	t.Helper()
	_, err := svc.RegisterApp(context.Background(), cycle.RegisterAppRequest{
		OwnerUserID: user, AppID: appID, ModuleCount: moduleCount, CreatedAt: created,
	})
	require.NoError(t, err)
}

// prorationSvc is appsSvc with the intent proposer this leg now REQUIRES.
//
// The creation-proration leg has no direct charge path left: it derives the
// amount it always did, seals it, and stops. It returns the capturing proposer
// beside the service because the SEALED CHARGE is where the amount assertions
// live now — no invoice item carries them any more.
func prorationSvc(store *fakeStore, sc *fakeStripe) (*cycle.Service, *capturingProposer) {
	p := &capturingProposer{}
	return appsSvc(store, sc).WithIntentProposer(p), p
}

// sealedProrationCents is the whole-cent total of the ONE charge the leg sealed.
//
// Lines are sealed at cents×microsPerCent (proration_charges.go), so this is
// exactly the integer the deleted invoice would have carried — which is what
// makes every amount below a real regression test on the derivation rather
// than a restatement of whatever the new path happens to produce.
func sealedProrationCents(t *testing.T, p *capturingProposer) int64 {
	t.Helper()
	require.Len(t, p.charges, 1, "the leg did not seal exactly one charge")
	total := p.charges[0].TotalMicros()
	require.Zero(t, total%10_000, "a sealed proration must be a whole number of cents")
	return total / 10_000
}

// freezeCombinedAttemptThenCrash commits the durable arming claim (the exact
// combined ownership header + apps.proration_attempted_at) and then fails,
// leaving the row in the state a crashed attempt leaves it in.
//
// It used to crash the Stripe DRAFT call, which no longer exists. It crashes
// the PROPOSAL instead, and that is the same instant in the leg's order: the
// claim commits first, the money step (now a seal) fails after it. What comes
// back is what the next sweep will find.
func freezeCombinedAttemptThenCrash(
	t *testing.T,
	svc *cycle.Service,
	store *fakeStore,
	sc *fakeStripe,
	p *capturingProposer,
	appID uuid.UUID,
) cycle.CombinedProrationAttempt {
	t.Helper()
	p.err = errors.New("simulated crash after the durable arming claim")
	_, err := svc.ChargeCreationProration(context.Background(), appID)
	require.Error(t, err)
	p.err = nil
	attempt, ok := store.combinedProrationAttempts[appID]
	require.True(t, ok, "the exact ownership header must commit before the charge is sealed")
	require.True(t, store.apps[appID].ProrationAttempted)
	require.Empty(t, p.charges, "the crash must leave no sealed charge behind")
	sc.invoiceCalls = nil
	return cloneCombinedProrationAttempt(attempt)
}

func seedCombinedAttemptItems(
	t *testing.T,
	sc *fakeStripe,
	invoiceID string,
	attempt cycle.CombinedProrationAttempt,
	timerIDs []uuid.UUID,
) {
	t.Helper()
	period := billingstripe.LinePeriod{
		Start: attempt.Shape.CoverageStart,
		End:   attempt.Shape.CoverageEnd,
	}
	_, err := sc.CreateCombinedProrationInvoiceItem(
		context.Background(),
		"cus_test",
		invoiceID,
		attempt.Shape.BaseChargeCents,
		attempt.Shape.Currency,
		attempt.Shape.BaseDescription,
		period,
		"seed-base:"+attempt.AppID.String(),
		billingstripe.CombinedProrationItemIdentity{AppID: attempt.AppID.String()},
	)
	require.NoError(t, err)
	for _, timerID := range timerIDs {
		_, err := sc.CreateCombinedProrationInvoiceItem(
			context.Background(),
			"cus_test",
			invoiceID,
			attempt.Shape.ModuleChargeCents,
			attempt.Shape.Currency,
			attempt.Shape.ModuleDescription,
			period,
			"seed-timer:"+timerID.String(),
			billingstripe.CombinedProrationItemIdentity{
				AppID:   attempt.AppID.String(),
				TimerID: timerID.String(),
			},
		)
		require.NoError(t, err)
	}
	sc.itemCalls = nil
}

// --- (a) grace holds: an app charged before GraceDays elapse is impossible ---

func TestSweep_SkipsAppsWithinGrace(t *testing.T) {
	store := newFakeStore()
	user, _ := registeredAccount(store)
	sc := newFakeStripe()
	svc := appsSvc(store, sc)
	appID := uuid.New()
	registerMirror(t, svc, user, appID, time.Date(2026, 7, 1, 8, 0, 0, 0, time.UTC), 0)

	// Sweep only 2 days later (< GraceDays=3): the app is INSIDE the grace window
	// (created_at Jul 1 > cutoff Jul 3 − 3d = Jun 30), so it is not even a
	// candidate — no charge, nothing pending.
	res, err := svc.SweepCreationProrations(context.Background(), time.Date(2026, 7, 3, 9, 0, 0, 0, time.UTC))
	require.NoError(t, err)
	require.Equal(t, 0, res.Pending, "an app younger than GraceDays is not swept")
	require.Equal(t, 0, res.Charged)
	require.Empty(t, sc.itemCalls, "charging within grace is impossible")
	require.Empty(t, store.apps[appID].ProrationInvoiceID)
}

// --- (b) an app deleted within grace is NEVER charged, even by a later sweep --

func TestSweep_NeverChargesAppDeletedWithinGrace(t *testing.T) {
	store := newFakeStore()
	user, _ := registeredAccount(store)
	sc := newFakeStripe()
	svc := appsSvc(store, sc)
	appID := uuid.New()
	registerMirror(t, svc, user, appID, time.Date(2026, 7, 1, 8, 0, 0, 0, time.UTC), 0)

	// Deleted on day 0–1 (well within grace).
	_, err := svc.SyncAppModules(context.Background(), cycle.SyncAppModulesRequest{AppID: appID, Deleted: true})
	require.NoError(t, err)
	require.True(t, store.apps[appID].Deleted)

	// A sweep 9 days later (long past grace) must still NEVER charge it: the
	// deleted row is excluded from the work list.
	res, err := svc.SweepCreationProrations(context.Background(), time.Date(2026, 7, 10, 0, 0, 0, 0, time.UTC))
	require.NoError(t, err)
	require.Equal(t, 0, res.Pending, "a deleted app is excluded from the sweep")
	require.Empty(t, sc.itemCalls, "an app deleted within grace is never charged")
	require.Empty(t, store.apps[appID].ProrationInvoiceID)
}

func TestChargeCreationProration_LegacyAttemptMarkerWithoutHeaderFailsBeforeStripe(t *testing.T) {
	store := newFakeStore()
	user, _ := registeredAccount(store)
	sc := newFakeStripe()
	svc := appsSvc(store, sc)
	appID := uuid.New()
	registerMirror(t, svc, user, appID, time.Date(2026, 6, 19, 12, 0, 0, 0, time.UTC), 7)
	app := store.apps[appID]
	app.ProrationAttempted = true
	store.apps[appID] = app

	_, err := svc.ChargeCreationProration(context.Background(), appID)
	require.Error(t, err)
	require.ErrorIs(t, err, cycle.ErrCombinedProrationAttemptUnknown)
	require.Empty(t, sc.findByRefCalls, "unknown ownership fails before even reading Stripe recovery truth")
	require.Empty(t, sc.invoiceCalls)
	require.Empty(t, sc.itemCalls)
	require.Empty(t, sc.finalizeCalls)
}

func TestChargeCreationProration_LegacyAttemptMarkerWithSkipStillFailsClosed(t *testing.T) {
	store := newFakeStore()
	user, _ := registeredAccount(store)
	sc := newFakeStripe()
	svc := appsSvc(store, sc)
	appID := uuid.New()
	registerMirror(t, svc, user, appID, time.Date(2026, 6, 19, 12, 0, 0, 0, time.UTC), 0)
	app := store.apps[appID]
	app.ProrationAttempted = true
	app.ProrationSkipped = true
	store.apps[appID] = app

	_, err := svc.ChargeCreationProration(context.Background(), appID)
	require.ErrorIs(t, err, cycle.ErrCombinedProrationAttemptUnknown)
	require.Empty(t, sc.findByRefCalls)
	require.Empty(t, sc.invoiceCalls)
	require.Empty(t, sc.itemCalls)
	require.Empty(t, sc.finalizeCalls)
}

func TestChargeCreationProration_FrozenRecoveryBypassesLaterPeriodClosedDecision(t *testing.T) {
	store := newFakeStore()
	user, accountID := registeredAccount(store)
	sc := newFakeStripe()
	svc, p := prorationSvc(store, sc)
	appID := uuid.New()
	createdAt := time.Date(2026, 6, 19, 12, 0, 0, 0, time.UTC)
	registerMirror(t, svc, user, appID, createdAt, 0)

	attempt := freezeCombinedAttemptThenCrash(t, svc, store, sc, p, appID)
	require.Empty(t, attempt.TimerIDs)
	// An INERT draft: the prior run created it and never finalized it, so no
	// money moved (auto_advance=false). The leg proposes over it rather than
	// finalizing it — nothing here finalizes anything any more.
	sc.setFindByRef("app-proration:"+appID.String(), billingstripe.Invoice{
		ID: "in_period_closed_recovery", CustomerID: store.stripeCustomer, Status: "draft", Currency: "usd",
	})
	seedCombinedAttemptItems(t, sc, "in_period_closed_recovery", attempt, nil)

	// Simulate a later gate read deciding that this creation period is closed.
	// Once an exact header exists this cannot convert recovery into a permanent
	// skip: the frozen request is what the charge is, whatever the mutable
	// roster says afterwards.
	store.activation[accountID] = time.Date(2026, 8, 4, 9, 0, 0, 0, time.UTC)
	result, err := svc.ChargeCreationProration(context.Background(), appID)
	require.NoError(t, err)
	require.Equal(t, cycle.ProrationStatusProposed, result.Status)
	require.NotEmpty(t, result.IntentDigest)
	require.False(t, store.apps[appID].ProrationSkipped)
	require.Empty(t, sc.finalizeCalls, "the leg holds no finalize call at all")
	require.Equal(t, attempt.Shape.BaseChargeCents, sealedProrationCents(t, p),
		"the sealed amount is the FROZEN shape's, not one re-derived through the later gate")
}

// A timer uninstall/rank improvement after the attempt freezes cannot rewrite
// ownership: recovery consumes the exact child IDs and Stripe metadata truth.
func TestChargeCreationProration_SealsTheFrozenSetAfterRemoval(t *testing.T) {
	store := newFakeStore()
	user, acct := registeredAccount(store)
	sc := newFakeStripe()
	svc, p := prorationSvc(store, sc)
	appID := uuid.New()
	// Created mid-period with 7 co-created modules → 2 over. Base 15/30 days of
	// $20 = $10 (1000¢); each overage line 15/30 of the $1.00 amortized
	// per-module rate = 50¢.
	created := time.Date(2026, 6, 19, 12, 0, 0, 0, time.UTC)
	registerMirror(t, svc, user, appID, created, 7)
	_ = acct
	attempt := freezeCombinedAttemptThenCrash(t, svc, store, sc, p, appID)
	require.Len(t, attempt.TimerIDs, 2)

	// Between crash and retry one co-created over-module is uninstalled — the
	// live set shrinks to 1.
	_, err := svc.SyncAppModules(context.Background(), cycle.SyncAppModulesRequest{AppID: appID, ModuleCount: intPtr(6)})
	require.NoError(t, err)

	resp, err := svc.ChargeCreationProration(context.Background(), appID)
	require.NoError(t, err, "a shrunk live set must not rewrite frozen ownership")
	require.Equal(t, cycle.ProrationStatusProposed, resp.Status)
	require.Equal(t, "intent:"+resp.IntentDigest, store.apps[appID].ProrationInvoiceID,
		"the terminal stamp — without it the app is re-swept forever")

	// The seal carries the FROZEN two timers, not the one that is still live.
	// This is the whole point of the header: ownership is decided once, before
	// the money step, and a later roster edit cannot re-price it.
	require.Len(t, p.charges, 1)
	require.Len(t, p.charges[0].Lines, 3, "base + the TWO frozen timer lines")
	sealedRefs := map[string]bool{}
	for _, line := range p.charges[0].Lines {
		sealedRefs[line.SourceRef] = true
	}
	for _, timerID := range attempt.TimerIDs {
		require.True(t, sealedRefs["timer:"+timerID.String()],
			"frozen timer %s lost its line, so its charge cannot be walked back to it", timerID)
	}
	require.EqualValues(t, 1000+50+50, sealedProrationCents(t, p))
}

// DELETED with the path it tested: TestChargeCreationProration_
// RecoveredPartialDraftCreatesOnlyMissingFrozenItem asserted that a half-built
// DRAFT was repaired — the missing frozen line created, then the invoice
// finalized. This leg creates no invoice items and finalizes nothing, so there
// is no partial draft to repair: an inert draft moved no money, and the charge
// it was going to collect is sealed as an intent instead (see
// TestChargeCreationProration_FrozenRecoveryBypassesLaterPeriodClosedDecision,
// which keeps the recovered-draft case).

// The remaining Stripe-reading path — adopting an invoice a legacy run already
// finalized — must still refuse an invoice whose lines are not exactly the
// frozen attempt's. It mirrors what it adopts, so adopting a stranger's line
// would record a charge this app never froze.
func TestChargeCreationProration_InvalidAdoptedItemMetadataFailsClosed(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*fakeStripe, string, cycle.CombinedProrationAttempt)
	}{
		{
			name: "missing ownership metadata",
			mutate: func(sc *fakeStripe, invoiceID string, attempt cycle.CombinedProrationAttempt) {
				sc.itemsByInvoice[invoiceID] = append(sc.itemsByInvoice[invoiceID], billingstripe.InvoiceItem{
					ID:          "ii_unattributed",
					AmountCents: 1,
					Currency:    attempt.Shape.Currency,
					Description: "foreign pending item",
					Period: billingstripe.LinePeriod{
						Start: attempt.Shape.CoverageStart,
						End:   attempt.Shape.CoverageEnd,
					},
				})
			},
		},
		{
			name: "duplicate frozen identity",
			mutate: func(sc *fakeStripe, invoiceID string, attempt cycle.CombinedProrationAttempt) {
				base := sc.itemsByInvoice[invoiceID][0]
				base.ID = "ii_duplicate_base"
				sc.itemsByInvoice[invoiceID] = append(sc.itemsByInvoice[invoiceID], base)
			},
		},
		{
			name: "unfrozen timer identity",
			mutate: func(sc *fakeStripe, invoiceID string, attempt cycle.CombinedProrationAttempt) {
				sc.itemsByInvoice[invoiceID] = append(sc.itemsByInvoice[invoiceID], billingstripe.InvoiceItem{
					ID:                         "ii_foreign_timer",
					AmountCents:                attempt.Shape.ModuleChargeCents,
					Currency:                   attempt.Shape.Currency,
					Description:                attempt.Shape.ModuleDescription,
					Period:                     billingstripe.LinePeriod{Start: attempt.Shape.CoverageStart, End: attempt.Shape.CoverageEnd},
					CombinedProrationComponent: billingstripe.CombinedProrationComponentModuleOverage,
					CombinedProrationAppID:     attempt.AppID.String(),
					CombinedProrationTimerID:   uuid.NewString(),
				})
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := newFakeStore()
			user, _ := registeredAccount(store)
			sc := newFakeStripe()
			svc, p := prorationSvc(store, sc)
			appID := uuid.New()
			registerMirror(t, svc, user, appID, time.Date(2026, 6, 19, 12, 0, 0, 0, time.UTC), 7)

			attempt := freezeCombinedAttemptThenCrash(t, svc, store, sc, p, appID)
			totalCents := attempt.Shape.BaseChargeCents +
				attempt.Shape.ModuleChargeCents*int64(len(attempt.TimerIDs))
			// FINALIZED, so the money moved and the leg must adopt rather than
			// propose. That is the only branch left that reads Stripe at all.
			sc.setFindByRef("app-proration:"+appID.String(), billingstripe.Invoice{
				ID: "in_invalid_metadata", CustomerID: store.stripeCustomer, Status: "paid",
				AmountDue: totalCents, AmountPaid: totalCents, Currency: "usd",
			})
			seedCombinedAttemptItems(t, sc, "in_invalid_metadata", attempt, attempt.TimerIDs)
			tt.mutate(sc, "in_invalid_metadata", attempt)

			_, err := svc.ChargeCreationProration(context.Background(), appID)
			require.Error(t, err)
			require.Empty(t, p.charges, "an unprovable invoice must not be sealed over either")
			require.Empty(t, store.apps[appID].ProrationInvoiceID)
			require.Empty(t, store.combinedProrationAttempts[appID].ResolvedInvoiceID)
		})
	}
}

// Regression (wave 2, D4): an app created pre-activation whose creation grace
// straddles into the first post-activation period used to be PERMANENTLY
// skipped (period_closed) — forgiving the straddled, fully-chargeable period.
// D1d forgives the creation period only; the straddled period is billed in
// full, and a fully pre-activation app stays forgiven.
func TestChargeCreationProration_D1dStraddleChargesThePostActivationPeriod(t *testing.T) {
	store := newFakeStore()
	user, _ := registeredAccount(store) // activated 2026-05-04 09:00 → anchor day 4
	sc := newFakeStripe()
	svc, p := prorationSvc(store, sc)
	appID := uuid.New()
	// Created May 2 — pre-activation (creation period [Apr 4, May 4) closes at
	// activation) — grace expires May 5, inside [May 4, Jun 4).
	registerMirror(t, svc, user, appID, time.Date(2026, 5, 2, 0, 0, 0, 0, time.UTC), 0)

	resp, err := svc.ChargeCreationProration(context.Background(), appID)
	require.NoError(t, err)
	require.Equal(t, cycle.ProrationStatusProposed, resp.Status)
	require.EqualValues(t, 2000, sealedProrationCents(t, p),
		"the straddled period's FULL $20 base — the creation period is forgiven")
	// The coverage window survives the seal as the execution window's start:
	// a collection may not happen before the period it covers has ended.
	require.Equal(t, time.Date(2026, 6, 4, 0, 0, 0, 0, time.UTC), p.charges[0].ExecuteNotBefore,
		"the seal covers the straddled period, not the forgiven creation period")
}

func TestChargeCreationProration_D1dFullyPreActivationStaysSkipped(t *testing.T) {
	store := newFakeStore()
	user, _ := registeredAccount(store) // activated 2026-05-04 09:00
	sc := newFakeStripe()
	svc := appsSvc(store, sc)
	appID := uuid.New()
	// Created Apr 10 — grace expired Apr 13, entirely pre-activation.
	registerMirror(t, svc, user, appID, time.Date(2026, 4, 10, 0, 0, 0, 0, time.UTC), 0)

	resp, err := svc.ChargeCreationProration(context.Background(), appID)
	require.NoError(t, err)
	require.Equal(t, cycle.ProrationStatusPeriodClosed, resp.Status)
	require.Empty(t, sc.itemCalls, "fully pre-activation coverage stays forgiven and permanently skipped")
	require.True(t, store.apps[appID].ProrationSkipped)
}

// 🔴 THE IN-FLIGHT EXCEPTION, and the reason this leg still reads Stripe.
//
// A legacy run finalized this invoice and died before its terminal
// transaction. Stripe is collecting it, or already has. Proposing over it
// would double-count the charge — once on the customer's statement, once in
// the intent ledger — and abandoning it would leave a charge on that statement
// this database has never heard of. So it is ADOPTED: mirrored, its timers
// marked against it, the guard armed with ITS id, and not one write sent to
// the provider.
//
// Originally a regression test for review 2026-07-06 H5 (recovery past
// Stripe's ~24h idempotency-key window, by the app's ms_charge_ref anchor);
// that property is unchanged and still asserted here.
func TestChargeCreationProration_LateRetryAdoptsFoundInvoice(t *testing.T) {
	store := newFakeStore()
	user, _ := registeredAccount(store)
	sc := newFakeStripe()
	svc, p := prorationSvc(store, sc)
	appID := uuid.New()
	registerMirror(t, svc, user, appID, time.Date(2026, 6, 19, 12, 0, 0, 0, time.UTC), 7)

	// A prior attempt froze its exact app + two FIFO-over timer identities,
	// finalized the complete invoice, and crashed before the terminal DB
	// transaction.
	attempt := freezeCombinedAttemptThenCrash(t, svc, store, sc, p, appID)
	require.Len(t, attempt.TimerIDs, 2)
	totalCents := attempt.Shape.BaseChargeCents +
		attempt.Shape.ModuleChargeCents*int64(len(attempt.TimerIDs))
	sc.setFindByRef("app-proration:"+appID.String(), billingstripe.Invoice{
		ID:         "in_prior_combined",
		CustomerID: store.stripeCustomer,
		Status:     "paid",
		AmountDue:  totalCents,
		AmountPaid: totalCents,
		Currency:   "usd",
	})
	seedCombinedAttemptItems(t, sc, "in_prior_combined", attempt, attempt.TimerIDs)

	resp, err := svc.ChargeCreationProration(context.Background(), appID)
	require.NoError(t, err)
	require.Equal(t, cycle.ProrationStatusCharged, resp.Status)
	require.Equal(t, "in_prior_combined", resp.ProrationInvoiceID)
	require.Equal(t, "in_prior_combined", store.apps[appID].ProrationInvoiceID, "the guard arms with the recovered invoice")
	require.Empty(t, sc.invoiceCalls, "no second draft")
	require.Empty(t, sc.itemCalls, "no re-attached lines")
	require.Empty(t, sc.finalizeCalls, "no second finalize — the money moved once")
	require.Empty(t, p.charges,
		"the leg sealed an intent for a charge already finalized at the provider — "+
			"that double-counts it, once on the card and once in the ledger")
	for _, timerID := range attempt.TimerIDs {
		timer := store.timers[timerID]
		require.True(t, timer.graceResolved)
		require.True(t, timer.graceCharged)
		require.Equal(t, "in_prior_combined", timer.graceInvoiceID)
		require.NotEmpty(t, timer.graceInvoiceItemID)
	}
	require.Equal(t, "in_prior_combined",
		store.combinedProrationAttempts[appID].ResolvedInvoiceID)
}

// Regression (review 2026-07-06, H10): a PREPAID account is never auto-charged
// off-session — the boundary spine always gated on this, but the creation-
// proration leg bypassed it. Transient skip (guard unarmed); a relax back to
// arrears charges through the same keys.
func TestChargeCreationProration_PrepaidAccountSkippedNotCharged(t *testing.T) {
	store := newFakeStore()
	user, _ := registeredAccount(store)
	store.collection.Mode = cycle.BillingModePrepaid
	sc := newFakeStripe()
	svc, p := prorationSvc(store, sc)
	appID := uuid.New()
	registerMirror(t, svc, user, appID, time.Date(2026, 6, 19, 12, 0, 0, 0, time.UTC), 0)

	resp, err := svc.ChargeCreationProration(context.Background(), appID)
	require.NoError(t, err)
	require.Equal(t, cycle.ProrationStatusPrepaid, resp.Status)
	require.Empty(t, p.charges,
		"a prepaid account is not auto-charged off-session, and sealing an intent for one "+
			"hands the executor a charge the gate refused")
	require.Empty(t, store.apps[appID].ProrationInvoiceID, "transient skip — the guard stays unarmed")

	// Relax → the deferred creation charge fires normally.
	store.collection.Mode = cycle.BillingModeArrears
	resp, err = svc.ChargeCreationProration(context.Background(), appID)
	require.NoError(t, err)
	require.Equal(t, cycle.ProrationStatusProposed, resp.Status)
	require.NotEmpty(t, store.apps[appID].ProrationInvoiceID)
}

// --- (c) a survivor is charged EXACTLY ONCE even if the sweep runs twice ------

func TestSweep_ChargesSurvivorExactlyOnceAcrossReruns(t *testing.T) {
	store := newFakeStore()
	user, _ := registeredAccount(store)
	sc := newFakeStripe()
	svc, p := prorationSvc(store, sc)
	appID := uuid.New()
	registerMirror(t, svc, user, appID, time.Date(2026, 7, 1, 8, 0, 0, 0, time.UTC), 0)

	// First sweep past grace → seals the creation proration once.
	first, err := svc.SweepCreationProrations(context.Background(), time.Date(2026, 7, 5, 0, 0, 0, 0, time.UTC))
	require.NoError(t, err)
	require.Equal(t, 1, first.Pending)
	require.Equal(t, 1, first.Proposed)
	require.Equal(t, 0, first.Skipped, "a billed app must not be tallied as skipped")
	// 3 of 30 days of $20 ($2) + the straddled [Jul 4, Aug 4) period in full
	// ($20) — created Jul 1 08:00, so the grace crosses the Jul 4 boundary and
	// the advance leg excludes the app there (coverage contract, H2).
	require.EqualValues(t, 2200, sealedProrationCents(t, p))
	armed := store.apps[appID].ProrationInvoiceID
	require.NotEmpty(t, armed)

	// Second sweep (a re-fire the next day): the terminal stamp is set, so the
	// app is no longer pending and NO SECOND INTENT is sealed. An unstamped
	// proposal would seal a fresh one on every sweep, for one charge.
	second, err := svc.SweepCreationProrations(context.Background(), time.Date(2026, 7, 6, 0, 0, 0, 0, time.UTC))
	require.NoError(t, err)
	require.Equal(t, 0, second.Pending, "an already-billed app drops out of the work list")
	require.Equal(t, 0, second.Proposed)
	require.Len(t, p.charges, 1, "the re-fire must never bill twice")
	require.Equal(t, armed, store.apps[appID].ProrationInvoiceID)
}

// --- (d) the proration $ amount, mirror window, and snapshots -----------------

func TestChargeCreationProration_AmountMatchesLegacyProration(t *testing.T) {
	// The creation-period part is the SAME number the pre-grace RegisterApp
	// charge produced: 20e6 × 3/30 = 2_000_000 micros. Created Jul 1 08:00, the
	// grace crosses the Jul 4 boundary (coverage contract, H2), so the charge
	// ALSO covers the straddled [Jul 4, Aug 4) period in full: 22e6 micros →
	// 2200 cents, mirrored with the window [creation day, straddled period end),
	// TWO snapshots frozen — the creation period's prorated amount and the
	// straddled period's full base.
	store := newFakeStore()
	user, acct := registeredAccount(store)
	_ = acct
	sc := newFakeStripe()
	svc, p := prorationSvc(store, sc)
	appID := uuid.New()
	_, err := svc.RegisterApp(context.Background(), cycle.RegisterAppRequest{
		OwnerUserID: user, AppID: appID, ModuleCount: 0,
		CreatedAt: time.Date(2026, 7, 1, 8, 0, 0, 0, time.UTC), Name: "My App",
	})
	require.NoError(t, err)

	resp, err := svc.ChargeCreationProration(context.Background(), appID)
	require.NoError(t, err)
	require.Equal(t, cycle.ProrationStatusProposed, resp.Status)
	require.EqualValues(t, 2200, sealedProrationCents(t, p),
		"the amount is unchanged by the cutover — only who collects it is")

	require.Empty(t, sc.invoiceCalls, "the leg mints no invoice")
	require.Empty(t, sc.itemCalls)
	require.Empty(t, sc.finalizeCalls)

	// The line still names the app a human will look for on their bill.
	require.Len(t, p.charges[0].Lines, 1)
	require.Contains(t, p.charges[0].Lines[0].Description, "My App")
	require.Contains(t, p.charges[0].Lines[0].Description, appID.String())
	require.Equal(t, time.Date(2026, 8, 4, 0, 0, 0, 0, time.UTC), p.charges[0].ExecuteNotBefore,
		"coverage runs through the straddled period's end")
	require.Equal(t, "intent:"+resp.IntentDigest, store.apps[appID].ProrationInvoiceID)
}

func TestChargeCreationProration_ChargesExactlyThePreviewedAmount(t *testing.T) {
	periodStart := time.Date(2026, 7, 11, 0, 0, 0, 0, time.UTC)
	periodEnd := time.Date(2026, 8, 11, 0, 0, 0, 0, time.UTC)
	tests := []struct {
		name              string
		createdAt         time.Time
		wantPreviewMicros int64
	}{
		{
			name:              "period start",
			createdAt:         periodStart,
			wantPreviewMicros: 20_000_000,
		},
		{
			name:              "mid-period reporter case",
			createdAt:         time.Date(2026, 7, 17, 12, 34, 0, 0, time.UTC),
			wantPreviewMicros: 16_129_032,
		},
		{
			name:              "period-end eve",
			createdAt:         time.Date(2026, 8, 10, 23, 0, 0, 0, time.UTC),
			wantPreviewMicros: 20_645_161,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := newFakeStore()
			user, acct := registeredAccount(store)
			// Anchor day 11 makes every creation instant in the table belong to
			// the exact preview/sweep window [2026-07-11, 2026-08-11).
			store.activation[acct] = time.Date(2026, 5, 11, 9, 0, 0, 0, time.UTC)

			sc := newFakeStripe()
			previewMicros := usage.CreationChargeBaseMicros(tt.createdAt, periodStart, periodEnd)
			require.EqualValues(t, tt.wantPreviewMicros, previewMicros)
			// centsFromMicros is package-private; for these non-negative amounts,
			// adding half a cent before division is its exact round-half-up rule.
			previewCents := (previewMicros + 5_000) / 10_000
			sc.invoiceAmountDue = previewCents

			now := usage.GraceExpiry(tt.createdAt).Add(time.Hour)
			p := &capturingProposer{}
			svc := cycle.NewService(store, sc).
				WithNow(func() time.Time { return now }).
				WithIntentProposer(p)
			appID := uuid.New()
			registerMirror(t, svc, user, appID, tt.createdAt, 0)

			resp, err := svc.ChargeCreationProration(context.Background(), appID)
			require.NoError(t, err)
			require.Equal(t, cycle.ProrationStatusProposed, resp.Status)
			require.Equal(t, previewCents, sealedProrationCents(t, p),
				"the customer is billed the previewed amount, rounded to cents exactly "+
					"where the deleted invoice rounded it")
		})
	}
}

func TestChargeCreationProration_ChargesFlatBaseNotFoldedOverage(t *testing.T) {
	// Migration 032: module overage is NO LONGER folded into the per-app base —
	// the creation proration is the FLAT $20 base regardless of module_count (a
	// 7-module app prorates EXACTLY like a 0-module app). 15 of 30 remaining days
	// → 20e6 × 15/30 = 10e6 micros → 1000 cents (NOT the pre-032 26e6 → 1300).
	// Overage for the modules co-created with the app is a SEPARATE line, added
	// by the module-overage grace leg (see overage tests).
	store := newFakeStore()
	user, _ := registeredAccount(store)
	sc := newFakeStripe()
	svc, p := prorationSvc(store, sc)
	appID := uuid.New()
	registerMirror(t, svc, user, appID, time.Date(2026, 6, 19, 12, 0, 0, 0, time.UTC), 7)

	resp, err := svc.ChargeCreationProration(context.Background(), appID)
	require.NoError(t, err)
	require.Equal(t, cycle.ProrationStatusProposed, resp.Status)
	// The base line is the flat prorated $20. The two co-created over-modules
	// ride the SAME sealed charge as their own lines (150¢ each), exactly as
	// they rode the same invoice as separate items.
	require.Len(t, p.charges[0].Lines, 3)
	require.EqualValues(t, 1000, p.charges[0].Lines[0].AmountMicros/10_000,
		"flat base only — the module overage is its own line, never folded into the base")
}

func TestChargeCreationProration_CreatedOnBoundaryChargesFullNewPeriodBase(t *testing.T) {
	// Created exactly ON an anchor boundary (Jul 4 00:00): half-open windows put
	// it at the START of the NEW period [Jul 4, Aug 4) → the FULL base ($20 →
	// 2000 cents), snapshot keyed on Jul 4. Unchanged from the pre-grace charge.
	store := newFakeStore()
	user, _ := registeredAccount(store)
	sc := newFakeStripe()
	svc, p := prorationSvc(store, sc)
	appID := uuid.New()
	registerMirror(t, svc, user, appID, time.Date(2026, 7, 4, 0, 0, 0, 0, time.UTC), 0)

	resp, err := svc.ChargeCreationProration(context.Background(), appID)
	require.NoError(t, err)
	require.Equal(t, cycle.ProrationStatusProposed, resp.Status)
	require.EqualValues(t, 2_000, sealedProrationCents(t, p), "creation-day == period start → full base")
	require.Equal(t, time.Date(2026, 8, 4, 0, 0, 0, 0, time.UTC), p.charges[0].ExecuteNotBefore,
		"the coverage this seals is the whole [Jul 4, Aug 4) period")
}

func TestChargeCreationProration_DelayedPastPeriodEndStillCharges(t *testing.T) {
	// Delayed billing (grace point 5): the charge is anchored to the TRUE
	// created_at, NOT now, so even when grace + ordinary sweep cadence pushes the
	// charge attempt past the creation period's end, the creation-period
	// proration STILL fires — that period is billed by NO other leg (the
	// boundary advance leg only ever bills an app's SUBSEQUENT periods), so
	// charging it is correct and never double-bills. This is NOT the D1d
	// retroactive-catch-up case (see TestChargeCreationProration_
	// SkipsPermanentlyWhenActivatedAfterPeriodClosed below): the account here was
	// ALREADY activated (May 4, registeredAccount) well before the app's period
	// even opened — the period-closed check in ChargeCreationProration compares
	// against activatedAt, not "now", so a healthy already-activated account is
	// never penalized for a sweep that simply runs late.
	// App created May 20 → period [May 4, Jun 4), long closed by the sweep in
	// July: 15 of 31 days of $20 = round_half_up(9_677_419.8) → 968 cents.
	store := newFakeStore()
	user, _ := registeredAccount(store)
	sc := newFakeStripe()
	svc, p := prorationSvc(store, sc)
	appID := uuid.New()
	registerMirror(t, svc, user, appID, time.Date(2026, 5, 20, 9, 0, 0, 0, time.UTC), 0)

	res, err := svc.SweepCreationProrations(context.Background(), time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC))
	require.NoError(t, err)
	require.Equal(t, 1, res.Proposed, "the late sweep still bills the creation period")
	require.EqualValues(t, 968, sealedProrationCents(t, p))
	require.NotEmpty(t, store.apps[appID].ProrationInvoiceID)
}

// --- ChargeCreationProration: idempotency + gates ----------------------------

func TestChargeCreationProration_IdempotentGuard(t *testing.T) {
	store := newFakeStore()
	user, _ := registeredAccount(store)
	sc := newFakeStripe()
	svc, p := prorationSvc(store, sc)
	appID := uuid.New()
	registerMirror(t, svc, user, appID, time.Date(2026, 7, 1, 8, 0, 0, 0, time.UTC), 0)

	first, err := svc.ChargeCreationProration(context.Background(), appID)
	require.NoError(t, err)
	require.Equal(t, cycle.ProrationStatusProposed, first.Status)

	second, err := svc.ChargeCreationProration(context.Background(), appID)
	require.NoError(t, err)
	require.Equal(t, cycle.ProrationStatusAlreadyCharged, second.Status)
	require.Equal(t, "intent:"+first.IntentDigest, second.ProrationInvoiceID)
	require.Len(t, p.charges, 1, "the one-shot guard prevents a second charge")
}

func TestChargeCreationProration_SkipsDeleted(t *testing.T) {
	store := newFakeStore()
	user, _ := registeredAccount(store)
	sc := newFakeStripe()
	svc := appsSvc(store, sc)
	appID := uuid.New()
	registerMirror(t, svc, user, appID, time.Date(2026, 7, 1, 8, 0, 0, 0, time.UTC), 0)
	_, err := svc.SyncAppModules(context.Background(), cycle.SyncAppModulesRequest{AppID: appID, Deleted: true})
	require.NoError(t, err)

	resp, err := svc.ChargeCreationProration(context.Background(), appID)
	require.NoError(t, err)
	require.Equal(t, cycle.ProrationStatusDeleted, resp.Status)
	require.Empty(t, sc.itemCalls)
}

func TestChargeCreationProration_SkipsUnactivatedAndNoPM(t *testing.T) {
	// Unactivated account → skipped_unactivated (D1d, no retroactive catch-up).
	// Registered while funded (the create gate), then the activation is dropped
	// to model a LEGACY pre-gate roster row — the sweep must keep handling those.
	store := newFakeStore()
	user, acct := registeredAccount(store)
	sc := newFakeStripe()
	svc := appsSvc(store, sc)
	appID := uuid.New()
	registerMirror(t, svc, user, appID, time.Date(2026, 7, 1, 8, 0, 0, 0, time.UTC), 0)
	delete(store.activation, acct)

	resp, err := svc.ChargeCreationProration(context.Background(), appID)
	require.NoError(t, err)
	require.Equal(t, cycle.ProrationStatusUnactivated, resp.Status)
	require.Empty(t, sc.itemCalls)

	// Activated but no usable PM → skipped_no_pm (re-attempted next sweep).
	store.activation[acct] = time.Date(2026, 5, 4, 9, 0, 0, 0, time.UTC)
	store.hasPM = false
	resp, err = svc.ChargeCreationProration(context.Background(), appID)
	require.NoError(t, err)
	require.Equal(t, cycle.ProrationStatusNoPM, resp.Status)
	require.Empty(t, sc.itemCalls)
	require.Empty(t, store.apps[appID].ProrationInvoiceID)
}

func TestChargeCreationProration_NotFound(t *testing.T) {
	svc := appsSvc(newFakeStore(), newFakeStripe())
	resp, err := svc.ChargeCreationProration(context.Background(), uuid.New())
	require.NoError(t, err)
	require.Equal(t, cycle.ProrationStatusNotFound, resp.Status)
}

func TestChargeCreationProration_Validation(t *testing.T) {
	svc := appsSvc(newFakeStore(), newFakeStripe())
	_, err := svc.ChargeCreationProration(context.Background(), uuid.Nil)
	requireCode(t, err, billing.CodeInvalidInput)
}

func TestSweepCreationProrations_Validation(t *testing.T) {
	svc := appsSvc(newFakeStore(), newFakeStripe())
	_, err := svc.SweepCreationProrations(context.Background(), time.Time{})
	requireCode(t, err, billing.CodeInvalidInput)
}

// --- Sweep: multiple apps, mixed states --------------------------------------

func TestSweep_ChargesOnlyPastGraceLiveUnchargedApps(t *testing.T) {
	store := newFakeStore()
	user, _ := registeredAccount(store)
	sc := newFakeStripe()
	svc, _ := prorationSvc(store, sc)

	past := uuid.New()  // past grace, live, uncharged → charged
	young := uuid.New() // within grace → skipped
	gone := uuid.New()  // deleted WITHIN grace → skipped (D11: only in-grace deletes are free)
	registerMirror(t, svc, user, past, time.Date(2026, 6, 20, 8, 0, 0, 0, time.UTC), 0)
	registerMirror(t, svc, user, young, time.Date(2026, 6, 29, 8, 0, 0, 0, time.UTC), 0)
	registerMirror(t, svc, user, gone, time.Date(2026, 6, 20, 8, 0, 0, 0, time.UTC), 0)
	svcDay1 := cycle.NewService(store, sc).WithNow(func() time.Time { return time.Date(2026, 6, 21, 8, 0, 0, 0, time.UTC) })
	_, err := svcDay1.SyncAppModules(context.Background(), cycle.SyncAppModulesRequest{AppID: gone, Deleted: true})
	require.NoError(t, err)

	// Sweep as of Jun 30 → cutoff Jun 27: past (Jun 20) qualifies; young (Jun 29)
	// is within grace; gone was deleted inside its grace.
	res, err := svc.SweepCreationProrations(context.Background(), time.Date(2026, 6, 30, 0, 0, 0, 0, time.UTC))
	require.NoError(t, err)
	require.Equal(t, 1, res.Pending)
	require.Equal(t, 1, res.Proposed)
	require.NotEmpty(t, store.apps[past].ProrationInvoiceID)
	require.Empty(t, store.apps[young].ProrationInvoiceID)
	require.Empty(t, store.apps[gone].ProrationInvoiceID)
}

// Regression (wave 2, D11): an app deleted AFTER its grace elapsed SURVIVED
// the grace — the creation charge is owed (grace only delays WHEN it fires).
// Pre-fix any deleted app was skipped, which — combined with the H2 boundary
// exclusion — made "delete in the grace-elapse→sweep window" a user-timable
// ~$22 dodge (creation proration + the straddled month), repeatable per app.
func TestSweep_AppDeletedAfterGraceStillPaysCreationCharge(t *testing.T) {
	store := newFakeStore()
	user, _ := registeredAccount(store)
	sc := newFakeStripe()
	svc, p := prorationSvc(store, sc)
	appID := uuid.New()
	// Created Jul 1 08:00 → grace ends Jul 4 08:00 (straddles the Jul 4 anchor
	// boundary). Deleted Jul 4 12:00 — AFTER grace, BEFORE the daily sweep.
	registerMirror(t, svc, user, appID, time.Date(2026, 7, 1, 8, 0, 0, 0, time.UTC), 0)
	svcPostGrace := cycle.NewService(store, sc).WithNow(func() time.Time { return time.Date(2026, 7, 4, 12, 0, 0, 0, time.UTC) })
	_, err := svcPostGrace.SyncAppModules(context.Background(), cycle.SyncAppModulesRequest{AppID: appID, Deleted: true})
	require.NoError(t, err)

	res, err := svc.SweepCreationProrations(context.Background(), time.Date(2026, 7, 5, 0, 0, 0, 0, time.UTC))
	require.NoError(t, err)
	require.Equal(t, 1, res.Proposed,
		"a post-grace delete survived the grace — the creation (+straddle) charge is owed, not dodgeable")
	require.NotEmpty(t, store.apps[appID].ProrationInvoiceID)
	require.EqualValues(t, 2200, sealedProrationCents(t, p))
}

// --- FINDING 1: the creation-proration charge must price off the module count
// FROZEN at RegisterApp time, never whatever module_count SyncAppModules moves
// it to during (or after) the grace window ----------------------------------

func TestChargeCreationProration_PricesFrozenCountNotLiveCountAfterMidGraceInstall(t *testing.T) {
	// Reproduces the exact failure scenario: an app registers with 0 modules,
	// the customer installs 7 MORE modules (via SyncAppModules) DURING the
	// mandatory grace window — before the sweep ever charges — and the sweep
	// fires after grace elapses. Pre-fix, the charge priced off the module_count
	// read FRESH at sweep time (7, live) → 20e6 + 2×3e6 = 26e6 base, 3 of 30 days
	// → 2_600_000 micros: a HIGHER tier retroactively applied to days that never
	// had 7 modules installed. Fixed: the charge prices off created_module_count
	// (frozen at registration, 0) → 20e6 base, 3 of 30 days → 2_000_000 micros,
	// plus the straddled [Jul 4, Aug 4) period's full base (created Jul 1 08:00 —
	// the grace crosses the boundary) → 22e6 → 2200 cents — identical to
	// TestChargeCreationProration_AmountMatchesLegacyProration's un-synced case.
	store := newFakeStore()
	user, _ := registeredAccount(store)
	sc := newFakeStripe()
	svc, p := prorationSvc(store, sc)
	appID := uuid.New()
	registerMirror(t, svc, user, appID, time.Date(2026, 7, 1, 8, 0, 0, 0, time.UTC), 0)

	// Mid-grace install: the live count jumps to 7 BEFORE the sweep ever runs.
	_, err := svc.SyncAppModules(context.Background(), cycle.SyncAppModulesRequest{
		AppID: appID, ModuleCount: intPtr(7),
	})
	require.NoError(t, err)
	require.Equal(t, 7, store.apps[appID].ModuleCount, "the live count DID move")
	require.Equal(t, 0, store.apps[appID].CreatedModuleCount, "the frozen count must NOT move")

	resp, err := svc.ChargeCreationProration(context.Background(), appID)
	require.NoError(t, err)
	require.Equal(t, cycle.ProrationStatusProposed, resp.Status)
	require.EqualValues(t, 2200, sealedProrationCents(t, p),
		"must price off the FROZEN count (0 modules → $20 base), not the live count (7 → $26 base)")
	require.Len(t, p.charges[0].Lines, 1,
		"the mid-grace installs were not co-created, so they own no line on this charge")

	// The LIVE count survives untouched for the boundary advance leg's future
	// periods — only the historical creation-period charge is frozen.
	require.Equal(t, 7, store.apps[appID].ModuleCount)
}

func TestChargeCreationProration_FlatBaseUnaffectedByMidGraceUninstall(t *testing.T) {
	// Migration 032: the creation base is FLAT, so a mid-grace uninstall (7 → 0
	// modules) cannot move it — it prorates the flat $20 either way: 15 of 30
	// remaining days → 10e6 micros → 1000 cents. The frozen created_module_count
	// (7) is still preserved for display (the snapshot ModuleCount), it just no
	// longer drives the base amount now that overage is a separate per-module leg.
	store := newFakeStore()
	user, _ := registeredAccount(store)
	sc := newFakeStripe()
	svc, p := prorationSvc(store, sc)
	appID := uuid.New()
	registerMirror(t, svc, user, appID, time.Date(2026, 6, 19, 12, 0, 0, 0, time.UTC), 7)

	_, err := svc.SyncAppModules(context.Background(), cycle.SyncAppModulesRequest{
		AppID: appID, ModuleCount: intPtr(0),
	})
	require.NoError(t, err)
	require.Equal(t, 0, store.apps[appID].ModuleCount)
	require.Equal(t, 7, store.apps[appID].CreatedModuleCount, "the frozen count must NOT move")

	resp, err := svc.ChargeCreationProration(context.Background(), appID)
	require.NoError(t, err)
	require.Equal(t, cycle.ProrationStatusProposed, resp.Status)
	require.EqualValues(t, 1_000, p.charges[0].Lines[0].AmountMicros/10_000,
		"flat base — unaffected by the module count or its mid-grace change")
}

// --- FINDING 2: no retroactive catch-up (D1d) when an account only activates
// after the app's anchored creation period already closed -------------------

func TestChargeCreationProration_SkipsPermanentlyWhenActivatedAfterPeriodClosed(t *testing.T) {
	// Reproduces the exact failure scenario: an app is created while its account
	// is unactivated. Every sweep correctly leaves it unbilled (skipped_
	// unactivated). MONTHS later the owner finally binds a card — with anchor
	// day 1 (activated Apr 1), the app's anchored creation period is
	// [Jan 1, Feb 1), long closed by Apr 1. Pre-fix, the very next charge
	// attempt would retroactively bill that period in FULL (2000 cents on 0
	// modules, since Jan 1 == the period start): exactly the retroactive
	// catch-up D1d forbids. Fixed: the charge is PERMANENTLY skipped — no
	// Stripe call, ever, and the app never resurfaces on a later sweep (it
	// would otherwise sit pending forever, since proration_invoice_id never
	// gets set for a skipped charge).
	store := newFakeStore()
	user, acct := registeredAccount(store)
	sc := newFakeStripe()
	svc := appsSvc(store, sc)
	appID := uuid.New()
	created := time.Date(2026, 1, 1, 8, 0, 0, 0, time.UTC)
	registerMirror(t, svc, user, appID, created, 0)
	delete(store.activation, acct) // legacy pre-gate row: unactivated at creation

	// Past grace, still unactivated → correctly pending, no charge (D1d's
	// existing unactivated gate — unchanged by this fix).
	first, err := svc.ChargeCreationProration(context.Background(), appID)
	require.NoError(t, err)
	require.Equal(t, cycle.ProrationStatusUnactivated, first.Status)
	require.Empty(t, sc.itemCalls)

	// MONTHS later: the owner binds a card (anchor day 1) and a PM.
	store.activation[acct] = time.Date(2026, 4, 1, 9, 0, 0, 0, time.UTC)
	store.hasPM = true

	second, err := svc.ChargeCreationProration(context.Background(), appID)
	require.NoError(t, err)
	require.Equal(t, cycle.ProrationStatusPeriodClosed, second.Status)
	require.Empty(t, sc.itemCalls, "no Stripe call — a retroactive catch-up charge must never fire")
	require.Empty(t, store.apps[appID].ProrationInvoiceID)
	require.True(t, store.apps[appID].ProrationSkipped, "permanently marked so it is never re-evaluated")

	// A cheap re-evaluation (e.g. a retried RPC) short-circuits on the marker
	// without even re-reading account activation — still no Stripe call.
	third, err := svc.ChargeCreationProration(context.Background(), appID)
	require.NoError(t, err)
	require.Equal(t, cycle.ProrationStatusPeriodClosed, third.Status)
	require.Empty(t, sc.itemCalls)

	// A LATER sweep must never resurface it — it would otherwise sit pending
	// forever (proration_invoice_id stays NULL for a permanently-skipped app).
	res, err := svc.SweepCreationProrations(context.Background(), time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC))
	require.NoError(t, err)
	require.Equal(t, 0, res.Pending, "a permanently-skipped app never resurfaces on a later sweep")
	require.Empty(t, sc.itemCalls)
}

func TestChargeCreationProration_ActivatedBeforePeriodClosesStillCharges(t *testing.T) {
	// Guard against an over-broad fix: an account that activates BEFORE the
	// app's anchored creation period closes must charge normally — D1d only
	// blocks a retroactive catch-up when the account was unactivated for the
	// app's ENTIRE creation period. The anchor day is DERIVED from activatedAt
	// itself (billingperiod.AnchorDay), so activating the SAME calendar day the
	// app was created (the common "sign up, create an app, add a card" onboarding
	// flow) anchors the period at that same day-of-month — putting its END a
	// full month out, safely after activation.
	store := newFakeStore()
	user, acct := registeredAccount(store)
	sc := newFakeStripe()
	svc, p := prorationSvc(store, sc)
	appID := uuid.New()
	registerMirror(t, svc, user, appID, time.Date(2026, 1, 10, 6, 0, 0, 0, time.UTC), 0)
	delete(store.activation, acct) // legacy pre-gate row: unactivated at creation

	first, err := svc.ChargeCreationProration(context.Background(), appID)
	require.NoError(t, err)
	require.Equal(t, cycle.ProrationStatusUnactivated, first.Status)

	// Activates a few hours later the SAME day (anchor day 10) → period
	// [Jan 10, Feb 10) — still wide open — and binds a PM.
	store.activation[acct] = time.Date(2026, 1, 10, 9, 0, 0, 0, time.UTC)
	store.hasPM = true

	resp, err := svc.ChargeCreationProration(context.Background(), appID)
	require.NoError(t, err)
	require.Equal(t, cycle.ProrationStatusProposed, resp.Status)
	require.EqualValues(t, 2_000, sealedProrationCents(t, p), "created on/after the period start → full base")
	require.False(t, store.apps[appID].ProrationSkipped)
}

// Recovery resolves the SAME funding hop the charge itself rides: a
// sponsor-funded org app's crashed proration attempt is looked up under the
// SPONSOR's Stripe customer — the org account has none, so a recovery
// resolving the attribution account directly would fail loudly, or worse miss
// money that already moved and seal a second charge for it.
func TestChargeCreationProration_RecoveryResolvesSponsorCustomer(t *testing.T) {
	store := newFakeStore()
	sc := newFakeStripe()
	svc, p := prorationSvc(store, sc)

	org, orgAcct, sponsorAcct := uuid.New(), uuid.New(), uuid.New()
	store.accountsByOrg[org] = orgAcct
	store.orgDesignations[org] = cycle.OrgDesignation{
		OrgID: org, Funding: cycle.OrgFundingSponsor, SponsorAccountID: sponsorAcct,
	}
	// Org account: activated (sponsor designation), NO PM, NO customer of its
	// own. Sponsor: usable PM + customer — the only chargeable instrument.
	created := appsNow.AddDate(0, 0, -7) // past grace, same anchored period
	store.activation[orgAcct] = created
	store.hasPMByAccount[orgAcct] = false
	store.hasPMByAccount[sponsorAcct] = true
	store.stripeCustomerByAccount[sponsorAcct] = "cus_sponsor"

	appID := uuid.New()
	_, err := svc.RegisterApp(context.Background(), cycle.RegisterAppRequest{
		OwnerOrgID: org, AppID: appID, CreatedAt: created,
	})
	require.NoError(t, err)
	require.Equal(t, orgAcct, store.apps[appID].AccountID, "funded org app registers on the org account")

	// A prior attempt committed exact ownership and crashed before any Stripe
	// object survived. Recovery must SEARCH the sponsor's customer, then charge
	// fresh through the same funding hop.
	attempt := freezeCombinedAttemptThenCrash(t, svc, store, sc, p, appID)
	require.Empty(t, attempt.TimerIDs)

	resp, err := svc.ChargeCreationProration(context.Background(), appID)
	require.NoError(t, err)
	require.Equal(t, cycle.ProrationStatusProposed, resp.Status)
	require.Equal(t, []string{"cus_sponsor"}, sc.findByRefCustIDs,
		"the recovery lookup must search the FUNDING customer, not the org account's")
	require.Empty(t, sc.invoiceCalls, "nothing moved at the provider — the charge is sealed instead")
	require.Positive(t, sealedProrationCents(t, p),
		"the recovery found nothing, so the charge is owed and must be sealed, not dropped")
}
