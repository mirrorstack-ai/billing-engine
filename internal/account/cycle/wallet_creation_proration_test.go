package cycle_test

// Credit-mode creation proration (billing-engine #99): a credit-mode /
// wallet-active account settles ChargeCreationProration through the credit
// wallet (an append-only ledger draw) and NEVER creates a Stripe invoice, while
// a standard account with no wallet stays on the byte-for-byte Stripe path.
// Reuses the in-memory fakeStore (service_test.go), fakeStripe (charge_test.go),
// and the registeredAccount / appsSvc / registerMirror / seedWalletSource
// helpers.

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/mirrorstack-ai/billing-engine/internal/account/cycle"
	"github.com/mirrorstack-ai/billing-engine/internal/account/usage"
	"github.com/mirrorstack-ai/billing-engine/internal/billingperiod"
)

// creationBaseMicros is the exact prorated creation base the leg draws for an app
// created mid-period on a fully-chargeable registeredAccount (0 co-created
// modules → no overage), computed through the SAME usage helper the leg uses.
func creationBaseMicros(store *fakeStore, acct uuid.UUID, created time.Time) int64 {
	ps, pe := billingperiod.AnchoredPeriodWindow(created.UTC(), billingperiod.AnchorDay(store.activation[acct]))
	return usage.CreationChargeBaseMicros(created, ps, pe)
}

// (1) credit mode: the creation base is DRAWN from a covering grant and NO Stripe
// invoice is ever created; the one-shot guard arms with the wallet reference.
func TestChargeCreationProration_CreditModeDrawsWalletNotStripe(t *testing.T) {
	store := newFakeStore()
	user, acct := registeredAccount(store)
	store.walletMode = cycle.CreditBillingModeCredits
	grant := seedWalletSource(store, "grant", 50_000_000, time.Time{}, timeUTC(2026, 5, 1, 0))
	sc := newFakeStripe()
	svc := appsSvc(store, sc)

	created := time.Date(2026, 6, 19, 12, 0, 0, 0, time.UTC)
	appID := uuid.New()
	registerMirror(t, svc, user, appID, created, 0)
	want := creationBaseMicros(store, acct, created)
	require.Positive(t, want, "a survived app owes a positive prorated base")

	resp, err := svc.ChargeCreationProration(context.Background(), appID)
	require.NoError(t, err)
	require.Equal(t, cycle.ProrationStatusWalletCharged, resp.Status)

	// The wallet was debited exactly the prorated base from the grant lot …
	require.EqualValues(t, want, store.creationDrawn[appID])
	require.EqualValues(t, 50_000_000-want, store.walletSources[grant].remaining)
	require.Zero(t, store.walletUnallocated, "a covering grant needs no unsecured residual")

	// … and NOTHING went to Stripe.
	require.Empty(t, sc.invoiceCalls, "credit mode never creates a Stripe invoice")
	require.Empty(t, sc.itemCalls)
	require.Empty(t, sc.finalizeCalls)

	// The one-shot guard armed with the synthetic wallet reference, so a later
	// sweep never resurfaces the app, and a display base snapshot was frozen.
	require.True(t, strings.HasPrefix(resp.ProrationInvoiceID, "wallet:"), "guard is a wallet reference, not a Stripe id")
	require.Equal(t, resp.ProrationInvoiceID, store.apps[appID].ProrationInvoiceID)
	snap, ok := store.baseSnapshots[snapKey{appID, billingPeriodStart(store, acct, created)}]
	require.True(t, ok, "the display base snapshot is frozen like the Stripe leg")
	require.Equal(t, "proration", snap.source)
}

// (2) standard mode with no wallet: the EXISTING Stripe path is unchanged — a
// draft→item→finalize invoice is created and the guard arms with its id; the
// wallet is never touched.
func TestChargeCreationProration_StandardModeUnchangedChargesStripe(t *testing.T) {
	store := newFakeStore() // default walletMode = standard, no wallet sources
	user := mustUser(registeredAccount(store))
	sc := newFakeStripe()
	svc := appsSvc(store, sc)

	created := time.Date(2026, 6, 19, 12, 0, 0, 0, time.UTC)
	appID := uuid.New()
	registerMirror(t, svc, user, appID, created, 0)

	resp, err := svc.ChargeCreationProration(context.Background(), appID)
	require.NoError(t, err)
	require.Equal(t, cycle.ProrationStatusCharged, resp.Status)
	require.Equal(t, sc.invoiceID, resp.ProrationInvoiceID, "standard mode arms the guard with the Stripe invoice id")
	require.Len(t, sc.invoiceCalls, 1, "the standard Stripe draft→item→finalize flow is unchanged")
	require.Len(t, sc.itemCalls, 1)
	require.Len(t, sc.finalizeCalls, 1)
	require.Zero(t, store.creationDrawn[appID], "the wallet is never touched on the standard path")
	require.Empty(t, store.walletDrawOrder)
}

// (3) credit mode with a lot smaller than the base spends THROUGH zero into the
// configured credit policy's unsecured remainder — still never Stripe.
func TestChargeCreationProration_CreditModeSpendsIntoUnsecuredRemainder(t *testing.T) {
	store := newFakeStore()
	user, acct := registeredAccount(store)
	store.walletMode = cycle.CreditBillingModeCredits
	small := seedWalletSource(store, "grant", 1_000_000, time.Time{}, timeUTC(2026, 5, 1, 0))
	sc := newFakeStripe()
	svc := appsSvc(store, sc)

	created := time.Date(2026, 6, 19, 12, 0, 0, 0, time.UTC)
	appID := uuid.New()
	registerMirror(t, svc, user, appID, created, 0)
	want := creationBaseMicros(store, acct, created)
	require.Greater(t, want, int64(1_000_000), "the base must exceed the tiny grant for this case")

	resp, err := svc.ChargeCreationProration(context.Background(), appID)
	require.NoError(t, err)
	require.Equal(t, cycle.ProrationStatusWalletCharged, resp.Status)
	require.EqualValues(t, want, store.creationDrawn[appID], "credit mode debits the FULL base")
	require.Zero(t, store.walletSources[small].remaining, "the grant lot is fully consumed")
	require.EqualValues(t, want-1_000_000, store.walletUnallocated, "the residual is unsecured (into the credit policy)")
	require.Empty(t, sc.invoiceCalls)
}

// (4) a STANDARD wallet-active account whose spendable balance can NOT fully
// cover the base draws NOTHING and stays UNSETTLED — it never falls through to
// Stripe (the standing gate blocks), and the next sweep re-attempts.
func TestChargeCreationProration_StandardWalletShortLeavesUnsettledNoStripe(t *testing.T) {
	store := newFakeStore() // standard mode
	user, _ := registeredAccount(store)
	small := seedWalletSource(store, "grant", 1_000_000, time.Time{}, timeUTC(2026, 5, 1, 0))
	sc := newFakeStripe()
	svc := appsSvc(store, sc)

	created := time.Date(2026, 6, 19, 12, 0, 0, 0, time.UTC)
	appID := uuid.New()
	registerMirror(t, svc, user, appID, created, 0)

	resp, err := svc.ChargeCreationProration(context.Background(), appID)
	require.NoError(t, err)
	require.Equal(t, cycle.ProrationStatusWalletUnsettled, resp.Status)
	require.Zero(t, store.creationDrawn[appID], "an all-or-nothing short draws nothing")
	require.EqualValues(t, 1_000_000, store.walletSources[small].remaining, "the lot is untouched")
	require.Empty(t, sc.invoiceCalls, "a wallet-active short never falls through to Stripe")
	require.Empty(t, store.apps[appID].ProrationInvoiceID, "the guard stays unarmed — the app is retried")
}

// (5) idempotency: a second call after a wallet settlement short-circuits at the
// armed guard and never double-draws.
func TestChargeCreationProration_CreditModeIsIdempotent(t *testing.T) {
	store := newFakeStore()
	user, _ := registeredAccount(store)
	store.walletMode = cycle.CreditBillingModeCredits
	grant := seedWalletSource(store, "grant", 50_000_000, time.Time{}, timeUTC(2026, 5, 1, 0))
	sc := newFakeStripe()
	svc := appsSvc(store, sc)

	created := time.Date(2026, 6, 19, 12, 0, 0, 0, time.UTC)
	appID := uuid.New()
	registerMirror(t, svc, user, appID, created, 0)

	first, err := svc.ChargeCreationProration(context.Background(), appID)
	require.NoError(t, err)
	require.Equal(t, cycle.ProrationStatusWalletCharged, first.Status)
	drawnOnce := store.creationDrawn[appID]
	remainingOnce := store.walletSources[grant].remaining

	second, err := svc.ChargeCreationProration(context.Background(), appID)
	require.NoError(t, err)
	require.Equal(t, cycle.ProrationStatusAlreadyCharged, second.Status)
	require.Equal(t, first.ProrationInvoiceID, second.ProrationInvoiceID)
	require.EqualValues(t, drawnOnce, store.creationDrawn[appID], "a replay never draws a second time")
	require.EqualValues(t, remainingOnce, store.walletSources[grant].remaining)
	require.Empty(t, sc.invoiceCalls)
}

// billingPeriodStart is the anchored period_start the primary base snapshot is
// keyed by for a non-straddle mid-period app.
func billingPeriodStart(store *fakeStore, acct uuid.UUID, created time.Time) time.Time {
	ps, _ := billingperiod.AnchoredPeriodWindow(created.UTC(), billingperiod.AnchorDay(store.activation[acct]))
	return ps
}

func mustUser(user, _ uuid.UUID) uuid.UUID { return user }
