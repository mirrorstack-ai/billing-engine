package cycle_test

// Credit-mode creation proration (billing-engine #99): a credits-mode account
// settles the creation base through the credit wallet (an append-only ledger
// draw), while standard accounts stay on the Stripe creation rail even when
// they carry a spendable wallet balance.
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
	billingstripe "github.com/mirrorstack-ai/billing-engine/internal/shared/stripe"
)

// creationBaseMicros is the exact prorated creation base the leg draws for an app
// created mid-period on a fully-chargeable registeredAccount, computed through
// the SAME usage helper the leg uses.
func creationBaseMicros(store *fakeStore, acct uuid.UUID, created time.Time) int64 {
	ps, pe := billingperiod.AnchoredPeriodWindow(created.UTC(), billingperiod.AnchorDay(store.activation[acct]))
	return usage.CreationChargeBaseMicros(created, ps, pe)
}

// (1) credits mode: the creation base alone is DRAWN from a covering grant and
// no creation invoice is sent to Stripe. Co-created module overage remains
// unresolved for Leg 1, which charges it after the wallet guard is armed.
func TestChargeCreationProration_CreditModeDrawsBaseOnlyAndLeavesOverageForLeg1(t *testing.T) {
	store := newFakeStore()
	user, acct := registeredAccount(store)
	store.walletMode = cycle.CreditBillingModeCredits
	grant := seedWalletSource(store, "grant", 50_000_000, time.Time{}, timeUTC(2026, 5, 1, 0))
	sc := newFakeStripe()
	svc := appsSvc(store, sc).WithCreditWallet(true)

	created := time.Date(2026, 6, 19, 12, 0, 0, 0, time.UTC)
	appID := uuid.New()
	registerMirror(t, svc, user, appID, created, 7)
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
	resolved := 0
	for _, timer := range store.timers {
		if timer.graceResolved {
			resolved++
		}
	}
	require.Zero(t, resolved, "the base-only wallet leg must not resolve any co-created timer")

	// The one-shot guard armed with the synthetic wallet reference, so a later
	// sweep never resurfaces the app, and a display base snapshot was frozen.
	require.True(t, strings.HasPrefix(resp.ProrationInvoiceID, "wallet:"), "guard is a wallet reference, not a Stripe id")
	require.Equal(t, resp.ProrationInvoiceID, store.apps[appID].ProrationInvoiceID)
	snap, ok := store.baseSnapshots[snapKey{appID, billingPeriodStart(store, acct, created)}]
	require.True(t, ok, "the display base snapshot is frozen like the Stripe leg")
	require.Equal(t, "proration", snap.source)

	// The synthetic guard stops Leg 1's defer-to-combined rule. Its ordinary
	// sweep now resolves the five included timers and charges the two overage
	// timers independently, proving the overage was not silently dropped.
	overage, err := svc.SweepModuleOverage(context.Background(), created.AddDate(0, 0, 4))
	require.NoError(t, err)
	require.Equal(t, 2, overage.Charged)
	require.Equal(t, 5, overage.Included)
}

// (2) standard mode stays on Stripe even with enough gifted credit to cover the
// creation. Standard balances are applied only by the boundary spine.
func TestChargeCreationProration_StandardModeWithBalanceChargesStripe(t *testing.T) {
	store := newFakeStore()
	user, _ := registeredAccount(store)
	grant := seedWalletSource(store, "grant", 50_000_000, time.Time{}, timeUTC(2026, 5, 1, 0))
	sc := newFakeStripe()
	svc := appsSvc(store, sc).WithCreditWallet(true)

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
	require.EqualValues(t, 50_000_000, store.walletSources[grant].remaining)
	require.Equal(t, 1, store.walletStateCalls, "the durable mode probe selects the rail")
	require.Zero(t, store.creationWalletDrawCalls, "a transient balance must not enter the creation wallet leg")
}

// (3) credit mode with a lot smaller than the base spends THROUGH zero into the
// configured credit policy's unsecured remainder — still never Stripe.
func TestChargeCreationProration_CreditModeSpendsIntoUnsecuredRemainder(t *testing.T) {
	store := newFakeStore()
	user, acct := registeredAccount(store)
	store.walletMode = cycle.CreditBillingModeCredits
	small := seedWalletSource(store, "grant", 1_000_000, time.Time{}, timeUTC(2026, 5, 1, 0))
	sc := newFakeStripe()
	svc := appsSvc(store, sc).WithCreditWallet(true)

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

// (4) a transactional WalletShort remains on the durable credits rail. Nothing
// is persisted, and the next sweep retries the wallet instead of falling to
// Stripe merely because the transient balance is still short.
func TestChargeCreationProration_CreditModeWalletShortRetriesWalletNotStripe(t *testing.T) {
	store := newFakeStore()
	user, _ := registeredAccount(store)
	store.walletMode = cycle.CreditBillingModeCredits
	store.creationWalletOutcomes = []cycle.ProrationOutcome{
		cycle.ProrationWalletShort,
		cycle.ProrationWalletShort,
	}
	sc := newFakeStripe()
	svc := appsSvc(store, sc).WithCreditWallet(true)

	created := time.Date(2026, 6, 19, 12, 0, 0, 0, time.UTC)
	appID := uuid.New()
	registerMirror(t, svc, user, appID, created, 0)

	first, err := svc.ChargeCreationProration(context.Background(), appID)
	require.NoError(t, err)
	require.Equal(t, cycle.ProrationStatusWalletUnsettled, first.Status)
	require.Zero(t, store.creationDrawn[appID], "an all-or-nothing short draws nothing")
	require.Empty(t, store.baseSnapshots, "a short draw freezes no durable snapshot")
	require.Empty(t, store.apps[appID].ProrationInvoiceID, "the guard stays unarmed — the app is retried")

	second, err := svc.SweepCreationProrations(context.Background(), created.AddDate(0, 0, 4))
	require.NoError(t, err)
	require.Equal(t, 1, second.Pending, "the unarmed short remains in the next sweep's work list")
	require.Equal(t, 1, second.Skipped)
	require.Zero(t, second.Charged)
	require.Equal(t, 2, store.creationWalletDrawCalls, "both sweeps re-enter the credits wallet rail")
	require.Equal(t, 2, store.walletStateCalls)
	require.Empty(t, sc.invoiceCalls, "a credits-mode short never falls through to Stripe")
}

// (5) idempotency: a second call after a wallet settlement short-circuits at the
// armed guard and never double-draws.
func TestChargeCreationProration_CreditModeIsIdempotent(t *testing.T) {
	store := newFakeStore()
	user, _ := registeredAccount(store)
	store.walletMode = cycle.CreditBillingModeCredits
	grant := seedWalletSource(store, "grant", 50_000_000, time.Time{}, timeUTC(2026, 5, 1, 0))
	sc := newFakeStripe()
	svc := appsSvc(store, sc).WithCreditWallet(true)

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

// The feature flag is fail-closed: even a credits-mode account with spendable
// credit executes no migration-048 query and follows the legacy Stripe path.
func TestChargeCreationProration_WalletFlagOffUsesLegacyStripePath(t *testing.T) {
	store := newFakeStore()
	user, _ := registeredAccount(store)
	store.walletMode = cycle.CreditBillingModeCredits
	grant := seedWalletSource(store, "grant", 50_000_000, time.Time{}, timeUTC(2026, 5, 1, 0))
	sc := newFakeStripe()
	svc := appsSvc(store, sc) // WithCreditWallet intentionally omitted.

	appID := uuid.New()
	registerMirror(t, svc, user, appID, time.Date(2026, 6, 19, 12, 0, 0, 0, time.UTC), 0)
	resp, err := svc.ChargeCreationProration(context.Background(), appID)
	require.NoError(t, err)
	require.Equal(t, cycle.ProrationStatusCharged, resp.Status)
	require.Len(t, sc.invoiceCalls, 1)
	require.Zero(t, store.walletStateCalls)
	require.Zero(t, store.creationWalletDrawCalls)
	require.EqualValues(t, 50_000_000, store.walletSources[grant].remaining)
}

// Defect #1: the unlocked mirror can race with a concurrent Stripe attempt.
// The wallet store's under-lock marker check wins, performs no debit, and hands
// the caller to ms_charge_ref recovery so the already-moved invoice is adopted.
func TestChargeCreationProration_LockedAttemptDefersWalletToStripeRecovery(t *testing.T) {
	store := newFakeStore()
	user, _ := registeredAccount(store)
	store.walletMode = cycle.CreditBillingModeCredits
	seedWalletSource(store, "grant", 50_000_000, time.Time{}, timeUTC(2026, 5, 1, 0))
	sc := newFakeStripe()
	svc := appsSvc(store, sc).WithCreditWallet(true)

	appID := uuid.New()
	created := time.Date(2026, 6, 19, 12, 0, 0, 0, time.UTC)
	registerMirror(t, svc, user, appID, created, 0)
	store.beforeCreationWalletDraw = func(f *fakeStore, id uuid.UUID) {
		app := f.apps[id]
		app.ProrationAttempted = true
		f.apps[id] = app
	}
	sc.setFindByRef("app-proration:"+appID.String(), cycleInvoice("in_race_winner", 1000))

	resp, err := svc.ChargeCreationProration(context.Background(), appID)
	require.NoError(t, err)
	require.Equal(t, cycle.ProrationStatusCharged, resp.Status)
	require.Equal(t, "in_race_winner", resp.ProrationInvoiceID)
	require.Equal(t, 1, store.creationWalletDrawCalls, "the attempted marker is discovered inside the wallet transaction")
	require.Zero(t, store.creationDrawn[appID], "the wallet must not draw beside a Stripe attempt")
	require.Equal(t, []string{"app-proration:" + appID.String()}, sc.findByRefCalls)
	require.Empty(t, sc.invoiceCalls, "recovery adopts the prior invoice instead of creating another")
}

func cycleInvoice(id string, amountDue int64) billingstripe.Invoice {
	return billingstripe.Invoice{
		ID: id, Status: "paid", AmountDue: amountDue, AmountPaid: amountDue, Currency: "usd",
	}
}

// billingPeriodStart is the anchored period_start the primary base snapshot is
// keyed by for a non-straddle mid-period app.
func billingPeriodStart(store *fakeStore, acct uuid.UUID, created time.Time) time.Time {
	ps, _ := billingperiod.AnchoredPeriodWindow(created.UTC(), billingperiod.AnchorDay(store.activation[acct]))
	return ps
}
