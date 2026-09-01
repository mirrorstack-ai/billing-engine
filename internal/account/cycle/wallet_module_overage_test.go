package cycle_test

// Credit-mode per-module overage — Leg 1 (billing-engine Job 3): a credits-mode
// account settles its per-module over-module overage through the credit wallet (an
// append-only ledger draw), EXACTLY mirroring how #99 settles the creation-proration
// base from the wallet. Standard accounts stay OFF the wallet even when they carry
// a spendable wallet balance — since the Leg-1 cutover their overage is SEALED as
// an intent rather than invoiced, but either way the wallet is not their rail —
// and the whole credit branch is dark unless the fail-closed credit-wallet flag is
// set.
// Reuses the in-memory fakeStore (service_test.go), fakeStripe (charge_test.go),
// and the registeredAccount / seedTimer / seedIncluded / seedWalletSource helpers.

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/mirrorstack-ai/billing-engine/internal/account/cycle"
)

// overageInstalled is a mid-period install on the registeredAccount (activation
// anchor day 4, 2026-05-04): its anchored period is [2026-06-04, 2026-07-04) and
// its grace expires 2026-06-13 — WELL inside the period, so the shape is a plain
// prorated overage (no boundary straddle), the cleanest amount to assert against.
var overageInstalled = time.Date(2026, 6, 10, 12, 0, 0, 0, time.UTC)

// overageIncludedInstall anchors the five bundled-allowance timers STRICTLY BEFORE
// overageInstalled, so the target timer lands at live-FIFO rank 5 ("over")
// deterministically — a shared installed_at would leave the rank to random id
// tie-breaks.
var overageIncludedInstall = time.Date(2026, 6, 8, 0, 0, 0, 0, time.UTC)

// (1) credits mode: the "over" timer's overage is DRAWN from a covering grant and
// no Stripe invoice is sent. The per-timer guard is armed with a synthetic wallet
// reference (never a Stripe id), so a later sweep never resurfaces it.
func TestChargeModuleOverage_CreditModeDrawsFromWallet(t *testing.T) {
	store := newFakeStore()
	_, acct := registeredAccount(store)
	store.walletMode = cycle.CreditBillingModeCredits
	grant := seedWalletSource(store, "grant", 50_000_000, time.Time{}, timeUTC(2026, 5, 1, 0))
	sc := newFakeStripe()
	observer := &fakeWalletMutationObserver{}
	svc := cycle.NewService(store, sc).
		WithCreditWallet(true).
		WithWalletMutationObserver(observer)
	ctx := context.Background()

	app := uuid.New()
	seedIncluded(store, acct, app, overageIncludedInstall, 5) // occupy FIFO ranks 0-4
	over := seedTimer(store, acct, app, overageInstalled)

	res, err := svc.SweepModuleOverage(ctx, overageInstalled.AddDate(0, 0, 4))
	require.NoError(t, err)
	require.Equal(t, 1, res.Pending, "only the unresolved over timer is a candidate")
	require.Equal(t, 1, res.Charged, "the wallet-settled over timer counts as charged")
	require.Zero(t, res.Skipped)

	// Drawn from the grant lot, nothing to Stripe.
	require.Positive(t, store.moduleOverageDrawn[over])
	require.EqualValues(t, 50_000_000-store.moduleOverageDrawn[over], store.walletSources[grant].remaining)
	require.Zero(t, store.walletUnallocated, "a covering grant needs no unsecured residual")
	require.Empty(t, sc.invoiceCalls, "credit mode never creates a Stripe invoice")
	require.Empty(t, sc.itemCalls)
	require.Empty(t, sc.finalizeCalls)
	require.Equal(t, []uuid.UUID{acct}, observer.calls,
		"the first committed wallet draw immediately refreshes standing")

	// The guard armed with the synthetic wallet reference + charged, item id NULL.
	require.True(t, store.timers[over].graceResolved)
	require.True(t, store.timers[over].graceCharged)
	require.True(t, strings.HasPrefix(store.timers[over].graceInvoiceID, "wallet:"),
		"the guard is a wallet reference, not a Stripe id")
	require.Empty(t, store.timers[over].graceInvoiceItemID, "a wallet draw has no Stripe invoice-item id")

	// Idempotent: a second sweep short-circuits at the resolved guard — no re-draw.
	drawnOnce := store.moduleOverageDrawn[over]
	second, err := svc.SweepModuleOverage(ctx, overageInstalled.AddDate(0, 0, 6))
	require.NoError(t, err)
	require.Zero(t, second.Pending, "the resolved timer drops out of the work list")
	require.EqualValues(t, drawnOnce, store.moduleOverageDrawn[over], "a replay never draws a second time")
	require.Len(t, observer.calls, 1, "an idempotent replay emits no second standing observation")
}

// (2) standard mode stays OFF the wallet even with enough gifted credit to cover
// the overage. Standard balances are applied only by the boundary spine, never
// here. Since the cutover the standard rail seals an intent instead of invoicing,
// so what this pins is that the covering grant is still untouched.
func TestChargeModuleOverage_StandardModeStaysOffTheWallet(t *testing.T) {
	store := newFakeStore()
	_, acct := registeredAccount(store) // walletMode defaults to standard
	grant := seedWalletSource(store, "grant", 50_000_000, time.Time{}, timeUTC(2026, 5, 1, 0))
	sc := newFakeStripe()
	observer := &fakeWalletMutationObserver{}
	p := &capturingProposer{}
	svc := cycle.NewService(store, sc).
		WithCreditWallet(true).
		WithWalletMutationObserver(observer).
		WithIntentProposer(p)
	ctx := context.Background()

	app := uuid.New()
	seedIncluded(store, acct, app, overageIncludedInstall, 5)
	over := seedTimer(store, acct, app, overageInstalled)

	res, err := svc.SweepModuleOverage(ctx, overageInstalled.AddDate(0, 0, 4))
	require.NoError(t, err)
	require.Zero(t, res.Charged, "a proposal collects nothing, so nothing is charged")
	require.Equal(t, 1, res.Skipped)
	require.Len(t, p.charges, 1, "standard mode seals the overage as an intent")
	require.Empty(t, sc.invoiceCalls, "the leg reaches no provider")
	require.Empty(t, sc.itemCalls)
	require.Empty(t, sc.finalizeCalls)
	require.Zero(t, store.moduleOverageDrawn[over], "the wallet is never drawn on the standard path")
	require.Zero(t, store.moduleOverageDrawCalls, "standard mode never enters the wallet draw leg")
	require.Equal(t, 1, store.walletStateCalls, "the durable mode probe still runs to select the rail")
	require.EqualValues(t, 50_000_000, store.walletSources[grant].remaining)
	// The guard armed with the intent reference, not a wallet ref.
	require.True(t, store.timers[over].graceCharged)
	require.True(t, strings.HasPrefix(store.timers[over].graceInvoiceID, "intent:"),
		"the guard must name the rail that settled it")
	require.Empty(t, observer.calls, "the standard rail emits no wallet observation")
}

// (3) the feature flag is fail-closed: even a credits-mode account with spendable
// credit executes NO wallet query and follows the ordinary provider-rail path,
// which since the cutover means it seals an intent.
func TestChargeModuleOverage_WalletFlagOffNeverReadsTheWallet(t *testing.T) {
	store := newFakeStore()
	_, acct := registeredAccount(store)
	store.walletMode = cycle.CreditBillingModeCredits
	grant := seedWalletSource(store, "grant", 50_000_000, time.Time{}, timeUTC(2026, 5, 1, 0))
	sc := newFakeStripe()
	p := &capturingProposer{}
	// WithCreditWallet intentionally omitted.
	svc := cycle.NewService(store, sc).WithIntentProposer(p)
	ctx := context.Background()

	app := uuid.New()
	seedIncluded(store, acct, app, overageIncludedInstall, 5)
	over := seedTimer(store, acct, app, overageInstalled)

	res, err := svc.SweepModuleOverage(ctx, overageInstalled.AddDate(0, 0, 4))
	require.NoError(t, err)
	require.Equal(t, 1, res.Skipped)
	require.Len(t, p.charges, 1, "the flag-off path is the ordinary provider-rail path")
	require.Empty(t, sc.itemCalls)
	require.Zero(t, store.walletStateCalls, "the credit branch never runs with the flag off")
	require.Zero(t, store.moduleOverageDrawCalls, "zero wallet store calls when the flag is off")
	require.Zero(t, store.moduleOverageDrawn[over])
	require.EqualValues(t, 50_000_000, store.walletSources[grant].remaining)
}

// (4) a transactional WalletShort remains on the durable credits rail. Nothing is
// persisted, and the next sweep retries the wallet instead of falling to Stripe
// merely because the transient balance is still short.
func TestChargeModuleOverage_CreditModeWalletShortUnsettledRetried(t *testing.T) {
	store := newFakeStore()
	_, acct := registeredAccount(store)
	store.walletMode = cycle.CreditBillingModeCredits
	store.moduleOverageWalletOutcomes = []cycle.ModuleOverageWalletOutcome{
		cycle.ModuleOverageWalletShort,
		cycle.ModuleOverageWalletShort,
	}
	sc := newFakeStripe()
	observer := &fakeWalletMutationObserver{}
	svc := cycle.NewService(store, sc).
		WithCreditWallet(true).
		WithWalletMutationObserver(observer)
	ctx := context.Background()

	app := uuid.New()
	seedIncluded(store, acct, app, overageIncludedInstall, 5)
	over := seedTimer(store, acct, app, overageInstalled)

	first, err := svc.SweepModuleOverage(ctx, overageInstalled.AddDate(0, 0, 4))
	require.NoError(t, err)
	require.Equal(t, 1, first.Pending)
	require.Zero(t, first.Charged)
	require.Equal(t, 1, first.Skipped, "a wallet-short overage is skipped, never charged")
	require.Zero(t, store.moduleOverageDrawn[over], "an all-or-nothing short draws nothing")
	require.False(t, store.timers[over].graceResolved, "the guard stays unarmed — the timer is retried")
	require.Empty(t, sc.itemCalls, "a credits-mode short never falls through to Stripe")

	second, err := svc.SweepModuleOverage(ctx, overageInstalled.AddDate(0, 0, 5))
	require.NoError(t, err)
	require.Equal(t, 1, second.Pending, "the unarmed short remains in the next sweep's work list")
	require.Equal(t, 1, second.Skipped)
	require.Zero(t, second.Charged)
	require.Equal(t, 2, store.moduleOverageDrawCalls, "both sweeps re-enter the credits wallet rail")
	require.Empty(t, sc.itemCalls, "a credits-mode short never falls through to Stripe")
	require.Empty(t, observer.calls, "a transactional no-draw short emits no standing observation")
}

// (5) under-lock stale: a concurrent sweep resolves the timer between the work-list
// read and the wallet transaction's under-lock re-check. The store's re-check wins,
// performs NO debit, and the timer is left resolved (nothing durable changes here).
func TestChargeModuleOverage_UnderLockAlreadyResolvedIsStaleNotDrawn(t *testing.T) {
	store := newFakeStore()
	_, acct := registeredAccount(store)
	store.walletMode = cycle.CreditBillingModeCredits
	grant := seedWalletSource(store, "grant", 50_000_000, time.Time{}, timeUTC(2026, 5, 1, 0))
	sc := newFakeStripe()
	svc := cycle.NewService(store, sc).WithCreditWallet(true)
	ctx := context.Background()

	app := uuid.New()
	seedIncluded(store, acct, app, overageIncludedInstall, 5)
	over := seedTimer(store, acct, app, overageInstalled)
	// A concurrent sweep resolves the timer AFTER the top-of-charge pending re-check
	// but before the wallet transaction's under-lock re-check.
	store.beforeModuleOverageDraw = func(f *fakeStore, id uuid.UUID) {
		f.timers[id].graceResolved = true
	}

	res, err := svc.SweepModuleOverage(ctx, overageInstalled.AddDate(0, 0, 4))
	require.NoError(t, err)
	require.Equal(t, 1, res.Skipped, "the under-lock stale re-check draws nothing")
	require.Zero(t, res.Charged)
	require.Equal(t, 1, store.moduleOverageDrawCalls, "the stale state is discovered inside the wallet transaction")
	require.Zero(t, store.moduleOverageDrawn[over], "a stale timer is never drawn")
	require.EqualValues(t, 50_000_000, store.walletSources[grant].remaining)
	require.Empty(t, sc.itemCalls)
}

// (6) under-lock defect-1 race: a concurrent Stripe attempt stamps charge_attempted_at
// under the lock. The store defers, the wallet performs NO debit, and the caller
// falls through to the provider leg (mirroring #99's caller), which since the
// cutover seals an intent rather than invoicing. The property under test survives
// the cutover unchanged: the wallet must not debit beside a charge that may
// already have moved money.
func TestChargeModuleOverage_UnderLockAttemptedDefersOutOfTheWallet(t *testing.T) {
	store := newFakeStore()
	_, acct := registeredAccount(store)
	store.walletMode = cycle.CreditBillingModeCredits
	seedWalletSource(store, "grant", 50_000_000, time.Time{}, timeUTC(2026, 5, 1, 0))
	sc := newFakeStripe()
	p := &capturingProposer{}
	svc := cycle.NewService(store, sc).WithCreditWallet(true).WithIntentProposer(p)
	ctx := context.Background()

	app := uuid.New()
	seedIncluded(store, acct, app, overageIncludedInstall, 5)
	over := seedTimer(store, acct, app, overageInstalled)
	store.beforeModuleOverageDraw = func(f *fakeStore, id uuid.UUID) {
		f.timers[id].chargeAttemptedAt = overageInstalled.AddDate(0, 0, 4)
	}

	res, err := svc.SweepModuleOverage(ctx, overageInstalled.AddDate(0, 0, 4))
	require.NoError(t, err)
	require.Equal(t, 1, res.Skipped)
	require.Len(t, p.charges, 1, "the defer falls through to the provider leg, which proposes")
	require.Equal(t, 1, store.moduleOverageDrawCalls, "the attempted marker is discovered inside the wallet transaction")
	require.Zero(t, store.moduleOverageDrawn[over], "the wallet must not draw beside a Stripe attempt")
	require.Empty(t, sc.itemCalls)
	require.True(t, store.timers[over].graceCharged)
	require.False(t, strings.HasPrefix(store.timers[over].graceInvoiceID, "wallet:"),
		"the settlement is not a wallet ref")
}

// (7) the initial rail classification is intentionally unlocked. If an owner
// flips credits→standard before the wallet transaction obtains the account lock,
// the locked durable mode wins: even a fully covering grant is untouched and the
// entire mid-period overage proceeds through the provider leg.
func TestChargeModuleOverage_ModeFlipsToStandardBeforeWalletLockDefersOutOfTheWallet(t *testing.T) {
	store := newFakeStore()
	_, acct := registeredAccount(store)
	store.walletMode = cycle.CreditBillingModeCredits
	grant := seedWalletSource(store, "grant", 50_000_000, time.Time{}, timeUTC(2026, 5, 1, 0))
	sc := newFakeStripe()
	observer := &fakeWalletMutationObserver{}
	p := &capturingProposer{}
	svc := cycle.NewService(store, sc).
		WithCreditWallet(true).
		WithWalletMutationObserver(observer).
		WithIntentProposer(p)
	ctx := context.Background()

	app := uuid.New()
	seedIncluded(store, acct, app, overageIncludedInstall, 5)
	over := seedTimer(store, acct, app, overageInstalled)
	store.beforeModuleOverageDraw = func(f *fakeStore, _ uuid.UUID) {
		f.walletMode = cycle.CreditBillingModeStandard
	}

	res, err := svc.SweepModuleOverage(ctx, overageInstalled.AddDate(0, 0, 4))
	require.NoError(t, err)
	require.Equal(t, 1, res.Skipped)
	require.Len(t, p.charges, 1, "the full charge stays on the provider rail")
	require.Equal(t, 1, store.moduleOverageDrawCalls,
		"the mode change is discovered inside the wallet transaction")
	require.Zero(t, store.moduleOverageDrawn[over], "standard mode never receives a mid-period wallet draw")
	require.EqualValues(t, 50_000_000, store.walletSources[grant].remaining,
		"a covering grant must remain untouched after the mode change")
	require.Empty(t, store.walletDrawOrder)
	require.Empty(t, observer.calls, "a no-draw defer emits no wallet mutation")
	require.Empty(t, sc.invoiceCalls)
	require.Empty(t, sc.itemCalls)
	require.Empty(t, sc.finalizeCalls)
	require.True(t, store.timers[over].graceCharged)
	require.True(t, strings.HasPrefix(store.timers[over].graceInvoiceID, "intent:"),
		"the settlement is an intent reference, not a wallet ref")
}

// (8) the WALLET-FIRST → concurrent STANDARD-Stripe double-charge window (Job 3
// hardening): flag on, and a credits worker wallet-settles the timer (arms
// grace_resolved) AFTER this standard worker's pending re-check — modeled by the
// concurrent settlement landing at this worker's WalletCreditState read, where the
// billing_mode flip to standard is also observed. The standard worker's
// charge-attempt stamp then matches 0 rows (grace_resolved already true) and MUST
// abort stale — never a second Stripe charge beside the wallet debit.
func TestChargeModuleOverage_WalletSettledThenStandardStripeAbortsStale(t *testing.T) {
	store := newFakeStore()
	_, acct := registeredAccount(store)
	store.walletMode = cycle.CreditBillingModeStandard // this worker observes standard (post-flip)
	seedWalletSource(store, "grant", 50_000_000, time.Time{}, timeUTC(2026, 5, 1, 0))
	sc := newFakeStripe()
	svc := cycle.NewService(store, sc).WithCreditWallet(true)
	ctx := context.Background()

	app := uuid.New()
	seedIncluded(store, acct, app, overageIncludedInstall, 5)
	over := seedTimer(store, acct, app, overageInstalled)

	// The concurrent credits worker wallet-settles the timer between this (standard)
	// worker's pending re-check and its charge-attempt stamp — injected at the
	// WalletCreditState read, the exact flip window.
	store.beforeWalletCreditState = func(f *fakeStore) {
		tmr := f.timers[over]
		tmr.graceResolved = true
		tmr.graceCharged = true
		tmr.graceChargedAt = overageInstalled.AddDate(0, 0, 4)
		tmr.graceInvoiceID = "wallet:mod-overage:" + over.String()
		f.moduleOverageDrawn[over] = 2_400_000
	}

	res, err := svc.SweepModuleOverage(ctx, overageInstalled.AddDate(0, 0, 4))
	require.NoError(t, err)
	require.Equal(t, 1, res.Skipped, "the standard worker aborts stale at the charge-attempt stamp")
	require.Zero(t, res.Charged)
	require.Empty(t, sc.itemCalls, "NO second Stripe charge fires beside the wallet debit")
	require.Empty(t, sc.invoiceCalls)
	require.Empty(t, sc.finalizeCalls)
	require.Zero(t, store.moduleOverageDrawCalls, "the standard worker never enters the wallet draw leg")
	// The credits worker's wallet settlement stands untouched.
	require.True(t, strings.HasPrefix(store.timers[over].graceInvoiceID, "wallet:"),
		"the timer keeps the concurrent wallet settlement's ref")
}
