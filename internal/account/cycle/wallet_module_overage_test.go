package cycle_test

// Credit-mode per-module overage — Leg 1 (billing-engine Job 3): a credits-mode
// account settles its per-module over-module overage through the credit wallet (an
// append-only ledger draw), EXACTLY mirroring how #99 settles the creation-proration
// base from the wallet. Standard accounts stay on the Stripe overage rail even when
// they carry a spendable wallet balance, and the whole credit branch is dark unless
// the fail-closed credit-wallet flag is set.
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
	svc := cycle.NewService(store, sc).WithCreditWallet(true)
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
}

// (2) standard mode stays on Stripe even with enough gifted credit to cover the
// overage. Standard balances are applied only by the boundary spine, never here.
func TestChargeModuleOverage_StandardModeChargesStripe(t *testing.T) {
	store := newFakeStore()
	_, acct := registeredAccount(store) // walletMode defaults to standard
	grant := seedWalletSource(store, "grant", 50_000_000, time.Time{}, timeUTC(2026, 5, 1, 0))
	sc := newFakeStripe()
	svc := cycle.NewService(store, sc).WithCreditWallet(true)
	ctx := context.Background()

	app := uuid.New()
	seedIncluded(store, acct, app, overageIncludedInstall, 5)
	over := seedTimer(store, acct, app, overageInstalled)

	res, err := svc.SweepModuleOverage(ctx, overageInstalled.AddDate(0, 0, 4))
	require.NoError(t, err)
	require.Equal(t, 1, res.Charged)
	require.Len(t, sc.itemCalls, 1, "standard mode keeps the Stripe overage draft→item→finalize flow")
	require.Len(t, sc.invoiceCalls, 1)
	require.Len(t, sc.finalizeCalls, 1)
	require.Zero(t, store.moduleOverageDrawn[over], "the wallet is never drawn on the standard path")
	require.Zero(t, store.moduleOverageDrawCalls, "standard mode never enters the wallet draw leg")
	require.Equal(t, 1, store.walletStateCalls, "the durable mode probe still runs to select the rail")
	require.EqualValues(t, 50_000_000, store.walletSources[grant].remaining)
	// The guard armed with the GENUINE Stripe invoice id, not a wallet ref.
	require.True(t, store.timers[over].graceCharged)
	require.False(t, strings.HasPrefix(store.timers[over].graceInvoiceID, "wallet:"))
}

// (3) the feature flag is fail-closed: even a credits-mode account with spendable
// credit executes NO wallet query and follows the legacy Stripe overage path.
func TestChargeModuleOverage_WalletFlagOffUsesStripe(t *testing.T) {
	store := newFakeStore()
	_, acct := registeredAccount(store)
	store.walletMode = cycle.CreditBillingModeCredits
	grant := seedWalletSource(store, "grant", 50_000_000, time.Time{}, timeUTC(2026, 5, 1, 0))
	sc := newFakeStripe()
	svc := cycle.NewService(store, sc) // WithCreditWallet intentionally omitted
	ctx := context.Background()

	app := uuid.New()
	seedIncluded(store, acct, app, overageIncludedInstall, 5)
	over := seedTimer(store, acct, app, overageInstalled)

	res, err := svc.SweepModuleOverage(ctx, overageInstalled.AddDate(0, 0, 4))
	require.NoError(t, err)
	require.Equal(t, 1, res.Charged)
	require.Len(t, sc.itemCalls, 1, "the flag-off path is the unchanged Stripe overage behavior")
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
	svc := cycle.NewService(store, sc).WithCreditWallet(true)
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
// falls through to the Stripe leg (mirroring #99's caller), which charges.
func TestChargeModuleOverage_UnderLockAttemptedDefersToStripe(t *testing.T) {
	store := newFakeStore()
	_, acct := registeredAccount(store)
	store.walletMode = cycle.CreditBillingModeCredits
	seedWalletSource(store, "grant", 50_000_000, time.Time{}, timeUTC(2026, 5, 1, 0))
	sc := newFakeStripe()
	svc := cycle.NewService(store, sc).WithCreditWallet(true)
	ctx := context.Background()

	app := uuid.New()
	seedIncluded(store, acct, app, overageIncludedInstall, 5)
	over := seedTimer(store, acct, app, overageInstalled)
	store.beforeModuleOverageDraw = func(f *fakeStore, id uuid.UUID) {
		f.timers[id].chargeAttemptedAt = overageInstalled.AddDate(0, 0, 4)
	}

	res, err := svc.SweepModuleOverage(ctx, overageInstalled.AddDate(0, 0, 4))
	require.NoError(t, err)
	require.Equal(t, 1, res.Charged, "the defer falls through to the Stripe leg, which charges")
	require.Equal(t, 1, store.moduleOverageDrawCalls, "the attempted marker is discovered inside the wallet transaction")
	require.Zero(t, store.moduleOverageDrawn[over], "the wallet must not draw beside a Stripe attempt")
	require.Len(t, sc.itemCalls, 1, "the Stripe overage leg charges after the defer")
	require.True(t, store.timers[over].graceCharged)
	require.False(t, strings.HasPrefix(store.timers[over].graceInvoiceID, "wallet:"),
		"the settlement is a Stripe id, not a wallet ref")
}

// (7) the WALLET-FIRST → concurrent STANDARD-Stripe double-charge window (Job 3
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
