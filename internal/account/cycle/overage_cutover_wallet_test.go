package cycle_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/mirrorstack-ai/billing-engine/internal/account/cycle"
)

// 🔴 "The module-overage leg is cut over" is FALSE for a credits-mode account,
// and this pins that rather than leaving it as a comment nobody reads.
//
// The credit-wallet block (overage.go:421) runs BEFORE the proposal
// (overage.go:498) and returns. So an operator who arms
// BILLING_CYCLE_INTENT_CUTOVER still sees credits-mode customers' prepaid
// value consumed for overage, with no intent sealed for it at all. Deleting the
// legacy collector did not change this: the wallet block was always upstream of
// the branch, and the branch is now the only thing downstream of it.
//
// It is the right behaviour TODAY, and it is not a small caveat:
//
//   - the wallet is a different collection rail, not a provider charge. The
//     AST scan behind legacy_money_paths counts FinalizeInvoice, PayInvoice
//     and PayInvoiceWithMethod, so it never counted this rail and deleting
//     every legacy provider path would not remove it.
//   - the intent design DOES have a place for it — predicate.FundingPlan
//     carries WalletAllocationMicros — but the executor hardcodes that to
//     zero (executor.go:321-325) and has no wallet rail. Proposing wallet
//     charges today would strand credits-mode overage forever: nothing
//     collected, nothing collectable.
//
// So the honest statement is that the cutover covers the PROVIDER rail only.
// When the executor grows a wallet rail, this test is the thing that should
// start failing.
func TestCreditsModeOverageBypassesTheCutoverEntirely(t *testing.T) {
	store := newFakeStore()
	_, acct := registeredAccount(store)
	store.walletMode = cycle.CreditBillingModeCredits
	grant := seedWalletSource(store, "grant", 50_000_000, time.Time{}, timeUTC(2026, 5, 1, 0))
	sc := newFakeStripe()
	p := &capturingProposer{}

	svc := cycle.NewService(store, sc).
		WithCreditWallet(true).
		WithIntentProposer(p) // ARMED, and it will make no difference

	app := uuid.New()
	seedIncluded(store, acct, app, overageIncludedInstall, 5)
	over := seedTimer(store, acct, app, overageInstalled)

	res, err := svc.SweepModuleOverage(context.Background(), overageInstalled.AddDate(0, 0, 4))
	require.NoError(t, err)
	require.Equal(t, 1, res.Charged, "the wallet-settled timer still counts as charged")

	// Customer value moved.
	require.Positive(t, store.moduleOverageDrawn[over],
		"the wallet was not drawn, so this test is no longer about the credits path")
	require.EqualValues(t, 50_000_000-store.moduleOverageDrawn[over], store.walletSources[grant].remaining)

	// And no intent records it. THIS is the gap.
	require.Empty(t, p.charges,
		"a credits-mode overage sealed an intent — if the executor has grown a "+
			"wallet rail, update this test and the cutover claim together")

	// The provider is untouched either way, which is why the gap is easy to miss —
	// and since the cutover NOTHING in this leg can finalize, so this assertion no
	// longer distinguishes the wallet path from the provider one. The p.charges
	// check above is what carries the test.
	require.Empty(t, sc.finalizeCalls)
}
