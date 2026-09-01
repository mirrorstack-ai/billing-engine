package cycle_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/mirrorstack-ai/billing-engine/internal/account/credit/rollout"
	"github.com/mirrorstack-ai/billing-engine/internal/account/cycle"
)

func cycleRolloutController(mode rollout.Mode, basisPoints string, out *bytes.Buffer) *rollout.Controller {
	allowlist := ""
	sum := sha256.Sum256([]byte(allowlist))
	policy := rollout.Parse(rollout.Config{
		MasterEnabled:   true,
		SchemaReady:     true,
		Component:       rollout.ComponentWorker,
		Mode:            string(mode),
		BasisPoints:     basisPoints,
		Allowlist:       allowlist,
		AllowlistSHA256: hex.EncodeToString(sum[:]),
		CoreManifestSHA: strings.Repeat("1", 40),
		BillingSHA:      strings.Repeat("2", 40),
	})
	return rollout.NewController(policy, rollout.NewReporter(out))
}

func TestRunBillingCycle_RolloutExcludedMakesZeroWalletSchemaCalls(t *testing.T) {
	store := newFakeStore()
	store.walletMode = cycle.CreditBillingModeCredits
	store.chargedTotal = 1_000_000
	store.hasPM = true
	store.stripeCustomer = "cus_rollout_excluded"
	sc := newFakeStripe()
	sc.invoiceAmountDue = 100
	var telemetry bytes.Buffer

	resp, err := chargeSvc(store, sc).
		WithCreditWallet(true).
		WithCreditRollout(cycleRolloutController(rollout.ModeEnforce, "0", &telemetry)).
		RunBillingCycle(context.Background(), chargeAccount, periodStart, periodEnd, 0)

	require.NoError(t, err)
	require.Equal(t, cycle.RunStatusProposed, resp.Status)
	require.EqualValues(t, 1_000_000, resp.ArrearsMicros)
	require.Zero(t, resp.WalletDrawnMicros)
	require.Zero(t, store.walletModeCalls, "selection must precede accounts.billing_mode")
	require.Zero(t, store.walletStateCalls, "excluded accounts must not read credit_ledger")
	require.Empty(t, store.walletDraws)
	require.Empty(t, sc.invoiceCalls, "excluded account takes the non-wallet rail, which no longer collects")
	require.Empty(t, telemetry.String(), "excluded operations emit no selected evaluation")
}

func TestRunBillingCycle_RolloutShadowReadsOnlyAndPreservesStripe(t *testing.T) {
	store := newFakeStore()
	store.walletMode = cycle.CreditBillingModeCredits
	store.chargedTotal = 1_000_000
	store.hasPM = true
	store.stripeCustomer = "cus_rollout_shadow"
	grant := seedWalletSource(store, "grant", 1_000_000, time.Time{}, timeUTC(2026, 1, 1, 0))
	sc := newFakeStripe()
	sc.invoiceAmountDue = 100
	var telemetry bytes.Buffer

	resp, err := chargeSvc(store, sc).
		WithCreditWallet(true).
		WithCreditRollout(cycleRolloutController(rollout.ModeShadow, "10000", &telemetry)).
		RunBillingCycle(context.Background(), chargeAccount, periodStart, periodEnd, 0)

	require.NoError(t, err)
	require.Equal(t, cycle.RunStatusProposed, resp.Status)
	require.EqualValues(t, 1_000_000, resp.ArrearsMicros)
	require.Zero(t, resp.WalletDrawnMicros)
	require.Equal(t, 1, store.walletModeCalls)
	require.Equal(t, 1, store.walletStateCalls)
	require.Empty(t, store.walletDraws, "shadow must never enter a wallet mutation")
	require.EqualValues(t, 1_000_000, store.walletSources[grant].remaining)
	require.Empty(t, sc.invoiceCalls, "shadow always leaves the non-wallet outcome alone")
	require.Contains(t, telemetry.String(), `"Mode":"shadow"`)
	require.Contains(t, telemetry.String(), `"EvaluationCount":1`)
	require.Contains(t, telemetry.String(), `"DivergenceCount":1`,
		"a credits-mode hypothetical wallet route truthfully diverges from Stripe")
	require.NotContains(t, telemetry.String(), `"ShadowMutationCount"`)
}

func TestRunBillingCycle_RolloutShadowStandardNeverReadsCreditLedger(t *testing.T) {
	store := newFakeStore()
	store.chargedTotal = 1_000_000
	store.hasPM = true
	store.stripeCustomer = "cus_rollout_shadow_standard"
	sc := newFakeStripe()
	sc.invoiceAmountDue = 100
	var telemetry bytes.Buffer

	resp, err := chargeSvc(store, sc).
		WithCreditWallet(true).
		WithCreditRollout(cycleRolloutController(rollout.ModeShadow, "10000", &telemetry)).
		RunBillingCycle(context.Background(), chargeAccount, periodStart, periodEnd, 0)

	require.NoError(t, err)
	require.Equal(t, cycle.RunStatusProposed, resp.Status)
	require.Equal(t, 1, store.walletModeCalls)
	require.Zero(t, store.walletStateCalls,
		"standard classification is already a complete shadow comparison")
	require.Empty(t, store.walletDraws)
	require.Empty(t, sc.invoiceCalls)
	require.Contains(t, telemetry.String(), `"DivergenceCount":0`)
}

func TestRunBillingCycle_RolloutEnforceStandardNeverReadsCreditLedger(t *testing.T) {
	store := newFakeStore()
	store.chargedTotal = 1_000_000
	store.hasPM = true
	store.stripeCustomer = "cus_rollout_standard"
	grant := seedWalletSource(store, "grant", 1_000_000, time.Time{}, timeUTC(2026, 1, 1, 0))
	sc := newFakeStripe()
	sc.invoiceAmountDue = 100
	var telemetry bytes.Buffer

	resp, err := chargeSvc(store, sc).
		WithCreditWallet(true).
		WithCreditRollout(cycleRolloutController(rollout.ModeEnforce, "10000", &telemetry)).
		RunBillingCycle(context.Background(), chargeAccount, periodStart, periodEnd, 0)

	require.NoError(t, err)
	require.Equal(t, cycle.RunStatusProposed, resp.Status)
	require.EqualValues(t, 1_000_000, resp.ArrearsMicros)
	require.Zero(t, resp.WalletDrawnMicros)
	require.Equal(t, 1, store.walletModeCalls)
	require.Zero(t, store.walletStateCalls,
		"selected standard classification must not cross into credit_ledger")
	require.Empty(t, store.walletDraws)
	require.EqualValues(t, 1_000_000, store.walletSources[grant].remaining,
		"production standard mode never spends a gift, even holding one")
	require.Empty(t, sc.invoiceCalls)
	require.Contains(t, telemetry.String(), `"Mode":"enforce"`)
	require.Contains(t, telemetry.String(), `"EvaluatorErrorCount":0`)
}

func TestRunBillingCycle_RolloutEnforceCreditsDrawsWallet(t *testing.T) {
	store := newFakeStore()
	store.walletMode = cycle.CreditBillingModeCredits
	store.chargedTotal = 1_000_000
	grant := seedWalletSource(store, "grant", 1_000_000, time.Time{}, timeUTC(2026, 1, 1, 0))
	sc := newFakeStripe()
	var telemetry bytes.Buffer

	resp, err := chargeSvc(store, sc).
		WithCreditWallet(true).
		WithCreditRollout(cycleRolloutController(rollout.ModeEnforce, "10000", &telemetry)).
		RunBillingCycle(context.Background(), chargeAccount, periodStart, periodEnd, 0)

	require.NoError(t, err)
	require.Equal(t, cycle.RunStatusInvoiced, resp.Status)
	require.EqualValues(t, 1_000_000, resp.WalletDrawnMicros)
	require.Zero(t, resp.ChargedCents)
	require.Equal(t, 1, store.walletModeCalls)
	require.Equal(t, 1, store.walletStateCalls)
	require.NotEmpty(t, store.walletDraws)
	require.Zero(t, store.walletSources[grant].remaining)
	require.Empty(t, sc.invoiceCalls, "selected enforce credits uses the wallet rail")
	require.Contains(t, telemetry.String(), `"Mode":"enforce"`)
	require.Contains(t, telemetry.String(), `"EvaluatorErrorCount":0`)
}

func TestChargeCreationProration_RolloutEnforceCreditsUsesWallet(t *testing.T) {
	store := newFakeStore()
	user, accountID := registeredAccount(store)
	store.walletMode = cycle.CreditBillingModeCredits
	grant := seedWalletSource(store, "grant", 50_000_000, time.Time{}, timeUTC(2026, 5, 1, 0))
	sc := newFakeStripe()
	var telemetry bytes.Buffer
	svc := appsSvc(store, sc).
		WithCreditWallet(true).
		WithCreditRollout(cycleRolloutController(rollout.ModeEnforce, "10000", &telemetry))

	created := time.Date(2026, 6, 19, 12, 0, 0, 0, time.UTC)
	appID := uuid.New()
	registerMirror(t, svc, user, appID, created, 0)
	want := creationBaseMicros(store, accountID, created)

	resp, err := svc.ChargeCreationProration(context.Background(), appID)

	require.NoError(t, err)
	require.Equal(t, cycle.ProrationStatusWalletCharged, resp.Status)
	require.EqualValues(t, want, store.creationDrawn[appID])
	require.EqualValues(t, 50_000_000-want, store.walletSources[grant].remaining)
	require.Equal(t, 1, store.walletModeCalls)
	require.Equal(t, 1, store.walletStateCalls)
	require.Equal(t, 1, store.creationWalletDrawCalls)
	require.Empty(t, sc.invoiceCalls)
	require.Contains(t, telemetry.String(), `"Mode":"enforce"`)
}

func TestChargeModuleOverage_RolloutShadowNeverDraws(t *testing.T) {
	store := newFakeStore()
	_, accountID := registeredAccount(store)
	store.walletMode = cycle.CreditBillingModeCredits
	grant := seedWalletSource(store, "grant", 50_000_000, time.Time{}, timeUTC(2026, 5, 1, 0))
	sc := newFakeStripe()
	p := &capturingProposer{}
	var telemetry bytes.Buffer
	svc := cycle.NewService(store, sc).
		WithCreditWallet(true).
		WithCreditRollout(cycleRolloutController(rollout.ModeShadow, "10000", &telemetry)).
		WithIntentProposer(p)

	appID := uuid.New()
	seedIncluded(store, accountID, appID, overageIncludedInstall, 5)
	timerID := seedTimer(store, accountID, appID, overageInstalled)

	resp, err := svc.SweepModuleOverage(
		context.Background(),
		overageInstalled.AddDate(0, 0, 4),
	)

	require.NoError(t, err)
	require.Equal(t, 1, resp.Skipped)
	require.Zero(t, store.moduleOverageDrawn[timerID])
	require.Zero(t, store.moduleOverageDrawCalls)
	require.EqualValues(t, 50_000_000, store.walletSources[grant].remaining)
	require.Equal(t, 1, store.walletModeCalls)
	require.Equal(t, 1, store.walletStateCalls)
	require.Len(t, p.charges, 1, "shadow keeps the timer on the provider rail")
	require.Empty(t, sc.invoiceCalls, "and that rail no longer mints an invoice")
	require.Contains(t, telemetry.String(), `"Mode":"shadow"`)
	require.Contains(t, telemetry.String(), `"DivergenceCount":1`)
	require.NotContains(t, telemetry.String(), `"ShadowMutationCount"`)
}
