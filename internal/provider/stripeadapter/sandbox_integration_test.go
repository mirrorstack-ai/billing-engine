//go:build integration

package stripeadapter_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/mirrorstack-ai/billing-engine/internal/intent"
	"github.com/mirrorstack-ai/billing-engine/internal/intent/executor"
	"github.com/mirrorstack-ai/billing-engine/internal/intent/predicate"
	"github.com/mirrorstack-ai/billing-engine/internal/intent/store"
	"github.com/mirrorstack-ai/billing-engine/internal/provider/stripeadapter"
	billingstripe "github.com/mirrorstack-ai/billing-engine/internal/shared/stripe"
	"github.com/mirrorstack-ai/billing-engine/internal/shared/testutil"
)

// TestAnIntentCollectsThroughTheStripeSandbox is the end-to-end this
// whole branch is building toward: a sealed intent, the predicate's
// verdict, the executor's claim, a real charge at a real provider, and
// the outcome written down.
//
// Everything is real except the money. A real Postgres with every
// migration applied, the real predicate, the real executor, the real
// adapter, and Stripe's own test mode — which is a real API making real
// objects, not a mock. What test mode means is that no money moves; it
// does not mean the plumbing is simulated.
//
// It skips without a sandbox key rather than failing, so the ordinary
// suite does not depend on a credential. Set REQUIRE_STRIPE=1 where the
// green is load-bearing, for the same reason REQUIRE_DOCKER exists: a
// skipped test still prints "ok".
func TestAnIntentCollectsThroughTheStripeSandbox(t *testing.T) {
	key := testutil.SandboxStripeKey(t)
	pool := testutil.NewTestDB(t)
	ctx := context.Background()

	client := billingstripe.NewIntentClient(key)
	customerID, paymentMethodID := seedSandboxCustomerWithACard(t, key)
	payer := fixedPayer{customer: customerID, paymentMethod: paymentMethodID}

	s := store.New(pool)
	sealed := seedIntentReadyToCollect(t, s, customerID)

	exec := executor.New(
		s,
		stripeadapter.New(client, payer),
		"sandbox-e2e",
		func() time.Time { return time.Now().UTC() },
		func(context.Context) executor.Environment { return fullyEvidenced() },
	)

	out, err := exec.Execute(ctx, sealed.Digest())
	require.NoError(t, err)
	require.Truef(t, out.Permitted, "the predicate refused: %v", out.Refused)
	require.Falsef(t, out.Unresolved, "the rail's answer was ambiguous, not merely pending")
	require.NotEmpty(t, out.Reference, "no provider reference was returned")

	// Stripe's finalize returns BEFORE it collects, so the ordinary
	// answer here is in-progress and the settlement arrives afterwards.
	// That is the shape docs/DESIGN.md §4 calls provider_in_progress,
	// and this test asserts it rather than pretending the call is
	// synchronous.
	require.True(t, out.Settled || out.InProgress,
		"the rail neither settled nor accepted the charge")

	if out.InProgress {
		state, err := s.State(ctx, sealed.Digest())
		require.NoError(t, err)
		require.Equal(t, "provider_in_progress", state,
			"a charge in flight is not visible in the intent's state, so no reconciler could find it")

		// No outcome is recorded while the rail has not reported. That
		// is the property that matters: the engine does not write down
		// a settlement it has not been told about.
		var outcome *string
		require.NoError(t, pool.QueryRow(ctx,
			`SELECT outcome FROM ms_billing.intent_settlement_claims WHERE intent_digest = $1`,
			sealed.Digest()).Scan(&outcome))
		require.Nil(t, outcome, "an outcome was recorded before the rail reported one")
	}

	// The money still has to arrive. Poll the provider until the
	// invoice reaches a terminal state, which is what a callback would
	// otherwise tell us.
	status, amountPaid := awaitTerminalInvoice(t, key, out.Reference)
	require.Equal(t, "paid", status, "Stripe did not collect the charge")
	require.Equal(t, centsOf(sealed.TotalMicros()), amountPaid,
		"Stripe collected a different amount than the intent sealed")

	// A second execution must not collect again, and it is refused
	// BEFORE the claim: the intent has left the eligible state, so the
	// predicate says no and nothing reaches the provider.
	//
	// Two independent things would each have stopped it — the state
	// clause and the settlement claim — which is the point. Asserting
	// the claim specifically would have been asserting the weaker of
	// them, and would break the day the state machine got stricter.
	second, err := exec.Execute(ctx, sealed.Digest())
	require.NoError(t, err)
	require.False(t, second.Settled, "a settled intent collected twice")
	require.False(t, second.Permitted, "a settled intent was permitted again")

	afterSecond := sandboxInvoiceCount(t, key, customerID)
	require.Equal(t, 1, afterSecond,
		"the second execution created another invoice at the provider")
}

// 🔴 The refusal half, against the same real rail. A test that only
// shows the happy path proves the plumbing works, not that the gate
// does — and the gate is the thing this repository exists for.
func TestARefusedIntentNeverReachesTheSandbox(t *testing.T) {
	key := testutil.SandboxStripeKey(t)
	pool := testutil.NewTestDB(t)
	ctx := context.Background()

	client := billingstripe.NewIntentClient(key)
	customerID, paymentMethodID := seedSandboxCustomerWithACard(t, key)
	payer := fixedPayer{customer: customerID, paymentMethod: paymentMethodID}

	s := store.New(pool)
	sealed := seedIntentReadyToCollect(t, s, customerID)

	env := fullyEvidenced()
	env.BuildIdentified = false // docs/VERIFICATION.md §2

	out, err := executor.New(
		s, stripeadapter.New(client, payer), "sandbox-e2e",
		func() time.Time { return time.Now().UTC() },
		func(context.Context) executor.Environment { return env },
	).Execute(ctx, sealed.Digest())

	require.NoError(t, err)
	require.False(t, out.Permitted)
	require.Contains(t, out.Refused, predicate.ClauseBuildIdentified)

	// Nothing was claimed, and Stripe has no invoice for this customer.
	var claims int
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT count(*) FROM ms_billing.intent_settlement_claims WHERE intent_digest = $1`,
		sealed.Digest()).Scan(&claims))
	require.Zero(t, claims)
	require.Zero(t, sandboxInvoiceCount(t, key, customerID),
		"a refused intent created an invoice at the provider")
}

// --- fixtures -------------------------------------------------------

type fixedPayer struct{ customer, paymentMethod string }

func (p fixedPayer) ResolvePayer(context.Context, string, string) (string, string, error) {
	return p.customer, p.paymentMethod, nil
}

func fullyEvidenced() executor.Environment {
	return executor.Environment{
		BuildIdentified:              true,
		PolicyDigestsMatch:           true,
		TimeReady:                    true,
		TaxIndependentlyReproducible: true,
		Unbuilt: predicate.UnbuiltEvidence{
			ProofHeadCurrent: true, ProofsApplied: true, CommercialIdentity: true,
			MerchantOfRecord: true, SourceAllocation: true, CreditLotsReserved: true,
			ExposureReservation: true, FundingMatchesAccepted: true,
			RailSupportsPlan: true, ProviderAutonomy: true, FirstStepMatchesPlan: true,
			InstrumentBinding: true, EnclaveReady: true, AttemptFrozen: true,
		},
	}
}

func seedIntentReadyToCollect(t *testing.T, s *store.Store, payerID string) intent.ChargeIntent {
	t.Helper()
	ctx := context.Background()
	now := time.Now().UTC()

	const kind intent.ChargeKind = intent.KindModuleUsage

	sealed, err := intent.Seal(intent.Draft{
		Payer:    intent.Subject{Kind: "org", ID: payerID},
		Currency: "USD",
		// $4.20: small enough to be unremarkable, and not a round
		// number, so a rounding error would be visible.
		Lines:             []intent.Line{intent.NewLine("quiz.render", "quiz-core", "1.4.0", 420, 10_000)},
		Kind:              kind,
		PriceBookRevision: "pb-2026-08",
		TermsRevision:     "terms-2026-01",
		Tax: intent.TaxDetermination{
			Resolved: true, Jurisdiction: "US-OR", RuleRevision: "tax-2026-05", AmountMicros: 0,
			Verification: intent.TaxNotApplicable,
		},
		AuthorizationID:       "auth-sandbox",
		NoticePolicy:          "email/v1",
		ExecuteNotBefore:      now.Add(-time.Hour),
		ExecuteNotAfter:       now.Add(time.Hour),
		SourceFactKeys:        []string{"fact-sandbox-1"},
		SelectedRail:          "stripe",
		RoutingPolicyRevision: "routing-2026-08",
	})
	require.NoError(t, err)
	require.NoError(t, s.SaveIntent(ctx, sealed))

	auth, err := intent.Authorize(intent.AuthorizationGrant{
		ID: "auth-sandbox", Scope: intent.ScopeStanding,
		Subject:  intent.Subject{Kind: "org", ID: payerID},
		Currency: "USD", Kinds: []intent.ChargeKind{kind},
		PerChargeCeiling: 10_000_000, PeriodCeiling: 50_000_000, FrequencyCeiling: 100, NoticeLeadTime: 24 * time.Hour, Provider: "stripe", MandateReference: "pm_test_1",
		TermsRevision: "terms-2026-01", PriceBook: "pb-2026-08",
		NoticePolicy:  "email/v1",
		EffectiveFrom: now.Add(-24 * time.Hour), ExpiresAt: now.Add(24 * time.Hour),
		AcceptanceDigest: "accept-sandbox",
	})
	require.NoError(t, err)
	require.NoError(t, s.SaveAuthorization(ctx, auth))

	require.NoError(t, s.RecordNotice(ctx, store.NoticeReceipt{
		IntentDigest: sealed.Digest(), DeliveredDigest: sealed.Digest(),
		Policy: "email/v1", TerminalStatus: "delivered",
		DeliveredAt:          now.Add(-26 * time.Hour),
		EligibilityNotBefore: now.Add(-2 * time.Hour), RevocationPathFresh: true,
	}))
	require.NoError(t, s.AdvanceState(ctx, sealed.Digest(), "proposed", "eligible"))
	return sealed
}

// seedSandboxCustomerWithACard makes a customer that can actually be
// charged, and removes it afterwards.
//
// pm_card_visa is Stripe's own test token. Using it rather than raw
// card numbers keeps this file free of anything resembling a real
// instrument — which matters, because this repository is public.
func seedSandboxCustomerWithACard(t *testing.T, key string) (customerID, paymentMethodID string) {
	t.Helper()

	customer := stripePost(t, key, "customers", url.Values{
		"email":             {"intent-e2e@mirrorstack.invalid"},
		"metadata[purpose]": {"billing-engine intent e2e"},
	})
	customerID, _ = customer["id"].(string)
	require.NotEmpty(t, customerID, "could not create a sandbox customer")

	t.Cleanup(func() {
		// Deleting the customer voids its invoices, so a failed run
		// leaves nothing behind that looks like an unpaid bill.
		stripeDelete(t, key, "customers/"+customerID)
	})

	pm := stripePost(t, key, "payment_methods/pm_card_visa/attach", url.Values{
		"customer": {customerID},
	})
	paymentMethodID, _ = pm["id"].(string)
	require.NotEmpty(t, paymentMethodID, "could not attach a test card")

	stripePost(t, key, "customers/"+customerID, url.Values{
		"invoice_settings[default_payment_method]": {paymentMethodID},
	})
	return customerID, paymentMethodID
}

// awaitTerminalInvoice polls until the invoice stops being open.
//
// The wait exists because collection is asynchronous, not because the
// test is flaky: an invoice that never leaves `open` is a real failure
// and the timeout says so.
func awaitTerminalInvoice(t *testing.T, key, invoiceID string) (status string, amountPaid int64) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for {
		status, amountPaid = sandboxInvoice(t, key, invoiceID)
		if status != "open" && status != "draft" {
			return status, amountPaid
		}
		if time.Now().After(deadline) {
			t.Fatalf("invoice %s was still %q after 30s; the charge never completed",
				invoiceID, status)
		}
		time.Sleep(time.Second)
	}
}

func sandboxInvoice(t *testing.T, key, invoiceID string) (status string, amountPaid int64) {
	t.Helper()
	inv := stripeGet(t, key, "invoices/"+invoiceID)
	status, _ = inv["status"].(string)
	if paid, ok := inv["amount_paid"].(float64); ok {
		amountPaid = int64(paid)
	}
	return status, amountPaid
}

func sandboxInvoiceCount(t *testing.T, key, customerID string) int {
	t.Helper()
	list := stripeGet(t, key, "invoices?customer="+customerID)
	data, _ := list["data"].([]any)
	return len(data)
}

func centsOf(micros int64) int64 { return (micros + 5_000) / 10_000 }

// --- a deliberately tiny Stripe client, for the test's own setup ----
//
// The adapter under test uses the real SDK. This is only for arranging
// and inspecting fixtures, and it is hand-rolled so that a bug in the
// thing being tested cannot also be the thing verifying it.

func stripeGet(t *testing.T, key, path string) map[string]any {
	t.Helper()
	return stripeDo(t, key, http.MethodGet, path, nil)
}

func stripePost(t *testing.T, key, path string, form url.Values) map[string]any {
	t.Helper()
	return stripeDo(t, key, http.MethodPost, path, form)
}

func stripeDelete(t *testing.T, key, path string) map[string]any {
	t.Helper()
	return stripeDo(t, key, http.MethodDelete, path, nil)
}

func stripeDo(t *testing.T, key, method, path string, form url.Values) map[string]any {
	t.Helper()

	var body strings.Reader
	if form != nil {
		body = *strings.NewReader(form.Encode())
	}
	req, err := http.NewRequest(method, "https://api.stripe.com/v1/"+path, &body)
	require.NoError(t, err)
	req.SetBasicAuth(key, "")
	if form != nil {
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	}

	resp, err := (&http.Client{Timeout: 30 * time.Second}).Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	var out map[string]any
	require.NoError(t, decodeJSON(resp.Body, &out))
	if errObj, ok := out["error"].(map[string]any); ok {
		msg, _ := errObj["message"].(string)
		t.Fatalf("stripe %s %s: %s", method, path, msg)
	}
	return out
}

func decodeJSON(r interface{ Read([]byte) (int, error) }, v any) error {
	return json.NewDecoder(r).Decode(v)
}
