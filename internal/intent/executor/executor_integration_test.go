//go:build integration

package executor

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/mirrorstack-ai/billing-engine/internal/intent"
	"github.com/mirrorstack-ai/billing-engine/internal/intent/evidence/evidencetest"
	"github.com/mirrorstack-ai/billing-engine/internal/intent/predicate"
	"github.com/mirrorstack-ai/billing-engine/internal/intent/store"
	"github.com/mirrorstack-ai/billing-engine/internal/shared/testutil"
)

var (
	windowStart = time.Date(2026, 8, 4, 0, 0, 0, 0, time.UTC)
	windowEnd   = time.Date(2026, 9, 4, 0, 0, 0, 0, time.UTC)
	evalNow     = time.Date(2026, 8, 30, 0, 0, 0, 0, time.UTC)
)

const kindCycle intent.ChargeKind = intent.KindModuleUsage

// recordingCollector observes every dispatch, so "the executor did not
// call the provider" is something the test can see rather than assume.
type recordingCollector struct {
	mu     sync.Mutex
	calls  []Debit
	result CollectResult
	err    error
	onCall func()
}

func (c *recordingCollector) Collect(_ context.Context, d Debit) (CollectResult, error) {
	c.mu.Lock()
	c.calls = append(c.calls, d)
	onCall := c.onCall
	c.mu.Unlock()
	if onCall != nil {
		onCall()
	}
	return c.result, c.err
}

func (c *recordingCollector) count() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.calls)
}

func fullyEvidencedEnv() Environment {
	return Environment{
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

func sealedFixture(t *testing.T) intent.ChargeIntent {
	t.Helper()
	sealed, err := intent.Seal(validExecutorDraft())
	require.NoError(t, err)
	return sealed
}

// validExecutorDraft is the draft every executor fixture seals, so a test that
// needs a variant changes one field rather than copying twenty.
func validExecutorDraft() intent.Draft {
	return intent.Draft{
		Payer:             intent.Subject{Kind: "org", ID: "org-1"},
		Currency:          "USD",
		Lines:             []intent.Line{intent.NewLine("quiz.render", "quiz-core", "1.4.0", 1_000, 25)},
		Kind:              kindCycle,
		PriceBookRevision: "pb-2026-08",
		TermsRevision:     "terms-2026-01",
		Tax: intent.TaxDetermination{
			Resolved: true, Jurisdiction: "TW", RuleRevision: "tax-2026-05", AmountMicros: 1_250,
			Verification: intent.TaxNotApplicable,
		},
		AuthorizationID:       "auth-1",
		NoticePolicy:          "email/v1",
		SelectedRail:          "stripe",
		RoutingPolicyRevision: "routing-2026-08",
		ExecuteNotBefore:      windowStart,
		ExecuteNotAfter:       windowEnd,
		SourceFactKeys:        []string{"fact-1"},
	}
}

// ready seeds an intent that every clause accepts, so each test can
// break exactly one thing.
func ready(t *testing.T, s *store.Store) intent.ChargeIntent {
	t.Helper()
	ctx := context.Background()
	sealed := sealedFixture(t)
	require.NoError(t, s.SaveIntent(ctx, sealed))

	auth, err := intent.AuthorizeAccepted(intent.AuthorizationGrant{
		ID: "auth-1", Scope: intent.ScopeStanding,
		Subject:  intent.Subject{Kind: "org", ID: "org-1"},
		Currency: "USD", Kinds: []intent.ChargeKind{kindCycle},
		PerChargeCeiling: 1_000_000, PeriodCeiling: 5_000_000, FrequencyCeiling: 100, NoticeLeadTime: 24 * time.Hour, Provider: "stripe", MandateReference: "pm_test_1",
		TermsRevision: "terms-2026-01", PriceBook: "pb-2026-08",
		NoticePolicy:  "email/v1",
		EffectiveFrom: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		ExpiresAt:     time.Date(2027, 1, 1, 0, 0, 0, 0, time.UTC),
	})
	require.NoError(t, err)
	require.NoError(t, s.SaveAuthorization(ctx, auth))

	// The engine-issued acceptance the standing gate rests on. Without it
	// ClauseAuthorityEvidence refuses, which is the whole point of §12 item
	// 16 option C piece 2: a standing authorization is not evidence of
	// consent on its own.
	issueAndAccept(t, s, auth, intent.Subject{Kind: "org", ID: "org-1"})

	require.NoError(t, s.RecordNotice(ctx, store.NoticeReceipt{
		IntentDigest: sealed.Digest(), DeliveredDigest: sealed.Digest(),
		Policy: "email/v1", TerminalStatus: "delivered",
		DeliveredAt:          evalNow.Add(-48 * time.Hour),
		EligibilityNotBefore: evalNow.Add(-24 * time.Hour), RevocationPathFresh: true,
	}))
	require.NoError(t, s.AdvanceState(ctx, sealed.Digest(), "proposed", "eligible"))
	return sealed
}

func newExecutor(t *testing.T, s *store.Store, c Collector, env Environment) *Executor {
	t.Helper()
	e, err := New(s, c, evidencetest.Recorder(t), "executor-test",
		func() time.Time { return evalNow },
		func(context.Context) Environment { return env })
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return e
}

func TestAPermittedIntentIsCollectedOnce(t *testing.T) {
	pool := testutil.NewTestDB(t)
	s := store.New(pool)
	sealed := ready(t, s)

	collector := &recordingCollector{result: CollectResult{Succeeded: true, Reference: "ref-1"}}
	out, err := newExecutor(t, s, collector, fullyEvidencedEnv()).Execute(context.Background(), sealed.Digest())

	require.NoError(t, err)
	require.True(t, out.Permitted, "refused: %v", out.Refused)
	require.True(t, out.Settled)
	require.Equal(t, "ref-1", out.Reference)
	require.Equal(t, 1, collector.count())

	// The amount dispatched is the sealed total, not something
	// reassembled on the way out.
	require.Equal(t, sealed.TotalMicros(), collector.calls[0].AmountMicros)
	require.Equal(t, "intent-"+sealed.Digest(), collector.calls[0].IdempotencyKey)
}

// docs/DESIGN.md §4: "A refusal here mutates no provider." It must also
// take no claim — a refused intent that is left claimed can never be
// retried once the refusal is fixed.
func TestARefusalTouchesNothing(t *testing.T) {
	pool := testutil.NewTestDB(t)
	s := store.New(pool)
	ctx := context.Background()
	sealed := ready(t, s)

	env := fullyEvidencedEnv()
	env.BuildIdentified = false // VERIFICATION §2

	collector := &recordingCollector{result: CollectResult{Succeeded: true, Reference: "in_fixture"}}
	out, err := newExecutor(t, s, collector, env).Execute(ctx, sealed.Digest())

	require.NoError(t, err)
	require.False(t, out.Permitted)
	require.Contains(t, out.Refused, predicate.ClauseBuildIdentified)
	require.Zero(t, collector.count(), "a refused intent reached the provider")

	var claims int
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT count(*) FROM ms_billing.intent_settlement_claims WHERE intent_digest = $1`,
		sealed.Digest()).Scan(&claims))
	require.Zero(t, claims, "a refused intent was claimed, so it can never be retried")
}

// 🔴 The case the design bends around. An ambiguous answer is not a
// failure: treating it as one releases the claim and lets a second
// attempt charge a customer who may already have been charged.
func TestAnAmbiguousResultRetainsTheClaimAndRecordsNothing(t *testing.T) {
	pool := testutil.NewTestDB(t)
	s := store.New(pool)
	ctx := context.Background()
	sealed := ready(t, s)

	collector := &recordingCollector{result: CollectResult{Ambiguous: true}}
	out, err := newExecutor(t, s, collector, fullyEvidencedEnv()).Execute(ctx, sealed.Digest())

	require.NoError(t, err)
	require.True(t, out.Unresolved)
	require.False(t, out.Settled)

	var claimed bool
	var outcome *string
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT true, outcome FROM ms_billing.intent_settlement_claims WHERE intent_digest = $1`,
		sealed.Digest()).Scan(&claimed, &outcome))
	require.True(t, claimed, "the claim was released after an ambiguous answer")
	require.Nil(t, outcome, "an outcome was recorded that nobody established")

	// And nothing else can now attempt it.
	//
	// It is refused BEFORE the claim: the unresolved attempt is prior use, so
	// the predicate says no (prior_attempt_unresolved) and nothing reaches the
	// provider. Two independent things would each have stopped it — that
	// refusal and the settlement claim — and this asserts the STRONGER one
	// plus the property that actually matters, that no second request was
	// sent. Asserting ErrAlreadyClaimed here would be asserting the weaker of
	// them, and it stopped being what happens on 2026-09-01, when PriorUse
	// was finally populated and RefusalAttemptUnresolved became reachable.
	// ErrAlreadyClaimed itself stays covered by
	// TestConcurrentExecutorsCollectExactlyOnce and the store's own suite.
	second := &recordingCollector{result: CollectResult{Succeeded: true, Reference: "in_fixture"}}
	out2, err := newExecutor(t, s, second, fullyEvidencedEnv()).Execute(ctx, sealed.Digest())
	require.NoError(t, err)
	require.False(t, out2.Permitted, "a second attempt was permitted while the first is unresolved")
	require.False(t, out2.Settled, "an intent whose first attempt is unresolved settled again")
	require.Zero(t, second.count(), "a second attempt reached the provider after an ambiguous first")

	// The reason, not just the refusal: the clause has several, and this must
	// be the one that knows an attempt may already have taken the money.
	auth, err := s.LoadAuthorization(ctx, sealed.AuthorizationID())
	require.NoError(t, err)
	prior, err := s.PriorUseFor(ctx, auth.ID(), auth.Grant().EffectiveFrom)
	require.NoError(t, err)
	require.Equal(t, 1, prior.Unresolved, "the in-flight claim was not seen as unresolved prior use")
	require.Contains(t, auth.Permits(sealed, evalNow, prior).Refusals, intent.RefusalAttemptUnresolved)
}

// A transport error is ambiguous for the same reason: the request may
// have arrived.
func TestATransportErrorIsTreatedAsAmbiguous(t *testing.T) {
	pool := testutil.NewTestDB(t)
	s := store.New(pool)
	sealed := ready(t, s)

	collector := &recordingCollector{err: errors.New("connection reset")}
	out, err := newExecutor(t, s, collector, fullyEvidencedEnv()).Execute(context.Background(), sealed.Digest())

	require.NoError(t, err)
	require.True(t, out.Unresolved, "a transport error was resolved one way or the other")
}

// Two executors racing one intent: one dispatches, one is refused, and
// the provider is called exactly once.
func TestConcurrentExecutorsCollectExactlyOnce(t *testing.T) {
	pool := testutil.NewTestDB(t)
	s := store.New(pool)
	sealed := ready(t, s)

	collector := &recordingCollector{
		result: CollectResult{Succeeded: true, Reference: "ref"},
		// Hold inside the dispatch so the racers genuinely overlap.
		onCall: func() { time.Sleep(50 * time.Millisecond) },
	}

	const racers = 6
	var wg sync.WaitGroup
	var mu sync.Mutex
	settled, stopped := 0, 0

	for i := 0; i < racers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			out, err := newExecutor(t, s, collector, fullyEvidencedEnv()).
				Execute(context.Background(), sealed.Digest())
			mu.Lock()
			defer mu.Unlock()
			switch {
			case err == nil && out.Settled:
				settled++
			case errors.Is(err, ErrAlreadyClaimed):
				// Lost the claim.
				stopped++
			case err == nil && !out.Permitted:
				// Lost to the PREDICATE instead: the winner had
				// already advanced the intent past eligible. Two
				// independent things stop a second settlement, and
				// which one wins is a matter of timing.
				stopped++
			default:
				t.Errorf("a racer neither settled nor was stopped: out=%+v err=%v", out, err)
			}
		}()
	}
	wg.Wait()

	// What matters is that exactly one settled and the provider was
	// touched once. An earlier version also asserted that every loser
	// lost at the CLAIM, which made it timing-dependent: CI showed a
	// racer that evaluated after the winner advanced the state and was
	// refused by the predicate instead. Both are correct stops, and
	// pinning which one wins tests the scheduler rather than the code.
	require.Equal(t, 1, settled, "more than one executor settled the same intent")
	require.Equal(t, racers-1, stopped, "a racer was neither settled nor stopped")
	require.Equal(t, 1, collector.count(), "the provider was called more than once for one intent")
}

// A missing authorization is a refusal, not an error. "Not found" must
// never read as "allowed".
func TestAMissingAuthorizationRefuses(t *testing.T) {
	pool := testutil.NewTestDB(t)
	s := store.New(pool)
	ctx := context.Background()

	sealed := sealedFixture(t)
	require.NoError(t, s.SaveIntent(ctx, sealed))
	require.NoError(t, s.AdvanceState(ctx, sealed.Digest(), "proposed", "eligible"))
	// No authorization saved.

	collector := &recordingCollector{result: CollectResult{Succeeded: true, Reference: "in_fixture"}}
	out, err := newExecutor(t, s, collector, fullyEvidencedEnv()).Execute(ctx, sealed.Digest())

	require.NoError(t, err)
	require.False(t, out.Permitted)
	require.Zero(t, collector.count())
}

// The unbuilt clauses refuse by default, so an executor handed an empty
// environment collects nothing. That is the honest state of this tree.
func TestAnEmptyEnvironmentRefuses(t *testing.T) {
	pool := testutil.NewTestDB(t)
	s := store.New(pool)
	sealed := ready(t, s)

	collector := &recordingCollector{result: CollectResult{Succeeded: true, Reference: "in_fixture"}}
	out, err := newExecutor(t, s, collector, Environment{}).Execute(context.Background(), sealed.Digest())

	require.NoError(t, err)
	require.False(t, out.Permitted)
	require.Zero(t, collector.count())
	require.NotEmpty(t, out.Refused)
}

// A row that no longer hashes to its own digest never reaches the
// predicate, let alone the provider.
func TestATamperedIntentIsNeverEvaluated(t *testing.T) {
	pool := testutil.NewTestDB(t)
	s := store.New(pool)
	ctx := context.Background()
	sealed := ready(t, s)

	_, err := pool.Exec(ctx,
		`UPDATE ms_billing.charge_intent_lines SET quantity = 1, amount_micros = 25
		  WHERE intent_digest = $1 AND line_index = 0`, sealed.Digest())
	require.NoError(t, err)

	collector := &recordingCollector{result: CollectResult{Succeeded: true, Reference: "in_fixture"}}
	_, err = newExecutor(t, s, collector, fullyEvidencedEnv()).Execute(ctx, sealed.Digest())

	require.ErrorIs(t, err, intent.ErrDigestMismatch)
	require.Zero(t, collector.count())
}

// docs/DESIGN.md §6 selects the obligation formula BY KIND:
//
//	grossObligation = serviceGrossObligation OR fundingGrossObligation OR
//	                  collectionGrossObligation, selected by intent kind
//
// and gives the reason in the same breath: "so a stored-value purchase cannot
// end up with zero principal by borrowing the service-line formula". The
// service formula subtracts rating credits; a credit purchase's principal is
// cash the customer is paying, which credits may not reduce.
//
// The three coincide today because rating credits are unimplemented. This pins
// the SELECTION so they cannot silently diverge later, and pins that an
// unrecognised kind refuses rather than borrowing whichever formula is first.
func TestTheFundingFormulaIsSelectedByKind(t *testing.T) {
	for _, kind := range []intent.ChargeKind{
		intent.KindPlatformBase, intent.KindModuleUsage, intent.KindTax,
		intent.KindCreditPurchase, intent.KindAutoTopUp, intent.KindSubscriptionStart,
		intent.KindCollectReceivable,
	} {
		t.Run(string(kind), func(t *testing.T) {
			sealed := sealKind(t, kind, 20_000_000)
			plan, err := fundingFor(sealed)
			if err != nil {
				t.Fatalf("a catalog kind has no funding formula: %v", err)
			}
			if plan.GrossMicros != 20_000_000 {
				t.Fatalf("GrossMicros = %d, want 20_000_000", plan.GrossMicros)
			}
			// §6 for the two stored-value kinds: "walletFunding = 0;
			// providerRemainder = grossObligation". A purchase of credit
			// cannot be paid for with credit.
			if kind == intent.KindCreditPurchase || kind == intent.KindAutoTopUp {
				if plan.WalletAllocationMicros != 0 {
					t.Fatalf("a stored-value purchase was funded from the wallet (%d) — "+
						"credit cannot pay for credit", plan.WalletAllocationMicros)
				}
			}
			if plan.WalletAllocationMicros+plan.ProviderRemainderMicros != plan.GrossMicros {
				t.Fatal("the funding plan does not balance against its own gross")
			}
		})
	}
}

// An unrecognised kind must refuse. Falling through to a formula would fund a
// document nobody wrote a rule for.
// An unknown charge kind is refused — now at SEAL rather than at funding.
//
// fundingFor used to carry the by-kind formula selection and error on an
// unrecognised kind. That selection moved into intent.Seal, where the kind is
// sealed, so an unknown kind refuses to become a document at all rather than
// refusing to execute one. Strictly earlier and cheaper.
//
// The original concern still holds and is what this asserts: a corrupted or
// future row carrying a kind outside the catalog must not settle. Rehydrate
// re-seals every stored row, so that path is where the refusal now lands.
func TestAnUnknownKindIsRefusedWhenRehydrated(t *testing.T) {
	sealed := sealKind(t, intent.KindModuleUsage, 1_000_000)

	notBefore, notAfter := sealed.ExecutionWindow()
	stored := intent.Stored{
		Digest:                 sealed.Digest(),
		Payer:                  sealed.Payer(),
		Currency:               sealed.Currency(),
		Lines:                  sealed.Lines(),
		PriceBookRevision:      sealed.PriceBookRevision(),
		TermsRevision:          sealed.TermsRevision(),
		Kind:                   intent.ChargeKind("not_in_the_catalog"),
		Tax:                    sealed.Tax(),
		AuthorizationID:        sealed.AuthorizationID(),
		NoticePolicy:           sealed.NoticePolicy(),
		ExecuteNotBefore:       notBefore,
		ExecuteNotAfter:        notAfter,
		SourceFactKeys:         sealed.SourceFactKeys(),
		SubtotalMicros:         sealed.SubtotalMicros(),
		TotalMicros:            sealed.TotalMicros(),
		WalletAllocationMicros: sealed.WalletAllocationMicros(),
	}

	if _, err := intent.Rehydrate(stored); err == nil {
		t.Fatal("a stored row with a kind outside the catalog rehydrated; " +
			"an unknown kind must never become an executable intent")
	}
}

// sealKind seals a minimal intent of the given kind, for tests about the
// KIND rather than about the amounts.
func sealKind(t *testing.T, kind intent.ChargeKind, micros int64) intent.ChargeIntent {
	t.Helper()
	sealed, err := intent.Seal(intent.Draft{
		Payer:                 intent.Subject{Kind: "user", ID: "acct-1"},
		Currency:              "usd",
		Lines:                 []intent.Line{intent.NewLine("d", "m", "1", 1, micros)},
		Kind:                  kind,
		PriceBookRevision:     "pb-1",
		TermsRevision:         "terms-1",
		Tax:                   intent.TaxDetermination{Resolved: true, Jurisdiction: "TW", RuleRevision: "tax-1", Verification: intent.TaxNotApplicable},
		AuthorizationID:       "auth-1",
		NoticePolicy:          "email/v1",
		SelectedRail:          "stripe",
		RoutingPolicyRevision: "routing-2026-08",
		ExecuteNotBefore:      windowStart,
		ExecuteNotAfter:       windowEnd,
		SourceFactKeys:        []string{"f"},
	})
	if err != nil {
		t.Fatalf("Seal(%s): %v", kind, err)
	}
	return sealed
}

// issueAndAccept records that the engine showed the customer this
// authorization's terms and that they answered.
//
// It is what a real deployment does before minting: the disclosure digest is
// the authorization's own, so the challenge is for exactly the document being
// charged under.
func issueAndAccept(t *testing.T, s *store.Store, auth intent.BillingAuthorization, payer intent.Subject) {
	t.Helper()
	ctx := context.Background()

	require.NoError(t, s.IssueAcceptance(ctx, store.IssuedAcceptance{
		AuthorizationID:  auth.ID(),
		DisclosureDigest: auth.AcceptanceDigest(),
		Payer:            payer,
		Nonce:            "nonce-" + auth.ID(),
		Audience:         "customer",
		ReplayIdentity:   "replay-" + auth.ID(),
		IssuedAt:         evalNow.Add(-72 * time.Hour),
		ExpiresAt:        evalNow.Add(365 * 24 * time.Hour),
	}))
	require.NoError(t, s.AcceptIssuedAcceptance(ctx,
		auth.ID(), auth.AcceptanceDigest(), evalNow.Add(-71*time.Hour)))
}

// 🔴 An authorization with NO issued acceptance must not collect.
//
// This is the case the whole of §12 item 16 option C piece 2 exists for: a
// standing authorization, valid in every other respect, whose terms the
// engine never showed anybody. Before this wave it collected, because the
// gate was `AcceptanceDigest() != ""` and the digest is always set.
//
// It also exercises the store's missing-row path, which nothing else does:
// LoadStandingAcceptance returns the ZERO value for a row that is not there,
// and the zero value must authorise nothing. A version that returned
// "issued and accepted" for a missing row passes every other test in this
// package.
func TestAnAuthorizationWithNoIssuedAcceptanceIsRefused(t *testing.T) {
	pool := testutil.NewTestDB(t)
	s := store.New(pool)
	ctx := context.Background()

	sealed := ready(t, s)

	// Remove the challenge the fixture issued, leaving the authorization
	// otherwise untouched — exactly the state of an authorization minted
	// without ever showing the customer its terms.
	_, err := pool.Exec(ctx,
		`DELETE FROM ms_billing.authorization_acceptances WHERE authorization_id = 'auth-1'`)
	require.Error(t, err, "the acceptance table is append-only; the fixture cannot be cleared this way")

	// So issue the intent against an authorization that never had one.
	other := freshAuthorizationWithoutAcceptance(t, s)
	orphan := sealAgainst(t, s, other)

	collector := &recordingCollector{result: CollectResult{Succeeded: true, Reference: "in_fixture"}}
	out, err := newExecutor(t, s, collector, fullyEvidencedEnv()).Execute(ctx, orphan.Digest())
	require.NoError(t, err)

	require.False(t, out.Permitted,
		"a standing authorization whose terms were never shown to anyone collected")
	require.Contains(t, out.Refused, predicate.ClauseAuthorityEvidence)
	require.Zero(t, collector.count(), "a refused intent reached the provider")

	_ = sealed
}

// freshAuthorizationWithoutAcceptance mints a standing authorization the
// engine never issued a document for.
func freshAuthorizationWithoutAcceptance(t *testing.T, s *store.Store) intent.BillingAuthorization {
	t.Helper()
	auth, err := intent.AuthorizeAccepted(intent.AuthorizationGrant{
		ID: "auth-no-acceptance", Scope: intent.ScopeStanding,
		Subject:  intent.Subject{Kind: "org", ID: "org-1"},
		Currency: "USD", Kinds: []intent.ChargeKind{kindCycle},
		PerChargeCeiling: 1_000_000, PeriodCeiling: 500_000_000, FrequencyCeiling: 100,
		NoticeLeadTime: 24 * time.Hour, Provider: "stripe", MandateReference: "pm_test_1",
		TermsRevision: "terms-2026-01", PriceBook: "pb-2026-08",
		NoticePolicy:  "email/v1",
		EffectiveFrom: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		ExpiresAt:     time.Date(2027, 1, 1, 0, 0, 0, 0, time.UTC),
	})
	require.NoError(t, err)
	require.NoError(t, s.SaveAuthorization(context.Background(), auth))
	// Deliberately no issueAndAccept: that is what this test is about.
	return auth
}

// sealAgainst seals an otherwise-executable intent naming a given
// authorization, and walks it to eligible with a delivered notice.
func sealAgainst(t *testing.T, s *store.Store, auth intent.BillingAuthorization) intent.ChargeIntent {
	t.Helper()
	ctx := context.Background()

	d := validExecutorDraft()
	d.AuthorizationID = auth.ID()
	sealed, err := intent.Seal(d)
	require.NoError(t, err)
	require.NoError(t, s.SaveIntent(ctx, sealed))

	require.NoError(t, s.RecordNotice(ctx, store.NoticeReceipt{
		IntentDigest: sealed.Digest(), DeliveredDigest: sealed.Digest(),
		Policy: "email/v1", TerminalStatus: "delivered",
		DeliveredAt:          evalNow.Add(-48 * time.Hour),
		EligibilityNotBefore: evalNow.Add(-24 * time.Hour), RevocationPathFresh: true,
	}))
	require.NoError(t, s.AdvanceState(ctx, sealed.Digest(), "proposed", "eligible"))
	return sealed
}
