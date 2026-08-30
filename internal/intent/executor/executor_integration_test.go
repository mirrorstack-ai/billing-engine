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
	sealed, err := intent.Seal(intent.Draft{
		Payer:             intent.Subject{Kind: "org", ID: "org-1"},
		Currency:          "USD",
		Lines:             []intent.Line{intent.NewLine("quiz.render", "quiz-core", "1.4.0", 1_000, 25)},
		Kind:              kindCycle,
		PriceBookRevision: "pb-2026-08",
		TermsRevision:     "terms-2026-01",
		Tax: intent.TaxDetermination{
			Resolved: true, Jurisdiction: "TW", RuleRevision: "tax-2026-05", AmountMicros: 1_250,
		},
		AuthorizationID:  "auth-1",
		NoticePolicy:     "email/v1",
		ExecuteNotBefore: windowStart,
		ExecuteNotAfter:  windowEnd,
		SourceFactKeys:   []string{"fact-1"},
	})
	require.NoError(t, err)
	return sealed
}

// ready seeds an intent that every clause accepts, so each test can
// break exactly one thing.
func ready(t *testing.T, s *store.Store) intent.ChargeIntent {
	t.Helper()
	ctx := context.Background()
	sealed := sealedFixture(t)
	require.NoError(t, s.SaveIntent(ctx, sealed))

	auth, err := intent.Authorize(intent.AuthorizationGrant{
		ID: "auth-1", Scope: intent.ScopeStanding,
		Subject:  intent.Subject{Kind: "org", ID: "org-1"},
		Currency: "USD", Kinds: []intent.ChargeKind{kindCycle},
		PerChargeCeiling: 1_000_000, PeriodCeiling: 5_000_000, FrequencyCeiling: 100, Provider: "stripe", MandateReference: "pm_test_1",
		TermsRevision: "terms-2026-01", PriceBook: "pb-2026-08",
		NoticePolicy:     "email/v1",
		EffectiveFrom:    time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		ExpiresAt:        time.Date(2027, 1, 1, 0, 0, 0, 0, time.UTC),
		AcceptanceDigest: "accept-1",
	})
	require.NoError(t, err)
	require.NoError(t, s.SaveAuthorization(ctx, auth))

	require.NoError(t, s.RecordNotice(ctx, store.NoticeReceipt{
		IntentDigest: sealed.Digest(), DeliveredDigest: sealed.Digest(),
		Policy: "email/v1", TerminalStatus: "delivered",
		EligibilityNotBefore: evalNow.Add(-24 * time.Hour), RevocationPathFresh: true,
	}))
	require.NoError(t, s.AdvanceState(ctx, sealed.Digest(), "proposed", "eligible"))
	return sealed
}

func newExecutor(s *store.Store, c Collector, env Environment) *Executor {
	return New(s, c, "executor-test",
		func() time.Time { return evalNow },
		func(context.Context) Environment { return env })
}

func TestAPermittedIntentIsCollectedOnce(t *testing.T) {
	pool := testutil.NewTestDB(t)
	s := store.New(pool)
	sealed := ready(t, s)

	collector := &recordingCollector{result: CollectResult{Succeeded: true, Reference: "ref-1"}}
	out, err := newExecutor(s, collector, fullyEvidencedEnv()).Execute(context.Background(), sealed.Digest())

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

	collector := &recordingCollector{result: CollectResult{Succeeded: true}}
	out, err := newExecutor(s, collector, env).Execute(ctx, sealed.Digest())

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
	out, err := newExecutor(s, collector, fullyEvidencedEnv()).Execute(ctx, sealed.Digest())

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
	second := &recordingCollector{result: CollectResult{Succeeded: true}}
	_, err = newExecutor(s, second, fullyEvidencedEnv()).Execute(ctx, sealed.Digest())
	require.ErrorIs(t, err, ErrAlreadyClaimed)
	require.Zero(t, second.count(), "a second attempt reached the provider after an ambiguous first")
}

// A transport error is ambiguous for the same reason: the request may
// have arrived.
func TestATransportErrorIsTreatedAsAmbiguous(t *testing.T) {
	pool := testutil.NewTestDB(t)
	s := store.New(pool)
	sealed := ready(t, s)

	collector := &recordingCollector{err: errors.New("connection reset")}
	out, err := newExecutor(s, collector, fullyEvidencedEnv()).Execute(context.Background(), sealed.Digest())

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
			out, err := newExecutor(s, collector, fullyEvidencedEnv()).
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

	collector := &recordingCollector{result: CollectResult{Succeeded: true}}
	out, err := newExecutor(s, collector, fullyEvidencedEnv()).Execute(ctx, sealed.Digest())

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

	collector := &recordingCollector{result: CollectResult{Succeeded: true}}
	out, err := newExecutor(s, collector, Environment{}).Execute(context.Background(), sealed.Digest())

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

	collector := &recordingCollector{result: CollectResult{Succeeded: true}}
	_, err = newExecutor(s, collector, fullyEvidencedEnv()).Execute(ctx, sealed.Digest())

	require.ErrorIs(t, err, intent.ErrDigestMismatch)
	require.Zero(t, collector.count())
}
