//go:build integration

package executor

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/require"

	"time"

	"github.com/mirrorstack-ai/billing-engine/internal/intent"
	"github.com/mirrorstack-ai/billing-engine/internal/intent/store"
	"github.com/mirrorstack-ai/billing-engine/internal/shared/testutil"
)

func quiet() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

// 🔴 THE ARMING PATH, EXERCISED.
//
// cmd/intent-executor carried no work loop, and said why: "a poller here
// would be an arming path that has never been exercised against a real
// intent, which docs/SECURITY.md treats as its own kind of defect."
//
// That objection is answered by exercising it, not by asserting it in a
// comment. This drives RunOnce against a real Postgres and a real sealed
// intent, all the way to a settlement.
func TestAPassSettlesAnEligibleIntent(t *testing.T) {
	pool := testutil.NewTestDB(t)
	s := store.New(pool)
	ctx := context.Background()
	sealed := ready(t, s)

	collector := &recordingCollector{result: CollectResult{Succeeded: true, Reference: "ref-loop"}}
	result, err := RunOnce(ctx, s, newExecutor(t, s, collector, fullyEvidencedEnv()), 10, quiet())
	require.NoError(t, err)

	require.Equal(t, 1, result.Considered, "the pass did not find the pending intent")
	require.Equal(t, 1, result.Settled)
	require.Zero(t, result.Refused)
	require.Zero(t, result.Errors)

	// The intent really settled: the claim carries an outcome.
	var outcome *string
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT outcome FROM ms_billing.intent_settlement_claims WHERE intent_digest = $1`,
		sealed.Digest()).Scan(&outcome))
	require.NotNil(t, outcome)
	require.Equal(t, "succeeded", *outcome)
}

// 🔴 A pass that finds work and refuses all of it is NOT the same as a pass
// that finds nothing, and the summary has to say which.
//
// Every deployment today has every gate false, so this is the state a real
// invocation is in. An executor whose summary collapsed the two would look
// idle while it was in fact refusing every intent it was given — the exact
// confusion cmd/intent-executor's readiness checks already refuse to create.
func TestAPassThatRefusesEverythingSaysSoAndNamesTheGate(t *testing.T) {
	pool := testutil.NewTestDB(t)
	s := store.New(pool)
	ctx := context.Background()
	ready(t, s)

	// The honest production Environment: nothing is evidenced.
	collector := &recordingCollector{result: CollectResult{Succeeded: true}}
	result, err := RunOnce(ctx, s, newExecutor(t, s, collector, Environment{}), 10, quiet())
	require.NoError(t, err)

	require.Equal(t, 1, result.Considered,
		"the pass reported no work, when in fact it had work it refused")
	require.Equal(t, 1, result.Refused)
	require.Zero(t, result.Settled)
	require.NotEmpty(t, result.RefusedClauses,
		"the pass refused an intent without naming a single clause, so an operator "+
			"cannot tell which gate is holding the deployment shut")

	require.Zero(t, collector.count(),
		"a refused intent reached the provider. A refusal must mutate nothing.")
}

// 🔴 An intent that is CLAIMED but still eligible must not be handed out
// again. This is the double-charge case.
//
// When the rail's answer does not establish whether money moved, the executor
// retains the claim and does NOT advance the state — deliberately, because
// releasing it "would let a second attempt charge a customer who may already
// have been charged". The intent therefore sits in `eligible` WITH a claim,
// and only the claim filter keeps a second pass off it.
//
// The first version of this test settled the intent instead, which advanced
// the state out of the executable set — so the state filter alone excluded it
// and the claim filter was never exercised. Removing the claim filter left
// that test green. This one is red without it.
func TestAnUnresolvedIntentIsNotOfferedAgain(t *testing.T) {
	pool := testutil.NewTestDB(t)
	s := store.New(pool)
	ctx := context.Background()
	sealed := ready(t, s)

	// Ambiguous: the rail errored after the claim was taken.
	first := &recordingCollector{err: errors.New("timeout after dispatch")}
	result, err := RunOnce(ctx, s, newExecutor(t, s, first, fullyEvidencedEnv()), 10, quiet())
	require.NoError(t, err)
	require.Equal(t, 1, result.Unresolved, "the fixture did not reach the ambiguous branch")

	// The intent is still eligible — the state was deliberately not advanced.
	state, err := s.State(ctx, sealed.Digest())
	require.NoError(t, err)
	require.Equal(t, "eligible", state,
		"the fixture no longer exercises the claim filter: the state moved out of the "+
			"executable set, so this test would pass with the filter removed")

	second := &recordingCollector{result: CollectResult{Succeeded: true}}
	again, err := RunOnce(ctx, s, newExecutor(t, s, second, fullyEvidencedEnv()), 10, quiet())
	require.NoError(t, err)

	require.Zero(t, again.Considered,
		"a claimed, unresolved intent was offered for execution again. The customer may "+
			"already have been charged, and this pass would charge them a second time.")
	require.Zero(t, second.count(),
		"the provider was called a second time for one intent (INV-008)")
}

// A settled intent is also not offered again — by the state filter this time.
// Both exclusions matter, and each has its own test so neither can be removed
// under cover of the other.
func TestASettledIntentIsNotOfferedAgain(t *testing.T) {
	pool := testutil.NewTestDB(t)
	s := store.New(pool)
	ctx := context.Background()
	ready(t, s)

	first := &recordingCollector{result: CollectResult{Succeeded: true, Reference: "ref-1"}}
	_, err := RunOnce(ctx, s, newExecutor(t, s, first, fullyEvidencedEnv()), 10, quiet())
	require.NoError(t, err)

	second := &recordingCollector{result: CollectResult{Succeeded: true, Reference: "ref-2"}}
	result, err := RunOnce(ctx, s, newExecutor(t, s, second, fullyEvidencedEnv()), 10, quiet())
	require.NoError(t, err)

	require.Zero(t, result.Considered, "a settled intent was offered for execution a second time")
	require.Zero(t, second.count(), "the provider was called a second time for one intent")
}

// A pass must be bounded. An unbounded one holds the deployment's only
// mutation-capable credential open for as long as a backlog takes, and a
// Lambda invocation that cannot finish is one that gets killed mid-collection.
func TestAPassIsBounded(t *testing.T) {
	pool := testutil.NewTestDB(t)
	s := store.New(pool)
	ctx := context.Background()

	for i := 0; i < 5; i++ {
		readyN(t, s, i)
	}

	collector := &recordingCollector{result: CollectResult{Succeeded: true}}
	result, err := RunOnce(ctx, s, newExecutor(t, s, collector, fullyEvidencedEnv()), 2, quiet())
	require.NoError(t, err)
	require.Equal(t, 2, result.Considered, "the limit was not honoured")
	require.Equal(t, 2, collector.count())

	// What was not executed is not claimed, so the next pass picks it up.
	next, err := s.PendingExecution(ctx, 10)
	require.NoError(t, err)
	require.Len(t, next, 3, "intents left by a bounded pass must remain available")
}

// A work source is required. A loop that ran with none would report a clean
// pass over nothing, which reads exactly like a healthy idle deployment.
func TestAPassWithNoWorkSourceIsRefused(t *testing.T) {
	pool := testutil.NewTestDB(t)
	s := store.New(pool)
	exec := newExecutor(t, s, &recordingCollector{}, fullyEvidencedEnv())

	_, err := RunOnce(context.Background(), nil, exec, 10, quiet())
	require.ErrorIs(t, err, ErrNoWorkSource)

	_, err = RunOnce(context.Background(), s, nil, 10, quiet())
	require.ErrorIs(t, err, ErrNoWorkSource)
}

// 🔴 One intent that ERRORS must not stop the pass.
//
// The first version of this test used a collector that returned an error,
// which the executor converts into an Unresolved OUTCOME and no error at all
// — so the loop's error branch was never reached and `return result, err`
// there left the test green. This makes Execute itself fail, by corrupting a
// stored intent so it cannot rehydrate.
//
// It matters because the corrupt intent is the OLDEST, and the query orders
// oldest-first: a pass that stopped on it would starve every intent behind it
// forever, and the backlog would grow silently.
func TestOneErroringIntentDoesNotStopThePass(t *testing.T) {
	pool := testutil.NewTestDB(t)
	s := store.New(pool)
	ctx := context.Background()

	poisoned := ready(t, s)

	// Rewrite a sealed column so Rehydrate refuses the row. The seal refuses
	// this, which is the point — reaching it takes a session that has
	// disabled the control, exactly as a restored backup or a stray migration
	// would.
	_, err := pool.Exec(ctx, `ALTER TABLE ms_billing.charge_intents DISABLE TRIGGER charge_intents_sealed`)
	require.NoError(t, err)
	_, err = pool.Exec(ctx,
		`UPDATE ms_billing.charge_intents SET notice_policy = 'sms/v1' WHERE digest = $1`,
		poisoned.Digest())
	require.NoError(t, err)
	_, err = pool.Exec(ctx, `ALTER TABLE ms_billing.charge_intents ENABLE TRIGGER charge_intents_sealed`)
	require.NoError(t, err)

	for i := 0; i < 2; i++ {
		readyN(t, s, i)
	}

	collector := &recordingCollector{result: CollectResult{Succeeded: true}}
	result, err := RunOnce(ctx, s, newExecutor(t, s, collector, fullyEvidencedEnv()), 10, quiet())
	require.NoError(t, err, "one intent's failure aborted the whole pass")

	require.Equal(t, 3, result.Considered)
	require.Equal(t, 1, result.Errors, "the corrupt intent did not error; the fixture is wrong")
	require.Equal(t, 2, result.Settled,
		"the intents behind the corrupt one were not executed, so one poisoned document "+
			"starves the backlog behind it")
	require.Equal(t, 2, collector.count())
}

// readyN seeds a distinct executable intent, so a test can fill a batch.
//
// It varies the quantity and the source fact, both of which are inside the
// digest, so each call produces a genuinely different document rather than a
// second reference to one.
func readyN(t *testing.T, s *store.Store, n int) intent.ChargeIntent {
	t.Helper()
	ctx := context.Background()

	sealed, err := intent.Seal(intent.Draft{
		Payer:             intent.Subject{Kind: "org", ID: "org-1"},
		Currency:          "USD",
		Lines:             []intent.Line{intent.NewLine("quiz.render", "quiz-core", "1.4.0", int64(1_000+n), 25)},
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
		SourceFactKeys:        []string{"fact-" + itoa(n)},
	})
	require.NoError(t, err)
	require.NoError(t, s.SaveIntent(ctx, sealed))

	// The same authorization ready() builds. Saving it again is a no-op on
	// its terms; the period ceiling is raised so a batch fits inside it.
	auth, err := intent.AuthorizeAccepted(intent.AuthorizationGrant{
		ID: "auth-1", Scope: intent.ScopeStanding,
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
	require.NoError(t, s.SaveAuthorization(ctx, auth))

	// The engine-issued acceptance the standing gate rests on. readyN builds
	// its own authorization, so it needs its own challenge — the sibling
	// helper in executor_integration_test.go issues one for ready()'s.
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

func itoa(v int) string {
	if v == 0 {
		return "0"
	}
	var b []byte
	for v > 0 {
		b = append([]byte{byte('0' + v%10)}, b...)
		v /= 10
	}
	return string(b)
}

// 🔴 An intent in a terminal non-settled state must never be executed.
//
// `voided`, `canceled` and `expired` are places an intent goes without ever
// being claimed, so the claim filter does not exclude them — only the
// lifecycle filter does. Executing an expired intent would charge a customer
// against a document that had already stopped applying to them.
//
// This is the case the state filter exists for, and the settled-intent test
// does NOT cover it: a settled intent carries a claim, so the claim filter
// excludes it either way and the state filter could be removed unnoticed.
func TestATerminalIntentIsNeverOffered(t *testing.T) {
	pool := testutil.NewTestDB(t)
	s := store.New(pool)
	ctx := context.Background()

	for _, state := range []string{"voided", "canceled", "expired"} {
		t.Run(state, func(t *testing.T) {
			sealed := readyN(t, s, terminalFixtureIndex(state))
			require.NoError(t, s.AdvanceState(ctx, sealed.Digest(), "eligible", state))

			collector := &recordingCollector{result: CollectResult{Succeeded: true}}
			result, err := RunOnce(ctx, s, newExecutor(t, s, collector, fullyEvidencedEnv()), 10, quiet())
			require.NoError(t, err)

			require.Zero(t, result.Considered,
				"an intent in state %q was offered for execution. It carries no claim, so "+
					"only the lifecycle filter stands between it and a charge.", state)
			require.Zero(t, collector.count(),
				"the provider was called for an intent in state %q", state)
		})
	}
}

// terminalFixtureIndex gives each subtest its own document, so one subtest's
// intent cannot be counted by another's pass.
func terminalFixtureIndex(state string) int {
	switch state {
	case "voided":
		return 101
	case "canceled":
		return 102
	default:
		return 103
	}
}
