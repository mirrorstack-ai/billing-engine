//go:build integration

package executor

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/mirrorstack-ai/billing-engine/internal/intent"
	"github.com/mirrorstack-ai/billing-engine/internal/intent/predicate"
	"github.com/mirrorstack-ai/billing-engine/internal/intent/store"
	"github.com/mirrorstack-ai/billing-engine/internal/shared/testutil"
)

// 🔴 THE TESTS THAT WOULD HAVE CAUGHT TWO INERT CEILINGS.
//
// intent.PriorUse existed, predicate.SealedState carried it, and
// BillingAuthorization.Permits read it — but nothing outside a test ever
// ASSIGNED it. The executor built its SealedState without the field, so every
// real evaluation saw the zero value, and:
//
//   - the PERIOD ceiling became a second per-charge ceiling. `0 + total >
//     ceiling` admits one charge at the ceiling, then another, forever.
//   - the FREQUENCY ceiling never refused. `0 + 1 > ceiling` is false for
//     every ceiling a grant can carry.
//
// Every existing ceiling test constructs a PriorUse by hand and calls Permits
// directly, so all of them passed against an executor that supplied none. The
// gap is only visible from the executor's own entry point with real prior use
// in the database, which is what these drive.

// priorSettlement puts a SUCCEEDED settlement of the given size on the same
// authorization the ready() fixture uses, so it counts as prior spend.
func priorSettlement(t *testing.T, s *store.Store, micros int64) {
	t.Helper()
	ctx := context.Background()

	sealed := sealKind(t, kindCycle, micros)
	require.NoError(t, s.SaveIntent(ctx, sealed))
	require.NoError(t, s.ClaimSettlement(ctx, sealed.Digest(), "prior-use-test"))
	require.NoError(t, s.RecordOutcome(ctx, sealed.Digest(), "succeeded", "in_test_ref", evalNow))
}

// priorAttempt puts a FAILED settlement on the same authorization. It consumes
// an attempt and no spend — "a failed attempt still consumed one, which is the
// point: retrying forever is the runaway the frequency ceiling exists to stop".
func priorAttempt(t *testing.T, s *store.Store, micros int64) {
	t.Helper()
	ctx := context.Background()

	sealed := sealKind(t, kindCycle, micros)
	require.NoError(t, s.SaveIntent(ctx, sealed))
	require.NoError(t, s.ClaimSettlement(ctx, sealed.Digest(), "prior-use-test"))
	require.NoError(t, s.RecordOutcome(ctx, sealed.Digest(), "failed", "", evalNow))
}

// The period ceiling must bound the PERIOD, not each charge.
//
// ready()'s grant carries PeriodCeiling 5_000_000 and the fixture intent is a
// few tens of thousands of micros, so it fits easily on its own. After
// 5_000_000 has already been collected under the same authorization it must
// not fit, and before this wiring it did.
func TestThePeriodCeilingCountsWhatWasAlreadyCollected(t *testing.T) {
	pool := testutil.NewTestDB(t)
	s := store.New(pool)
	sealed := ready(t, s)

	priorSettlement(t, s, 5_000_000)

	collector := &recordingCollector{result: CollectResult{Succeeded: true, Reference: "in_1"}}
	out, err := newExecutor(t, s, collector, fullyEvidencedEnv()).Execute(context.Background(), sealed.Digest())
	require.NoError(t, err)

	require.False(t, out.Permitted,
		"the period ceiling admitted a charge after the whole period ceiling had already "+
			"been collected under the same authorization — it is bounding each charge, not the period")
	require.Contains(t, out.Refused, predicate.ClauseWithinCeilings)
	require.Zero(t, collector.count(), "a refused intent reached the provider")

	// Outcome carries the clause, not the reason, and the ceilings clause has
	// three of them. Pinning the REASON at the seam is what distinguishes
	// "the period ceiling refused" from "the per-charge ceiling did" — the
	// two are indistinguishable from the clause alone, and the per-charge one
	// worked before this change.
	auth, err := s.LoadAuthorization(context.Background(), sealed.AuthorizationID())
	require.NoError(t, err)
	prior, err := s.PriorUseFor(context.Background(), auth.ID(), auth.Grant().EffectiveFrom)
	require.NoError(t, err)
	require.Contains(t, auth.Permits(sealed, evalNow, prior).Refusals, intent.RefusalOverPeriod,
		"the ceilings clause refused for some other reason than the period bound")
}

// And it must NOT refuse when the prior spend leaves room, or the test above
// would pass against a ceiling that refuses everything.
func TestThePeriodCeilingStillAdmitsAChargeThatFits(t *testing.T) {
	pool := testutil.NewTestDB(t)
	s := store.New(pool)
	sealed := ready(t, s)

	priorSettlement(t, s, 1_000_000)

	collector := &recordingCollector{result: CollectResult{Succeeded: true, Reference: "in_1"}}
	out, err := newExecutor(t, s, collector, fullyEvidencedEnv()).Execute(context.Background(), sealed.Digest())
	require.NoError(t, err)

	require.Truef(t, out.Permitted,
		"a charge well inside the remaining period ceiling was refused: %v", out.Refused)
}

// A FAILED prior attempt consumes frequency but no spend. Both halves matter:
// counting it as spend would refuse charges that never took money, and not
// counting it as an attempt is the runaway the frequency ceiling exists for.
func TestAFailedAttemptConsumesFrequencyButNotSpend(t *testing.T) {
	pool := testutil.NewTestDB(t)
	s := store.New(pool)
	ctx := context.Background()

	ready(t, s)
	priorAttempt(t, s, 4_000_000)

	prior, err := s.PriorUseFor(ctx, "auth-1", time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	require.NoError(t, err)

	require.Equal(t, 1, prior.Attempts, "a failed attempt did not consume one")
	require.Zero(t, prior.SpendMicros,
		"a FAILED attempt counted as spend — the ceiling would refuse charges that never took money")
	require.Zero(t, prior.Unresolved, "a failed attempt is resolved, not unknown")
}

// An in-flight claim — submitted and never confirmed — is an attempt whose
// outcome is UNKNOWN. It may have taken the customer's money.
func TestAnInFlightClaimIsAnUnresolvedAttempt(t *testing.T) {
	pool := testutil.NewTestDB(t)
	s := store.New(pool)
	ctx := context.Background()

	ready(t, s)
	inflight := sealKind(t, kindCycle, 2_000_000)
	require.NoError(t, s.SaveIntent(ctx, inflight))
	require.NoError(t, s.ClaimSettlement(ctx, inflight.Digest(), "prior-use-test"))

	prior, err := s.PriorUseFor(ctx, "auth-1", time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	require.NoError(t, err)

	require.Equal(t, 1, prior.Attempts)
	require.Equal(t, 1, prior.Unresolved,
		"an in-flight claim was not counted as unresolved; a second attempt would start "+
			"while the first may already have taken the money")
	require.Zero(t, prior.SpendMicros, "an unconfirmed attempt counted as collected money")
}

// Prior use is scoped to ONE authorization. Another grant's spend must not
// consume this one's ceiling.
func TestPriorUseDoesNotLeakAcrossAuthorizations(t *testing.T) {
	pool := testutil.NewTestDB(t)
	s := store.New(pool)
	ctx := context.Background()

	ready(t, s)
	priorSettlement(t, s, 3_000_000)

	other, err := s.PriorUseFor(ctx, "auth-somebody-else", time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	require.NoError(t, err)
	require.Zero(t, other.SpendMicros, "one authorization's spend was charged to another's ceiling")
	require.Zero(t, other.Attempts)

	mine, err := s.PriorUseFor(ctx, "auth-1", time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	require.NoError(t, err)
	require.EqualValues(t, 3_000_000, mine.SpendMicros,
		"the fixture recorded no spend at all, so the isolation above proves nothing")
}

// The window is a bound, not decoration: use recorded before it must not count.
func TestPriorUseIgnoresSettlementsBeforeTheWindow(t *testing.T) {
	pool := testutil.NewTestDB(t)
	s := store.New(pool)
	ctx := context.Background()

	ready(t, s)
	priorSettlement(t, s, 3_000_000)

	future := time.Now().UTC().Add(24 * time.Hour)
	prior, err := s.PriorUseFor(ctx, "auth-1", future)
	require.NoError(t, err)
	require.Zero(t, prior.SpendMicros,
		"a settlement claimed before the window start was counted inside it")
	require.Zero(t, prior.Attempts)
}

// sealUnder seals an intent naming a specific authorization, so a test can
// give one its own ceilings instead of borrowing ready()'s.
func sealUnder(t *testing.T, authID string, micros int64) intent.ChargeIntent {
	t.Helper()
	d := validExecutorDraft()
	d.AuthorizationID = authID
	d.Lines = []intent.Line{intent.NewLine("quiz.render", "quiz-core", "1.4.0", 1, micros)}
	sealed, err := intent.Seal(d)
	require.NoError(t, err)
	return sealed
}

// 🔴 The frequency ceiling, which before this wiring could not refuse ANYTHING.
//
// `prior.Attempts+1 > ceiling` with a hardcoded zero is `1 > ceiling`, false
// for every ceiling a grant can carry — Authorize rejects a non-positive one,
// so the smallest real ceiling is 1 and even that admitted attempt after
// attempt. This gives an authorization a ceiling of ONE, spends it on a failed
// attempt, and requires the next to be refused for that reason.
func TestTheFrequencyCeilingRefusesOnceItsAttemptsAreSpent(t *testing.T) {
	pool := testutil.NewTestDB(t)
	s := store.New(pool)
	ctx := context.Background()

	const authID = "auth-freq-1"
	payer := intent.Subject{Kind: "org", ID: "org-1"}
	auth, err := intent.AuthorizeAccepted(intent.AuthorizationGrant{
		ID: authID, Scope: intent.ScopeStanding,
		Subject:  payer,
		Currency: "USD", Kinds: []intent.ChargeKind{kindCycle},
		PerChargeCeiling: 10_000_000, PeriodCeiling: 50_000_000,
		FrequencyCeiling: 1,
		NoticeLeadTime:   24 * time.Hour, Provider: "stripe", MandateReference: "pm_test_1",
		TermsRevision: "terms-2026-01", PriceBook: "pb-2026-08",
		NoticePolicy:  "email/v1",
		EffectiveFrom: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		ExpiresAt:     time.Date(2027, 1, 1, 0, 0, 0, 0, time.UTC),
	})
	require.NoError(t, err)
	require.NoError(t, s.SaveAuthorization(ctx, auth))
	issueAndAccept(t, s, auth, payer)

	// The one attempt this grant permits, spent and failed.
	spent := sealUnder(t, authID, 1_000)
	require.NoError(t, s.SaveIntent(ctx, spent))
	require.NoError(t, s.ClaimSettlement(ctx, spent.Digest(), "prior-use-test"))
	require.NoError(t, s.RecordOutcome(ctx, spent.Digest(), "failed", "", evalNow))

	next := sealUnder(t, authID, 2_000)
	require.NoError(t, s.SaveIntent(ctx, next))

	prior, err := s.PriorUseFor(ctx, authID, auth.Grant().EffectiveFrom)
	require.NoError(t, err)
	require.Equal(t, 1, prior.Attempts, "the spent attempt was not counted, so the ceiling cannot bite")

	decision := auth.Permits(next, evalNow, prior)
	require.Contains(t, decision.Refusals, intent.RefusalOverFrequency,
		"a second attempt was permitted under a frequency ceiling of one")

	// And the bound is a bound, not a blanket refusal: the FIRST attempt,
	// with nothing spent, must be permitted.
	require.NotContains(t,
		auth.Permits(next, evalNow, intent.PriorUse{}).Refusals, intent.RefusalOverFrequency,
		"the frequency ceiling refused the first attempt too, so the test above proves nothing")
}
