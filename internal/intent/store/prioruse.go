package store

import (
	"context"
	"fmt"
	"time"

	"github.com/mirrorstack-ai/billing-engine/internal/intent"
)

// PriorUseFor is what an authorization has already been used for, so that its
// period and frequency ceilings bound something.
//
// 🔴 UNTIL 2026-09-01 NOTHING PRODUCED THIS AND BOTH CEILINGS WERE INERT.
//
// intent.PriorUse exists, predicate.SealedState carries it, and
// BillingAuthorization.Permits reads it (authorization.go:499,505). But no
// non-test caller ever assigned it: the executor built its SealedState without
// the field (executor.go:308-324), so it was the zero value on every real
// evaluation. The consequences were not "a ceiling was slightly loose":
//
//   - the PERIOD ceiling degenerated into a second per-charge ceiling —
//     `0 + total > ceiling` — so an authorization capped at $50 per period
//     admitted $50 an unlimited number of times;
//   - the FREQUENCY ceiling never refused at all — `0 + 1 > ceiling` is false
//     for every ceiling of 1 or more, which is every ceiling a grant can carry
//     (Authorize rejects a non-positive one).
//
// Both were declared, tested in isolation, and unreachable in production. The
// struct's own doc comment says a bound was made a field precisely so adding
// one would be "a COMPILE ERROR at every call site rather than a silently
// defaulted zero" — and that protects the call sites that CONSTRUCT a
// PriorUse, not the one that forgot to.
//
// # What counts
//
// An ATTEMPT is a settlement claim, whatever became of it. A failed attempt
// still consumed one — "retrying forever is the runaway the frequency ceiling
// exists to stop" (authorization.go:415-418). SPEND is only what actually
// succeeded, because a ceiling bounds money taken, not money attempted.
//
// The current intent is deliberately NOT excluded. If it carries an earlier
// failed claim, that attempt really did happen and this execution is the next
// one, which is exactly what `prior.Attempts+1` means.
//
// # 🔴 What "the period" means is NOT settled
//
// A period ceiling names a period, and nothing in the grant says which:
// AuthorizationGrant carries PeriodCeiling but no period length, and the field
// docs only ever say "the current period". That is docs/DESIGN.md §12 item 1,
// which is OWNER-ONLY and unanswered — the same decision that owes the ceilings
// their numbers.
//
// So `since` is supplied by the caller rather than invented here, and the
// executor passes the authorization's own EffectiveFrom: the whole life of the
// grant. That is the most conservative reading available — a longer window can
// only ever accumulate MORE prior use and therefore REFUSE more, and
// "a ceiling can only ever reduce, so a wrong one refuses a charge that should
// have gone through — a failure in the safe direction" (authorization.go:64-67).
// When item 1 settles, the window narrows to whatever it names and this
// signature already takes it.
func (s *Store) PriorUseFor(ctx context.Context, authorizationID string, since time.Time) (intent.PriorUse, error) {
	if authorizationID == "" {
		// A blank id must not match rows; it is what a missing
		// authorization resolves to, and that permits nothing.
		return intent.PriorUse{}, nil
	}

	var (
		spend      int64
		attempts   int
		unresolved int
	)
	err := s.pool.QueryRow(ctx, `
		SELECT COALESCE(SUM(ci.total_micros) FILTER (WHERE c.outcome = 'succeeded'), 0),
		       COUNT(*),
		       COUNT(*) FILTER (WHERE c.outcome IS NULL)
		  FROM ms_billing.intent_settlement_claims c
		  JOIN ms_billing.charge_intents ci ON ci.digest = c.intent_digest
		 WHERE ci.authorization_id = $1
		   AND c.claimed_at >= $2`,
		authorizationID, since).Scan(&spend, &attempts, &unresolved)
	if err != nil {
		return intent.PriorUse{}, fmt.Errorf("prior use for authorization %s: %w", authorizationID, err)
	}

	return intent.PriorUse{
		SpendMicros: spend,
		Attempts:    attempts,
		Unresolved:  unresolved,
	}, nil
}
