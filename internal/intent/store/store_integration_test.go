//go:build integration

package store

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/mirrorstack-ai/billing-engine/internal/intent"
	"github.com/mirrorstack-ai/billing-engine/internal/shared/testutil"
)

var (
	windowStart = time.Date(2026, 8, 4, 0, 0, 0, 0, time.UTC)
	windowEnd   = time.Date(2026, 9, 4, 0, 0, 0, 0, time.UTC)
)

func sealedFixture(t *testing.T, quantity int64) intent.ChargeIntent {
	t.Helper()
	sealed, err := intent.Seal(intent.Draft{
		Payer:    intent.Subject{Kind: "org", ID: "org-1"},
		Currency: "USD",
		Lines: []intent.Line{
			intent.NewLine("quiz.render", "quiz-core", "1.4.0", quantity, 25),
			intent.NewLine("quiz.grade", "quiz-core", "1.4.0", 10, 40),
		},
		PriceBookRevision: "pb-2026-08",
		TermsRevision:     "terms-2026-01",
		Tax: intent.TaxDetermination{
			Resolved: true, Jurisdiction: "TW", RuleRevision: "tax-2026-05", AmountMicros: 1_250,
		},
		AuthorizationID:  "auth-1",
		NoticePolicy:     "email/v1",
		ExecuteNotBefore: windowStart,
		ExecuteNotAfter:  windowEnd,
		SourceFactKeys:   []string{"fact-1", "fact-2"},
	})
	require.NoError(t, err)
	return sealed
}

func TestSaveAndLoadRoundTripsThroughTheDigest(t *testing.T) {
	pool := testutil.NewTestDB(t)
	s := New(pool)
	ctx := context.Background()

	original := sealedFixture(t, 1_000)
	require.NoError(t, s.SaveIntent(ctx, original))

	loaded, err := s.LoadIntent(ctx, original.Digest())
	require.NoError(t, err)

	require.Equal(t, original.Digest(), loaded.Digest())
	require.Equal(t, original.TotalMicros(), loaded.TotalMicros())
	require.Equal(t, original.SubtotalMicros(), loaded.SubtotalMicros())
	require.Len(t, loaded.Lines(), 2)
	require.Equal(t, original.SourceFactKeys(), loaded.SourceFactKeys())
	require.Equal(t, original.TermsRevision(), loaded.TermsRevision())
}

// The digest is the identity of the content, so a duplicate save is the
// same document arriving twice. A retrying caller should not have to
// tell the difference.
func TestSavingTheSameIntentTwiceIsANoOp(t *testing.T) {
	pool := testutil.NewTestDB(t)
	s := New(pool)
	ctx := context.Background()

	sealed := sealedFixture(t, 1_000)
	require.NoError(t, s.SaveIntent(ctx, sealed))
	require.NoError(t, s.SaveIntent(ctx, sealed), "a re-delivered intent was rejected")

	var lines int
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT count(*) FROM ms_billing.charge_intent_lines WHERE intent_digest = $1`,
		sealed.Digest()).Scan(&lines))
	require.Equal(t, 2, lines, "the second save duplicated the lines")
}

// 🔴 The load path is where a tampered row has to be caught, because it
// is the only point between the database and a charge.
func TestLoadRefusesARowEditedInPlace(t *testing.T) {
	pool := testutil.NewTestDB(t)
	s := New(pool)
	ctx := context.Background()

	sealed := sealedFixture(t, 1_000)
	require.NoError(t, s.SaveIntent(ctx, sealed))

	// The trigger blocks an UPDATE of a sealed column, which is the
	// first line of defence. Editing a LINE is not covered by it — and
	// the digest still is.
	_, err := pool.Exec(ctx,
		`UPDATE ms_billing.charge_intent_lines
		    SET quantity = 1, amount_micros = 25
		  WHERE intent_digest = $1 AND line_index = 0`, sealed.Digest())
	require.NoError(t, err, "the fixture could not perform the edit it is testing")

	_, err = s.LoadIntent(ctx, sealed.Digest())
	require.ErrorIs(t, err, intent.ErrDigestMismatch,
		"an intent whose lines were edited under it still loaded")
}

// INV-008 under real concurrency: one winner, no window.
func TestOnlyOneClaimerWins(t *testing.T) {
	pool := testutil.NewTestDB(t)
	s := New(pool)
	ctx := context.Background()

	sealed := sealedFixture(t, 1_000)
	require.NoError(t, s.SaveIntent(ctx, sealed))

	const racers = 8
	var (
		wg      sync.WaitGroup
		mu      sync.Mutex
		won     int
		refused int
	)
	for i := 0; i < racers; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			err := s.ClaimSettlement(ctx, sealed.Digest(), "executor")
			mu.Lock()
			defer mu.Unlock()
			switch {
			case err == nil:
				won++
			case errors.Is(err, ErrAlreadyClaimed):
				refused++
			default:
				t.Errorf("unexpected error: %v", err)
			}
		}(i)
	}
	wg.Wait()

	require.Equal(t, 1, won, "more than one executor claimed the same intent")
	require.Equal(t, racers-1, refused)
}

func TestClaimingAnUnknownIntentIsNotFound(t *testing.T) {
	pool := testutil.NewTestDB(t)
	s := New(pool)

	err := s.ClaimSettlement(context.Background(), "no-such-digest", "executor")
	require.ErrorIs(t, err, ErrNotFound)
}

// An intent that settled and then "settled differently" is a record
// nobody can reason about.
func TestAnOutcomeIsRecordedOnce(t *testing.T) {
	pool := testutil.NewTestDB(t)
	s := New(pool)
	ctx := context.Background()

	sealed := sealedFixture(t, 1_000)
	require.NoError(t, s.SaveIntent(ctx, sealed))
	require.NoError(t, s.ClaimSettlement(ctx, sealed.Digest(), "executor"))

	now := time.Date(2026, 8, 30, 0, 0, 0, 0, time.UTC)
	require.NoError(t, s.RecordOutcome(ctx, sealed.Digest(), "succeeded", now))

	err := s.RecordOutcome(ctx, sealed.Digest(), "failed", now)
	require.ErrorIs(t, err, ErrStateChanged, "an outcome was overwritten")

	var outcome string
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT outcome FROM ms_billing.intent_settlement_claims WHERE intent_digest = $1`,
		sealed.Digest()).Scan(&outcome))
	require.Equal(t, "succeeded", outcome)
}

// A transition made against stale knowledge must fail rather than
// silently overwrite a state something else set.
func TestStateTransitionsAreConditional(t *testing.T) {
	pool := testutil.NewTestDB(t)
	s := New(pool)
	ctx := context.Background()

	sealed := sealedFixture(t, 1_000)
	require.NoError(t, s.SaveIntent(ctx, sealed))

	state, err := s.State(ctx, sealed.Digest())
	require.NoError(t, err)
	require.Equal(t, "proposed", state)

	require.NoError(t, s.AdvanceState(ctx, sealed.Digest(), "proposed", "eligible"))

	err = s.AdvanceState(ctx, sealed.Digest(), "proposed", "executing")
	require.ErrorIs(t, err, ErrStateChanged, "a stale transition overwrote the current state")

	state, err = s.State(ctx, sealed.Digest())
	require.NoError(t, err)
	require.Equal(t, "eligible", state)
}

func TestLoadingAnUnknownIntentIsNotFound(t *testing.T) {
	pool := testutil.NewTestDB(t)
	_, err := New(pool).LoadIntent(context.Background(), "no-such-digest")
	require.ErrorIs(t, err, ErrNotFound)
}

// A superseding correction is a distinct row that points at what it
// replaced, and both must load.
func TestSupersedingIntentsBothLoad(t *testing.T) {
	pool := testutil.NewTestDB(t)
	s := New(pool)
	ctx := context.Background()

	original := sealedFixture(t, 1_000)
	require.NoError(t, s.SaveIntent(ctx, original))

	corrected := sealedFixture(t, 1_001)
	replacement, err := original.Supersede(intent.Draft{
		Payer:             corrected.Payer(),
		Currency:          corrected.Currency(),
		Lines:             corrected.Lines(),
		PriceBookRevision: corrected.PriceBookRevision(),
		TermsRevision:     corrected.TermsRevision(),
		Tax:               corrected.Tax(),
		AuthorizationID:   corrected.AuthorizationID(),
		NoticePolicy:      corrected.NoticePolicy(),
		ExecuteNotBefore:  windowStart,
		ExecuteNotAfter:   windowEnd,
		SourceFactKeys:    corrected.SourceFactKeys(),
	})
	require.NoError(t, err)
	require.NoError(t, s.SaveIntent(ctx, replacement))

	loadedOriginal, err := s.LoadIntent(ctx, original.Digest())
	require.NoError(t, err)
	require.Empty(t, loadedOriginal.Supersedes())

	loadedReplacement, err := s.LoadIntent(ctx, replacement.Digest())
	require.NoError(t, err)
	require.Equal(t, original.Digest(), loadedReplacement.Supersedes(),
		"the supersede link did not survive storage")
}
