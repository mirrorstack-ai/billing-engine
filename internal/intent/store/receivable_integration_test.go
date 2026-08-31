//go:build integration

package store_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/mirrorstack-ai/billing-engine/internal/intent"
	"github.com/mirrorstack-ai/billing-engine/internal/intent/store"
	"github.com/mirrorstack-ai/billing-engine/internal/shared/testutil"
)

// 🔴 Two receivables must not both collect the same remainder.
//
// They are DIFFERENT documents, each individually valid, so INV-008's
// one-settlement-per-intent claim sees two intents rather than one obligation
// claimed twice. Nothing in the intent model stops it; only the source-capacity
// reservation §6 asks for does.
func TestTwoReceivablesCannotBothClaimTheSameRemainder(t *testing.T) {
	pool := testutil.NewTestDB(t)
	s := store.New(pool)
	ctx := context.Background()

	source := sealSource(t, 20_000_000)
	require.NoError(t, s.SaveIntent(ctx, source))

	// First receivable takes the whole outstanding amount.
	first, err := source.CollectRemainderOf(receivableFor(20_000_000))
	require.NoError(t, err)
	require.NoError(t, s.SaveIntent(ctx, first))
	require.NoError(t, s.ReserveRemainder(ctx, first))

	left, err := s.UnreservedRemainder(ctx, source.Digest())
	require.NoError(t, err)
	require.Zero(t, left, "the whole amount was reserved")

	// A second, differently-shaped receivable for the same source.
	second, err := source.CollectRemainderOf(receivableFor(19_999_999))
	require.NoError(t, err)
	require.NotEqual(t, first.Digest(), second.Digest(), "the two receivables must be distinct documents")
	require.NoError(t, s.SaveIntent(ctx, second))

	err = s.ReserveRemainder(ctx, second)
	require.ErrorIs(t, err, store.ErrRemainderUnavailable,
		"a second receivable claimed a remainder that was already fully reserved — "+
			"both would collect, and the customer pays the same debt twice")
}

// Partial claims add up to the whole and no further.
func TestReservationsAccumulateToTheSourceTotal(t *testing.T) {
	pool := testutil.NewTestDB(t)
	s := store.New(pool)
	ctx := context.Background()

	source := sealSource(t, 10_000_000)
	require.NoError(t, s.SaveIntent(ctx, source))

	for _, micros := range []int64{4_000_000, 6_000_000} {
		r, err := source.CollectRemainderOf(receivableFor(micros))
		require.NoError(t, err)
		require.NoError(t, s.SaveIntent(ctx, r))
		require.NoError(t, s.ReserveRemainder(ctx, r))
	}

	left, err := s.UnreservedRemainder(ctx, source.Digest())
	require.NoError(t, err)
	require.Zero(t, left)

	// One micro more is one micro too many.
	over, err := source.CollectRemainderOf(receivableFor(1))
	require.NoError(t, err)
	require.NoError(t, s.SaveIntent(ctx, over))
	require.ErrorIs(t, s.ReserveRemainder(ctx, over), store.ErrRemainderUnavailable)
}

// A replayed proposal must reserve once. Without the link table's primary key
// a retry eats the remainder twice.
func TestReservingTheSameReceivableTwiceIsANoOp(t *testing.T) {
	pool := testutil.NewTestDB(t)
	s := store.New(pool)
	ctx := context.Background()

	source := sealSource(t, 10_000_000)
	require.NoError(t, s.SaveIntent(ctx, source))

	r, err := source.CollectRemainderOf(receivableFor(4_000_000))
	require.NoError(t, err)
	require.NoError(t, s.SaveIntent(ctx, r))

	require.NoError(t, s.ReserveRemainder(ctx, r))
	require.NoError(t, s.ReserveRemainder(ctx, r), "a replay must not error")

	left, err := s.UnreservedRemainder(ctx, source.Digest())
	require.NoError(t, err)
	require.EqualValues(t, 6_000_000, left, "the replay reserved a second time")
}

func sealSource(t *testing.T, micros int64) intent.ChargeIntent {
	t.Helper()
	d := receivableFor(micros)
	d.Kind = intent.KindModuleUsage
	s, err := intent.Seal(d)
	require.NoError(t, err)
	return s
}

func receivableFor(micros int64) intent.Draft {
	return intent.Draft{
		Payer:                 intent.Subject{Kind: "user", ID: "acct-1"},
		Currency:              "usd",
		Lines:                 []intent.Line{intent.NewLine("d", "m", "1", 1, micros)},
		Kind:                  intent.KindCollectReceivable,
		PriceBookRevision:     "pb-1",
		TermsRevision:         "terms-1",
		Tax:                   intent.TaxDetermination{Resolved: true, Jurisdiction: "TW", RuleRevision: "tax-1", Verification: intent.TaxNotApplicable},
		AuthorizationID:       "auth-1",
		NoticePolicy:          "email/v1",
		ExecuteNotBefore:      time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC),
		ExecuteNotAfter:       time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC),
		SourceFactKeys:        []string{"f"},
		SelectedRail:          "stripe",
		RoutingPolicyRevision: "routing-2026-08",
	}
}

// 🔴 A receivable must survive a round trip through the database.
//
// `collects` is inside ChargeIntent.computeDigest (chargeintent.go:615) and
// had NO column, no field on intent.Stored and no restore in Rehydrate until
// migration 063. So every receivable this file writes was unloadable: read it
// back and the digest recomputes over an empty link, Rehydrate refuses it, and
// the document is gone for good.
//
// It went unnoticed because this file calls SaveIntent eight times and
// LoadIntent zero times, and no non-test caller of CollectRemainderOf exists
// yet. A defect that only fires on the first real receivable is still a
// defect; it just fires later, in production, on a charge someone is owed.
//
// This is the test that would have caught it, and the one that keeps the next
// sealed link from being added the same way.
func TestAReceivableRoundTripsThroughTheStore(t *testing.T) {
	pool := testutil.NewTestDB(t)
	s := store.New(pool)
	ctx := context.Background()

	source := sealSource(t, 20_000_000)
	require.NoError(t, s.SaveIntent(ctx, source))

	receivable, err := source.CollectRemainderOf(receivableFor(7_500_000))
	require.NoError(t, err)
	require.Equal(t, source.Digest(), receivable.Collects(),
		"the receivable does not name what it collects; the fixture is wrong")
	require.NoError(t, s.SaveIntent(ctx, receivable))

	loaded, err := s.LoadIntent(ctx, receivable.Digest())
	require.NoError(t, err,
		"the receivable does not load back. Its digest is taken over the link "+
			"it collects, so a link the database drops makes the document "+
			"unreproducible and Rehydrate refuses it permanently.")

	require.Equal(t, receivable.Digest(), loaded.Digest())
	require.Equal(t, source.Digest(), loaded.Collects(),
		"the link came back empty, so the loaded document is not the one that was sealed")
}

// The other direction: a link the database has FORGOTTEN must not load.
//
// Without this, a Rehydrate that quietly ignored collects would pass the test
// above — the round trip would work because both sides omitted the field
// identically. Editing the stored link away has to break the digest, or
// nothing about the link is actually attested.
func TestAReceivableWhoseLinkWasErasedDoesNotLoad(t *testing.T) {
	pool := testutil.NewTestDB(t)
	s := store.New(pool)
	ctx := context.Background()

	source := sealSource(t, 20_000_000)
	require.NoError(t, s.SaveIntent(ctx, source))
	receivable, err := source.CollectRemainderOf(receivableFor(7_500_000))
	require.NoError(t, err)
	require.NoError(t, s.SaveIntent(ctx, receivable))

	// The seal trigger refuses this, which is itself the point: reaching the
	// Rehydrate check at all takes a session that has disabled the control.
	_, err = pool.Exec(ctx,
		`UPDATE ms_billing.charge_intents SET collects_digest = NULL WHERE digest = $1`,
		receivable.Digest())
	require.Error(t, err, "the sealed link was editable in place")
	require.Contains(t, err.Error(), "sealed")

	// So do it the way a restored backup or a stray migration would: with the
	// trigger out of the way. Rehydrate is the control that still has to hold.
	_, err = pool.Exec(ctx, `ALTER TABLE ms_billing.charge_intents DISABLE TRIGGER charge_intents_sealed`)
	require.NoError(t, err)
	_, err = pool.Exec(ctx,
		`UPDATE ms_billing.charge_intents SET collects_digest = NULL WHERE digest = $1`,
		receivable.Digest())
	require.NoError(t, err)
	_, err = pool.Exec(ctx, `ALTER TABLE ms_billing.charge_intents ENABLE TRIGGER charge_intents_sealed`)
	require.NoError(t, err)

	_, err = s.LoadIntent(ctx, receivable.Digest())
	require.ErrorIs(t, err, intent.ErrDigestMismatch,
		"a receivable whose link was erased loaded anyway, so the link is not "+
			"really inside what the digest attests")
}
