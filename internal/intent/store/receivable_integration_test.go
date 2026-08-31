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
