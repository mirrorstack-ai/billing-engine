//go:build integration

package store

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/mirrorstack-ai/billing-engine/internal/intent"
	"github.com/mirrorstack-ai/billing-engine/internal/shared/testutil"
)

const kindTopUp intent.ChargeKind = intent.KindAutoTopUp

func standingAuth(t *testing.T) intent.BillingAuthorization {
	t.Helper()
	auth, err := intent.Authorize(intent.AuthorizationGrant{
		ID: "auth-1", Scope: intent.ScopeStanding,
		Subject:  intent.Subject{Kind: "org", ID: "org-1"},
		Currency: "USD", Kinds: []intent.ChargeKind{kindTopUp, intent.KindModuleUsage},
		PerChargeCeiling: 50_000, PeriodCeiling: 200_000,
		TermsRevision: "terms-2026-01", PriceBook: "pb-2026-08",
		NoticePolicy:     "email/v1",
		EffectiveFrom:    time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		ExpiresAt:        time.Date(2027, 1, 1, 0, 0, 0, 0, time.UTC),
		AcceptanceDigest: "accept-1",
	})
	require.NoError(t, err)
	return auth
}

// Storage round-trips through Authorize, so every validation runs again
// on the way out. A stored row is not exempt from the rules its own
// creation had to pass.
func TestAuthorizationRoundTrips(t *testing.T) {
	pool := testutil.NewTestDB(t)
	s := New(pool)
	ctx := context.Background()

	original := standingAuth(t)
	require.NoError(t, s.SaveAuthorization(ctx, original))

	loaded, err := s.LoadAuthorization(ctx, "auth-1")
	require.NoError(t, err)

	sealed := sealedFixture(t, 100)
	now := time.Date(2026, 8, 30, 0, 0, 0, 0, time.UTC)

	// The intent's authorization id must match, so use one that does.
	require.Equal(t, original.Grant().PerChargeCeiling, loaded.Grant().PerChargeCeiling)
	require.Equal(t, original.Grant().Kinds, loaded.Grant().Kinds)
	require.Equal(t, original.AcceptanceDigest(), loaded.AcceptanceDigest())
	require.Equal(t, original.TermsRevision(), loaded.TermsRevision())

	before := original.Permits(sealed, now, 0)
	after := loaded.Permits(sealed, now, 0)
	require.Equal(t, before.Permitted, after.Permitted)
	require.Equal(t, before.Refusals, after.Refusals)
}

// The terms are what the customer accepted, so rewriting them under a
// stored acceptance would make the acceptance describe something else.
// Only revocation legitimately arrives later, and it can only reduce.
func TestResavingCannotWidenAnAuthorization(t *testing.T) {
	pool := testutil.NewTestDB(t)
	s := New(pool)
	ctx := context.Background()

	require.NoError(t, s.SaveAuthorization(ctx, standingAuth(t)))

	widened, err := intent.Authorize(intent.AuthorizationGrant{
		ID: "auth-1", Scope: intent.ScopeStanding,
		Subject:  intent.Subject{Kind: "org", ID: "org-1"},
		Currency: "USD", Kinds: []intent.ChargeKind{kindTopUp},
		PerChargeCeiling: 999_999_999, PeriodCeiling: 999_999_999,
		TermsRevision: "terms-2026-01", PriceBook: "pb-2026-08",
		NoticePolicy:     "email/v1",
		EffectiveFrom:    time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		ExpiresAt:        time.Date(2027, 1, 1, 0, 0, 0, 0, time.UTC),
		AcceptanceDigest: "accept-1",
	})
	require.NoError(t, err)
	require.NoError(t, s.SaveAuthorization(ctx, widened))

	loaded, err := s.LoadAuthorization(ctx, "auth-1")
	require.NoError(t, err)
	require.EqualValues(t, 50_000, loaded.Grant().PerChargeCeiling,
		"a re-save widened a stored ceiling under the customer's acceptance")
}

// Revocation does arrive later, and once set it stays.
func TestRevocationPersistsAndCannotBeUndone(t *testing.T) {
	pool := testutil.NewTestDB(t)
	s := New(pool)
	ctx := context.Background()

	auth := standingAuth(t)
	require.NoError(t, s.SaveAuthorization(ctx, auth))

	revokedAt := time.Date(2026, 8, 15, 0, 0, 0, 0, time.UTC)
	require.NoError(t, s.SaveAuthorization(ctx, auth.Revoke(revokedAt)))

	loaded, err := s.LoadAuthorization(ctx, "auth-1")
	require.NoError(t, err)
	require.False(t, loaded.RevokedAt().IsZero(), "the revocation was not stored")

	// Saving the un-revoked value again must not clear it.
	require.NoError(t, s.SaveAuthorization(ctx, auth))
	loaded, err = s.LoadAuthorization(ctx, "auth-1")
	require.NoError(t, err)
	require.False(t, loaded.RevokedAt().IsZero(),
		"a later save cleared a revocation; a customer who asked us to stop would be charged again")
}

func TestLoadingAnUnknownAuthorizationIsNotFound(t *testing.T) {
	pool := testutil.NewTestDB(t)
	_, err := New(pool).LoadAuthorization(context.Background(), "no-such-auth")
	require.ErrorIs(t, err, ErrNotFound)
}

// INV-005: the clock starts when the bytes arrive. Re-sending them is
// not a new arrival, so a second receipt must not restart the wait.
func TestASecondNoticeDoesNotRestartTheWait(t *testing.T) {
	pool := testutil.NewTestDB(t)
	s := New(pool)
	ctx := context.Background()

	sealed := sealedFixture(t, 1_000)
	require.NoError(t, s.SaveIntent(ctx, sealed))

	first := time.Date(2026, 8, 10, 0, 0, 0, 0, time.UTC)
	require.NoError(t, s.RecordNotice(ctx, NoticeReceipt{
		IntentDigest: sealed.Digest(), DeliveredDigest: sealed.Digest(),
		Policy: "email/v1", TerminalStatus: "delivered",
		EligibilityNotBefore: first, RevocationPathFresh: true,
	}))

	later := first.Add(72 * time.Hour)
	require.NoError(t, s.RecordNotice(ctx, NoticeReceipt{
		IntentDigest: sealed.Digest(), DeliveredDigest: sealed.Digest(),
		Policy: "email/v1", TerminalStatus: "delivered",
		EligibilityNotBefore: later, RevocationPathFresh: true,
	}))

	got, found, err := s.LoadNotice(ctx, sealed.Digest())
	require.NoError(t, err)
	require.True(t, found)
	require.True(t, got.EligibilityNotBefore.Equal(first),
		"a re-delivery pushed the eligibility time out; the clock started when the bytes arrived")
}

// "No notice yet" is an ordinary state for an intent awaiting one.
func TestMissingNoticeIsNotAnError(t *testing.T) {
	pool := testutil.NewTestDB(t)
	s := New(pool)
	ctx := context.Background()

	sealed := sealedFixture(t, 1_000)
	require.NoError(t, s.SaveIntent(ctx, sealed))

	_, found, err := s.LoadNotice(ctx, sealed.Digest())
	require.NoError(t, err)
	require.False(t, found)
}
