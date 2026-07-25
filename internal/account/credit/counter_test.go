package credit

import (
	"context"
	"math"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/google/uuid"
	redis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

func TestEstimateKeyIncludesAccountAndUTCPeriod(t *testing.T) {
	accountID := uuid.MustParse("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")
	period := time.Date(2026, time.July, 1, 8, 0, 0, 0, time.FixedZone("UTC+8", 8*60*60))
	require.Equal(t, "billing:credit-estimate:aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:1782864000", estimateKey(accountID, period))
}

func TestNewRedisCounterWithoutURLIsUnavailable(t *testing.T) {
	counter, err := NewRedisCounter("")
	require.ErrorIs(t, err, ErrUnavailable)
	require.Nil(t, counter)
}

func TestNewCounterFailureReturnsTrueNilAndCoordinatorFallsBackLive(t *testing.T) {
	for _, rawURL := range []string{"", "not-a-redis-url"} {
		t.Run(rawURL, func(t *testing.T) {
			counter, err := NewCounter(rawURL)
			require.Error(t, err)
			require.Nil(t, counter, "construction failure must not escape as a typed nil")

			accountID := uuid.New()
			coordinator := testCoordinator(
				counter,
				testSnapshot(accountID, 100),
				&fakeProjection{projection: Projection{
					AmountMicros: 50, PeriodStart: coordinatorStart,
				}},
				nil,
			)
			require.NotPanics(t, func() {
				blocked, gateErr := coordinator.OutOfCredits(context.Background(), accountID)
				require.NoError(t, gateErr)
				require.False(t, blocked)
			})
		})
	}
}

func testRedisCounter(t *testing.T) *RedisCounter {
	t.Helper()
	server := miniredis.RunT(t)
	return &RedisCounter{
		client: redis.NewClient(&redis.Options{Addr: server.Addr()}),
		ttl:    defaultEstimateTTL,
	}
}

func TestRedisCounterAddIfPresentIsExactAndNeverCreatesDeltaOnlyKey(t *testing.T) {
	counter := testRedisCounter(t)
	ctx := context.Background()
	accountID := uuid.New()
	start := time.Now().UTC()

	previous, estimate, found, err := counter.AddIfPresent(ctx, accountID, start, 5)
	require.NoError(t, err)
	require.False(t, found)
	require.Zero(t, previous)
	require.Zero(t, estimate)
	_, found, err = counter.Get(ctx, accountID, start)
	require.NoError(t, err)
	require.False(t, found)

	const aboveFloatExact = int64(9_007_199_254_740_993)
	require.NoError(t, counter.Set(ctx, accountID, start, aboveFloatExact))
	previous, estimate, found, err = counter.AddIfPresent(ctx, accountID, start, 1)
	require.NoError(t, err)
	require.True(t, found)
	require.EqualValues(t, aboveFloatExact, previous)
	require.EqualValues(t, aboveFloatExact+1, estimate)
}

func TestRedisCounterSetMaxPreservesExactInt64AndWinningWriter(t *testing.T) {
	counter := testRedisCounter(t)
	ctx := context.Background()
	accountID := uuid.New()
	start := time.Now().UTC()
	const aboveFloatExact = int64(9_007_199_254_740_993)

	first, err := counter.SetMax(ctx, accountID, start, aboveFloatExact)
	require.NoError(t, err)
	require.False(t, first.FoundBefore)
	require.True(t, first.Advanced)
	require.EqualValues(t, aboveFloatExact, first.StoredMicros)

	lower, err := counter.SetMax(ctx, accountID, start, aboveFloatExact-1)
	require.NoError(t, err)
	require.True(t, lower.FoundBefore)
	require.False(t, lower.Advanced)
	require.EqualValues(t, aboveFloatExact, lower.PreviousMicros)
	require.EqualValues(t, aboveFloatExact, lower.StoredMicros)

	maximum, err := counter.SetMax(ctx, accountID, start, math.MaxInt64)
	require.NoError(t, err)
	require.True(t, maximum.Advanced)
	require.EqualValues(t, aboveFloatExact, maximum.PreviousMicros)
	require.EqualValues(t, math.MaxInt64, maximum.StoredMicros)

	stored, found, err := counter.Get(ctx, accountID, start)
	require.NoError(t, err)
	require.True(t, found)
	require.EqualValues(t, math.MaxInt64, stored)
}

func TestRedisCounterSetIfEqualUsesExactDecimalIdentity(t *testing.T) {
	counter := testRedisCounter(t)
	ctx := context.Background()
	accountID := uuid.New()
	start := time.Now().UTC()
	const exact = int64(9_007_199_254_740_993)
	require.NoError(t, counter.Set(ctx, accountID, start, exact))

	applied, err := counter.SetIfEqual(ctx, accountID, start, exact-1, 1)
	require.NoError(t, err)
	require.False(t, applied)
	applied, err = counter.SetIfEqual(ctx, accountID, start, exact, 42)
	require.NoError(t, err)
	require.True(t, applied)

	stored, found, err := counter.Get(ctx, accountID, start)
	require.NoError(t, err)
	require.True(t, found)
	require.EqualValues(t, 42, stored)
}

func TestRedisCounterNotificationClaimsAreIndependentAndIdempotent(t *testing.T) {
	counter := testRedisCounter(t)
	ctx := context.Background()
	accountID := uuid.New()
	start := time.Now().UTC()

	claimed, err := counter.ClaimBlockNotification(ctx, accountID, start)
	require.NoError(t, err)
	require.True(t, claimed)
	claimed, err = counter.ClaimBlockNotification(ctx, accountID, start)
	require.NoError(t, err)
	require.False(t, claimed)

	claimed, err = counter.ClaimBoundaryNotification(ctx, accountID, start)
	require.NoError(t, err)
	require.True(t, claimed)
	require.NoError(t, counter.ClearBoundaryNotification(ctx, accountID, start))
	claimed, err = counter.ClaimBoundaryNotification(ctx, accountID, start)
	require.NoError(t, err)
	require.True(t, claimed)
	require.NoError(t, counter.ClearBlockNotification(ctx, accountID, start))
	claimed, err = counter.ClaimBlockNotification(ctx, accountID, start)
	require.NoError(t, err)
	require.True(t, claimed)
}
