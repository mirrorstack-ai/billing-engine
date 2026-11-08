//go:build integration

package standing_test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/mirrorstack-ai/billing-engine/internal/account/standing"
	"github.com/mirrorstack-ai/billing-engine/internal/shared/testutil"
)

const testOwnerKey = "user:11111111-1111-4111-8111-111111111111"

func TestStateStore_TransitionsAndChangedAt(t *testing.T) {
	pool := testutil.NewTestDB(t)
	s := standing.NewStateStore(pool)
	ctx := context.Background()
	t0 := time.Unix(1794000000, 0)

	prior, ch, err := s.Observe(ctx, testOwnerKey, false, "ELIGIBLE", t0)
	require.NoError(t, err)
	require.Nil(t, prior, "first observation")
	require.True(t, ch.Equal(t0))

	prior, ch, err = s.Observe(ctx, testOwnerKey, true, "UNPAID_INVOICES", t0.Add(time.Minute))
	require.NoError(t, err)
	require.Equal(t, &standing.PriorState{Blocked: false, Reason: "ELIGIBLE"}, prior)
	require.True(t, ch.Equal(t0.Add(time.Minute)), "flip moves changed_at")

	prior, ch, err = s.Observe(ctx, testOwnerKey, true, "TOO_MANY_FAILURES", t0.Add(2*time.Minute))
	require.NoError(t, err)
	require.True(t, prior.Blocked)
	require.True(t, ch.Equal(t0.Add(time.Minute)), "reason change alone keeps the epoch")
}

func TestStateStore_ConcurrentObserveSeesTheFlipOnce(t *testing.T) {
	pool := testutil.NewTestDB(t)
	s := standing.NewStateStore(pool)
	ctx := context.Background()
	_, _, err := s.Observe(ctx, testOwnerKey, false, "ELIGIBLE", time.Now())
	require.NoError(t, err)

	var saw atomic.Int32
	var wg sync.WaitGroup
	for i := 0; i < 12; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			prior, _, err := s.Observe(ctx, testOwnerKey, true, "UNPAID_INVOICES", time.Now())
			if err == nil && prior != nil && !prior.Blocked {
				saw.Add(1)
			}
		}()
	}
	wg.Wait()
	require.EqualValues(t, 1, saw.Load(), "exactly one concurrent caller observes eligible -> blocked")
}

func TestStateStore_ConcurrentFirstObservationSeesNilOnce(t *testing.T) {
	pool := testutil.NewTestDB(t)
	s := standing.NewStateStore(pool)
	ctx := context.Background()
	var first atomic.Int32
	var wg sync.WaitGroup
	for i := 0; i < 12; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			prior, _, err := s.Observe(ctx, testOwnerKey, true, "UNPAID_INVOICES", time.Now())
			if err == nil && prior == nil {
				first.Add(1)
			}
		}()
	}
	wg.Wait()
	require.EqualValues(t, 1, first.Load())
}
