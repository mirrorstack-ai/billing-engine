package legacyrestamp_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/mirrorstack-ai/billing-engine/internal/account/legacyrestamp"
)

func orderedUUID(value int) uuid.UUID {
	return uuid.MustParse(fmt.Sprintf(
		"00000000-0000-4000-8000-%012d",
		value,
	))
}

type fakeSnapshot struct {
	mu        sync.Mutex
	owners    []legacyrestamp.Owner
	listErr   error
	closed    int
	closeErr  error
	cursors   []uuid.UUID
	pageSizes []int32
}

func (s *fakeSnapshot) CountOwners(context.Context) (int64, error) {
	return int64(len(s.owners)), nil
}

func (s *fakeSnapshot) ListOwners(
	_ context.Context,
	after uuid.UUID,
	limit int32,
) ([]legacyrestamp.Owner, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.cursors = append(s.cursors, after)
	s.pageSizes = append(s.pageSizes, limit)
	if s.listErr != nil {
		return nil, s.listErr
	}
	out := make([]legacyrestamp.Owner, 0, limit)
	for _, owner := range s.owners {
		if owner.AccountID.String() <= after.String() {
			continue
		}
		out = append(out, owner)
		if len(out) == int(limit) {
			break
		}
	}
	return out, nil
}

func (s *fakeSnapshot) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed++
	return s.closeErr
}

type fakeSource struct {
	snapshot *fakeSnapshot
	acquired bool
	err      error
	calls    int
}

func (s *fakeSource) TryBegin(
	context.Context,
) (legacyrestamp.Snapshot, bool, error) {
	s.calls++
	return s.snapshot, s.acquired, s.err
}

type fakeNotifier struct {
	mu            sync.Mutex
	enabled       bool
	calls         []uuid.UUID
	blocked       map[uuid.UUID]bool
	failRemaining map[uuid.UUID]int
}

func (n *fakeNotifier) Enabled() bool { return n.enabled }

func (n *fakeNotifier) NotifyOwner(
	_ context.Context,
	userID uuid.UUID,
	orgID uuid.UUID,
) (bool, error) {
	ownerID := userID
	if orgID != uuid.Nil {
		ownerID = orgID
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	n.calls = append(n.calls, ownerID)
	if n.failRemaining[ownerID] > 0 {
		n.failRemaining[ownerID]--
		return false, errors.New("status or POST failed")
	}
	return n.blocked[ownerID], nil
}

func TestRunnerEnumeratesAllLegacyOwnersWithKeysetPages(t *testing.T) {
	userOne, org, userTwo := orderedUUID(101), orderedUUID(102), orderedUUID(103)
	snapshot := &fakeSnapshot{owners: []legacyrestamp.Owner{
		{AccountID: orderedUUID(1), UserID: userOne},
		{AccountID: orderedUUID(2), OrgID: org},
		{AccountID: orderedUUID(3), UserID: userTwo},
	}}
	source := &fakeSource{snapshot: snapshot, acquired: true}
	notifier := &fakeNotifier{
		enabled: true,
		blocked: map[uuid.UUID]bool{
			userOne: true, // genuine legacy block must remain true
			org:     false,
			userTwo: false,
		},
		failRemaining: map[uuid.UUID]int{},
	}
	runner := legacyrestamp.NewRunner(source, notifier, 2, 2)

	first, err := runner.RunPage(context.Background(), uuid.Nil)
	require.NoError(t, err)
	require.Equal(t, legacyrestamp.Result{
		Pages: 1, Scanned: 2, Delivered: 2, Blocked: 1, TotalOwners: 3,
		NextCursor: orderedUUID(2).String(),
	}, first)

	second, err := runner.RunPage(context.Background(), orderedUUID(2))
	require.NoError(t, err)
	require.Equal(t, legacyrestamp.Result{
		Pages: 1, Scanned: 1, Delivered: 1, TotalOwners: 3, Complete: true,
	}, second)

	require.ElementsMatch(t, []uuid.UUID{userOne, org, userTwo}, notifier.calls)
	require.Equal(t, []uuid.UUID{uuid.Nil, orderedUUID(2)}, snapshot.cursors)
	require.Equal(t, 2, snapshot.closed)
}

func TestRunnerFailedPageRetriesSameCursorThenAdvancesWithoutSkippingTail(t *testing.T) {
	first, second, third := orderedUUID(101), orderedUUID(102), orderedUUID(103)
	snapshot := &fakeSnapshot{owners: []legacyrestamp.Owner{
		{AccountID: orderedUUID(1), UserID: first},
		{AccountID: orderedUUID(2), UserID: second},
		{AccountID: orderedUUID(3), UserID: third},
	}}
	source := &fakeSource{snapshot: snapshot, acquired: true}
	notifier := &fakeNotifier{
		enabled:       true,
		blocked:       map[uuid.UUID]bool{},
		failRemaining: map[uuid.UUID]int{second: 1},
	}
	runner := legacyrestamp.NewRunner(source, notifier, 2, 2)

	failed, err := runner.RunPage(context.Background(), uuid.Nil)
	require.Error(t, err)
	require.Equal(t, 2, failed.Scanned)
	require.Equal(t, 1, failed.Delivered)
	require.Equal(t, 1, failed.Failed)
	require.Empty(t, failed.NextCursor, "a failed page retains its input cursor")

	replayed, err := runner.RunPage(context.Background(), uuid.Nil)
	require.NoError(t, err)
	require.Equal(t, orderedUUID(2).String(), replayed.NextCursor)
	require.Equal(t, 2, replayed.Delivered)

	tail, err := runner.RunPage(context.Background(), orderedUUID(2))
	require.NoError(t, err)
	require.True(t, tail.Complete)
	require.Equal(t, 1, tail.Delivered)
	require.ElementsMatch(
		t,
		[]uuid.UUID{first, second, first, second},
		notifier.calls[:4],
		"the failed page replays both owners idempotently",
	)
	require.Equal(t, third, notifier.calls[4], "the tail is never skipped")
}

func TestRunnerMoreThan256OwnersCompletesAcrossBoundedInvocations(t *testing.T) {
	const ownerCount = 600
	owners := make([]legacyrestamp.Owner, 0, ownerCount)
	for index := 1; index <= ownerCount; index++ {
		owners = append(owners, legacyrestamp.Owner{
			AccountID: orderedUUID(index),
			UserID:    orderedUUID(10_000 + index),
		})
	}
	snapshot := &fakeSnapshot{owners: owners}
	source := &fakeSource{snapshot: snapshot, acquired: true}
	notifier := &fakeNotifier{
		enabled: true, blocked: map[uuid.UUID]bool{},
		failRemaining: map[uuid.UUID]int{},
	}
	runner := legacyrestamp.NewRunner(
		source,
		notifier,
		legacyrestamp.DefaultPageSize,
		legacyrestamp.DefaultConcurrency,
	)

	cursor := uuid.Nil
	invocations := 0
	attempted := 0
	for {
		result, err := runner.RunPage(context.Background(), cursor)
		require.NoError(t, err)
		invocations++
		attempted += result.Scanned
		if result.Complete {
			break
		}
		next, err := uuid.Parse(result.NextCursor)
		require.NoError(t, err)
		require.Greater(t, next.String(), cursor.String())
		cursor = next
	}

	require.Equal(t, ownerCount, attempted)
	require.Equal(t, ownerCount, len(notifier.calls))
	require.Equal(t, 3, invocations)
}

func TestRunnerInvalidOwnerFailsPageAfterNotifyingEveryValidOwner(t *testing.T) {
	first, last := orderedUUID(101), orderedUUID(103)
	snapshot := &fakeSnapshot{owners: []legacyrestamp.Owner{
		{AccountID: orderedUUID(1), UserID: first},
		{AccountID: orderedUUID(2), UserID: orderedUUID(102), OrgID: orderedUUID(202)},
		{AccountID: orderedUUID(3), UserID: last},
	}}
	source := &fakeSource{snapshot: snapshot, acquired: true}
	notifier := &fakeNotifier{
		enabled: true, blocked: map[uuid.UUID]bool{},
		failRemaining: map[uuid.UUID]int{},
	}

	result, err := legacyrestamp.NewRunner(
		source,
		notifier,
		3,
		2,
	).RunPage(context.Background(), uuid.Nil)

	require.Error(t, err)
	require.Equal(t, 3, result.Scanned)
	require.Equal(t, 2, result.Delivered)
	require.Equal(t, 1, result.Failed)
	require.Empty(t, result.NextCursor)
	require.ElementsMatch(t, []uuid.UUID{first, last}, notifier.calls)
}

func TestRunnerDisabledNotifierAndConcurrentMutexHardFail(t *testing.T) {
	t.Run("disabled notifier", func(t *testing.T) {
		source := &fakeSource{snapshot: &fakeSnapshot{}, acquired: true}
		notifier := &fakeNotifier{
			enabled: false, blocked: map[uuid.UUID]bool{},
			failRemaining: map[uuid.UUID]int{},
		}
		_, err := legacyrestamp.NewRunner(
			source,
			notifier,
			2,
			2,
		).RunPage(context.Background(), uuid.Nil)
		require.ErrorContains(t, err, "notifier is disabled")
		require.Zero(t, source.calls)
	})

	t.Run("mutex busy", func(t *testing.T) {
		source := &fakeSource{snapshot: &fakeSnapshot{}, acquired: false}
		notifier := &fakeNotifier{
			enabled: true, blocked: map[uuid.UUID]bool{},
			failRemaining: map[uuid.UUID]int{},
		}
		_, err := legacyrestamp.NewRunner(
			source,
			notifier,
			2,
			2,
		).RunPage(context.Background(), orderedUUID(50))
		require.ErrorIs(t, err, legacyrestamp.ErrAlreadyRunning)
		require.Empty(t, notifier.calls)
	})
}

func TestRunnerSnapshotCleanupFailureRetainsInputCursor(t *testing.T) {
	closeErr := errors.New("unlock failed")
	source := &fakeSource{
		snapshot: &fakeSnapshot{
			owners:   []legacyrestamp.Owner{{AccountID: orderedUUID(51), UserID: orderedUUID(151)}},
			closeErr: closeErr,
		},
		acquired: true,
	}
	notifier := &fakeNotifier{
		enabled: true, blocked: map[uuid.UUID]bool{},
		failRemaining: map[uuid.UUID]int{},
	}

	result, err := legacyrestamp.NewRunner(
		source,
		notifier,
		2,
		2,
	).RunPage(context.Background(), orderedUUID(50))

	require.ErrorIs(t, err, closeErr)
	require.Equal(t, orderedUUID(50).String(), result.NextCursor)
	require.False(t, result.Complete)
}
