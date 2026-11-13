package cycle_test

// SweepUnattachedUserUsage / GetUserUnbilledBacklog (migration 088,
// core-v2#340). Reuses the in-memory fakeStore (service_test.go), whose lazy
// user rows re-implement the SQL's D1d window filter, so these tests assert
// which rows the service's window lets through — not just that it called.

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/mirrorstack-ai/billing-engine/internal/account/billing"
	"github.com/mirrorstack-ai/billing-engine/internal/account/cycle"
)

// Fixed clock: 2026-07-20 12:00 UTC. An account activated on 2026-06-10 has
// anchor day 10, so its open window is [2026-07-10, 2026-08-10). A card bound
// NOW would anchor on day 20: [2026-07-20, 2026-08-20).
var (
	userNow             = time.Date(2026, 7, 20, 12, 0, 0, 0, time.UTC)
	userActivatedAt     = time.Date(2026, 6, 10, 15, 30, 0, 0, time.UTC)
	userOpenWindowStart = time.Date(2026, 7, 10, 0, 0, 0, 0, time.UTC)
	userBindNowStart    = time.Date(2026, 7, 20, 0, 0, 0, 0, time.UTC)
)

func userSvc(store *fakeStore) *cycle.Service {
	return cycle.NewService(store, nil).WithNow(func() time.Time { return userNow })
}

func TestSweepUnattachedUserUsage_RepointsIntoActivationAnchoredWindow(t *testing.T) {
	store := newFakeStore()
	user, acct := uuid.New(), uuid.New()
	store.userUnswept = []cycle.UserUsageAccount{{AccountID: acct, UserID: user}}
	store.activation[acct] = userActivatedAt
	inWindow := fakeLazyEvent{at: time.Date(2026, 7, 12, 8, 0, 0, 0, time.UTC), micros: 100}
	beforeWindow := fakeLazyEvent{at: time.Date(2026, 7, 9, 23, 59, 0, 0, time.UTC), micros: 50}
	store.userLazyEvents[user] = []fakeLazyEvent{inWindow, beforeWindow}

	summary, err := userSvc(store).SweepUnattachedUserUsage(context.Background())
	require.NoError(t, err)
	require.Equal(t, &cycle.UserUsageSweepSummary{Accounts: 1, Swept: 1, RepointedEvents: 1}, summary)
	require.Equal(t, []userRepointCall{{user, acct, userOpenWindowStart}}, store.userRepointCalls)
	// D1d: the row from before the open window is left NULL, never caught up.
	require.Equal(t, []fakeLazyEvent{beforeWindow}, store.userLazyEvents[user])
}

func TestSweepUnattachedUserUsage_PerAccountFailureDoesNotAbortPass(t *testing.T) {
	store := newFakeStore()
	users := []uuid.UUID{uuid.New(), uuid.New(), uuid.New()}
	accts := []uuid.UUID{uuid.New(), uuid.New(), uuid.New()}
	for i := range users {
		store.userUnswept = append(store.userUnswept, cycle.UserUsageAccount{AccountID: accts[i], UserID: users[i]})
		store.activation[accts[i]] = userActivatedAt
		store.userLazyEvents[users[i]] = []fakeLazyEvent{{at: userNow.Add(-time.Hour), micros: 1}}
	}
	store.errUserRepoint = map[uuid.UUID]error{accts[1]: errors.New("transient")}

	summary, err := userSvc(store).SweepUnattachedUserUsage(context.Background())
	require.NoError(t, err)
	require.Equal(t, &cycle.UserUsageSweepSummary{Accounts: 3, Swept: 2, RepointedEvents: 2, Failed: 1}, summary)
	require.Len(t, store.userRepointCalls, 3) // the account after the failure was still swept
}

func TestSweepUnattachedUserUsage_EmptyListIsNoOp(t *testing.T) {
	store := newFakeStore()
	summary, err := userSvc(store).SweepUnattachedUserUsage(context.Background())
	require.NoError(t, err)
	require.Equal(t, &cycle.UserUsageSweepSummary{}, summary)
	require.Empty(t, store.userRepointCalls)
}

func TestSweepUnattachedUserUsage_UnactivatedAccountSkipped(t *testing.T) {
	store := newFakeStore()
	user, acct := uuid.New(), uuid.New()
	store.userUnswept = []cycle.UserUsageAccount{{AccountID: acct, UserID: user}}
	store.userLazyEvents[user] = []fakeLazyEvent{{at: userNow.Add(-time.Hour), micros: 1}}

	summary, err := userSvc(store).SweepUnattachedUserUsage(context.Background())
	require.NoError(t, err)
	require.Equal(t, &cycle.UserUsageSweepSummary{Accounts: 1}, summary) // skipped, not failed
	require.Empty(t, store.userRepointCalls)
	require.Len(t, store.userLazyEvents[user], 1)
}

func TestGetUserUnbilledBacklog_ActivatedPendsFromActivationAnchoredWindow(t *testing.T) {
	store := newFakeStore()
	user, acct := uuid.New(), uuid.New()
	store.accountsByUser[user] = acct
	store.activation[acct] = userActivatedAt
	store.userLazyEvents[user] = []fakeLazyEvent{
		{at: time.Date(2026, 7, 12, 0, 0, 0, 0, time.UTC), micros: 100}, // inside the open window
		{at: time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC), micros: 50},   // D1d: before it
		{at: time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC), micros: 25},   // D1d: long before it
	}

	resp, err := userSvc(store).GetUserUnbilledBacklog(context.Background(), cycle.GetUserUnbilledBacklogRequest{UserID: user})
	require.NoError(t, err)
	require.Equal(t, &cycle.GetUserUnbilledBacklogResponse{
		AccountID:             acct,
		Activated:             true,
		UnbilledBacklogMicros: 175,
		PendingBacklogMicros:  100,
	}, resp)
	require.Equal(t, []time.Time{{}, userOpenWindowStart}, store.userBacklogWindows)
}

func TestGetUserUnbilledBacklog_NoAccountPendsFromWindowAnchoredNow(t *testing.T) {
	store := newFakeStore()
	user := uuid.New()
	store.userLazyEvents[user] = []fakeLazyEvent{
		{at: time.Date(2026, 7, 20, 1, 0, 0, 0, time.UTC), micros: 40}, // today: a card bound now bills it
		{at: time.Date(2026, 7, 19, 23, 0, 0, 0, time.UTC), micros: 60},
	}

	resp, err := userSvc(store).GetUserUnbilledBacklog(context.Background(), cycle.GetUserUnbilledBacklogRequest{UserID: user})
	require.NoError(t, err)
	require.Equal(t, &cycle.GetUserUnbilledBacklogResponse{
		UnbilledBacklogMicros: 100,
		PendingBacklogMicros:  40,
	}, resp)
	require.Equal(t, []time.Time{{}, userBindNowStart}, store.userBacklogWindows)
}

func TestGetUserUnbilledBacklog_UnactivatedAccountPendsFromWindowAnchoredNow(t *testing.T) {
	store := newFakeStore()
	user, acct := uuid.New(), uuid.New()
	store.accountsByUser[user] = acct // a row, but no card bound yet

	resp, err := userSvc(store).GetUserUnbilledBacklog(context.Background(), cycle.GetUserUnbilledBacklogRequest{UserID: user})
	require.NoError(t, err)
	require.Equal(t, acct, resp.AccountID)
	require.False(t, resp.Activated)
	require.Equal(t, []time.Time{{}, userBindNowStart}, store.userBacklogWindows)
}

func TestGetUserUnbilledBacklog_RequiresUser(t *testing.T) {
	_, err := userSvc(newFakeStore()).GetUserUnbilledBacklog(context.Background(), cycle.GetUserUnbilledBacklogRequest{})
	requireCode(t, err, billing.CodeInvalidInput)
}
