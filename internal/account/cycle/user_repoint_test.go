package cycle_test

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

func TestRepointUserUsage_ActivatedRepointsIntoOpenWindowAndReportsBacklog(t *testing.T) {
	store := newFakeStore()
	user, acct := uuid.New(), uuid.New()
	store.accountsByUser[user] = acct
	store.activation[acct] = orgNow.AddDate(0, 0, -2) // card bound Jul 4 → anchor day 4
	store.userNullEvents[acct] = 3
	store.userBacklog[acct] = 1_250_000
	svc := orgSvc(store)

	resp, err := svc.RepointUserUsage(context.Background(), cycle.RepointUserUsageRequest{OwnerUserID: user})
	require.NoError(t, err)
	require.Equal(t, &cycle.RepointUserUsageResponse{
		AccountID: acct, Funded: true, RepointedEvents: 3, UnbilledBacklogMicros: 1_250_000,
	}, resp)
	require.Equal(t, []userRepointCall{{acct, time.Date(2026, 7, 4, 0, 0, 0, 0, time.UTC)}}, store.userRepointCalls)

	// Re-fire: idempotent — nothing new repoints.
	resp, err = svc.RepointUserUsage(context.Background(), cycle.RepointUserUsageRequest{OwnerUserID: user})
	require.NoError(t, err)
	require.Zero(t, resp.RepointedEvents)
}

func TestRepointUserUsage_NoAccountOrUnactivatedIsUnfundedNoOp(t *testing.T) {
	store := newFakeStore()
	svc := orgSvc(store)

	resp, err := svc.RepointUserUsage(context.Background(), cycle.RepointUserUsageRequest{OwnerUserID: uuid.New()})
	require.NoError(t, err)
	require.False(t, resp.Funded)

	user := uuid.New()
	store.accountsByUser[user] = uuid.New() // row exists, never bound a card
	resp, err = svc.RepointUserUsage(context.Background(), cycle.RepointUserUsageRequest{OwnerUserID: user})
	require.NoError(t, err)
	require.False(t, resp.Funded)
	require.Empty(t, store.userRepointCalls)

	_, err = svc.RepointUserUsage(context.Background(), cycle.RepointUserUsageRequest{})
	requireCode(t, err, billing.CodeInvalidInput)
}

func TestSweepUnattachedUserUsage_RepointsAndCountsFailures(t *testing.T) {
	store := newFakeStore()
	accts := []uuid.UUID{uuid.New(), uuid.New(), uuid.New(), uuid.New()}
	store.userUnswept = accts
	for _, a := range accts[:3] {
		store.activation[a] = orgNow
		store.userNullEvents[a] = 2
	}
	store.errUserRepoint = map[uuid.UUID]error{accts[1]: errors.New("transient")}
	// accts[3] is unactivated: the work list never names one, so it is a failure.

	summary, err := orgSvc(store).SweepUnattachedUserUsage(context.Background())
	require.NoError(t, err)
	require.Equal(t, &cycle.UserUsageSweepSummary{Accounts: 4, Swept: 2, RepointedEvents: 4, Failed: 2}, summary)
	require.Len(t, store.userRepointCalls, 2)
}

func TestSweepUnattachedUserUsage_EmptyListIsNoOp(t *testing.T) {
	store := newFakeStore()
	summary, err := orgSvc(store).SweepUnattachedUserUsage(context.Background())
	require.NoError(t, err)
	require.Equal(t, &cycle.UserUsageSweepSummary{}, summary)
	require.Empty(t, store.userRepointCalls)
}
