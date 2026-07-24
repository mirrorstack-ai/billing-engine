package billing_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/mirrorstack-ai/billing-engine/internal/account/billing"
)

type fixedCreditGate struct {
	blocked bool
	err     error
	calls   int
}

func (g *fixedCreditGate) OutOfCredits(context.Context, uuid.UUID) (bool, error) {
	g.calls++
	return g.blocked, g.err
}

func TestGetServiceStatusCreditGateIsFlaggedAndAdditive(t *testing.T) {
	store := newFakeStore()
	userID, accountID := uuid.New(), uuid.New()
	store.accountsByUser[userID] = fakeAccount{id: accountID}
	store.serviceSignals[accountID] = billing.ServiceSignals{
		UsableCardCount: 1, FirstChargeStatus: "paid",
	}
	gate := &fixedCreditGate{blocked: true}
	svc := billing.NewService(store, &fakeStripe{}, "").
		WithCreditCoordinator(gate, nil)

	legacy, err := svc.GetServiceStatus(context.Background(), billing.GetServiceStatusRequest{UserID: userID})
	require.NoError(t, err)
	require.Equal(t, "ELIGIBLE", legacy.Reason)
	require.Zero(t, gate.calls)

	svc.WithCreditWallet(true)
	wallet, err := svc.GetServiceStatus(context.Background(), billing.GetServiceStatusRequest{UserID: userID})
	require.NoError(t, err)
	require.Equal(t, "OUT_OF_CREDITS", wallet.Reason)
	require.Equal(t, []string{"OUT_OF_CREDITS"}, wallet.Reasons)
}

func TestGetServiceStatusLegacyReasonStaysPrimaryAndGateFailureFailsOpen(t *testing.T) {
	store := newFakeStore()
	userID, accountID := uuid.New(), uuid.New()
	store.accountsByUser[userID] = fakeAccount{id: accountID}
	store.serviceSignals[accountID] = billing.ServiceSignals{FirstChargeStatus: "paid"}

	gate := &fixedCreditGate{blocked: true}
	svc := billing.NewService(store, &fakeStripe{}, "").
		WithCreditWallet(true).
		WithCreditCoordinator(gate, nil)
	resp, err := svc.GetServiceStatus(context.Background(), billing.GetServiceStatusRequest{UserID: userID})
	require.NoError(t, err)
	require.Equal(t, "NO_USABLE_CARD", resp.Reason)
	require.Equal(t, []string{"NO_USABLE_CARD", "OUT_OF_CREDITS"}, resp.Reasons)

	gate.blocked, gate.err = false, errors.New("projection unavailable")
	resp, err = svc.GetServiceStatus(context.Background(), billing.GetServiceStatusRequest{UserID: userID})
	require.NoError(t, err)
	require.Equal(t, []string{"NO_USABLE_CARD"}, resp.Reasons)
}

func TestGetServiceStatusWalletEnabledStandardAccountIsByteIdentical(t *testing.T) {
	store := newFakeStore()
	userID, accountID := uuid.New(), uuid.New()
	store.accountsByUser[userID] = fakeAccount{id: accountID}
	store.serviceSignals[accountID] = billing.ServiceSignals{
		UsableCardCount: 1, FirstChargeStatus: "paid",
	}
	gate := &fixedCreditGate{}
	svc := billing.NewService(store, &fakeStripe{}, "").
		WithCreditCoordinator(gate, nil)
	req := billing.GetServiceStatusRequest{UserID: userID}

	legacy, err := svc.GetServiceStatus(context.Background(), req)
	require.NoError(t, err)
	legacyBytes, err := json.Marshal(legacy)
	require.NoError(t, err)

	svc.WithCreditWallet(true)
	standard, err := svc.GetServiceStatus(context.Background(), req)
	require.NoError(t, err)
	standardBytes, err := json.Marshal(standard)
	require.NoError(t, err)

	require.Equal(t, string(legacyBytes), string(standardBytes))
	require.Equal(t, 1, gate.calls)
}
