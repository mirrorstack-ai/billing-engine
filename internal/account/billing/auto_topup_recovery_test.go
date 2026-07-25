package billing_test

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/mirrorstack-ai/billing-engine/internal/account/autotopup"
	"github.com/mirrorstack-ai/billing-engine/internal/account/billing"
	"github.com/mirrorstack-ai/billing-engine/internal/account/creditrecovery"
)

type fakeAutoTopUpRecovery struct {
	calls  []uuid.UUID
	result autotopup.Result
	err    error
}

func (f *fakeAutoTopUpRecovery) Recover(
	_ context.Context,
	accountID uuid.UUID,
) (autotopup.Result, error) {
	f.calls = append(f.calls, accountID)
	return f.result, f.err
}

func TestRecoverAutoTopUpBypassesCurrentRolloutOnlyForOwnedExistingAccount(t *testing.T) {
	userID, accountID, attemptID := uuid.New(), uuid.New(), uuid.New()
	store := newFakeStore()
	store.accountsByUser[userID] = fakeAccount{id: accountID}
	recovery := &fakeAutoTopUpRecovery{result: autotopup.Result{
		Triggered: true,
		AttemptID: attemptID,
		Status:    "settled",
	}}
	svc := billing.NewService(store, &fakeStripe{}, "").
		WithCreditWallet(false).
		WithCreditAccess(func(uuid.UUID) bool { return false }).
		WithAutoTopUpRecovery(recovery)

	response, err := svc.RecoverAutoTopUp(
		context.Background(),
		billing.RecoverAutoTopUpRequest{OwnerUserID: userID},
	)

	require.NoError(t, err)
	require.Equal(t, &billing.RecoverAutoTopUpResponse{
		Recovered: true,
		AttemptID: attemptID.String(),
		Status:    "settled",
	}, response)
	require.Equal(t, []uuid.UUID{accountID}, recovery.calls)
}

func TestRecoverAutoTopUpMissingOwnerAccountIsNoOp(t *testing.T) {
	recovery := &fakeAutoTopUpRecovery{err: errors.New("must not run")}
	svc := billing.NewService(newFakeStore(), &fakeStripe{}, "").
		WithAutoTopUpRecovery(recovery)

	response, err := svc.RecoverAutoTopUp(
		context.Background(),
		billing.RecoverAutoTopUpRequest{OwnerOrgID: uuid.New()},
	)

	require.NoError(t, err)
	require.Equal(t, &billing.RecoverAutoTopUpResponse{}, response)
	require.Empty(t, recovery.calls)
}

func TestRecoverAutoTopUpRequiresRecoveryWiringForExistingAccount(t *testing.T) {
	userID, accountID := uuid.New(), uuid.New()
	store := newFakeStore()
	store.accountsByUser[userID] = fakeAccount{id: accountID}
	svc := billing.NewService(store, &fakeStripe{}, "")

	response, err := svc.RecoverAutoTopUp(
		context.Background(),
		billing.RecoverAutoTopUpRequest{OwnerUserID: userID},
	)

	require.Nil(t, response)
	requireBillingErrorCode(t, err, billing.CodeUnavailable)
}

func TestRecoverAutoTopUpCapabilityFalseIsUnavailableBeforeExecutorAndReprobes(t *testing.T) {
	userID, accountID := uuid.New(), uuid.New()
	store := newFakeStore()
	store.accountsByUser[userID] = fakeAccount{id: accountID}
	recovery := &fakeAutoTopUpRecovery{
		result: autotopup.Result{Triggered: true, Status: "settled"},
	}
	ready := false
	probeCalls := 0
	capability := creditrecovery.NewRuntimeCapability(
		func(context.Context) (bool, error) {
			probeCalls++
			return ready, nil
		},
	)
	svc := billing.NewService(store, &fakeStripe{}, "").
		WithCreditWallet(false).
		WithAutoTopUpRecovery(creditrecovery.GuardAutoTopUpRecovery(
			capability,
			recovery,
		))
	req := billing.RecoverAutoTopUpRequest{OwnerUserID: userID}

	response, err := svc.RecoverAutoTopUp(context.Background(), req)
	require.Nil(t, response)
	requireBillingErrorCode(t, err, billing.CodeUnavailable)
	require.Empty(t, recovery.calls)
	require.Equal(t, 1, probeCalls)

	ready = true
	response, err = svc.RecoverAutoTopUp(context.Background(), req)
	require.NoError(t, err)
	require.True(t, response.Recovered)
	require.Equal(t, []uuid.UUID{accountID}, recovery.calls)
	require.Equal(t, 2, probeCalls, "false is re-probed after migration expansion")

	ready = false
	_, err = svc.RecoverAutoTopUp(context.Background(), req)
	require.NoError(t, err, "cached TRUE permits already-authorized recovery with master OFF")
	require.Equal(t, 2, probeCalls, "TRUE is cached")
	require.Len(t, recovery.calls, 2)
}

func TestRecoverAutoTopUpCapabilityProbeErrorIsUnavailableBeforeExecutor(t *testing.T) {
	userID, accountID := uuid.New(), uuid.New()
	store := newFakeStore()
	store.accountsByUser[userID] = fakeAccount{id: accountID}
	recovery := &fakeAutoTopUpRecovery{}
	capability := creditrecovery.NewRuntimeCapability(
		func(context.Context) (bool, error) {
			return false, errors.New("catalog unavailable")
		},
	)
	svc := billing.NewService(store, &fakeStripe{}, "").
		WithAutoTopUpRecovery(creditrecovery.GuardAutoTopUpRecovery(
			capability,
			recovery,
		))

	response, err := svc.RecoverAutoTopUp(
		context.Background(),
		billing.RecoverAutoTopUpRequest{OwnerUserID: userID},
	)

	require.Nil(t, response)
	requireBillingErrorCode(t, err, billing.CodeUnavailable)
	require.Empty(t, recovery.calls)
}
