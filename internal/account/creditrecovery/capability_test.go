package creditrecovery_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/mirrorstack-ai/billing-engine/internal/account/autotopup"
	"github.com/mirrorstack-ai/billing-engine/internal/account/creditledger"
	"github.com/mirrorstack-ai/billing-engine/internal/account/creditpurchase"
	"github.com/mirrorstack-ai/billing-engine/internal/account/creditrecovery"
)

type fakeRecoveryExecutor struct {
	recoverCalls atomic.Int32
	resumeCalls  atomic.Int32
	paidCalls    atomic.Int32
	failureCalls atomic.Int32
}

func (f *fakeRecoveryExecutor) Recover(
	context.Context,
	uuid.UUID,
) (autotopup.Result, error) {
	f.recoverCalls.Add(1)
	return autotopup.Result{Triggered: true}, nil
}

func (f *fakeRecoveryExecutor) Resume(
	context.Context,
	creditpurchase.Attempt,
) (creditpurchase.Result, error) {
	f.resumeCalls.Add(1)
	return creditpurchase.Result{}, nil
}

func (f *fakeRecoveryExecutor) ReconcileWebhookPaid(
	context.Context,
	string,
) (creditledger.Settlement, error) {
	f.paidCalls.Add(1)
	return creditledger.Settlement{Found: true}, nil
}

func (f *fakeRecoveryExecutor) ReconcileWebhookFailure(
	context.Context,
	string,
	string,
) (creditledger.FailureReconciliation, error) {
	f.failureCalls.Add(1)
	return creditledger.FailureReconciliation{Found: true}, nil
}

func TestRuntimeCapabilityReprobesFalseAndErrorThenCachesTrue(t *testing.T) {
	var calls atomic.Int32
	results := []struct {
		ready bool
		err   error
	}{
		{},
		{err: errors.New("database unavailable")},
		{ready: true},
	}
	capability := creditrecovery.NewRuntimeCapability(
		func(context.Context) (bool, error) {
			index := int(calls.Add(1)) - 1
			return results[index].ready, results[index].err
		},
	)

	require.ErrorIs(t, capability.Require(context.Background()), creditrecovery.ErrUnavailable)
	require.ErrorIs(t, capability.Require(context.Background()), creditrecovery.ErrUnavailable)
	require.NoError(t, capability.Require(context.Background()))
	require.NoError(t, capability.Require(context.Background()))
	require.EqualValues(t, 3, calls.Load(), "only TRUE is cached")
}

func TestRuntimeCapabilityColdConstructionDoesNotProbe(t *testing.T) {
	var calls atomic.Int32
	_ = creditrecovery.NewRuntimeCapability(func(context.Context) (bool, error) {
		calls.Add(1)
		return true, nil
	})
	require.Zero(t, calls.Load())
}

func TestRuntimeCapabilityConcurrentTrueProbeRunsOnce(t *testing.T) {
	var calls atomic.Int32
	capability := creditrecovery.NewRuntimeCapability(
		func(context.Context) (bool, error) {
			calls.Add(1)
			return true, nil
		},
	)

	var wg sync.WaitGroup
	for range 32 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			require.NoError(t, capability.Require(context.Background()))
		}()
	}
	wg.Wait()
	require.EqualValues(t, 1, calls.Load())
}

func TestGuardedRecoveryEntrancesDoNotCallExecutorsWhileUnavailable(t *testing.T) {
	capability := creditrecovery.NewRuntimeCapability(
		func(context.Context) (bool, error) { return false, nil },
	)
	executor := &fakeRecoveryExecutor{}

	auto := creditrecovery.GuardAutoTopUpRecovery(capability, executor)
	_, err := auto.Recover(context.Background(), uuid.New())
	require.ErrorIs(t, err, creditrecovery.ErrUnavailable)

	manual := creditrecovery.GuardManualPurchaseExecutor(capability, executor)
	_, err = manual.Resume(context.Background(), creditpurchase.Attempt{
		ID: uuid.New(), AccountID: uuid.New(),
	})
	require.ErrorIs(t, err, creditrecovery.ErrUnavailable)

	webhook := creditrecovery.GuardWebhookReconciler(capability, executor)
	_, err = webhook.ReconcileWebhookPaid(context.Background(), "in_paid")
	require.ErrorIs(t, err, creditrecovery.ErrUnavailable)
	_, err = webhook.ReconcileWebhookFailure(
		context.Background(),
		"in_failed",
		"payment_failed",
	)
	require.ErrorIs(t, err, creditrecovery.ErrUnavailable)

	require.Zero(t, executor.recoverCalls.Load())
	require.Zero(t, executor.resumeCalls.Load())
	require.Zero(t, executor.paidCalls.Load())
	require.Zero(t, executor.failureCalls.Load())
}

func TestGuardedRecoveryEntrancesProceedAfterCapabilityBecomesReady(t *testing.T) {
	var ready atomic.Bool
	capability := creditrecovery.NewRuntimeCapability(
		func(context.Context) (bool, error) { return ready.Load(), nil },
	)
	executor := &fakeRecoveryExecutor{}
	auto := creditrecovery.GuardAutoTopUpRecovery(capability, executor)

	_, err := auto.Recover(context.Background(), uuid.New())
	require.ErrorIs(t, err, creditrecovery.ErrUnavailable)
	ready.Store(true)
	_, err = auto.Recover(context.Background(), uuid.New())
	require.NoError(t, err)
	ready.Store(false)
	_, err = auto.Recover(context.Background(), uuid.New())
	require.NoError(t, err, "a proven runtime remains usable after master rollout OFF")
	require.EqualValues(t, 2, executor.recoverCalls.Load())
}
