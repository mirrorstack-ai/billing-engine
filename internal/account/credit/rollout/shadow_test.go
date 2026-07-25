package rollout

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/mirrorstack-ai/billing-engine/internal/account/credit"
)

var shadowPeriodStart = time.Date(2026, time.July, 1, 0, 0, 0, 0, time.UTC)

// allCapabilitySpy intentionally exposes mutation-looking methods in addition
// to the two read providers. CreditShadowEvaluator receives it only through
// SnapshotProvider and ProjectionProvider and must never discover or invoke
// any of the extra capabilities.
type allCapabilitySpy struct {
	snapshot   CreditSnapshot
	projection credit.Projection
	snapshotErr,
	projectionErr error

	snapshotCalls   int
	projectionCalls int
	counterWrites   int
	notifications   int
	autoTopUps      int
	moneyWrites     int
}

func (s *allCapabilitySpy) CreditSnapshot(context.Context, uuid.UUID) (CreditSnapshot, error) {
	s.snapshotCalls++
	return s.snapshot, s.snapshotErr
}

func (s *allCapabilitySpy) ProjectedCreditCharge(context.Context, uuid.UUID, uuid.UUID) (credit.Projection, error) {
	s.projectionCalls++
	return s.projection, s.projectionErr
}

func (s *allCapabilitySpy) SetCounter() {
	s.counterWrites++
}

func (s *allCapabilitySpy) NotifyOwner() {
	s.notifications++
}

func (s *allCapabilitySpy) TriggerAutoTopUp() {
	s.autoTopUps++
}

func (s *allCapabilitySpy) WriteMoney() {
	s.moneyWrites++
}

func creditsSnapshot(spendable int64) CreditSnapshot {
	return CreditSnapshot{
		OwnerUserID:            uuid.New(),
		BillingMode:            creditsBillingMode,
		SettledBalanceMicros:   spendable,
		SpendableBalanceMicros: spendable,
	}
}

func TestCreditShadowEvaluatorExactStrictShortfallFacts(t *testing.T) {
	for _, tc := range []struct {
		name       string
		mutate     func(*CreditSnapshot)
		projection int64
		blocked    bool
		project    bool
	}{
		{name: "below", projection: 99, project: true},
		{name: "equality is eligible", projection: 100, project: true},
		{name: "strict shortfall", projection: 101, blocked: true, project: true},
		{
			name: "negative settled residual blocks even at zero projection",
			mutate: func(snapshot *CreditSnapshot) {
				snapshot.SettledBalanceMicros = -1
				snapshot.SpendableBalanceMicros = 0
			},
			blocked: true, project: true,
		},
		{
			name: "pending auto topup grants grace",
			mutate: func(snapshot *CreditSnapshot) {
				snapshot.PendingAutoTopUp = true
			},
			projection: 101, project: true,
		},
		{
			name: "nonzero credit limit remains legacy eligible",
			mutate: func(snapshot *CreditSnapshot) {
				snapshot.CreditLimitMicros = 1
			},
			projection: 101, project: true,
		},
		{
			name: "standard never projects",
			mutate: func(snapshot *CreditSnapshot) {
				snapshot.BillingMode = "standard"
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			snapshot := creditsSnapshot(100)
			if tc.mutate != nil {
				tc.mutate(&snapshot)
			}
			spy := &allCapabilitySpy{
				snapshot: snapshot,
				projection: credit.Projection{
					AmountMicros: tc.projection,
					PeriodStart:  shadowPeriodStart,
					PeriodEnd:    shadowPeriodStart.AddDate(0, 1, 0),
				},
			}
			evaluator := NewCreditShadowEvaluator(spy, spy)

			result, err := evaluator.EvaluateCreditReadOnly(context.Background(), uuid.New())

			require.NoError(t, err)
			require.Equal(t, tc.blocked, result.Blocked)
			require.Equal(t, snapshot.BillingMode, result.Facts.BillingMode)
			require.Equal(t, snapshot.SettledBalanceMicros, result.Facts.SettledBalanceMicros)
			require.Equal(t, snapshot.SpendableBalanceMicros, result.Facts.SpendableBalanceMicros)
			require.Equal(t, snapshot.CreditLimitMicros, result.Facts.CreditLimitMicros)
			require.Equal(t, snapshot.PendingAutoTopUp, result.Facts.PendingAutoTopUp)
			require.Equal(t, 1, spy.snapshotCalls)
			if tc.project {
				require.Equal(t, 1, spy.projectionCalls)
				require.Equal(t, tc.projection, result.Facts.ProjectedChargeMicros)
				require.Equal(t, shadowPeriodStart, result.Facts.PeriodStart)
			} else {
				require.Zero(t, spy.projectionCalls)
				require.Zero(t, result.Facts.ProjectedChargeMicros)
			}
			require.Zero(t, spy.counterWrites)
			require.Zero(t, spy.notifications)
			require.Zero(t, spy.autoTopUps)
			require.Zero(t, spy.moneyWrites)
		})
	}
}

func TestCreditShadowEvaluatorHasOnlyReadProviderFields(t *testing.T) {
	evaluatorType := reflect.TypeOf(CreditShadowEvaluator{})
	require.Equal(t, 2, evaluatorType.NumField())
	require.Equal(t, "SnapshotProvider", evaluatorType.Field(0).Type.Name())
	require.Equal(t, "ProjectionProvider", evaluatorType.Field(1).Type.Name())
}

func TestCreditShadowEvaluatorErrorsAreReadOnlyAndFactsReachController(t *testing.T) {
	accountID := uuid.MustParse("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa")
	spy := &allCapabilitySpy{
		snapshot: creditsSnapshot(100),
		projection: credit.Projection{
			AmountMicros: 101,
			PeriodStart:  shadowPeriodStart,
			PeriodEnd:    shadowPeriodStart.AddDate(0, 1, 0),
		},
	}
	evaluator := NewCreditShadowEvaluator(spy, spy)
	cfg := validConfig()
	cfg.BasisPoints = "0"
	setTestAllowlist(&cfg, accountID.String())

	result := NewController(Parse(cfg), nil).CompareBoolean(
		context.Background(),
		accountID,
		false,
		evaluator,
	)
	require.NoError(t, result.Err)
	require.True(t, result.Evaluated)
	require.True(t, result.Wallet)
	require.True(t, result.Diverged)
	require.False(t, result.Effective, "shadow retains legacy")
	require.NotNil(t, result.Facts)
	require.EqualValues(t, 101, result.Facts.ProjectedChargeMicros)

	spy.projectionErr = errors.New("projection unavailable")
	result = NewController(Parse(cfg), nil).CompareBoolean(
		context.Background(),
		accountID,
		false,
		evaluator,
	)
	require.ErrorContains(t, result.Err, "projection unavailable")
	require.False(t, result.Diverged)
	require.NotNil(t, result.Facts,
		"snapshot/projection facts remain available to diagnose evaluator errors")
	require.Zero(t, spy.counterWrites)
	require.Zero(t, spy.notifications)
	require.Zero(t, spy.autoTopUps)
	require.Zero(t, spy.moneyWrites)
}

func TestCreditShadowEvaluatorNilProvidersFailClosed(t *testing.T) {
	_, err := NewCreditShadowEvaluator(nil, nil).
		EvaluateCreditReadOnly(context.Background(), uuid.New())
	require.ErrorIs(t, err, ErrEvaluatorUnavailable)
}
