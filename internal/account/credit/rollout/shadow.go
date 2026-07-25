package rollout

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/mirrorstack-ai/billing-engine/internal/account/credit"
)

const creditsBillingMode = "credits"

// CreditSnapshot is the complete read-only wallet state needed to reproduce
// the credits-only serving verdict. It deliberately carries no store, counter,
// notifier, executor, or transaction capability.
type CreditSnapshot struct {
	OwnerUserID            uuid.UUID
	OwnerOrgID             uuid.UUID
	BillingMode            string
	SettledBalanceMicros   int64
	SpendableBalanceMicros int64
	CreditLimitMicros      int64
	PendingAutoTopUp       bool
}

// SnapshotProvider is the shadow evaluator's only wallet-state dependency.
// Implementations must be read-only; production integration should adapt the
// account snapshot query directly rather than wrapping a Coordinator.
type SnapshotProvider interface {
	CreditSnapshot(context.Context, uuid.UUID) (CreditSnapshot, error)
}

// SnapshotProviderFunc adapts a read-only snapshot function.
type SnapshotProviderFunc func(context.Context, uuid.UUID) (CreditSnapshot, error)

func (f SnapshotProviderFunc) CreditSnapshot(ctx context.Context, accountID uuid.UUID) (CreditSnapshot, error) {
	if f == nil {
		return CreditSnapshot{}, ErrEvaluatorUnavailable
	}
	return f(ctx, accountID)
}

// CreditFacts are non-mutating facts useful for explaining an expected
// legacy-vs-wallet divergence. Owner/account identifiers are intentionally
// absent so the value is safe to attach to low-cardinality diagnostics.
type CreditFacts struct {
	BillingMode            string
	SettledBalanceMicros   int64
	SpendableBalanceMicros int64
	CreditLimitMicros      int64
	PendingAutoTopUp       bool
	ProjectedChargeMicros  int64
	PeriodStart            time.Time
	PeriodEnd              time.Time
}

// CreditEvaluation is the exact read-only wallet verdict plus its supporting
// facts. Blocked uses the same strict-shortfall rule as the enforce
// coordinator: equality is eligible.
type CreditEvaluation struct {
	Blocked bool
	Facts   CreditFacts
}

// ReadOnlyCreditEvaluator lets Controller retain supporting facts while still
// satisfying its generic boolean comparison seam.
type ReadOnlyCreditEvaluator interface {
	ReadOnlyBooleanEvaluator
	EvaluateCreditReadOnly(context.Context, uuid.UUID) (CreditEvaluation, error)
}

// CreditShadowEvaluator is structurally incapable of mutating wallet state:
// its only fields are a read-only snapshot provider and the authoritative
// projection provider. There is no Counter, Notifier, auto-top-up executor, or
// money-store seam to invoke accidentally.
type CreditShadowEvaluator struct {
	snapshots  SnapshotProvider
	projection credit.ProjectionProvider
}

func NewCreditShadowEvaluator(
	snapshots SnapshotProvider,
	projection credit.ProjectionProvider,
) *CreditShadowEvaluator {
	return &CreditShadowEvaluator{snapshots: snapshots, projection: projection}
}

func (e *CreditShadowEvaluator) EvaluateReadOnly(
	ctx context.Context,
	accountID uuid.UUID,
) (bool, error) {
	evaluation, err := e.EvaluateCreditReadOnly(ctx, accountID)
	return evaluation.Blocked, err
}

func (e *CreditShadowEvaluator) EvaluateCreditReadOnly(
	ctx context.Context,
	accountID uuid.UUID,
) (CreditEvaluation, error) {
	if e == nil || e.snapshots == nil || e.projection == nil {
		return CreditEvaluation{}, ErrEvaluatorUnavailable
	}

	snapshot, err := e.snapshots.CreditSnapshot(ctx, accountID)
	if err != nil {
		return CreditEvaluation{}, fmt.Errorf("credit shadow snapshot: %w", err)
	}
	evaluation := CreditEvaluation{Facts: CreditFacts{
		BillingMode:            snapshot.BillingMode,
		SettledBalanceMicros:   snapshot.SettledBalanceMicros,
		SpendableBalanceMicros: snapshot.SpendableBalanceMicros,
		CreditLimitMicros:      snapshot.CreditLimitMicros,
		PendingAutoTopUp:       snapshot.PendingAutoTopUp,
	}}
	if snapshot.BillingMode != creditsBillingMode {
		return evaluation, nil
	}

	projection, err := e.projection.ProjectedCreditCharge(
		ctx,
		snapshot.OwnerUserID,
		snapshot.OwnerOrgID,
	)
	evaluation.Facts.ProjectedChargeMicros = projection.AmountMicros
	evaluation.Facts.PeriodStart = projection.PeriodStart
	evaluation.Facts.PeriodEnd = projection.PeriodEnd
	if err != nil {
		return evaluation, fmt.Errorf("credit shadow projection: %w", err)
	}

	evaluation.Blocked =
		snapshot.CreditLimitMicros == 0 &&
			!snapshot.PendingAutoTopUp &&
			(snapshot.SettledBalanceMicros < 0 ||
				projection.AmountMicros > snapshot.SpendableBalanceMicros)
	return evaluation, nil
}
