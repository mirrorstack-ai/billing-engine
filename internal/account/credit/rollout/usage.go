package rollout

import (
	"context"

	"github.com/mirrorstack-ai/billing-engine/internal/account/credit"
)

// ReadOnlyUsageEvaluator is the shadow-only usage seam. Implementations may
// read authoritative wallet/projection state, but must not write Redis, notify
// standing, execute auto-top-up, or enter a money transaction.
type ReadOnlyUsageEvaluator interface {
	EvaluateUsageReadOnly(context.Context, credit.UsageEvent) error
}

// ReadOnlyUsageEvaluatorFunc adapts a function to ReadOnlyUsageEvaluator.
type ReadOnlyUsageEvaluatorFunc func(context.Context, credit.UsageEvent) error

func (f ReadOnlyUsageEvaluatorFunc) EvaluateUsageReadOnly(ctx context.Context, event credit.UsageEvent) error {
	if f == nil {
		return ErrEvaluatorUnavailable
	}
	return f(ctx, event)
}

// UsageEvaluator is the account-scoped production adapter injected into usage
// ingest. Off/excluded events call neither implementation. Shadow calls only
// the read-only implementation. Enforce calls only the mutation-capable
// coordinator.
type UsageEvaluator struct {
	controller *Controller
	shadow     ReadOnlyUsageEvaluator
	enforce    credit.UsageEvaluator
}

func NewUsageEvaluator(
	controller *Controller,
	shadow ReadOnlyUsageEvaluator,
	enforce credit.UsageEvaluator,
) *UsageEvaluator {
	return &UsageEvaluator{
		controller: controller,
		shadow:     shadow,
		enforce:    enforce,
	}
}

func (e *UsageEvaluator) EvaluateCreditUsage(ctx context.Context, event credit.UsageEvent) error {
	if e == nil || e.controller == nil {
		return nil
	}
	decision := e.controller.Decide(event.AccountID)
	if !decision.Selected {
		return nil
	}

	started := e.controller.nowFn()
	var err error
	switch decision.Mode {
	case ModeShadow:
		if e.shadow == nil {
			err = ErrEvaluatorUnavailable
		} else {
			err = e.shadow.EvaluateUsageReadOnly(ctx, event)
		}
	case ModeEnforce:
		if e.enforce == nil {
			err = ErrEvaluatorUnavailable
		} else {
			err = e.enforce.EvaluateCreditUsage(ctx, event)
		}
	default:
		return nil
	}
	e.controller.Observe(decision, e.controller.nowFn().Sub(started), err)
	return err
}
