package rollout

import (
	"context"

	"github.com/google/uuid"

	"github.com/mirrorstack-ai/billing-engine/internal/account/credit"
)

// Gate is the account-scoped production adapter for serving eligibility.
// Off/excluded accounts call no wallet implementation. Shadow invokes only the
// read-only evaluator and always preserves the legacy false verdict. Enforce
// invokes only the mutation-capable coordinator gate and applies its verdict
// only after a successful evaluation.
type Gate struct {
	controller *Controller
	shadow     ReadOnlyBooleanEvaluator
	enforce    credit.Gate
}

func NewGate(
	controller *Controller,
	shadow ReadOnlyBooleanEvaluator,
	enforce credit.Gate,
) *Gate {
	return &Gate{
		controller: controller,
		shadow:     shadow,
		enforce:    enforce,
	}
}

// EvaluateStanding compares the already-computed legacy standing verdict with
// the selected wallet result. Standing is additive: a wallet result may block
// an otherwise eligible account, but can never unblock a legacy-blocked one.
// Shadow evaluates only the read-only seam and preserves legacy. Evaluator
// errors also preserve legacy while remaining visible to the caller and EMF.
func (g *Gate) EvaluateStanding(
	ctx context.Context,
	accountID uuid.UUID,
	legacyBlocked bool,
) BooleanResult {
	result := BooleanResult{
		Legacy:    legacyBlocked,
		Effective: legacyBlocked,
	}
	if g == nil || g.controller == nil {
		result.Decision = offPolicy("").Decide(accountID)
		return result
	}

	result.Decision = g.controller.Decide(accountID)
	if !result.Decision.Selected {
		return result
	}

	started := g.controller.nowFn()
	switch result.Decision.Mode {
	case ModeShadow:
		switch evaluator := g.shadow.(type) {
		case nil:
			result.Err = ErrEvaluatorUnavailable
		case ReadOnlyCreditEvaluator:
			var evaluation CreditEvaluation
			evaluation, result.Err = evaluator.EvaluateCreditReadOnly(ctx, accountID)
			result.Wallet = evaluation.Blocked
			result.Facts = &evaluation.Facts
			result.Evaluated = true
		default:
			result.Wallet, result.Err = evaluator.EvaluateReadOnly(ctx, accountID)
			result.Evaluated = true
		}
	case ModeEnforce:
		if g.enforce == nil {
			result.Err = ErrEvaluatorUnavailable
		} else {
			result.Wallet, result.Err = g.enforce.OutOfCredits(ctx, accountID)
			result.Evaluated = true
		}
	default:
		return result
	}

	duration := g.controller.nowFn().Sub(started)
	if result.Err == nil {
		additive := legacyBlocked || result.Wallet
		result.Diverged = additive != legacyBlocked
		if result.Decision.Enforced() {
			result.Effective = additive
		}
	}
	g.controller.emit(Observation{
		Decision:       result.Decision,
		Duration:       duration,
		Diverged:       result.Diverged,
		EvaluatorError: result.Err != nil,
	})
	return result
}

func (g *Gate) OutOfCredits(
	ctx context.Context,
	accountID uuid.UUID,
) (bool, error) {
	if g == nil || g.controller == nil {
		return false, nil
	}

	decision := g.controller.Decide(accountID)
	if !decision.Selected {
		return false, nil
	}

	switch decision.Mode {
	case ModeShadow:
		started := g.controller.nowFn()
		var err error
		if g.shadow == nil {
			err = ErrEvaluatorUnavailable
		} else {
			_, err = g.shadow.EvaluateReadOnly(ctx, accountID)
		}
		g.controller.Observe(decision, g.controller.nowFn().Sub(started), err)
		// Gate exposes only the wallet out-of-credit fact. It cannot know the
		// already-computed legacy standing verdict, so shadow preserves false
		// and deliberately emits no misleading final-verdict divergence.
		return false, err
	case ModeEnforce:
		started := g.controller.nowFn()
		var (
			blocked bool
			err     error
		)
		if g.enforce == nil {
			err = ErrEvaluatorUnavailable
		} else {
			blocked, err = g.enforce.OutOfCredits(ctx, accountID)
		}
		duration := g.controller.nowFn().Sub(started)
		// The billing service combines this fact additively with its legacy
		// standing verdict. Gate therefore reports evaluation/error/latency but
		// cannot truthfully report final-verdict divergence.
		g.controller.Observe(decision, duration, err)
		if err != nil {
			return false, err
		}
		return blocked, nil
	default:
		return false, nil
	}
}
