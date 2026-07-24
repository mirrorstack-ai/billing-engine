package rollout

import (
	"context"
	"time"

	"github.com/google/uuid"
)

// ReadOnlyBooleanEvaluator is a wallet comparison read. Implementations used
// for shadow must not update Redis, notify standing, trigger auto-top-up, or
// enter a money transaction.
type ReadOnlyBooleanEvaluator interface {
	EvaluateReadOnly(context.Context, uuid.UUID) (bool, error)
}

// ReadOnlyBooleanEvaluatorFunc adapts a function to ReadOnlyBooleanEvaluator.
type ReadOnlyBooleanEvaluatorFunc func(context.Context, uuid.UUID) (bool, error)

func (f ReadOnlyBooleanEvaluatorFunc) EvaluateReadOnly(ctx context.Context, accountID uuid.UUID) (bool, error) {
	if f == nil {
		return false, ErrEvaluatorUnavailable
	}
	return f(ctx, accountID)
}

// BooleanResult records one account-scoped comparison. Effective is always the
// legacy value for off, excluded, shadow, and evaluator-error paths. Only a
// successful selected enforce evaluation may replace it.
type BooleanResult struct {
	Decision  Decision
	Legacy    bool
	Wallet    bool
	Effective bool
	Evaluated bool
	Diverged  bool
	Facts     *CreditFacts
	Err       error
}

// Controller applies one immutable policy and reports every selected
// evaluation. It does not retain account identifiers.
type Controller struct {
	policy   Policy
	reporter *Reporter
	nowFn    func() time.Time
}

func NewController(policy Policy, reporter *Reporter) *Controller {
	return &Controller{policy: policy, reporter: reporter, nowFn: time.Now}
}

func (c *Controller) WithNow(nowFn func() time.Time) *Controller {
	if nowFn != nil {
		c.nowFn = nowFn
	}
	return c
}

// Decide exposes the stable result for mutation gates. A caller may execute a
// wallet mutation only when the returned decision's Enforced method is true.
func (c *Controller) Decide(accountID uuid.UUID) Decision {
	if c == nil {
		return offPolicy("").Decide(accountID)
	}
	return c.policy.Decide(accountID)
}

// CompareBoolean evaluates a selected account through an explicitly read-only
// seam. Off and excluded accounts return before invoking evaluator. Shadow
// always preserves legacy. Enforce applies the wallet result only when the
// evaluator succeeds; callers can log Err without changing the legacy path.
func (c *Controller) CompareBoolean(
	ctx context.Context,
	accountID uuid.UUID,
	legacy bool,
	evaluator ReadOnlyBooleanEvaluator,
) BooleanResult {
	result := BooleanResult{
		Decision:  c.Decide(accountID),
		Legacy:    legacy,
		Effective: legacy,
	}
	if !result.Decision.Selected {
		return result
	}

	started := c.nowFn()
	switch evaluator := evaluator.(type) {
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
	duration := c.nowFn().Sub(started)
	result.Diverged = result.Err == nil && result.Wallet != legacy
	if result.Err == nil && result.Decision.Enforced() {
		result.Effective = result.Wallet
	}
	if c.reporter != nil {
		_ = c.reporter.Emit(Observation{
			Decision:       result.Decision,
			Duration:       duration,
			Diverged:       result.Diverged,
			EvaluatorError: result.Err != nil,
		})
	}
	return result
}
