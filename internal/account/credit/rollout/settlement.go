package rollout

import (
	"context"

	"github.com/google/uuid"

	"github.com/mirrorstack-ai/billing-engine/internal/account/credit"
)

// SettlementObserver is the account-scoped adapter for post-commit runtime
// reconciliation. Recovery of an already-authorized invoice may commit durable
// ledger truth after an account leaves a cohort; this adapter lets that money
// recovery complete while preventing the excluded/off account from entering
// the coordinator graph afterward.
type SettlementObserver struct {
	controller *Controller
	enforce    credit.SettlementObserver
}

func NewSettlementObserver(
	controller *Controller,
	enforce credit.SettlementObserver,
) *SettlementObserver {
	return &SettlementObserver{controller: controller, enforce: enforce}
}

func (o *SettlementObserver) ObserveAccount(
	ctx context.Context,
	accountID uuid.UUID,
) error {
	if o == nil || o.controller == nil {
		return nil
	}
	decision := o.controller.Decide(accountID)
	if !decision.Enforced() {
		return nil
	}

	started := o.controller.nowFn()
	var err error
	if o.enforce == nil {
		err = ErrEvaluatorUnavailable
	} else {
		err = o.enforce.ObserveAccount(ctx, accountID)
	}
	o.controller.Observe(decision, o.controller.nowFn().Sub(started), err)
	return err
}
