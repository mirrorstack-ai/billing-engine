package rollout

import "github.com/google/uuid"

// ReadOnlySelectedAccess authorizes wallet-aware projection reads for any
// selected shadow or enforce account. It must only be installed on a dedicated
// read-only evaluator store: Decision.Selected is deliberately broader than
// Enforced and never authorizes a mutation or a public response change.
func ReadOnlySelectedAccess(controller *Controller) func(uuid.UUID) bool {
	return func(accountID uuid.UUID) bool {
		if controller == nil {
			return false
		}
		return controller.Decide(accountID).Selected
	}
}
