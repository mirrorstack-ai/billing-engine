package billing

import (
	"context"
	"errors"

	"github.com/google/uuid"

	"github.com/mirrorstack-ai/billing-engine/internal/account/autotopup"
	"github.com/mirrorstack-ai/billing-engine/internal/account/creditrecovery"
)

// AutoTopUpRecovery is the narrow explicit-recovery seam. The executor owns
// every frozen payment fact; the billing RPC supplies only the resolved owner
// account and can never create a replacement attempt.
type AutoTopUpRecovery interface {
	Recover(context.Context, uuid.UUID) (autotopup.Result, error)
}

// WithAutoTopUpRecovery installs the explicit liveness path for a previously
// authorized pending attempt. Production wires it regardless of current
// rollout mode; construction performs no database or Stripe operation.
func (s *Service) WithAutoTopUpRecovery(recovery AutoTopUpRecovery) *Service {
	s.autoTopUpRecovery = recovery
	return s
}

// RecoverAutoTopUp resumes only an existing owner-scoped pending attempt.
// Rollout-off and cohort exclusion deliberately do not block this operation:
// a rollback must not strand already-authorized Stripe state. A missing owner
// account or missing pending attempt is an idempotent no-op.
func (s *Service) RecoverAutoTopUp(
	ctx context.Context,
	req RecoverAutoTopUpRequest,
) (*RecoverAutoTopUpResponse, error) {
	if err := validateCreditOwner(req.OwnerUserID, req.OwnerOrgID); err != nil {
		return nil, err
	}
	accountID, found, err := s.ownerAccount(
		ctx,
		req.OwnerUserID,
		req.OwnerOrgID,
	)
	if err != nil {
		return nil, Internal("account lookup failed", err)
	}
	if !found {
		return &RecoverAutoTopUpResponse{}, nil
	}
	if s.autoTopUpRecovery == nil {
		return nil, Unavailable("automatic credit top-up recovery is not configured")
	}

	result, err := s.autoTopUpRecovery.Recover(ctx, accountID)
	if err != nil {
		if errors.Is(err, creditrecovery.ErrUnavailable) {
			return nil, Unavailable("automatic credit top-up recovery runtime is unavailable")
		}
		return nil, Internal("recover automatic credit top-up failed", err)
	}
	response := &RecoverAutoTopUpResponse{
		Recovered:   result.Triggered,
		Status:      result.Status,
		FailureCode: result.FailureCode,
	}
	if result.AttemptID != uuid.Nil {
		response.AttemptID = result.AttemptID.String()
	}
	return response, nil
}
