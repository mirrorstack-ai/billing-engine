package cycle

import (
	"context"

	"github.com/google/uuid"

	"github.com/mirrorstack-ai/billing-engine/internal/account/billing"
)

// FinalizeOrgDeletionRequest is the control-plane hand-off after application
// and domain teardown. OperationID is the durable replay identity shared with
// api-platform's organization-deletion operation.
type FinalizeOrgDeletionRequest struct {
	OrgID       uuid.UUID `json:"org_id"`
	OperationID uuid.UUID `json:"operation_id"`
}

type FinalizeOrgDeletionResponse struct {
	Finalized bool `json:"finalized"`
}

// OrgDeletionFinalizationOutcome keeps storage race decisions out of wire
// error parsing. Only the first two outcomes are successful responses.
type OrgDeletionFinalizationOutcome uint8

const (
	OrgDeletionFinalized OrgDeletionFinalizationOutcome = iota
	OrgDeletionAlreadyFinalized
	OrgDeletionCollectibleInvoices
	OrgDeletionMoneyInFlight
	OrgDeletionOperationConflict
)

// FinalizeOrgDeletion creates the immutable billing tombstone and retires
// future funding/entitlement in one transaction. Retained accounts, invoices,
// payment-method audit rows, usage and ledger history are never deleted.
func (s *Service) FinalizeOrgDeletion(ctx context.Context, req FinalizeOrgDeletionRequest) (*FinalizeOrgDeletionResponse, error) {
	if req.OrgID == uuid.Nil {
		return nil, billing.InvalidInput("org_id required")
	}
	if req.OperationID == uuid.Nil {
		return nil, billing.InvalidInput("operation_id required")
	}

	outcome, err := s.store.FinalizeOrgDeletionBilling(
		ctx, req.OrgID, req.OperationID, s.nowFn().UTC(),
	)
	if err != nil {
		return nil, billing.Internal("finalize organization billing failed", err)
	}

	switch outcome {
	case OrgDeletionFinalized, OrgDeletionAlreadyFinalized:
		return &FinalizeOrgDeletionResponse{Finalized: true}, nil
	case OrgDeletionCollectibleInvoices:
		return nil, billing.PaymentRequired("organization has collectible invoices")
	case OrgDeletionMoneyInFlight:
		return nil, billing.Internal("organization has an in-flight billing operation; retry finalization after reconciliation", nil)
	case OrgDeletionOperationConflict:
		return nil, billing.InvalidInput("organization billing was finalized by a different deletion operation")
	default:
		return nil, billing.Internal("unknown organization billing finalization outcome", nil)
	}
}
