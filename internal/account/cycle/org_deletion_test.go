package cycle_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/mirrorstack-ai/billing-engine/internal/account/billing"
	"github.com/mirrorstack-ai/billing-engine/internal/account/cycle"
)

func TestFinalizeOrgDeletion_Validation(t *testing.T) {
	svc := cycle.NewService(newFakeStore(), nil)

	_, err := svc.FinalizeOrgDeletion(context.Background(), cycle.FinalizeOrgDeletionRequest{
		OperationID: uuid.New(),
	})
	requireBillingCode(t, err, billing.CodeInvalidInput)

	_, err = svc.FinalizeOrgDeletion(context.Background(), cycle.FinalizeOrgDeletionRequest{
		OrgID: uuid.New(),
	})
	requireBillingCode(t, err, billing.CodeInvalidInput)
}

func TestFinalizeOrgDeletion_FirstAndReplaySucceed(t *testing.T) {
	for _, outcome := range []cycle.OrgDeletionFinalizationOutcome{
		cycle.OrgDeletionFinalized,
		cycle.OrgDeletionAlreadyFinalized,
	} {
		t.Run(outcomeName(outcome), func(t *testing.T) {
			store := newFakeStore()
			store.orgDeletionOutcome = outcome
			now := time.Date(2026, 8, 22, 12, 30, 0, 0, time.UTC)
			svc := cycle.NewService(store, nil).WithNow(func() time.Time { return now })
			orgID, operationID := uuid.New(), uuid.New()

			resp, err := svc.FinalizeOrgDeletion(context.Background(), cycle.FinalizeOrgDeletionRequest{
				OrgID: orgID, OperationID: operationID,
			})
			require.NoError(t, err)
			require.Equal(t, &cycle.FinalizeOrgDeletionResponse{Finalized: true}, resp)
			require.Equal(t, orgID, store.orgDeletionOrg)
			require.Equal(t, operationID, store.orgDeletionOp)
			require.Equal(t, now, store.orgDeletionAt)
		})
	}
}

func TestFinalizeOrgDeletion_FailsClosedOutcomes(t *testing.T) {
	tests := []struct {
		name    string
		outcome cycle.OrgDeletionFinalizationOutcome
		code    billing.Code
	}{
		{"collectible invoices", cycle.OrgDeletionCollectibleInvoices, billing.CodePaymentRequired},
		{"money in flight", cycle.OrgDeletionMoneyInFlight, billing.CodeInternal},
		{"different operation", cycle.OrgDeletionOperationConflict, billing.CodeInvalidInput},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := newFakeStore()
			store.orgDeletionOutcome = tt.outcome
			svc := cycle.NewService(store, nil)
			_, err := svc.FinalizeOrgDeletion(context.Background(), cycle.FinalizeOrgDeletionRequest{
				OrgID: uuid.New(), OperationID: uuid.New(),
			})
			requireBillingCode(t, err, tt.code)
		})
	}
}

func TestFinalizeOrgDeletion_StoreErrorIsInternal(t *testing.T) {
	store := newFakeStore()
	store.errOrgDeletion = errors.New("database unavailable")
	svc := cycle.NewService(store, nil)

	_, err := svc.FinalizeOrgDeletion(context.Background(), cycle.FinalizeOrgDeletionRequest{
		OrgID: uuid.New(), OperationID: uuid.New(),
	})
	requireBillingCode(t, err, billing.CodeInternal)
}

func requireBillingCode(t *testing.T, err error, code billing.Code) {
	t.Helper()
	var billingErr *billing.Error
	require.ErrorAs(t, err, &billingErr)
	require.Equal(t, code, billingErr.Code)
}

func outcomeName(outcome cycle.OrgDeletionFinalizationOutcome) string {
	if outcome == cycle.OrgDeletionAlreadyFinalized {
		return "same operation replay"
	}
	return "first finalization"
}
