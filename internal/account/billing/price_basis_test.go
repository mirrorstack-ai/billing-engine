package billing_test

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/mirrorstack-ai/billing-engine/internal/account/billing"
)

// TestSetPayerPriceBasis_RefusesInvalidInput pins the RPC's validation gate
// (billing-engine#258): a basis above 10000 or below 1, and a missing reason,
// are both refused before any store write.
//
// 🔴 "Without the internal secret" is enforced by transport
// (cmd/account-api/main.go's InternalSecret route group), not by this
// Service method — there is no other caller of account-api's local HTTP
// surface, and the Lambda dispatch role gates the whole action dispatcher
// (CLAUDE.md "Two secrets, not one"). This test covers what the Service can
// enforce: the request-shape gate.
func TestSetPayerPriceBasis_RefusesInvalidInput(t *testing.T) {
	store := newFakeStore()
	svc := billing.NewService(store, &fakeStripe{}, "")
	org := uuid.New()

	_, err := svc.SetPayerPriceBasis(context.Background(), billing.SetPayerPriceBasisRequest{
		OwnerOrgID: org, PriceBasisBP: 10001, Reason: "test", SetBy: "ops@example.com",
	})
	require.Error(t, err, "above 10000 (above list) must be refused")

	_, err = svc.SetPayerPriceBasis(context.Background(), billing.SetPayerPriceBasisRequest{
		OwnerOrgID: org, PriceBasisBP: 0, Reason: "test", SetBy: "ops@example.com",
	})
	require.Error(t, err, "0 (free) must be refused")

	_, err = svc.SetPayerPriceBasis(context.Background(), billing.SetPayerPriceBasisRequest{
		OwnerOrgID: org, PriceBasisBP: 6000, SetBy: "ops@example.com",
	})
	require.Error(t, err, "a missing reason must be refused")

	_, err = svc.SetPayerPriceBasis(context.Background(), billing.SetPayerPriceBasisRequest{
		PriceBasisBP: 6000, Reason: "test", SetBy: "ops@example.com",
	})
	require.Error(t, err, "neither owner set must be refused")

	_, err = svc.SetPayerPriceBasis(context.Background(), billing.SetPayerPriceBasisRequest{
		OwnerUserID: uuid.New(), OwnerOrgID: org, PriceBasisBP: 6000, Reason: "test", SetBy: "ops@example.com",
	})
	require.Error(t, err, "both owners set must be refused")
}

// TestSetPayerPriceBasis_WritesAuditRowAndIsReadable pins D8-wholesale-60's
// operator flow end to end: set 6000 with a reason, read it back as the
// effective basis, then clear it back to the 10000 default — every step
// recorded in the audit trail with old/new/reason/who.
func TestSetPayerPriceBasis_WritesAuditRowAndIsReadable(t *testing.T) {
	store := newFakeStore()
	svc := billing.NewService(store, &fakeStripe{}, "")
	distributorOrg := uuid.New()

	got, err := svc.GetPayerPriceBasis(context.Background(), billing.GetPayerPriceBasisRequest{OwnerOrgID: distributorOrg})
	require.NoError(t, err)
	require.False(t, got.IsSet, "no row yet")
	require.EqualValues(t, 10000, got.PriceBasisBP, "no row means list")

	set, err := svc.SetPayerPriceBasis(context.Background(), billing.SetPayerPriceBasisRequest{
		OwnerOrgID:   distributorOrg,
		PriceBasisBP: 6000,
		Note:         "D8-wholesale-60: first distributor",
		Reason:       "D8-wholesale-60, owner 2026-10-25",
		SetBy:        "ops@example.com",
	})
	require.NoError(t, err)
	require.EqualValues(t, 6000, set.PriceBasisBP)

	got, err = svc.GetPayerPriceBasis(context.Background(), billing.GetPayerPriceBasisRequest{OwnerOrgID: distributorOrg})
	require.NoError(t, err)
	require.True(t, got.IsSet)
	require.EqualValues(t, 6000, got.PriceBasisBP)

	require.Len(t, store.priceBasisAudits, 1, "the set must write exactly one audit row")
	audit := store.priceBasisAudits[0]
	require.Nil(t, audit.oldBP, "no prior row: old is nil")
	require.NotNil(t, audit.newBP)
	require.EqualValues(t, 6000, *audit.newBP)
	require.Equal(t, "D8-wholesale-60, owner 2026-10-25", audit.reason)
	require.Equal(t, "ops@example.com", audit.setBy)

	cleared, err := svc.ClearPayerPriceBasis(context.Background(), billing.ClearPayerPriceBasisRequest{
		OwnerOrgID: distributorOrg, Reason: "contract ended", SetBy: "ops@example.com",
	})
	require.NoError(t, err)
	require.True(t, cleared.Removed)

	got, err = svc.GetPayerPriceBasis(context.Background(), billing.GetPayerPriceBasisRequest{OwnerOrgID: distributorOrg})
	require.NoError(t, err)
	require.False(t, got.IsSet, "cleared reverts to the default")
	require.EqualValues(t, 10000, got.PriceBasisBP)

	require.Len(t, store.priceBasisAudits, 2, "the clear must write exactly one more audit row")
	clearAudit := store.priceBasisAudits[1]
	require.NotNil(t, clearAudit.oldBP)
	require.EqualValues(t, 6000, *clearAudit.oldBP)
	require.Nil(t, clearAudit.newBP, "new is nil: reverted to the default")

	// A second clear is a no-op: no additional audit row.
	cleared, err = svc.ClearPayerPriceBasis(context.Background(), billing.ClearPayerPriceBasisRequest{
		OwnerOrgID: distributorOrg, Reason: "already gone", SetBy: "ops@example.com",
	})
	require.NoError(t, err)
	require.False(t, cleared.Removed)
	require.Len(t, store.priceBasisAudits, 2, "a no-op clear writes no audit row")
}
