//go:build integration

package standing_test

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/mirrorstack-ai/billing-engine/internal/account/standing"
	"github.com/mirrorstack-ai/billing-engine/internal/shared/testutil"
)

func TestOwnerByStripeInvoice_FallsBackToCreditLedger(t *testing.T) {
	pool := testutil.NewTestDB(t)
	ctx := context.Background()
	accountID := uuid.New()
	ownerUserID := uuid.New()
	_, err := pool.Exec(ctx,
		`INSERT INTO ms_billing.accounts (id, owner_kind, owner_user_id)
		 VALUES ($1, 'user', $2)`,
		accountID, ownerUserID,
	)
	require.NoError(t, err)
	_, err = pool.Exec(ctx,
		`INSERT INTO ms_billing.credit_ledger
		   (account_id, amount_micros, type, status, balance_after_micros,
		    actor, idempotency_key, stripe_invoice_id)
		 VALUES ($1, 5000000, 'purchase', 'pending', 5000000,
		         'self', $2, 'in_ledger_only')`,
		accountID, "purchase:"+uuid.NewString(),
	)
	require.NoError(t, err)

	owner, found, err := standing.NewStore(pool).OwnerByStripeInvoice(ctx, "in_ledger_only")

	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, ownerUserID, owner.UserID)
	require.Equal(t, uuid.Nil, owner.OrgID)
}
