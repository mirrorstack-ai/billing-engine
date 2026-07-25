//go:build integration

package standing_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/mirrorstack-ai/billing-engine/internal/account/standing"
	"github.com/mirrorstack-ai/billing-engine/internal/shared/testutil"
)

func TestOwnerByStripeInvoice_OrdinaryRouteNeverReadsCreditLedger(t *testing.T) {
	pool := testutil.NewTestDB(t)
	ctx := context.Background()
	accountID, ownerUserID := uuid.New(), uuid.New()
	_, err := pool.Exec(ctx,
		`INSERT INTO ms_billing.accounts (id, owner_kind, owner_user_id)
		 VALUES ($1, 'user', $2)`,
		accountID, ownerUserID,
	)
	require.NoError(t, err)
	_, err = pool.Exec(ctx,
		`INSERT INTO ms_billing.invoices (
			id, account_id, stripe_invoice_id, status,
			amount_due, amount_paid, currency
		 ) VALUES ($1, $2, 'in_ordinary_only', 'paid', 500, 500, 'usd')`,
		uuid.New(), accountID,
	)
	require.NoError(t, err)

	lock, err := pool.Begin(ctx)
	require.NoError(t, err)
	defer func() { _ = lock.Rollback(ctx) }()
	_, err = lock.Exec(ctx, `LOCK TABLE ms_billing.credit_ledger IN ACCESS EXCLUSIVE MODE`)
	require.NoError(t, err)

	store := standing.NewStore(pool)
	ordinaryCtx, cancelOrdinary := context.WithTimeout(ctx, time.Second)
	defer cancelOrdinary()
	owner, found, err := store.OwnerByStripeInvoice(ordinaryCtx, "in_ordinary_only")
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, ownerUserID, owner.UserID)

	controlCtx, cancelControl := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancelControl()
	_, _, err = store.OwnerByCreditInvoice(controlCtx, "in_missing_credit")
	require.Error(t, err,
		"the explicit credit route must hit the locked ledger, proving the no-touch tripwire")
}

func TestOwnerByCreditInvoice_UsesExplicitCreditLedgerRoute(t *testing.T) {
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

	owner, found, err := standing.NewStore(pool).OwnerByCreditInvoice(ctx, "in_ledger_only")

	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, ownerUserID, owner.UserID)
	require.Equal(t, uuid.Nil, owner.OrgID)
}
