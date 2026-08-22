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
		`INSERT INTO ms_billing.accounts
		    (id, owner_kind, owner_user_id, stripe_customer_id)
		 VALUES ($1, 'user', $2, $3)`,
		accountID, ownerUserID, "cus_"+accountID.String(),
	)
	require.NoError(t, err)
	_, err = pool.Exec(ctx,
		`INSERT INTO ms_billing.invoices (
			id, account_id, charge_funding_account_id, charge_funding_generation,
			stripe_invoice_id, status,
			amount_due, amount_paid, currency
		 ) SELECT $1, $2, auth.funding_account_id, auth.generation,
		          'in_ordinary_only', 'paid', 500, 500, 'usd'
		   FROM ms_billing.account_funding_authorizations auth
		   WHERE auth.account_id = $2`,
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
		`INSERT INTO ms_billing.accounts
		    (id, owner_kind, owner_user_id, stripe_customer_id)
		 VALUES ($1, 'user', $2, $3)`,
		accountID, ownerUserID, "cus_"+accountID.String(),
	)
	require.NoError(t, err)
	_, err = pool.Exec(ctx,
		`INSERT INTO ms_billing.credit_ledger
		   (account_id, amount_micros, type, status, balance_after_micros,
		    actor, idempotency_key, stripe_invoice_id,
		    attempt_stripe_customer_id, charge_funding_account_id,
		    charge_funding_generation)
		 SELECT $1, 5000000, 'purchase', 'pending', 5000000,
		        'self', $2, 'in_ledger_only', account.stripe_customer_id,
		        funding.funding_account_id, funding.generation
		 FROM ms_billing.account_funding_authorizations funding
		 JOIN ms_billing.accounts account ON account.id=funding.funding_account_id
		 WHERE funding.account_id=$1`,
		accountID, "purchase:"+uuid.NewString(),
	)
	require.NoError(t, err)

	owner, found, err := standing.NewStore(pool).OwnerByCreditInvoice(ctx, "in_ledger_only")

	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, ownerUserID, owner.UserID)
	require.Equal(t, uuid.Nil, owner.OrgID)
}
