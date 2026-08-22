//go:build integration

package billing_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/mirrorstack-ai/billing-engine/internal/account/billing"
	"github.com/mirrorstack-ai/billing-engine/internal/account/cycle"
	"github.com/mirrorstack-ai/billing-engine/internal/shared/testutil"
)

type distributorDeletionFixture struct {
	distributorOrgID     uuid.UUID
	distributorAccountID uuid.UUID
	customerOrgID        uuid.UUID
	customerAccountID    uuid.UUID
	operationID          uuid.UUID
}

func seedDistributorDeletionFixture(t *testing.T, pool *pgxpool.Pool) distributorDeletionFixture {
	t.Helper()
	ctx := context.Background()
	f := distributorDeletionFixture{
		distributorOrgID:     uuid.New(),
		distributorAccountID: uuid.New(),
		customerOrgID:        uuid.New(),
		customerAccountID:    uuid.New(),
		operationID:          uuid.New(),
	}
	_, err := pool.Exec(ctx, `
		INSERT INTO ms_billing.accounts (id, owner_kind, owner_org_id)
		VALUES ($1, 'org', $2), ($3, 'org', $4)`,
		f.distributorAccountID, f.distributorOrgID,
		f.customerAccountID, f.customerOrgID)
	require.NoError(t, err)
	_, err = pool.Exec(ctx, `
		INSERT INTO ms_billing.org_billing_designations
		    (org_id, funding, sponsor_account_id, sponsor_user_id, updated_by)
		VALUES ($1, 'sponsor', $2, $3, $3)`,
		f.customerOrgID, f.distributorAccountID, uuid.New())
	require.NoError(t, err)
	return f
}

func TestDistributorMutationAuthority_FinalizerWinsBeforeStaleWrites(t *testing.T) {
	pool := testutil.NewTestDB(t)
	ctx := context.Background()
	f := seedDistributorDeletionFixture(t, pool)
	cycleStore := cycle.NewStore(pool)
	billingStore := billing.NewStore(pool)
	authority := &billing.DistributorMutationAuthority{
		DistributorOrgID: f.distributorOrgID,
		CustomerOrgID:    f.customerOrgID,
	}

	outcome, err := cycleStore.FinalizeOrgDeletionBilling(
		ctx, f.distributorOrgID, f.operationID, time.Now().UTC(),
	)
	require.NoError(t, err)
	require.Equal(t, cycle.OrgDeletionFinalized, outcome)

	changed, err := billingStore.SetCreditBillingMode(
		ctx,
		f.customerAccountID,
		authority,
		billing.BillingModeCredits,
		billing.DefaultCreditsLimitMicros,
	)
	require.Error(t, err)
	require.False(t, changed)
	_, err = billingStore.InsertCreditGrant(
		ctx,
		f.customerAccountID,
		authority,
		1_000_000,
		"distributor",
		"stale-distributor:"+uuid.NewString(),
		nil,
	)
	require.Error(t, err)

	var mode string
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT billing_mode FROM ms_billing.accounts WHERE id=$1`,
		f.customerAccountID).Scan(&mode))
	require.Equal(t, "standard", mode)
	var grants int
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT count(*) FROM ms_billing.credit_ledger
		WHERE account_id=$1 AND type='grant'`, f.customerAccountID).Scan(&grants))
	require.Zero(t, grants)
}

func TestDistributorMutationAuthority_WriterWinsBeforeFinalizer(t *testing.T) {
	pool := testutil.NewTestDB(t)
	ctx := context.Background()
	f := seedDistributorDeletionFixture(t, pool)
	cycleStore := cycle.NewStore(pool)

	writer, err := pool.Begin(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { _ = writer.Rollback(context.Background()) })
	_, err = writer.Exec(ctx, `
		SELECT ms_billing.assert_org_billing_active_pair($1, $2)`,
		f.customerOrgID, f.distributorOrgID)
	require.NoError(t, err)
	var authorizedAccountID uuid.UUID
	require.NoError(t, writer.QueryRow(ctx, `
		SELECT customer.id
		FROM ms_billing.org_billing_designations designation
		JOIN ms_billing.accounts distributor
		  ON distributor.id=designation.sponsor_account_id
		 AND distributor.owner_kind='org'
		 AND distributor.owner_org_id=$1
		JOIN ms_billing.accounts customer
		  ON customer.owner_kind='org'
		 AND customer.owner_org_id=designation.org_id
		WHERE designation.org_id=$2 AND designation.funding='sponsor'`,
		f.distributorOrgID, f.customerOrgID).Scan(&authorizedAccountID))
	require.Equal(t, f.customerAccountID, authorizedAccountID)

	type result struct {
		outcome cycle.OrgDeletionFinalizationOutcome
		err     error
	}
	finished := make(chan result, 1)
	go func() {
		outcome, finalizeErr := cycleStore.FinalizeOrgDeletionBilling(
			context.Background(), f.distributorOrgID, f.operationID, time.Now().UTC(),
		)
		finished <- result{outcome: outcome, err: finalizeErr}
	}()
	select {
	case got := <-finished:
		t.Fatalf("finalizer bypassed authorized distributor writer: %+v", got)
	case <-time.After(150 * time.Millisecond):
	}

	_, err = writer.Exec(ctx, `
		UPDATE ms_billing.accounts
		SET billing_mode='credits', credit_limit_micros=$2
		WHERE id=$1`, f.customerAccountID, billing.DefaultCreditsLimitMicros)
	require.NoError(t, err)
	_, err = writer.Exec(ctx, `
		INSERT INTO ms_billing.credit_ledger
		    (account_id, amount_micros, type, status, balance_after_micros,
		     actor, idempotency_key)
		VALUES ($1, 1000000, 'grant', 'settled', 1000000,
		        'distributor', $2)`,
		f.customerAccountID, "writer-first:"+uuid.NewString())
	require.NoError(t, err)
	require.NoError(t, writer.Commit(ctx))

	select {
	case got := <-finished:
		require.NoError(t, got.err)
		require.Equal(t, cycle.OrgDeletionFinalized, got.outcome)
	case <-time.After(5 * time.Second):
		t.Fatal("finalizer did not resume after distributor writer committed")
	}
}

func TestStartAddPaymentMethod_FinalizerWinsBeforeSetupIntentStamp(t *testing.T) {
	pool := testutil.NewTestDB(t)
	ctx := context.Background()
	orgID, accountID, operationID := uuid.New(), uuid.New(), uuid.New()
	_, err := pool.Exec(ctx, `
		INSERT INTO ms_billing.accounts
		    (id, owner_kind, owner_org_id, stripe_customer_id)
		VALUES ($1, 'org', $2, $3)`,
		accountID, orgID, "cus_add_card_"+accountID.String())
	require.NoError(t, err)

	checkoutEntered := make(chan struct{})
	releaseCheckout := make(chan struct{})
	stripe := &fakeStripe{beforeCheckout: func() {
		close(checkoutEntered)
		<-releaseCheckout
	}}
	billingStore := billing.NewStore(pool)
	service := billing.NewService(billingStore, stripe, "")
	type startResult struct {
		response *billing.StartAddPaymentMethodResponse
		err      error
	}
	started := make(chan startResult, 1)
	go func() {
		response, startErr := service.StartAddPaymentMethod(
			context.Background(),
			billing.StartAddPaymentMethodRequest{OrgID: orgID},
		)
		started <- startResult{response: response, err: startErr}
	}()
	select {
	case <-checkoutEntered:
	case <-time.After(5 * time.Second):
		t.Fatal("Start did not reach Stripe after persisting its pending request")
	}

	cycleStore := cycle.NewStore(pool)
	outcome, err := cycleStore.FinalizeOrgDeletionBilling(
		ctx, orgID, operationID, time.Now().UTC(),
	)
	require.NoError(t, err)
	require.Equal(t, cycle.OrgDeletionFinalized, outcome)
	close(releaseCheckout)

	select {
	case got := <-started:
		require.Error(t, got.err)
		require.Nil(t, got.response)
		var billingErr *billing.Error
		require.ErrorAs(t, got.err, &billingErr)
		require.Equal(t, billing.CodeUnavailable, billingErr.Code)
	case <-time.After(5 * time.Second):
		t.Fatal("Start did not reject its session after finalization canceled the request")
	}

	var status string
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT status::text FROM ms_billing.add_card_requests
		WHERE account_id=$1`, accountID).Scan(&status))
	require.Equal(t, "failed", status)
	_, err = billingStore.InsertAddCardRequest(ctx, accountID)
	require.Error(t, err, "retired org cannot open another add-card request")
}
