//go:build integration

package cycle_test

import (
	"context"
	"math/big"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/mirrorstack-ai/billing-engine/internal/account/cycle"
	"github.com/mirrorstack-ai/billing-engine/internal/account/db"
	"github.com/mirrorstack-ai/billing-engine/internal/shared/testutil"
)

type orgDeletionFixture struct {
	orgID, accountID, appID, timerID, domainID, paymentMethodID uuid.UUID
	operationID                                                 uuid.UUID
	paidInvoiceID                                               string
}

type sponsoredFundingFixture struct {
	sponsorOrgID, sponsorAccountID, sponsorOperationID    uuid.UUID
	customerOrgID, customerAccountID, customerOperationID uuid.UUID
	appID, domainID, timerID                              uuid.UUID
	appCreatedAt                                          time.Time
}

func seedSponsorAccount(t *testing.T, pool *pgxpool.Pool, customerPrefix string) (uuid.UUID, uuid.UUID) {
	t.Helper()
	ctx := context.Background()
	orgID, accountID := uuid.New(), uuid.New()
	_, err := pool.Exec(ctx, `
		INSERT INTO ms_billing.accounts
		    (id, owner_kind, owner_org_id, stripe_customer_id, activated_at)
		VALUES ($1, 'org', $2, $3, now())`,
		accountID, orgID, customerPrefix+accountID.String())
	require.NoError(t, err)
	_, err = pool.Exec(ctx, `
		INSERT INTO ms_billing.payment_methods_mirror
		    (id, account_id, stripe_payment_method_id, brand, last4,
		     exp_month, exp_year, is_default)
		VALUES ($1, $2, $3, 'visa', '4242', 12, 2099, true)`,
		uuid.New(), accountID, "pm_"+accountID.String())
	require.NoError(t, err)
	return orgID, accountID
}

func seedSponsoredFundingFixture(t *testing.T, pool *pgxpool.Pool) sponsoredFundingFixture {
	t.Helper()
	ctx := context.Background()
	f := sponsoredFundingFixture{
		sponsorOperationID: uuid.New(), customerOrgID: uuid.New(),
		customerAccountID: uuid.New(), customerOperationID: uuid.New(),
		appID: uuid.New(), domainID: uuid.New(), timerID: uuid.New(),
		appCreatedAt: time.Now().UTC().Add(-5 * 24 * time.Hour),
	}
	f.sponsorOrgID, f.sponsorAccountID = seedSponsorAccount(t, pool, "cus_sponsor_")
	_, err := pool.Exec(ctx, `
		INSERT INTO ms_billing.accounts
		    (id, owner_kind, owner_org_id, activated_at)
		VALUES ($1, 'org', $2, now() - interval '30 days')`,
		f.customerAccountID, f.customerOrgID)
	require.NoError(t, err)
	_, err = pool.Exec(ctx, `
		INSERT INTO ms_billing.org_billing_designations
		    (org_id, funding, sponsor_account_id, sponsor_user_id, updated_by)
		VALUES ($1, 'sponsor', $2, $3, $3)`,
		f.customerOrgID, f.sponsorAccountID, uuid.New())
	require.NoError(t, err)
	_, err = pool.Exec(ctx, `
		INSERT INTO ms_billing.apps
		    (app_id, account_id, owner_org_id, module_count,
		     created_module_count, created_at, name)
		VALUES ($1, $2, $3, 1, 1, $4, 'sponsored funding race')`,
		f.appID, f.customerAccountID, f.customerOrgID, f.appCreatedAt)
	require.NoError(t, err)
	_, err = pool.Exec(ctx, `
		INSERT INTO ms_billing.app_custom_domains
		    (id, account_id, app_id, hostname, activated_at)
		VALUES ($1, $2, $3, $4, $5)`,
		f.domainID, f.customerAccountID, f.appID,
		"funding-"+f.domainID.String()+".test", f.appCreatedAt)
	require.NoError(t, err)
	_, err = pool.Exec(ctx, `
		INSERT INTO ms_billing.app_module_overage_timers
		    (id, account_id, app_id, installed_at, grace_expires_at)
		VALUES ($1, $2, $3, $4::timestamptz,
		        $4::timestamptz + interval '3 days')`,
		f.timerID, f.customerAccountID, f.appID, f.appCreatedAt)
	require.NoError(t, err)
	return f
}

func seedUnarmedManualPurchase(t *testing.T, pool *pgxpool.Pool, accountID uuid.UUID) uuid.UUID {
	t.Helper()
	purchaseID := uuid.New()
	_, err := pool.Exec(context.Background(), `
		INSERT INTO ms_billing.credit_ledger
		    (id, account_id, amount_micros, type, status,
		     balance_after_micros, actor, idempotency_key)
		VALUES ($1, $2, 5000000, 'purchase', 'pending',
		        5000000, 'self', $3)`,
		purchaseID, accountID, "purchase:"+purchaseID.String())
	require.NoError(t, err)
	return purchaseID
}

func TestMigration052_RoundTrip(t *testing.T) {
	pool := testutil.NewTestDB(t)
	ctx := context.Background()
	_, err := pool.Exec(ctx, migrationSQL(t, "052_org_deletion_finalizations.down.sql"))
	require.NoError(t, err)
	_, err = pool.Exec(ctx, migrationSQL(t, "052_org_deletion_finalizations.up.sql"))
	require.NoError(t, err)
	var finalizationTable, retiredSponsorshipTable string
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT to_regclass('ms_billing.org_deletion_finalizations')::text,
		       to_regclass('ms_billing.org_deletion_retired_sponsorships')::text`).
		Scan(&finalizationTable, &retiredSponsorshipTable))
	require.Equal(t, "ms_billing.org_deletion_finalizations", finalizationTable)
	require.Equal(t, "ms_billing.org_deletion_retired_sponsorships", retiredSponsorshipTable)
}

func TestMigration052_QuarantinesLegacyPayerProvenanceWithoutGuessingCurrentDesignation(t *testing.T) {
	pool := testutil.NewTestDB(t)
	ctx := context.Background()
	_, err := pool.Exec(ctx, migrationSQL(t, "052_org_deletion_finalizations.down.sql"))
	require.NoError(t, err)
	f := seedSponsoredFundingFixture(t, pool)
	_, err = pool.Exec(ctx, `
		UPDATE ms_billing.app_custom_domains
		SET charge_attempted_at=now()
		WHERE id=$1`, f.domainID)
	require.NoError(t, err)
	invoiceID := "in_legacy_unknown_" + uuid.NewString()
	_, err = pool.Exec(ctx, `
		INSERT INTO ms_billing.invoices
		    (account_id, stripe_invoice_id, status, amount_due, amount_paid)
		VALUES ($1, $2, 'open', 900, 0)`, f.customerAccountID, invoiceID)
	require.NoError(t, err)

	_, err = pool.Exec(ctx, migrationSQL(t, "052_org_deletion_finalizations.up.sql"))
	require.NoError(t, err)
	var (
		domainLegacy, invoiceLegacy bool
		domainFunder, invoiceFunder pgtype.UUID
	)
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT charge_funding_legacy_unresolved, charge_funding_account_id
		FROM ms_billing.app_custom_domains WHERE id=$1`, f.domainID).
		Scan(&domainLegacy, &domainFunder))
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT charge_funding_legacy_unresolved, charge_funding_account_id
		FROM ms_billing.invoices WHERE stripe_invoice_id=$1`, invoiceID).
		Scan(&invoiceLegacy, &invoiceFunder))
	require.True(t, domainLegacy)
	require.False(t, domainFunder.Valid)
	require.True(t, invoiceLegacy)
	require.False(t, invoiceFunder.Valid)

	_, err = db.New(pool).ArmDomainStripeCharge(ctx, db.ArmDomainStripeChargeParams{
		DomainID: f.domainID.String(), AttemptedAt: time.Now().UTC(),
	})
	require.Error(t, err, "a quarantined attempt must never adopt today's sponsor")

	store := cycle.NewStore(pool)
	outcome, err := store.FinalizeOrgDeletionBilling(
		ctx, f.sponsorOrgID, f.sponsorOperationID, time.Now().UTC(),
	)
	require.NoError(t, err)
	require.Equal(t, cycle.OrgDeletionCollectibleInvoices, outcome,
		"unknown historical invoice payer is a fail-closed collectible barrier")
	_, err = pool.Exec(ctx, `
		UPDATE ms_billing.invoices SET status='paid', amount_due=0, amount_paid=900
		WHERE stripe_invoice_id=$1`, invoiceID)
	require.NoError(t, err)
	outcome, err = store.FinalizeOrgDeletionBilling(
		ctx, f.sponsorOrgID, f.sponsorOperationID, time.Now().UTC(),
	)
	require.NoError(t, err)
	require.Equal(t, cycle.OrgDeletionMoneyInFlight, outcome,
		"unknown historical attempt payer stays quarantined until reconciled")
}

func seedOrgDeletionFixture(t *testing.T, pool *pgxpool.Pool) orgDeletionFixture {
	t.Helper()
	ctx := context.Background()
	f := orgDeletionFixture{
		orgID: uuid.New(), accountID: uuid.New(), appID: uuid.New(),
		timerID: uuid.New(), domainID: uuid.New(), paymentMethodID: uuid.New(),
		operationID: uuid.New(),
	}
	f.paidInvoiceID = "in_paid_" + f.accountID.String()
	createdAt := time.Date(2026, 8, 1, 8, 0, 0, 0, time.UTC)

	_, err := pool.Exec(ctx, `
		INSERT INTO ms_billing.accounts
		    (id, owner_kind, owner_org_id, stripe_customer_id, activated_at)
		VALUES ($1, 'org', $2, $3, $4)`,
		f.accountID, f.orgID, "cus_"+f.accountID.String(), createdAt)
	require.NoError(t, err)
	_, err = pool.Exec(ctx, `
		INSERT INTO ms_billing.org_billing_designations
		    (org_id, funding, updated_by)
		VALUES ($1, 'org', $2)`, f.orgID, uuid.New())
	require.NoError(t, err)
	_, err = pool.Exec(ctx, `
		INSERT INTO ms_billing.org_billing_designations
		    (org_id, funding, sponsor_account_id, sponsor_user_id, updated_by)
		VALUES ($1, 'sponsor', $2, $3, $3)`, uuid.New(), f.accountID, uuid.New())
	require.NoError(t, err)
	_, err = pool.Exec(ctx, `
		INSERT INTO ms_billing.apps
		    (app_id, account_id, owner_org_id, module_count,
		     created_module_count, created_at, name)
		VALUES ($1, $2, $3, 1, 1, $4, 'retained app')`,
		f.appID, f.accountID, f.orgID, createdAt)
	require.NoError(t, err)
	_, err = pool.Exec(ctx, `
		INSERT INTO ms_billing.app_module_overage_timers
		    (id, account_id, app_id, installed_at, grace_expires_at)
		VALUES ($1, $2, $3, $4::timestamptz, $4::timestamptz + interval '72 hours')`,
		f.timerID, f.accountID, f.appID, createdAt)
	require.NoError(t, err)
	_, err = pool.Exec(ctx, `
		INSERT INTO ms_billing.app_custom_domains
		    (id, account_id, app_id, hostname, activated_at)
		VALUES ($1, $2, $3, $4, $5)`,
		f.domainID, f.accountID, f.appID, "org-"+f.orgID.String()+".example.test", createdAt)
	require.NoError(t, err)
	_, err = pool.Exec(ctx, `
		INSERT INTO ms_billing.payment_methods_mirror
		    (id, account_id, stripe_payment_method_id, brand, last4,
		     exp_month, exp_year, is_default)
		VALUES ($1, $2, $3, 'visa', '4242', 12, 2035, true)`,
		f.paymentMethodID, f.accountID, "pm_"+f.paymentMethodID.String())
	require.NoError(t, err)
	_, err = pool.Exec(ctx, `
		INSERT INTO ms_billing.credit_auto_topup_configs
		    (account_id, enabled, threshold_micros, amount_micros, payment_method_id)
		VALUES ($1, true, 5000000, 10000000, $2)`,
		f.accountID, f.paymentMethodID)
	require.NoError(t, err)
	_, err = pool.Exec(ctx, `
		INSERT INTO ms_billing.add_card_requests (account_id, status)
		VALUES ($1, 'pending')`, f.accountID)
	require.NoError(t, err)
	_, err = pool.Exec(ctx, `
		INSERT INTO ms_billing.invoices
		    (account_id, stripe_invoice_id, status, amount_due, amount_paid,
		     charge_funding_account_id, charge_funding_generation)
		SELECT $1, $2, 'paid', 100, 100, funding_account_id, generation
		FROM ms_billing.account_funding_authorizations
		WHERE account_id=$1`,
		f.accountID, f.paidInvoiceID)
	require.NoError(t, err)
	_, err = pool.Exec(ctx, `
		INSERT INTO ms_billing.usage_events
		    (event_id, account_id, app_id, module_id, metric, kind, value, recorded_at)
		VALUES ($1, $2, $3, $4, 'orders.count', 'count', 1, $5)`,
		uuid.NewString(), f.accountID, f.appID, uuid.New(), createdAt)
	require.NoError(t, err)
	return f
}

func TestFinalizeOrgDeletionBilling_RetiresFutureStateAndRetainsHistory(t *testing.T) {
	pool := testutil.NewTestDB(t)
	store := cycle.NewStore(pool)
	ctx := context.Background()
	f := seedOrgDeletionFixture(t, pool)
	finalizedAt := time.Date(2026, 8, 22, 13, 0, 0, 0, time.UTC)
	secondaryAccountID := uuid.New()
	_, err := pool.Exec(ctx, `
		INSERT INTO ms_billing.accounts (id, owner_kind, owner_org_id)
		VALUES ($1, 'org', $2)`, secondaryAccountID, f.orgID)
	require.NoError(t, err)
	_, err = pool.Exec(ctx, `
		INSERT INTO ms_billing.org_billing_designations
		    (org_id, funding, sponsor_account_id, sponsor_user_id, updated_by)
		VALUES ($1, 'sponsor', $2, $3, $3)`, uuid.New(), secondaryAccountID, uuid.New())
	require.NoError(t, err)

	outcome, err := store.FinalizeOrgDeletionBilling(ctx, f.orgID, f.operationID, finalizedAt)
	require.NoError(t, err)
	require.Equal(t, cycle.OrgDeletionFinalized, outcome)

	var tombstoneOperation uuid.UUID
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT operation_id
		FROM ms_billing.org_deletion_finalizations WHERE org_id=$1`, f.orgID).
		Scan(&tombstoneOperation))
	require.Equal(t, f.operationID, tombstoneOperation)

	var designations, accounts, invoices, usage, activeCards int
	require.NoError(t, pool.QueryRow(ctx, `SELECT count(*) FROM ms_billing.org_billing_designations WHERE org_id=$1`, f.orgID).Scan(&designations))
	require.NoError(t, pool.QueryRow(ctx, `SELECT count(*) FROM ms_billing.accounts WHERE id=$1`, f.accountID).Scan(&accounts))
	require.NoError(t, pool.QueryRow(ctx, `SELECT count(*) FROM ms_billing.invoices WHERE account_id=$1`, f.accountID).Scan(&invoices))
	require.NoError(t, pool.QueryRow(ctx, `SELECT count(*) FROM ms_billing.usage_events WHERE account_id=$1`, f.accountID).Scan(&usage))
	require.NoError(t, pool.QueryRow(ctx, `SELECT count(*) FROM ms_billing.payment_methods_mirror WHERE account_id=$1 AND deleted_at IS NULL`, f.accountID).Scan(&activeCards))
	require.Zero(t, designations)
	var outboundSponsorships int
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT count(*) FROM ms_billing.org_billing_designations
		WHERE sponsor_account_id IN ($1, $2)`, f.accountID, secondaryAccountID).Scan(&outboundSponsorships))
	require.Zero(t, outboundSponsorships)
	require.Equal(t, 1, accounts)
	require.Equal(t, 1, invoices)
	require.Equal(t, 1, usage)
	require.Zero(t, activeCards)

	// Retained invoice upserts must keep working for Stripe webhook
	// reconciliation. The BEFORE INSERT guard recognizes this exact existing
	// invoice/account and lets ON CONFLICT reach its update arm.
	_, err = pool.Exec(ctx, `
		INSERT INTO ms_billing.invoices
		    (account_id, stripe_invoice_id, status, amount_due, amount_paid,
		     charge_funding_account_id, charge_funding_generation)
		SELECT $1, $2, 'paid', 100, 100, funding_account_id, generation
		FROM ms_billing.account_funding_authorizations
		WHERE account_id=$1
		ON CONFLICT (stripe_invoice_id) DO UPDATE
		SET status=EXCLUDED.status, amount_due=EXCLUDED.amount_due,
		    amount_paid=EXCLUDED.amount_paid`, f.accountID, f.paidInvoiceID)
	require.NoError(t, err)

	var appDeleted, timerRemoved, domainRemoved bool
	require.NoError(t, pool.QueryRow(ctx, `SELECT deleted_at IS NOT NULL FROM ms_billing.apps WHERE app_id=$1`, f.appID).Scan(&appDeleted))
	require.NoError(t, pool.QueryRow(ctx, `SELECT removed_at IS NOT NULL FROM ms_billing.app_module_overage_timers WHERE id=$1`, f.timerID).Scan(&timerRemoved))
	require.NoError(t, pool.QueryRow(ctx, `SELECT removed_at IS NOT NULL FROM ms_billing.app_custom_domains WHERE id=$1`, f.domainID).Scan(&domainRemoved))
	require.True(t, appDeleted)
	require.True(t, timerRemoved)
	require.True(t, domainRemoved)

	var autoTopUpEnabled bool
	var cardRequestStatus string
	require.NoError(t, pool.QueryRow(ctx, `SELECT enabled FROM ms_billing.credit_auto_topup_configs WHERE account_id=$1`, f.accountID).Scan(&autoTopUpEnabled))
	require.NoError(t, pool.QueryRow(ctx, `SELECT status::text FROM ms_billing.add_card_requests WHERE account_id=$1`, f.accountID).Scan(&cardRequestStatus))
	require.False(t, autoTopUpEnabled)
	require.Equal(t, "failed", cardRequestStatus)

	// Same-operation replay is success; another operation can never claim the
	// immutable tombstone.
	outcome, err = store.FinalizeOrgDeletionBilling(ctx, f.orgID, f.operationID, finalizedAt.Add(time.Hour))
	require.NoError(t, err)
	require.Equal(t, cycle.OrgDeletionAlreadyFinalized, outcome)
	outcome, err = store.FinalizeOrgDeletionBilling(ctx, f.orgID, uuid.New(), finalizedAt.Add(time.Hour))
	require.NoError(t, err)
	require.Equal(t, cycle.OrgDeletionOperationConflict, outcome)
	_, err = pool.Exec(ctx, `
		UPDATE ms_billing.org_deletion_finalizations
		SET finalized_at=now() WHERE org_id=$1`, f.orgID)
	require.Error(t, err, "the finalization tombstone must reject updates")
	_, err = pool.Exec(ctx, `
		DELETE FROM ms_billing.org_deletion_finalizations WHERE org_id=$1`, f.orgID)
	require.Error(t, err, "the finalization tombstone must reject deletes")

	// The trigger-level guard is defence in depth for every caller, including
	// late versions that do not yet know the service-level tombstone contract.
	_, err = pool.Exec(ctx, `
		INSERT INTO ms_billing.org_billing_designations (org_id, funding, updated_by)
		VALUES ($1, 'org', $2)`, f.orgID, uuid.New())
	require.Error(t, err)
	_, err = pool.Exec(ctx, `
		INSERT INTO ms_billing.apps
		    (app_id, account_id, owner_org_id, module_count, created_module_count, created_at)
		VALUES ($1, $2, $3, 0, 0, now())`, uuid.New(), f.accountID, f.orgID)
	require.Error(t, err)
	_, err = pool.Exec(ctx, `
		INSERT INTO ms_billing.invoices
		    (account_id, stripe_invoice_id, status, amount_due,
		     charge_funding_account_id, charge_funding_generation)
		SELECT $1, $2, 'open', 100, funding_account_id, generation
		FROM ms_billing.account_funding_authorizations
		WHERE account_id=$1`, f.accountID, "in_late_"+uuid.NewString())
	require.Error(t, err)
	_, err = pool.Exec(ctx, `
		INSERT INTO ms_billing.usage_events
		    (event_id, account_id, app_id, module_id, metric, kind, value, recorded_at)
		VALUES ($1, $2, $3, $4, 'orders.count', 'count', 1, now())`,
		uuid.NewString(), f.accountID, f.appID, uuid.New())
	require.Error(t, err)
}

func TestFinalizeOrgDeletionBilling_CollectibleAndInFlightFailWithoutMutation(t *testing.T) {
	for _, test := range []struct {
		name string
		seed func(context.Context, *pgxpool.Pool, orgDeletionFixture)
		want cycle.OrgDeletionFinalizationOutcome
	}{
		{
			name: "collectible invoice",
			seed: func(ctx context.Context, pool *pgxpool.Pool, f orgDeletionFixture) {
				_, err := pool.Exec(ctx, `
					INSERT INTO ms_billing.invoices
					    (account_id, stripe_invoice_id, status, amount_due,
					     charge_funding_account_id, charge_funding_generation)
					SELECT $1, $2, 'uncollectible', 250, funding_account_id, generation
					FROM ms_billing.account_funding_authorizations
					WHERE account_id=$1`,
					f.accountID, "in_due_"+uuid.NewString())
				require.NoError(t, err)
			},
			want: cycle.OrgDeletionCollectibleInvoices,
		},
		{
			name: "pending money movement",
			seed: func(ctx context.Context, pool *pgxpool.Pool, f orgDeletionFixture) {
				_, err := pool.Exec(ctx, `
					INSERT INTO ms_billing.billing_runs
					    (account_id, period_start, period_end, status)
					VALUES ($1, $2, $3, 'pending')`,
					f.accountID,
					time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC),
					time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC))
				require.NoError(t, err)
			},
			want: cycle.OrgDeletionMoneyInFlight,
		},
		{
			name: "failed recoverable billing run",
			seed: func(ctx context.Context, pool *pgxpool.Pool, f orgDeletionFixture) {
				_, err := pool.Exec(ctx, `
					INSERT INTO ms_billing.billing_runs
					    (account_id, period_start, period_end, status,
					     frozen_charge_cents, frozen_charge_with_base,
					     charge_funding_account_id, charge_funding_generation)
					SELECT $1, $2, $3, 'failed', 500, true,
					       funding_account_id, generation
					FROM ms_billing.account_funding_authorizations
					WHERE account_id=$1`,
					f.accountID,
					time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC),
					time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC))
				require.NoError(t, err)
			},
			want: cycle.OrgDeletionMoneyInFlight,
		},
		{
			name: "deferred no-payment-method run",
			seed: func(ctx context.Context, pool *pgxpool.Pool, f orgDeletionFixture) {
				_, err := pool.Exec(ctx, `
					INSERT INTO ms_billing.billing_runs
					    (account_id, period_start, period_end, status)
					VALUES ($1, $2, $3, 'skipped_no_pm')`,
					f.accountID,
					time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC),
					time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC))
				require.NoError(t, err)
			},
			want: cycle.OrgDeletionMoneyInFlight,
		},
		{
			name: "deferred prepaid run",
			seed: func(ctx context.Context, pool *pgxpool.Pool, f orgDeletionFixture) {
				_, err := pool.Exec(ctx, `
					INSERT INTO ms_billing.billing_runs
					    (account_id, period_start, period_end, status)
					VALUES ($1, $2, $3, 'skipped_prepaid')`,
					f.accountID,
					time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC),
					time.Date(2026, 4, 1, 0, 0, 0, 0, time.UTC))
				require.NoError(t, err)
			},
			want: cycle.OrgDeletionMoneyInFlight,
		},
		{
			name: "deferred spend-ceiling run",
			seed: func(ctx context.Context, pool *pgxpool.Pool, f orgDeletionFixture) {
				_, err := pool.Exec(ctx, `
					INSERT INTO ms_billing.billing_runs
					    (account_id, period_start, period_end, status)
					VALUES ($1, $2, $3, 'skipped_ceiling')`,
					f.accountID,
					time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC),
					time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC))
				require.NoError(t, err)
			},
			want: cycle.OrgDeletionMoneyInFlight,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			pool := testutil.NewTestDB(t)
			store := cycle.NewStore(pool)
			ctx := context.Background()
			f := seedOrgDeletionFixture(t, pool)
			test.seed(ctx, pool, f)

			outcome, err := store.FinalizeOrgDeletionBilling(ctx, f.orgID, f.operationID, time.Now().UTC())
			require.NoError(t, err)
			require.Equal(t, test.want, outcome)

			var tombstones, designations int
			require.NoError(t, pool.QueryRow(ctx, `SELECT count(*) FROM ms_billing.org_deletion_finalizations WHERE org_id=$1`, f.orgID).Scan(&tombstones))
			require.NoError(t, pool.QueryRow(ctx, `SELECT count(*) FROM ms_billing.org_billing_designations WHERE org_id=$1`, f.orgID).Scan(&designations))
			require.Zero(t, tombstones)
			require.Equal(t, 1, designations)
		})
	}
}

func TestFinalizeOrgDeletionBilling_ChecksEveryHistoricalOrgAccount(t *testing.T) {
	pool := testutil.NewTestDB(t)
	store := cycle.NewStore(pool)
	ctx := context.Background()
	f := seedOrgDeletionFixture(t, pool)
	secondaryAccountID := uuid.New()

	_, err := pool.Exec(ctx, `
		INSERT INTO ms_billing.accounts (id, owner_kind, owner_org_id)
		VALUES ($1, 'org', $2)`, secondaryAccountID, f.orgID)
	require.NoError(t, err)
	_, err = pool.Exec(ctx, `
		INSERT INTO ms_billing.invoices
		    (account_id, stripe_invoice_id, status, amount_due,
		     charge_funding_account_id, charge_funding_generation)
		SELECT $1, $2, 'open', 900, funding_account_id, generation
		FROM ms_billing.account_funding_authorizations
		WHERE account_id=$1`, secondaryAccountID, "in_due_"+uuid.NewString())
	require.NoError(t, err)

	outcome, err := store.FinalizeOrgDeletionBilling(
		ctx, f.orgID, f.operationID, time.Now().UTC(),
	)
	require.NoError(t, err)
	require.Equal(t, cycle.OrgDeletionCollectibleInvoices, outcome)

	var tombstones int
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT count(*) FROM ms_billing.org_deletion_finalizations
		WHERE org_id=$1`, f.orgID).Scan(&tombstones))
	require.Zero(t, tombstones)
}

func TestFinalizeOrgDeletionBilling_SerializesWithConcurrentInvoiceWriter(t *testing.T) {
	pool := testutil.NewTestDB(t)
	store := cycle.NewStore(pool)
	ctx := context.Background()
	f := seedOrgDeletionFixture(t, pool)

	writer, err := pool.Begin(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { _ = writer.Rollback(context.Background()) })
	_, err = writer.Exec(ctx, `SELECT ms_billing.assert_org_billing_active($1)`, f.orgID)
	require.NoError(t, err)

	type result struct {
		outcome cycle.OrgDeletionFinalizationOutcome
		err     error
	}
	done := make(chan result, 1)
	go func() {
		outcome, err := store.FinalizeOrgDeletionBilling(
			context.Background(), f.orgID, f.operationID, time.Now().UTC(),
		)
		done <- result{outcome: outcome, err: err}
	}()

	select {
	case got := <-done:
		t.Fatalf("finalization bypassed the lifecycle lock: outcome=%v err=%v", got.outcome, got.err)
	case <-time.After(150 * time.Millisecond):
		// Expected: finalization waits for the writer that acquired the lock.
	}

	_, err = writer.Exec(ctx, `
		INSERT INTO ms_billing.invoices
		    (account_id, stripe_invoice_id, status, amount_due,
		     charge_funding_account_id, charge_funding_generation)
		SELECT $1, $2, 'open', 500, funding_account_id, generation
		FROM ms_billing.account_funding_authorizations
		WHERE account_id=$1`, f.accountID, "in_race_"+uuid.NewString())
	require.NoError(t, err)
	require.NoError(t, writer.Commit(ctx))

	select {
	case got := <-done:
		require.NoError(t, got.err)
		require.Equal(t, cycle.OrgDeletionCollectibleInvoices, got.outcome)
	case <-time.After(5 * time.Second):
		t.Fatal("finalization did not resume after concurrent writer committed")
	}
}

func TestFinalizeOrgDeletionBilling_NoAccountStillTombstones(t *testing.T) {
	pool := testutil.NewTestDB(t)
	store := cycle.NewStore(pool)
	ctx := context.Background()
	orgID, operationID := uuid.New(), uuid.New()

	outcome, err := store.FinalizeOrgDeletionBilling(ctx, orgID, operationID, time.Now().UTC())
	require.NoError(t, err)
	require.Equal(t, cycle.OrgDeletionFinalized, outcome)

	var tombstones int
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT count(*) FROM ms_billing.org_deletion_finalizations
		WHERE org_id=$1 AND operation_id=$2`, orgID, operationID).Scan(&tombstones))
	require.Equal(t, 1, tombstones)

	_, err = pool.Exec(ctx, `
		INSERT INTO ms_billing.accounts (owner_kind, owner_org_id)
		VALUES ('org', $1)`, orgID)
	require.Error(t, err, "a late first account create must not revive the org")
}

func TestFinalizeOrgDeletionBilling_OperationCannotIdentifyTwoOrganizations(t *testing.T) {
	pool := testutil.NewTestDB(t)
	store := cycle.NewStore(pool)
	ctx := context.Background()
	firstOrgID, secondOrgID, operationID := uuid.New(), uuid.New(), uuid.New()

	outcome, err := store.FinalizeOrgDeletionBilling(
		ctx, firstOrgID, operationID, time.Now().UTC(),
	)
	require.NoError(t, err)
	require.Equal(t, cycle.OrgDeletionFinalized, outcome)

	outcome, err = store.FinalizeOrgDeletionBilling(
		ctx, secondOrgID, operationID, time.Now().UTC(),
	)
	require.NoError(t, err)
	require.Equal(t, cycle.OrgDeletionOperationConflict, outcome)

	var secondTombstones int
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT count(*) FROM ms_billing.org_deletion_finalizations
		WHERE org_id=$1`, secondOrgID).Scan(&secondTombstones))
	require.Zero(t, secondTombstones)
}

func TestFinalizeOrgDeletionBilling_SharedWritersRemainConcurrentAndExclusiveFinalizerWaits(t *testing.T) {
	pool := testutil.NewTestDB(t)
	store := cycle.NewStore(pool)
	ctx := context.Background()
	f := seedOrgDeletionFixture(t, pool)

	first, err := pool.Begin(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { _ = first.Rollback(context.Background()) })
	_, err = first.Exec(ctx, `SELECT ms_billing.assert_org_billing_active($1)`, f.orgID)
	require.NoError(t, err)
	_, err = first.Exec(ctx, `
		INSERT INTO ms_billing.usage_events
		    (event_id, account_id, app_id, module_id, metric, kind, value, recorded_at)
		VALUES ($1, $2, $3, $4, 'concurrent.meter', 'count', 1, now())`,
		uuid.NewString(), f.accountID, f.appID, uuid.New())
	require.NoError(t, err)

	second, err := pool.Begin(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { _ = second.Rollback(context.Background()) })
	secondLocked := make(chan error, 1)
	go func() {
		_, lockErr := second.Exec(context.Background(),
			`SELECT ms_billing.assert_org_billing_active($1)`, f.orgID)
		secondLocked <- lockErr
	}()
	select {
	case lockErr := <-secondLocked:
		require.NoError(t, lockErr, "same-org writers must share, not serialize, the lifecycle barrier")
	case <-time.After(time.Second):
		t.Fatal("second same-org writer blocked behind the first")
	}
	_, err = second.Exec(ctx, `
		INSERT INTO ms_billing.usage_events
		    (event_id, account_id, app_id, module_id, metric, kind, value, recorded_at)
		VALUES ($1, $2, $3, $4, 'concurrent.meter', 'count', 1, now())`,
		uuid.NewString(), f.accountID, f.appID, uuid.New())
	require.NoError(t, err)

	type finalizeResult struct {
		outcome cycle.OrgDeletionFinalizationOutcome
		err     error
	}
	finalized := make(chan finalizeResult, 1)
	go func() {
		outcome, finalizeErr := store.FinalizeOrgDeletionBilling(
			context.Background(), f.orgID, f.operationID, time.Now().UTC(),
		)
		finalized <- finalizeResult{outcome: outcome, err: finalizeErr}
	}()
	select {
	case got := <-finalized:
		t.Fatalf("exclusive finalizer bypassed shared writers: outcome=%v err=%v", got.outcome, got.err)
	case <-time.After(150 * time.Millisecond):
	}

	require.NoError(t, first.Commit(ctx))
	require.NoError(t, second.Commit(ctx))
	select {
	case got := <-finalized:
		require.NoError(t, got.err)
		require.Equal(t, cycle.OrgDeletionFinalized, got.outcome)
	case <-time.After(5 * time.Second):
		t.Fatal("finalizer did not resume after both shared writers committed")
	}
}

func TestFinalizeOrgDeletionBilling_ExactInvoiceUpsertAndApplyRaces(t *testing.T) {
	t.Run("existing upsert commits collectible state before final check", func(t *testing.T) {
		pool := testutil.NewTestDB(t)
		store := cycle.NewStore(pool)
		ctx := context.Background()
		f := seedOrgDeletionFixture(t, pool)

		writer, err := pool.Begin(ctx)
		require.NoError(t, err)
		t.Cleanup(func() { _ = writer.Rollback(context.Background()) })
		var fundingAccountID, fundingGeneration uuid.UUID
		require.NoError(t, writer.QueryRow(ctx, `
			SELECT funding_account_id, generation
			FROM ms_billing.account_funding_authorizations
			WHERE account_id=$1`, f.accountID).
			Scan(&fundingAccountID, &fundingGeneration))
		err = db.New(writer).UpsertInvoice(ctx, db.UpsertInvoiceParams{
			AccountID:               f.accountID.String(),
			StripeInvoiceID:         f.paidInvoiceID,
			Status:                  "open",
			AmountDue:               pgtype.Numeric{Int: big.NewInt(500), Valid: true},
			AmountPaid:              pgtype.Numeric{Int: big.NewInt(0), Valid: true},
			Currency:                "usd",
			ChargeFundingAccountID:  fundingAccountID.String(),
			ChargeFundingGeneration: fundingGeneration.String(),
		})
		require.NoError(t, err)

		result := make(chan struct {
			outcome cycle.OrgDeletionFinalizationOutcome
			err     error
		}, 1)
		go func() {
			outcome, finalizeErr := store.FinalizeOrgDeletionBilling(
				context.Background(), f.orgID, f.operationID, time.Now().UTC(),
			)
			result <- struct {
				outcome cycle.OrgDeletionFinalizationOutcome
				err     error
			}{outcome, finalizeErr}
		}()
		select {
		case got := <-result:
			t.Fatalf("finalizer bypassed exact existing invoice upsert: %+v", got)
		case <-time.After(150 * time.Millisecond):
		}
		require.NoError(t, writer.Commit(ctx))
		got := <-result
		require.NoError(t, got.err)
		require.Equal(t, cycle.OrgDeletionCollectibleInvoices, got.outcome)
	})

	t.Run("paid apply commits before final check", func(t *testing.T) {
		pool := testutil.NewTestDB(t)
		store := cycle.NewStore(pool)
		ctx := context.Background()
		f := seedOrgDeletionFixture(t, pool)
		_, err := pool.Exec(ctx, `
			UPDATE ms_billing.invoices
			SET status='open', amount_due=500, amount_paid=0
			WHERE stripe_invoice_id=$1`, f.paidInvoiceID)
		require.NoError(t, err)

		writer, err := pool.Begin(ctx)
		require.NoError(t, err)
		t.Cleanup(func() { _ = writer.Rollback(context.Background()) })
		_, err = db.New(writer).ApplyInvoiceStatus(ctx, db.ApplyInvoiceStatusParams{
			Status:          "paid",
			AmountPaid:      pgtype.Numeric{Int: big.NewInt(500), Valid: true},
			AmountDue:       pgtype.Numeric{Int: big.NewInt(0), Valid: true},
			StripeInvoiceID: f.paidInvoiceID,
		})
		require.NoError(t, err)

		result := make(chan struct {
			outcome cycle.OrgDeletionFinalizationOutcome
			err     error
		}, 1)
		go func() {
			outcome, finalizeErr := store.FinalizeOrgDeletionBilling(
				context.Background(), f.orgID, f.operationID, time.Now().UTC(),
			)
			result <- struct {
				outcome cycle.OrgDeletionFinalizationOutcome
				err     error
			}{outcome, finalizeErr}
		}()
		select {
		case got := <-result:
			t.Fatalf("finalizer bypassed invoice status apply: %+v", got)
		case <-time.After(150 * time.Millisecond):
		}
		require.NoError(t, writer.Commit(ctx))
		got := <-result
		require.NoError(t, got.err)
		require.Equal(t, cycle.OrgDeletionFinalized, got.outcome)
	})
}

func TestFinalizeOrgDeletionBilling_StaleDomainAndTimerCandidatesCannotArmAfterRetirement(t *testing.T) {
	pool := testutil.NewTestDB(t)
	store := cycle.NewStore(pool)
	ctx := context.Background()
	f := seedOrgDeletionFixture(t, pool)
	selectedAt := time.Date(2026, 8, 23, 0, 0, 0, 0, time.UTC)
	domainCandidates, err := store.DomainsPendingCharge(ctx, selectedAt)
	require.NoError(t, err)
	require.Len(t, domainCandidates, 1)
	require.Equal(t, f.domainID, domainCandidates[0].ID)
	timerCandidates, err := store.ModuleOverageTimersPastGrace(ctx, selectedAt)
	require.NoError(t, err)
	require.Len(t, timerCandidates, 1)
	require.Equal(t, f.timerID, timerCandidates[0].ID)

	outcome, err := store.FinalizeOrgDeletionBilling(
		ctx, f.orgID, f.operationID, time.Now().UTC(),
	)
	require.NoError(t, err)
	require.Equal(t, cycle.OrgDeletionFinalized, outcome)

	domainClaim, err := store.ArmDomainStripeCharge(ctx, f.domainID, time.Now().UTC())
	require.NoError(t, err)
	require.Equal(t, cycle.StripeRailStale, domainClaim.Outcome,
		"a domain candidate selected before retirement must lose after removal")
	timerClaim, err := store.ArmModuleTimerStripeCharge(ctx, f.timerID, time.Now().UTC())
	require.NoError(t, err)
	require.Equal(t, cycle.StripeRailStale, timerClaim.Outcome,
		"a timer candidate selected before retirement must lose after removal")
	require.Error(t, store.MarkDomainCharged(
		ctx, f.domainID, time.Now().UTC(), "in_stale_domain", "ii_stale_domain",
	), "a post-retirement terminal domain transition must be rejected")
	require.Error(t, store.MarkModuleTimerCharged(
		ctx, f.timerID, time.Now().UTC(), "in_stale_timer", "ii_stale_timer",
	), "a post-retirement terminal timer transition must be rejected")

	var domainAttempted, timerAttempted bool
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT charge_attempted_at IS NOT NULL
		FROM ms_billing.app_custom_domains WHERE id=$1`, f.domainID).Scan(&domainAttempted))
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT charge_attempted_at IS NOT NULL
		FROM ms_billing.app_module_overage_timers WHERE id=$1`, f.timerID).Scan(&timerAttempted))
	require.False(t, domainAttempted)
	require.False(t, timerAttempted)
}

func TestFinalizeOrgDeletionBilling_DistributorWaitsForSponsoredCustomerChargeSurfaces(t *testing.T) {
	for _, tc := range []struct {
		name string
		seed func(context.Context, *pgxpool.Pool, uuid.UUID, uuid.UUID, uuid.UUID)
	}{
		{
			name: "recoverable billing run",
			seed: func(ctx context.Context, pool *pgxpool.Pool, accountID, _, _ uuid.UUID) {
				_, err := pool.Exec(ctx, `
					INSERT INTO ms_billing.billing_runs
					    (account_id, period_start, period_end, status,
					     frozen_charge_cents, frozen_charge_with_base,
					     charge_funding_account_id, charge_funding_generation)
					SELECT $1, $2, $3, 'failed', 100, false,
					       funding_account_id, generation
					FROM ms_billing.account_funding_authorizations
					WHERE account_id = $1`,
					accountID,
					time.Date(2026, 4, 1, 0, 0, 0, 0, time.UTC),
					time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC))
				require.NoError(t, err)
			},
		},
		{
			name: "attempted custom domain",
			seed: func(ctx context.Context, pool *pgxpool.Pool, accountID, appID, customerOrgID uuid.UUID) {
				_, err := pool.Exec(ctx, `
					INSERT INTO ms_billing.app_custom_domains
					    (account_id, app_id, hostname, activated_at, charge_attempted_at,
					     charge_funding_account_id, charge_funding_generation)
					SELECT $1, $2, $3, now(), now(), funding_account_id, generation
					FROM ms_billing.account_funding_authorizations
					WHERE account_id = $1`,
					accountID, appID, "sponsored-"+customerOrgID.String()+".test")
				require.NoError(t, err)
			},
		},
		{
			name: "attempted module timer",
			seed: func(ctx context.Context, pool *pgxpool.Pool, accountID, appID, _ uuid.UUID) {
				_, err := pool.Exec(ctx, `
					INSERT INTO ms_billing.app_module_overage_timers
					    (account_id, app_id, installed_at, grace_expires_at, charge_attempted_at,
					     charge_funding_account_id, charge_funding_generation)
					SELECT $1, $2, now() - interval '4 days', now() - interval '1 day', now(),
					       funding_account_id, generation
					FROM ms_billing.account_funding_authorizations
					WHERE account_id = $1`,
					accountID, appID)
				require.NoError(t, err)
			},
		},
		{
			name: "combined creation proration",
			seed: func(ctx context.Context, pool *pgxpool.Pool, accountID, appID, _ uuid.UUID) {
				_, outcome, err := cycle.NewStore(pool).FreezeCombinedProrationAttempt(
					ctx,
					appID,
					time.Now().UTC(),
					combinedAttemptShape(appID, accountID),
					false,
				)
				require.NoError(t, err)
				require.Equal(t, cycle.StripeRailClaimed, outcome)
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pool := testutil.NewTestDB(t)
			store := cycle.NewStore(pool)
			ctx := context.Background()
			distributor := seedOrgDeletionFixture(t, pool)
			customerOrgID, customerAccountID, appID := uuid.New(), uuid.New(), uuid.New()
			_, err := pool.Exec(ctx, `
				INSERT INTO ms_billing.accounts
				    (id, owner_kind, owner_org_id, activated_at)
				VALUES ($1, 'org', $2, now())`, customerAccountID, customerOrgID)
			require.NoError(t, err)
			_, err = pool.Exec(ctx, `
				INSERT INTO ms_billing.org_billing_designations
				    (org_id, funding, sponsor_account_id, sponsor_user_id, updated_by)
				VALUES ($1, 'sponsor', $2, $3, $3)`,
				customerOrgID, distributor.accountID, uuid.New())
			require.NoError(t, err)
			_, err = pool.Exec(ctx, `
				INSERT INTO ms_billing.apps
				    (app_id, account_id, owner_org_id, module_count,
				     created_module_count, created_at)
				VALUES ($1, $2, $3, 0, 0, now())`, appID, customerAccountID, customerOrgID)
			require.NoError(t, err)
			tc.seed(ctx, pool, customerAccountID, appID, customerOrgID)

			outcome, err := store.FinalizeOrgDeletionBilling(
				ctx, distributor.orgID, distributor.operationID, time.Now().UTC(),
			)
			require.NoError(t, err)
			require.Equal(t, cycle.OrgDeletionMoneyInFlight, outcome)
		})
	}
}

func TestFinalizeOrgDeletionBilling_RetiredSponsorEdgeRejectsStaleCustomerCandidate(t *testing.T) {
	pool := testutil.NewTestDB(t)
	store := cycle.NewStore(pool)
	ctx := context.Background()
	distributor := seedOrgDeletionFixture(t, pool)
	customerOrgID, customerAccountID, appID, domainID := uuid.New(), uuid.New(), uuid.New(), uuid.New()
	_, err := pool.Exec(ctx, `
		INSERT INTO ms_billing.accounts
		    (id, owner_kind, owner_org_id, activated_at)
		VALUES ($1, 'org', $2, now())`, customerAccountID, customerOrgID)
	require.NoError(t, err)
	_, err = pool.Exec(ctx, `
		INSERT INTO ms_billing.org_billing_designations
		    (org_id, funding, sponsor_account_id, sponsor_user_id, updated_by)
		VALUES ($1, 'sponsor', $2, $3, $3)`,
		customerOrgID, distributor.accountID, uuid.New())
	require.NoError(t, err)
	_, err = pool.Exec(ctx, `
		INSERT INTO ms_billing.apps
		    (app_id, account_id, owner_org_id, module_count,
		     created_module_count, created_at)
		VALUES ($1, $2, $3, 0, 0, now())`, appID, customerAccountID, customerOrgID)
	require.NoError(t, err)
	_, err = pool.Exec(ctx, `
		INSERT INTO ms_billing.app_custom_domains
		    (id, account_id, app_id, hostname, activated_at)
		VALUES ($1, $2, $3, $4, now())`,
		domainID, customerAccountID, appID, "stale-"+customerOrgID.String()+".test")
	require.NoError(t, err)

	outcome, err := store.FinalizeOrgDeletionBilling(
		ctx, distributor.orgID, distributor.operationID, time.Now().UTC(),
	)
	require.NoError(t, err)
	require.Equal(t, cycle.OrgDeletionFinalized, outcome)

	claim, err := store.ArmDomainStripeCharge(ctx, domainID, time.Now().UTC())
	require.NoError(t, err)
	require.Equal(t, cycle.StripeRailNoPaymentMethod, claim.Outcome,
		"an unarmed stale candidate must use the rotated self-funding authority")
	var retainedEdges int
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT count(*)
		FROM ms_billing.org_deletion_retired_sponsorships
		WHERE retired_sponsor_org_id=$1 AND customer_org_id=$2`,
		distributor.orgID, customerOrgID).Scan(&retainedEdges))
	require.Equal(t, 1, retainedEdges)
}

func TestFinalizeOrgDeletionBilling_SponsoredArmWinsExactPostgresRace(t *testing.T) {
	for _, surface := range []string{"domain", "module timer"} {
		t.Run(surface, func(t *testing.T) {
			pool := testutil.NewTestDB(t)
			store := cycle.NewStore(pool)
			ctx := context.Background()
			f := seedSponsoredFundingFixture(t, pool)

			armTx, err := pool.Begin(ctx)
			require.NoError(t, err)
			t.Cleanup(func() { _ = armTx.Rollback(context.Background()) })
			qtx := db.New(armTx)
			var armed bool
			var funder string
			switch surface {
			case "domain":
				row, armErr := qtx.ArmDomainStripeCharge(ctx, db.ArmDomainStripeChargeParams{
					DomainID: f.domainID.String(), AttemptedAt: time.Now().UTC(),
				})
				require.NoError(t, armErr)
				armed, funder = row.Armed, row.FundingAccountID
			case "module timer":
				row, armErr := qtx.ArmModuleTimerStripeCharge(ctx, db.ArmModuleTimerStripeChargeParams{
					TimerID: f.timerID.String(), AttemptedAt: time.Now().UTC(),
				})
				require.NoError(t, armErr)
				armed, funder = row.Armed, row.FundingAccountID
			}
			require.True(t, armed)
			require.Equal(t, f.sponsorAccountID.String(), funder)

			type finalizeResult struct {
				outcome cycle.OrgDeletionFinalizationOutcome
				err     error
			}
			finalized := make(chan finalizeResult, 1)
			go func() {
				outcome, finalizeErr := store.FinalizeOrgDeletionBilling(
					context.Background(), f.sponsorOrgID, f.sponsorOperationID, time.Now().UTC(),
				)
				finalized <- finalizeResult{outcome: outcome, err: finalizeErr}
			}()
			select {
			case got := <-finalized:
				t.Fatalf("sponsor finalizer bypassed the uncommitted funding arm: %+v", got)
			case <-time.After(150 * time.Millisecond):
			}

			require.NoError(t, armTx.Commit(ctx))
			select {
			case got := <-finalized:
				require.NoError(t, got.err)
				require.Equal(t, cycle.OrgDeletionMoneyInFlight, got.outcome,
					"the committed A-funded marker must keep sponsor A collectible")
			case <-time.After(5 * time.Second):
				t.Fatal("sponsor finalizer did not resume after the funding arm committed")
			}

			switch surface {
			case "domain":
				require.NoError(t, store.MarkDomainCharged(
					ctx, f.domainID, time.Now().UTC(), "in_domain_race", "ii_domain_race",
				))
			case "module timer":
				require.NoError(t, store.MarkModuleTimerCharged(
					ctx, f.timerID, time.Now().UTC(), "in_timer_race", "ii_timer_race",
				))
			}
			outcome, err := store.FinalizeOrgDeletionBilling(
				ctx, f.sponsorOrgID, f.sponsorOperationID, time.Now().UTC(),
			)
			require.NoError(t, err)
			require.Equal(t, cycle.OrgDeletionFinalized, outcome)
		})
	}
}

func TestFinalizeOrgDeletionBilling_FinalizerWinsExactPostgresRaceBeforeSponsoredArm(t *testing.T) {
	for _, surface := range []string{"domain", "module timer"} {
		t.Run(surface, func(t *testing.T) {
			pool := testutil.NewTestDB(t)
			store := cycle.NewStore(pool)
			ctx := context.Background()
			f := seedSponsoredFundingFixture(t, pool)

			// Hold the sponsor account row after the public finalizer has acquired
			// its exclusive lifecycle lock. This opens a deterministic window in
			// the real finalizer transaction before it rotates B away from A.
			accountBlocker, err := pool.Begin(ctx)
			require.NoError(t, err)
			t.Cleanup(func() { _ = accountBlocker.Rollback(context.Background()) })
			_, err = accountBlocker.Exec(ctx,
				`SELECT id FROM ms_billing.accounts WHERE id=$1 FOR UPDATE`,
				f.sponsorAccountID,
			)
			require.NoError(t, err)

			type finalizeResult struct {
				outcome cycle.OrgDeletionFinalizationOutcome
				err     error
			}
			finalized := make(chan finalizeResult, 1)
			go func() {
				outcome, finalizeErr := store.FinalizeOrgDeletionBilling(
					context.Background(), f.sponsorOrgID, f.sponsorOperationID, time.Now().UTC(),
				)
				finalized <- finalizeResult{outcome: outcome, err: finalizeErr}
			}()
			select {
			case got := <-finalized:
				t.Fatalf("finalizer did not block on the controlled account-row lock: %+v", got)
			case <-time.After(150 * time.Millisecond):
			}

			type armResult struct {
				claim cycle.StripeChargeClaim
				err   error
			}
			armed := make(chan armResult, 1)
			go func() {
				var claim cycle.StripeChargeClaim
				var armErr error
				switch surface {
				case "domain":
					claim, armErr = store.ArmDomainStripeCharge(
						context.Background(), f.domainID, time.Now().UTC(),
					)
				case "module timer":
					claim, armErr = store.ArmModuleTimerStripeCharge(
						context.Background(), f.timerID, time.Now().UTC(),
					)
				}
				armed <- armResult{claim: claim, err: armErr}
			}()
			select {
			case got := <-armed:
				t.Fatalf("funding arm bypassed the sponsor finalizer lifecycle lock: %+v", got)
			case <-time.After(150 * time.Millisecond):
			}

			require.NoError(t, accountBlocker.Commit(ctx))
			select {
			case got := <-finalized:
				require.NoError(t, got.err)
				require.Equal(t, cycle.OrgDeletionFinalized, got.outcome)
			case <-time.After(5 * time.Second):
				t.Fatal("finalizer did not resume after the account-row lock released")
			}
			select {
			case got := <-armed:
				if got.err == nil {
					require.NotEqual(t, cycle.StripeRailClaimed, got.claim.Outcome)
					require.NotEqual(t, f.sponsorAccountID, got.claim.FundingAccountID)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("funding arm did not resume after sponsor finalization")
			}

			var attempted, pinnedToRetiredSponsor bool
			table, id := "ms_billing.app_custom_domains", f.domainID
			if surface == "module timer" {
				table, id = "ms_billing.app_module_overage_timers", f.timerID
			}
			require.NoError(t, pool.QueryRow(ctx, `
				SELECT charge_attempted_at IS NOT NULL,
				       COALESCE(charge_funding_account_id = $2, false)
				FROM `+table+` WHERE id=$1`, id, f.sponsorAccountID).
				Scan(&attempted, &pinnedToRetiredSponsor))
			require.False(t, attempted)
			require.False(t, pinnedToRetiredSponsor)

			// The same stale in-memory ID is safe on retry: the committed
			// designation deletion rotated B to self. B has no PM, so no marker is
			// armed and retired sponsor A can never be charged.
			var retry cycle.StripeChargeClaim
			if surface == "domain" {
				retry, err = store.ArmDomainStripeCharge(ctx, f.domainID, time.Now().UTC())
			} else {
				retry, err = store.ArmModuleTimerStripeCharge(ctx, f.timerID, time.Now().UTC())
			}
			require.NoError(t, err)
			require.Equal(t, cycle.StripeRailNoPaymentMethod, retry.Outcome)
			require.Equal(t, f.customerAccountID, retry.FundingAccountID)
		})
	}
}

func TestFinalizeOrgDeletionBilling_ManualPurchaseArmWinsExactPostgresRace(t *testing.T) {
	pool := testutil.NewTestDB(t)
	store := cycle.NewStore(pool)
	ctx := context.Background()
	f := seedSponsoredFundingFixture(t, pool)
	purchaseID := seedUnarmedManualPurchase(t, pool, f.customerAccountID)

	armTx, err := pool.Begin(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { _ = armTx.Rollback(context.Background()) })
	armed, err := db.New(armTx).ArmPendingCreditPurchase(
		ctx,
		db.ArmPendingCreditPurchaseParams{
			PurchaseID: purchaseID.String(),
			AccountID:  f.customerAccountID.String(),
		},
	)
	require.NoError(t, err)
	require.Equal(t, f.sponsorAccountID, uuid.UUID(armed.ChargeFundingAccountID.Bytes))
	require.NotEmpty(t, armed.AttemptStripeCustomerID)

	type finalizeResult struct {
		outcome cycle.OrgDeletionFinalizationOutcome
		err     error
	}
	finalized := make(chan finalizeResult, 1)
	go func() {
		outcome, finalizeErr := store.FinalizeOrgDeletionBilling(
			context.Background(), f.sponsorOrgID, f.sponsorOperationID, time.Now().UTC(),
		)
		finalized <- finalizeResult{outcome: outcome, err: finalizeErr}
	}()
	select {
	case got := <-finalized:
		t.Fatalf("sponsor finalizer bypassed uncommitted purchase arm: %+v", got)
	case <-time.After(150 * time.Millisecond):
	}

	require.NoError(t, armTx.Commit(ctx))
	select {
	case got := <-finalized:
		require.NoError(t, got.err)
		require.Equal(t, cycle.OrgDeletionMoneyInFlight, got.outcome)
	case <-time.After(5 * time.Second):
		t.Fatal("sponsor finalizer did not resume after purchase arm committed")
	}
}

func TestFinalizeOrgDeletionBilling_FinalizerWinsExactPostgresRaceBeforeManualPurchaseArm(t *testing.T) {
	pool := testutil.NewTestDB(t)
	store := cycle.NewStore(pool)
	ctx := context.Background()
	f := seedSponsoredFundingFixture(t, pool)
	purchaseID := seedUnarmedManualPurchase(t, pool, f.customerAccountID)

	accountBlocker, err := pool.Begin(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { _ = accountBlocker.Rollback(context.Background()) })
	_, err = accountBlocker.Exec(ctx,
		`SELECT id FROM ms_billing.accounts WHERE id=$1 FOR UPDATE`,
		f.sponsorAccountID,
	)
	require.NoError(t, err)

	type finalizeResult struct {
		outcome cycle.OrgDeletionFinalizationOutcome
		err     error
	}
	finalized := make(chan finalizeResult, 1)
	go func() {
		outcome, finalizeErr := store.FinalizeOrgDeletionBilling(
			context.Background(), f.sponsorOrgID, f.sponsorOperationID, time.Now().UTC(),
		)
		finalized <- finalizeResult{outcome: outcome, err: finalizeErr}
	}()
	select {
	case got := <-finalized:
		t.Fatalf("finalizer did not block at the controlled account lock: %+v", got)
	case <-time.After(150 * time.Millisecond):
	}

	armed := make(chan error, 1)
	go func() {
		_, armErr := db.New(pool).ArmPendingCreditPurchase(
			context.Background(),
			db.ArmPendingCreditPurchaseParams{
				PurchaseID: purchaseID.String(),
				AccountID:  f.customerAccountID.String(),
			},
		)
		armed <- armErr
	}()
	select {
	case armErr := <-armed:
		t.Fatalf("purchase arm bypassed sponsor finalizer lifecycle lock: %v", armErr)
	case <-time.After(150 * time.Millisecond):
	}

	require.NoError(t, accountBlocker.Commit(ctx))
	select {
	case got := <-finalized:
		require.NoError(t, got.err)
		require.Equal(t, cycle.OrgDeletionFinalized, got.outcome)
	case <-time.After(5 * time.Second):
		t.Fatal("sponsor finalizer did not resume")
	}
	select {
	case armErr := <-armed:
		require.Error(t, armErr,
			"an arm holding stale sponsor A must be rejected after A retires")
	case <-time.After(5 * time.Second):
		t.Fatal("purchase arm did not resume after sponsor finalization")
	}

	var pinnedToRetiredSponsor, hasCustomer bool
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT COALESCE(charge_funding_account_id=$2, false),
		       attempt_stripe_customer_id IS NOT NULL
		FROM ms_billing.credit_ledger WHERE id=$1`, purchaseID, f.sponsorAccountID).
		Scan(&pinnedToRetiredSponsor, &hasCustomer))
	require.False(t, pinnedToRetiredSponsor)
	require.False(t, hasCustomer)

	_, err = db.New(pool).ArmPendingCreditPurchase(
		ctx,
		db.ArmPendingCreditPurchaseParams{
			PurchaseID: purchaseID.String(),
			AccountID:  f.customerAccountID.String(),
		},
	)
	require.Error(t, err,
		"the rotated self-funded customer has no Stripe Customer and cannot arm")

	// The unarmed pending row remains the customer's operation, not the retired
	// sponsor's. Once it is terminal, customer deletion is independent of the
	// former sponsorship edge.
	_, err = pool.Exec(ctx, `
		UPDATE ms_billing.credit_ledger SET status='failed' WHERE id=$1`, purchaseID)
	require.NoError(t, err)
	outcome, err := store.FinalizeOrgDeletionBilling(
		ctx, f.customerOrgID, f.customerOperationID, time.Now().UTC(),
	)
	require.NoError(t, err)
	require.Equal(t, cycle.OrgDeletionFinalized, outcome)
}

func TestFinalizeOrgDeletionBilling_UnarmedManualPurchaseUsesNewSponsorGeneration(t *testing.T) {
	pool := testutil.NewTestDB(t)
	store := cycle.NewStore(pool)
	ctx := context.Background()
	f := seedSponsoredFundingFixture(t, pool)
	purchaseID := seedUnarmedManualPurchase(t, pool, f.customerAccountID)

	outcome, err := store.FinalizeOrgDeletionBilling(
		ctx, f.sponsorOrgID, f.sponsorOperationID, time.Now().UTC(),
	)
	require.NoError(t, err)
	require.Equal(t, cycle.OrgDeletionFinalized, outcome,
		"an unarmed purchase has no authority over the current sponsor")

	_, newSponsorAccountID := seedSponsorAccount(t, pool, "cus_purchase_new_sponsor_")
	_, err = pool.Exec(ctx, `
		INSERT INTO ms_billing.org_billing_designations
		    (org_id, funding, sponsor_account_id, sponsor_user_id, updated_by)
		VALUES ($1, 'sponsor', $2, $3, $3)`,
		f.customerOrgID, newSponsorAccountID, uuid.New())
	require.NoError(t, err)

	armed, err := db.New(pool).ArmPendingCreditPurchase(
		ctx,
		db.ArmPendingCreditPurchaseParams{
			PurchaseID: purchaseID.String(),
			AccountID:  f.customerAccountID.String(),
		},
	)
	require.NoError(t, err)
	require.Equal(t, newSponsorAccountID, uuid.UUID(armed.ChargeFundingAccountID.Bytes))
	var generation uuid.UUID
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT generation FROM ms_billing.account_funding_authorizations
		WHERE account_id=$1`, f.customerAccountID).Scan(&generation))
	require.Equal(t, generation, uuid.UUID(armed.ChargeFundingGeneration.Bytes))
}

func TestFinalizeOrgDeletionBilling_AutoTopUpBlocksExactCustomerNotCurrentSponsor(t *testing.T) {
	pool := testutil.NewTestDB(t)
	store := cycle.NewStore(pool)
	ctx := context.Background()
	f := seedSponsoredFundingFixture(t, pool)
	customerStripeID := "cus_customer_" + f.customerAccountID.String()
	_, err := pool.Exec(ctx, `
		UPDATE ms_billing.accounts SET stripe_customer_id=$2 WHERE id=$1`,
		f.customerAccountID, customerStripeID)
	require.NoError(t, err)
	paymentMethodID := uuid.New()
	_, err = pool.Exec(ctx, `
		INSERT INTO ms_billing.payment_methods_mirror
		    (id, account_id, stripe_payment_method_id, brand, last4,
		     exp_month, exp_year, is_default)
		VALUES ($1, $2, $3, 'visa', '4242', 12, 2099, true)`,
		paymentMethodID, f.customerAccountID, "pm_"+paymentMethodID.String())
	require.NoError(t, err)
	_, err = pool.Exec(ctx, `
		INSERT INTO ms_billing.credit_ledger
		    (account_id, amount_micros, type, status, balance_after_micros,
		     actor, idempotency_key, attempt_payment_method_id,
		     attempt_stripe_payment_method_id, attempt_stripe_customer_id,
		     created_at, attempt_expires_at)
		VALUES ($1, 5000000, 'auto_topup', 'pending', 5000000,
		        'system', $2, $3, $4, $5, now(), now()+interval '5 minutes')`,
		f.customerAccountID, "auto:"+uuid.NewString(), paymentMethodID,
		"pm_"+paymentMethodID.String(), customerStripeID)
	require.NoError(t, err)

	outcome, err := store.FinalizeOrgDeletionBilling(
		ctx, f.sponsorOrgID, f.sponsorOperationID, time.Now().UTC(),
	)
	require.NoError(t, err)
	require.Equal(t, cycle.OrgDeletionFinalized, outcome,
		"a customer-local frozen auto-top-up must not follow current sponsorship")
	outcome, err = store.FinalizeOrgDeletionBilling(
		ctx, f.customerOrgID, f.customerOperationID, time.Now().UTC(),
	)
	require.NoError(t, err)
	require.Equal(t, cycle.OrgDeletionMoneyInFlight, outcome)
}

func TestFinalizeOrgDeletionBilling_StaleCandidatesUseNewSponsorGenerationAfterRedesignation(t *testing.T) {
	pool := testutil.NewTestDB(t)
	store := cycle.NewStore(pool)
	ctx := context.Background()
	f := seedSponsoredFundingFixture(t, pool)

	domains, err := store.DomainsPendingCharge(ctx, time.Now().UTC())
	require.NoError(t, err)
	require.Len(t, domains, 1)
	timers, err := store.ModuleOverageTimersPastGrace(ctx, time.Now().UTC())
	require.NoError(t, err)
	require.Len(t, timers, 1)

	outcome, err := store.FinalizeOrgDeletionBilling(
		ctx, f.sponsorOrgID, f.sponsorOperationID, time.Now().UTC(),
	)
	require.NoError(t, err)
	require.Equal(t, cycle.OrgDeletionFinalized, outcome)

	_, newSponsorAccountID := seedSponsorAccount(t, pool, "cus_new_sponsor_")
	_, err = pool.Exec(ctx, `
		INSERT INTO ms_billing.org_billing_designations
		    (org_id, funding, sponsor_account_id, sponsor_user_id, updated_by)
		VALUES ($1, 'sponsor', $2, $3, $3)`,
		f.customerOrgID, newSponsorAccountID, uuid.New())
	require.NoError(t, err)

	domainClaim, err := store.ArmDomainStripeCharge(ctx, domains[0].ID, time.Now().UTC())
	require.NoError(t, err)
	require.Equal(t, cycle.StripeRailClaimed, domainClaim.Outcome)
	require.Equal(t, newSponsorAccountID, domainClaim.FundingAccountID)
	require.NotEqual(t, f.sponsorAccountID, domainClaim.FundingAccountID)
	timerClaim, err := store.ArmModuleTimerStripeCharge(ctx, timers[0].ID, time.Now().UTC())
	require.NoError(t, err)
	require.Equal(t, cycle.StripeRailClaimed, timerClaim.Outcome)
	require.Equal(t, newSponsorAccountID, timerClaim.FundingAccountID)

	attempt, claim, err := store.FreezeCombinedProrationAttempt(
		ctx,
		f.appID,
		time.Now().UTC(),
		combinedAttemptShape(f.appID, f.customerAccountID),
		false,
	)
	require.NoError(t, err)
	require.Equal(t, cycle.StripeRailClaimed, claim)
	require.Equal(t, newSponsorAccountID, attempt.ChargeFundingAccountID)

	var currentFunder, currentGeneration uuid.UUID
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT funding_account_id, generation
		FROM ms_billing.account_funding_authorizations
		WHERE account_id=$1`, f.customerAccountID).
		Scan(&currentFunder, &currentGeneration))
	require.Equal(t, newSponsorAccountID, currentFunder)
	require.Equal(t, currentGeneration, domainClaim.FundingGeneration)
	require.Equal(t, currentGeneration, timerClaim.FundingGeneration)
	require.Equal(t, currentGeneration, attempt.ChargeFundingGeneration)

	var retired bool
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT EXISTS (
		    SELECT 1 FROM ms_billing.org_deletion_finalizations WHERE org_id=$1
		)`, f.sponsorOrgID).Scan(&retired))
	require.True(t, retired)
}

func TestFinalizeOrgDeletionBilling_CustomerCanDeleteAfterSponsorRetirement(t *testing.T) {
	pool := testutil.NewTestDB(t)
	store := cycle.NewStore(pool)
	ctx := context.Background()
	f := seedSponsoredFundingFixture(t, pool)

	outcome, err := store.FinalizeOrgDeletionBilling(
		ctx, f.sponsorOrgID, f.sponsorOperationID, time.Now().UTC(),
	)
	require.NoError(t, err)
	require.Equal(t, cycle.OrgDeletionFinalized, outcome)
	outcome, err = store.FinalizeOrgDeletionBilling(
		ctx, f.customerOrgID, f.customerOperationID, time.Now().UTC(),
	)
	require.NoError(t, err)
	require.Equal(t, cycle.OrgDeletionFinalized, outcome,
		"retired sponsorship audit is history, not live authority over customer deletion")
}

func TestFinalizeOrgDeletionBilling_SponsoredCollectibleInvoiceBlocksExactFunderUntilSettled(t *testing.T) {
	pool := testutil.NewTestDB(t)
	store := cycle.NewStore(pool)
	ctx := context.Background()
	f := seedSponsoredFundingFixture(t, pool)
	_, err := pool.Exec(ctx, `
		INSERT INTO ms_billing.invoices
		    (account_id, stripe_invoice_id, status, amount_due, amount_paid,
		     charge_funding_account_id, charge_funding_generation)
		SELECT $1, $2, 'open', 1700, 0, funding_account_id, generation
		FROM ms_billing.account_funding_authorizations
		WHERE account_id=$1`,
		f.customerAccountID, "in_customer_debt_"+f.customerAccountID.String())
	require.NoError(t, err)

	outcome, err := store.FinalizeOrgDeletionBilling(
		ctx, f.sponsorOrgID, f.sponsorOperationID, time.Now().UTC(),
	)
	require.NoError(t, err)
	require.Equal(t, cycle.OrgDeletionCollectibleInvoices, outcome,
		"Stripe may retry the exact sponsor Customer while the invoice is collectible")

	var status string
	var amountDue int64
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT status, amount_due::bigint
		FROM ms_billing.invoices
		WHERE account_id=$1`, f.customerAccountID).Scan(&status, &amountDue))
	require.Equal(t, "open", status)
	require.EqualValues(t, 1700, amountDue)

	outcome, err = store.FinalizeOrgDeletionBilling(
		ctx, f.customerOrgID, f.customerOperationID, time.Now().UTC(),
	)
	require.NoError(t, err)
	require.Equal(t, cycle.OrgDeletionCollectibleInvoices, outcome,
		"the collectible debt also blocks its customer organization")

	_, err = pool.Exec(ctx, `
		UPDATE ms_billing.invoices
		SET status='paid', amount_due=0, amount_paid=1700
		WHERE account_id=$1`, f.customerAccountID)
	require.NoError(t, err)
	outcome, err = store.FinalizeOrgDeletionBilling(
		ctx, f.sponsorOrgID, f.sponsorOperationID, time.Now().UTC(),
	)
	require.NoError(t, err)
	require.Equal(t, cycle.OrgDeletionFinalized, outcome)
	outcome, err = store.FinalizeOrgDeletionBilling(
		ctx, f.customerOrgID, f.customerOperationID, time.Now().UTC(),
	)
	require.NoError(t, err)
	require.Equal(t, cycle.OrgDeletionFinalized, outcome)
}
