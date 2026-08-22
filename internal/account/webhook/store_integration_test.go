//go:build integration

package webhook_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
	stripego "github.com/stripe/stripe-go/v85"

	"github.com/mirrorstack-ai/billing-engine/internal/account/billing"
	"github.com/mirrorstack-ai/billing-engine/internal/account/creditledger"
	"github.com/mirrorstack-ai/billing-engine/internal/account/creditpurchase"
	"github.com/mirrorstack-ai/billing-engine/internal/account/webhook"
	"github.com/mirrorstack-ai/billing-engine/internal/account/webhook/webhooktest"
	billingstripe "github.com/mirrorstack-ai/billing-engine/internal/shared/stripe"
	"github.com/mirrorstack-ai/billing-engine/internal/shared/testutil"
)

type integrationManualPurchaseStripe struct {
	invoice  billingstripe.Invoice
	items    []billingstripe.InvoiceItem
	payments []billingstripe.InvoicePaymentProof
}

func (s *integrationManualPurchaseStripe) CreateCreditPurchaseInvoice(
	context.Context,
	string,
	string,
	string,
	string,
) (billingstripe.Invoice, error) {
	return billingstripe.Invoice{}, fmt.Errorf("unexpected create invoice")
}

func (s *integrationManualPurchaseStripe) CreateInvoiceItem(
	context.Context,
	string,
	string,
	int64,
	string,
	string,
	billingstripe.LinePeriod,
	string,
) (billingstripe.InvoiceItem, error) {
	return billingstripe.InvoiceItem{}, fmt.Errorf("unexpected create item")
}

func (s *integrationManualPurchaseStripe) ListInvoiceItems(
	context.Context,
	string,
) ([]billingstripe.InvoiceItem, error) {
	return append([]billingstripe.InvoiceItem(nil), s.items...), nil
}

func (s *integrationManualPurchaseStripe) ListInvoicePayments(
	context.Context,
	string,
) ([]billingstripe.InvoicePaymentProof, error) {
	return append([]billingstripe.InvoicePaymentProof(nil), s.payments...), nil
}

func (s *integrationManualPurchaseStripe) FinalizeInvoice(
	context.Context,
	string,
	string,
) (billingstripe.Invoice, error) {
	return billingstripe.Invoice{}, fmt.Errorf("unexpected finalize invoice")
}

func (s *integrationManualPurchaseStripe) GetInvoice(
	context.Context,
	string,
) (billingstripe.Invoice, error) {
	return s.invoice, nil
}

func (s *integrationManualPurchaseStripe) FindInvoiceByRef(
	context.Context,
	string,
	string,
) (billingstripe.Invoice, bool, error) {
	return billingstripe.Invoice{}, false, fmt.Errorf("unexpected invoice search")
}

func (s *integrationManualPurchaseStripe) VoidInvoice(
	context.Context,
	string,
	string,
) (billingstripe.Invoice, error) {
	return billingstripe.Invoice{}, fmt.Errorf("unexpected void invoice")
}

func TestPgxStore_MarkEventProcessed_FirstTimeAndDuplicate(t *testing.T) {
	pool := testutil.NewTestDB(t)
	store := webhook.NewStore(pool)

	firstTime, err := store.MarkEventProcessed(context.Background(), "evt_test_1", "customer.created")
	require.NoError(t, err)
	require.True(t, firstTime)

	firstTime2, err := store.MarkEventProcessed(context.Background(), "evt_test_1", "customer.created")
	require.NoError(t, err)
	require.False(t, firstTime2, "second insert of same event_id should return firstTime=false")
}

func TestRouter_LateInvoicePaidForRetiredOrgACKsWithoutRelaxingCollection(t *testing.T) {
	pool := testutil.NewTestDB(t)
	ctx := context.Background()
	orgID, accountID, operationID := uuid.New(), uuid.New(), uuid.New()
	_, err := pool.Exec(ctx, `
		INSERT INTO ms_billing.accounts
		    (id, owner_kind, owner_org_id, usage_billing_mode)
		VALUES ($1, 'org', $2, 'prepaid')`, accountID, orgID)
	require.NoError(t, err)
	_, err = pool.Exec(ctx, `
		INSERT INTO ms_billing.invoices
		    (account_id, charge_funding_account_id, charge_funding_generation,
		     stripe_invoice_id, status, amount_due, amount_paid)
		SELECT $1, auth.funding_account_id, auth.generation,
		       'in_retired_paid', 'paid', 0, 1200
		FROM ms_billing.account_funding_authorizations auth
		WHERE auth.account_id = $1`, accountID)
	require.NoError(t, err)
	_, err = pool.Exec(ctx, `
		INSERT INTO ms_billing.org_deletion_finalizations
		    (org_id, operation_id, finalized_at)
		VALUES ($1, $2, now())`, orgID, operationID)
	require.NoError(t, err)

	verifier := &webhooktest.FakeVerifier{
		Event: invoiceEvent(
			"evt_retired_paid",
			"invoice.paid",
			"in_retired_paid",
			"paid",
			1200,
			0,
		),
	}
	stripe := &webhooktest.FakeChargeRetriever{}
	router := webhook.NewRouter(
		verifier,
		webhook.NewStore(pool),
		stripe,
		stripe,
		webhooktest.SilentLogger(),
	)

	result := router.Process(ctx, []byte(`{}`), "sig")
	require.Equal(t, 200, result.HTTPStatus)
	require.Equal(t, webhook.StatusOK, result.Status)
	var mode string
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT usage_billing_mode FROM ms_billing.accounts WHERE id=$1`,
		accountID).Scan(&mode))
	require.Equal(t, "prepaid", mode,
		"late invoice.paid reconciliation must not reactivate a retired account")
}

func TestPgxStore_RelaxPaidInvoiceWaitsForFinalizerThenNoOps(t *testing.T) {
	pool := testutil.NewTestDB(t)
	ctx := context.Background()
	orgID, accountID, operationID := uuid.New(), uuid.New(), uuid.New()
	_, err := pool.Exec(ctx, `
		INSERT INTO ms_billing.accounts
		    (id, owner_kind, owner_org_id, usage_billing_mode)
		VALUES ($1, 'org', $2, 'prepaid')`, accountID, orgID)
	require.NoError(t, err)
	_, err = pool.Exec(ctx, `
		INSERT INTO ms_billing.invoices
		    (account_id, charge_funding_account_id, charge_funding_generation,
		     stripe_invoice_id, status, amount_due, amount_paid)
		SELECT $1, auth.funding_account_id, auth.generation,
		       'in_relax_race', 'paid', 0, 1200
		FROM ms_billing.account_funding_authorizations auth
		WHERE auth.account_id = $1`, accountID)
	require.NoError(t, err)

	finalizer, err := pool.Begin(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { _ = finalizer.Rollback(context.Background()) })
	_, err = finalizer.Exec(ctx, `
		SELECT pg_advisory_xact_lock(
		    hashtextextended('ms_billing.org.lifecycle:' || $1::uuid::text, 0)
		)`, orgID)
	require.NoError(t, err)
	_, err = finalizer.Exec(ctx, `
		INSERT INTO ms_billing.org_deletion_finalizations
		    (org_id, operation_id, finalized_at)
		VALUES ($1, $2, now())`, orgID, operationID)
	require.NoError(t, err)

	type result struct {
		relaxed bool
		err     error
	}
	done := make(chan result, 1)
	store := webhook.NewStore(pool)
	go func() {
		relaxed, relaxErr := store.RelaxCollectionOnPaidInvoice(
			context.Background(), "in_relax_race",
		)
		done <- result{relaxed: relaxed, err: relaxErr}
	}()
	select {
	case got := <-done:
		t.Fatalf("relax bypassed in-progress finalization: %+v", got)
	case <-time.After(150 * time.Millisecond):
	}
	require.NoError(t, finalizer.Commit(ctx))
	select {
	case got := <-done:
		require.NoError(t, got.err)
		require.False(t, got.relaxed)
	case <-time.After(5 * time.Second):
		t.Fatal("relax did not resume after finalization committed")
	}
}

func TestRouter_InvoicePaidSettlesManualCreditWithoutInvoiceMirror(t *testing.T) {
	pool := testutil.NewTestDB(t)
	ctx := context.Background()
	accountID := seedAccount(t, pool, "cus_credit_webhook")
	ledgerID := uuid.New()
	_, err := pool.Exec(ctx,
		`INSERT INTO ms_billing.credit_ledger
		   (id, account_id, amount_micros, type, status, balance_after_micros,
		    actor, idempotency_key, stripe_invoice_id,
		    attempt_stripe_customer_id, charge_funding_account_id,
		    charge_funding_generation)
		 SELECT $1, $2, 5000000, 'purchase', 'pending', 5000000,
		        'self', $3, 'in_credit_webhook', account.stripe_customer_id,
		        funding.funding_account_id, funding.generation
		 FROM ms_billing.account_funding_authorizations funding
		 JOIN ms_billing.accounts account ON account.id=funding.funding_account_id
		 WHERE funding.account_id=$2`,
		ledgerID, accountID, "purchase:"+ledgerID.String(),
	)
	require.NoError(t, err)

	verifier := &webhooktest.FakeVerifier{
		Event: creditInvoiceEvent(
			"evt_credit_webhook_1",
			"invoice.paid",
			"in_credit_webhook",
			"paid",
			500,
			500,
			"purchase",
			accountID,
			ledgerID,
		),
	}
	stripe := &webhooktest.FakeChargeRetriever{}
	observer := &creditObserver{}
	manualStripe := &integrationManualPurchaseStripe{
		invoice: billingstripe.Invoice{
			ID:                  "in_credit_webhook",
			CustomerID:          "cus_credit_webhook",
			Status:              "paid",
			CollectionMethod:    "charge_automatically",
			ChargeRef:           "credit-purchase:" + ledgerID.String(),
			CreditOperation:     "purchase",
			CreditAccountID:     accountID.String(),
			CreditLedgerID:      ledgerID.String(),
			AmountDue:           500,
			AmountPaid:          500,
			AmountRemaining:     0,
			AmountPaidOffStripe: 0,
			Total:               500,
			Currency:            "usd",
			HostedInvoiceURL:    "https://stripe.test/in_credit_webhook",
		},
		items: []billingstripe.InvoiceItem{{
			ID: "ii_credit_webhook", AmountCents: 500, Currency: "usd",
		}},
	}
	manualStripe.payments = []billingstripe.InvoicePaymentProof{{
		ID:                    "inpay_credit_webhook",
		InvoiceID:             "in_credit_webhook",
		Status:                "paid",
		IsDefault:             true,
		AmountPaid:            500,
		AmountRequested:       500,
		Currency:              "usd",
		PaymentType:           string(stripego.InvoicePaymentPaymentTypePaymentIntent),
		PaymentIntentID:       "pi_credit_webhook",
		PaymentIntentStatus:   string(stripego.PaymentIntentStatusSucceeded),
		PaymentIntentCustomer: "cus_credit_webhook",
		PaymentMethodID:       "pm_credit_webhook",
		PaymentIntentAmount:   500,
		AmountReceived:        500,
		PaymentIntentCurrency: "usd",
	}}
	manualExecutor := creditpurchase.NewExecutor(
		creditpurchase.NewStore(pool),
		creditledger.NewStore(pool),
		manualStripe,
	)
	router := webhook.NewRouter(
		verifier,
		webhook.NewStore(pool),
		stripe,
		stripe,
		webhooktest.SilentLogger(),
	).WithCreditSettlementObserver(observer).
		WithManualCreditPaidReconciler(manualExecutor)

	result := router.Process(ctx, []byte(`{}`), "sig")

	require.Equal(t, webhook.StatusDriftWarning, result.Status)
	var (
		status       string
		balanceAfter int64
	)
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT status, balance_after_micros
		   FROM ms_billing.credit_ledger WHERE id=$1`,
		ledgerID,
	).Scan(&status, &balanceAfter))
	require.Equal(t, "settled", status)
	require.Equal(t, int64(5_000_000), balanceAfter)
	require.Equal(t, []uuid.UUID{accountID}, observer.accounts)
	require.Equal(t, []bool{true}, observer.settlementObservations)

	verifier.Event = creditInvoiceEvent(
		"evt_credit_webhook_2",
		"invoice.paid",
		"in_credit_webhook",
		"paid",
		500,
		500,
		"purchase",
		accountID,
		ledgerID,
	)
	result = router.Process(ctx, []byte(`{}`), "sig")
	require.Equal(t, webhook.StatusDriftWarning, result.Status)
	require.Equal(t, []uuid.UUID{accountID}, observer.accounts, "ledger replay must not observe twice")
}

func TestRouter_InvoicePaymentActionRequiredIsNonLatchingButFailuresLatch(t *testing.T) {
	pool := testutil.NewTestDB(t)
	ctx := context.Background()
	accountID := seedAccount(t, pool, "cus_invoice_signals")
	for _, invoiceID := range []string{
		"in_action_required",
		"in_payment_failed",
		"in_uncollectible",
	} {
		seedInvoice(t, pool, accountID, invoiceID, "open", 0, 1200)
	}

	verifier := &webhooktest.FakeVerifier{}
	stripe := &webhooktest.FakeChargeRetriever{}
	router := webhook.NewRouter(
		verifier,
		webhook.NewStore(pool),
		stripe,
		stripe,
		webhooktest.SilentLogger(),
	)

	tests := []struct {
		name           string
		eventID        string
		eventType      string
		invoiceID      string
		eventStatus    string
		wantEverFailed bool
	}{
		{
			name:        "3DS action required stays transient",
			eventID:     "evt_action_required",
			eventType:   "invoice.payment_action_required",
			invoiceID:   "in_action_required",
			eventStatus: "open",
		},
		{
			name:           "payment failed latches",
			eventID:        "evt_payment_failed",
			eventType:      "invoice.payment_failed",
			invoiceID:      "in_payment_failed",
			eventStatus:    "open",
			wantEverFailed: true,
		},
		{
			name:           "marked uncollectible latches",
			eventID:        "evt_uncollectible",
			eventType:      "invoice.marked_uncollectible",
			invoiceID:      "in_uncollectible",
			eventStatus:    "uncollectible",
			wantEverFailed: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			verifier.Event = invoiceEvent(
				tt.eventID,
				tt.eventType,
				tt.invoiceID,
				tt.eventStatus,
				0,
				1200,
			)

			result := router.Process(ctx, []byte(`{}`), "sig")

			require.Equal(t, 200, result.HTTPStatus)
			require.Equal(t, webhook.StatusOK, result.Status)

			var (
				status     string
				everFailed bool
				processed  bool
			)
			require.NoError(t, pool.QueryRow(ctx,
				`SELECT status, ever_failed
				   FROM ms_billing.invoices
				  WHERE stripe_invoice_id = $1`,
				tt.invoiceID,
			).Scan(&status, &everFailed))
			require.Equal(t, tt.eventStatus, status)
			require.Equal(t, tt.wantEverFailed, everFailed)
			require.NoError(t, pool.QueryRow(ctx,
				`SELECT EXISTS (
				   SELECT 1 FROM ms_billing.webhook_events_processed
				    WHERE event_id = $1
				 )`,
				tt.eventID,
			).Scan(&processed))
			require.True(t, processed, "successful signal remains acknowledged")
		})
	}
}

func TestPgxStore_TouchAccountByStripeCustomer_FoundAndNotFound(t *testing.T) {
	pool := testutil.NewTestDB(t)
	store := webhook.NewStore(pool)
	_ = seedAccount(t, pool, "cus_touch_a")

	found, err := store.TouchAccountByStripeCustomer(context.Background(), "cus_touch_a")
	require.NoError(t, err)
	require.True(t, found)

	found, err = store.TouchAccountByStripeCustomer(context.Background(), "cus_nonexistent")
	require.NoError(t, err)
	require.False(t, found, "non-existent stripe_customer_id should return found=false")
}

func TestPgxStore_InsertPaymentMethod_Success(t *testing.T) {
	pool := testutil.NewTestDB(t)
	store := webhook.NewStore(pool)
	_ = seedAccount(t, pool, "cus_pm_x")

	found, becameDefault, retired, err := store.InsertPaymentMethod(context.Background(), "cus_pm_x", webhook.InsertPaymentMethodParams{
		StripePaymentMethodID: "pm_new_1",
		Brand:                 "visa",
		Last4:                 "4242",
		ExpMonth:              12,
		ExpYear:               2099,
	})
	require.NoError(t, err)
	require.True(t, found)
	require.True(t, becameDefault)
	require.False(t, retired)

	var count int
	err = pool.QueryRow(context.Background(),
		`SELECT count(*) FROM ms_billing.payment_methods_mirror WHERE stripe_payment_method_id = $1`,
		"pm_new_1").Scan(&count)
	require.NoError(t, err)
	require.Equal(t, 1, count)
}

func TestPgxStore_InsertPaymentMethod_Drift_NoAccountsRow(t *testing.T) {
	// The CTE inserts via `SELECT acct.id ... FROM acct` — if the acct
	// CTE produces zero rows (no matching stripe_customer_id), nothing
	// is returned and the :one query yields pgx.ErrNoRows. The follow-up
	// existence check confirms drift; store returns found=false.
	pool := testutil.NewTestDB(t)
	store := webhook.NewStore(pool)

	found, becameDefault, retired, err := store.InsertPaymentMethod(context.Background(), "cus_orphan", webhook.InsertPaymentMethodParams{
		StripePaymentMethodID: "pm_orphan",
		Brand:                 "visa",
		Last4:                 "4242",
		ExpMonth:              12,
		ExpYear:               2099,
	})
	require.NoError(t, err)
	require.False(t, found, "expected drift signal (no accounts row)")
	require.False(t, becameDefault)
	require.False(t, retired)

	var count int
	err = pool.QueryRow(context.Background(),
		`SELECT count(*) FROM ms_billing.payment_methods_mirror WHERE stripe_payment_method_id = $1`,
		"pm_orphan").Scan(&count)
	require.NoError(t, err)
	require.Equal(t, 0, count, "no mirror row should have been inserted")
}

func TestPgxStore_InsertPaymentMethod_DefaultRedeliveryReturnsPersistedDefault(t *testing.T) {
	// A redelivered attach for the same first-card PM hits ON CONFLICT DO
	// NOTHING. The store must recover the persisted advisory is_default=true
	// so the router can retry an earlier failed Stripe default sync.
	pool := testutil.NewTestDB(t)
	store := webhook.NewStore(pool)
	_ = seedAccount(t, pool, "cus_idem")

	found, becameDefault, retired, err := store.InsertPaymentMethod(context.Background(), "cus_idem", webhook.InsertPaymentMethodParams{
		StripePaymentMethodID: "pm_idem_1",
		Brand:                 "visa",
		Last4:                 "4242",
		ExpMonth:              12,
		ExpYear:               2099,
	})
	require.NoError(t, err)
	require.True(t, found)
	require.True(t, becameDefault)
	require.False(t, retired)

	// Duplicate insert (simulated Stripe webhook retry).
	found, becameDefault, retired, err = store.InsertPaymentMethod(context.Background(), "cus_idem", webhook.InsertPaymentMethodParams{
		StripePaymentMethodID: "pm_idem_1",
		Brand:                 "visa",
		Last4:                 "4242",
		ExpMonth:              12,
		ExpYear:               2099,
	})
	require.NoError(t, err)
	require.True(t, found, "redelivered insert should return found=true")
	require.True(t, becameDefault, "redelivery should return the persisted advisory default")
	require.False(t, retired)

	var count int
	err = pool.QueryRow(context.Background(),
		`SELECT count(*) FROM ms_billing.payment_methods_mirror WHERE stripe_payment_method_id = $1`,
		"pm_idem_1").Scan(&count)
	require.NoError(t, err)
	require.Equal(t, 1, count)
}

func TestPgxStore_InsertPaymentMethod_NonDefaultRedeliveryReturnsPersistedFalse(t *testing.T) {
	pool := testutil.NewTestDB(t)
	store := webhook.NewStore(pool)
	_ = seedAccount(t, pool, "cus_idem_nondefault")
	ctx := context.Background()

	found, becameDefault, retired, err := store.InsertPaymentMethod(ctx, "cus_idem_nondefault", webhook.InsertPaymentMethodParams{
		StripePaymentMethodID: "pm_idem_first",
		Brand:                 "visa",
		Last4:                 "4242",
		ExpMonth:              12,
		ExpYear:               2099,
	})
	require.NoError(t, err)
	require.True(t, found)
	require.True(t, becameDefault)
	require.False(t, retired)

	found, becameDefault, retired, err = store.InsertPaymentMethod(ctx, "cus_idem_nondefault", webhook.InsertPaymentMethodParams{
		StripePaymentMethodID: "pm_idem_second",
		Brand:                 "mastercard",
		Last4:                 "4444",
		ExpMonth:              6,
		ExpYear:               2098,
	})
	require.NoError(t, err)
	require.True(t, found)
	require.False(t, becameDefault)
	require.False(t, retired)

	// Redelivery of the same second-card PM takes the ON CONFLICT path and
	// returns its persisted advisory is_default=false.
	found, becameDefault, retired, err = store.InsertPaymentMethod(ctx, "cus_idem_nondefault", webhook.InsertPaymentMethodParams{
		StripePaymentMethodID: "pm_idem_second",
		Brand:                 "mastercard",
		Last4:                 "4444",
		ExpMonth:              6,
		ExpYear:               2098,
	})
	require.NoError(t, err)
	require.True(t, found)
	require.False(t, becameDefault)
	require.False(t, retired)

	var count int
	err = pool.QueryRow(ctx,
		`SELECT count(*) FROM ms_billing.payment_methods_mirror WHERE stripe_payment_method_id = $1`,
		"pm_idem_second").Scan(&count)
	require.NoError(t, err)
	require.Equal(t, 1, count)
}

func TestPgxStore_InsertPaymentMethod_DedupeSkipHasNoPMRow(t *testing.T) {
	pool := testutil.NewTestDB(t)
	store := webhook.NewStore(pool)
	_ = seedAccount(t, pool, "cus_insert_dedupe")
	ctx := context.Background()

	found, becameDefault, retired, err := store.InsertPaymentMethod(ctx, "cus_insert_dedupe", webhook.InsertPaymentMethodParams{
		StripePaymentMethodID: "pm_dedupe_original",
		Brand:                 "visa",
		Last4:                 "4242",
		ExpMonth:              12,
		ExpYear:               2099,
	})
	require.NoError(t, err)
	require.True(t, found)
	require.True(t, becameDefault)
	require.False(t, retired)

	// A different Stripe PM id with the same card tuple is skipped by the
	// insert-time dedupe. No row exists under this PM id, so the persisted
	// default lookup yields ErrNoRows and the store reports false.
	found, becameDefault, retired, err = store.InsertPaymentMethod(ctx, "cus_insert_dedupe", webhook.InsertPaymentMethodParams{
		StripePaymentMethodID: "pm_dedupe_skipped",
		Brand:                 "visa",
		Last4:                 "4242",
		ExpMonth:              12,
		ExpYear:               2099,
	})
	require.NoError(t, err)
	require.True(t, found)
	require.False(t, becameDefault)
	require.False(t, retired)

	var count int
	err = pool.QueryRow(ctx,
		`SELECT count(*) FROM ms_billing.payment_methods_mirror WHERE stripe_payment_method_id = $1`,
		"pm_dedupe_skipped").Scan(&count)
	require.NoError(t, err)
	require.Equal(t, 0, count, "dedupe skip must not mirror the new Stripe PM id")
}

func TestPgxStore_InsertPaymentMethod_RetiredOrgNoOpsIncludingDeletedRedelivery(t *testing.T) {
	pool := testutil.NewTestDB(t)
	ctx := context.Background()
	orgID, accountID, operationID := uuid.New(), uuid.New(), uuid.New()
	_, err := pool.Exec(ctx, `
		INSERT INTO ms_billing.accounts
		    (id, owner_kind, owner_org_id, stripe_customer_id)
		VALUES ($1, 'org', $2, 'cus_retired_attach')`, accountID, orgID)
	require.NoError(t, err)
	_, err = pool.Exec(ctx, `
		INSERT INTO ms_billing.payment_methods_mirror
		    (account_id, stripe_payment_method_id, brand, last4, exp_month, exp_year,
		     is_default, deleted_at)
		VALUES ($1, 'pm_retired_existing', 'visa', '4242', 12, 2099,
		        false, now())`, accountID)
	require.NoError(t, err)
	_, err = pool.Exec(ctx, `
		INSERT INTO ms_billing.org_deletion_finalizations
		    (org_id, operation_id, finalized_at)
		VALUES ($1, $2, now())`, orgID, operationID)
	require.NoError(t, err)

	store := webhook.NewStore(pool)
	for _, pmID := range []string{"pm_retired_late", "pm_retired_existing"} {
		found, becameDefault, retired, insertErr := store.InsertPaymentMethod(ctx, "cus_retired_attach", webhook.InsertPaymentMethodParams{
			StripePaymentMethodID: pmID,
			Brand:                 "visa",
			Last4:                 "4242",
			ExpMonth:              12,
			ExpYear:               2099,
		})
		require.NoError(t, insertErr, pmID)
		require.True(t, found, pmID)
		require.True(t, retired, pmID)
		require.False(t, becameDefault, pmID)
	}

	var activeCount, lateCount, existingDeletedCount int
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT count(*) FROM ms_billing.payment_methods_mirror
		WHERE account_id = $1 AND deleted_at IS NULL`, accountID).Scan(&activeCount))
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT count(*) FROM ms_billing.payment_methods_mirror
		WHERE stripe_payment_method_id = 'pm_retired_late'`,
	).Scan(&lateCount))
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT count(*) FROM ms_billing.payment_methods_mirror
		WHERE stripe_payment_method_id = 'pm_retired_existing'
		  AND deleted_at IS NOT NULL`,
	).Scan(&existingDeletedCount))
	require.Zero(t, activeCount, "a late attach must not restore any active card")
	require.Zero(t, lateCount, "a previously unseen late card must not be mirrored")
	require.Equal(t, 1, existingDeletedCount, "redelivery must preserve the audit row as soft-deleted")
}

func TestPgxStore_InsertPaymentMethod_WaitsForFinalizerThenNoOps(t *testing.T) {
	pool := testutil.NewTestDB(t)
	ctx := context.Background()
	orgID, accountID, operationID := uuid.New(), uuid.New(), uuid.New()
	_, err := pool.Exec(ctx, `
		INSERT INTO ms_billing.accounts
		    (id, owner_kind, owner_org_id, stripe_customer_id)
		VALUES ($1, 'org', $2, 'cus_attach_finalize_race')`, accountID, orgID)
	require.NoError(t, err)

	finalizer, err := pool.Begin(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { _ = finalizer.Rollback(context.Background()) })
	_, err = finalizer.Exec(ctx, `
		SELECT pg_advisory_xact_lock(
		    hashtextextended('ms_billing.org.lifecycle:' || $1::uuid::text, 0)
		)`, orgID)
	require.NoError(t, err)
	_, err = finalizer.Exec(ctx, `
		INSERT INTO ms_billing.org_deletion_finalizations
		    (org_id, operation_id, finalized_at)
		VALUES ($1, $2, now())`, orgID, operationID)
	require.NoError(t, err)

	type result struct {
		found, becameDefault, retired bool
		err                           error
	}
	done := make(chan result, 1)
	store := webhook.NewStore(pool)
	go func() {
		found, becameDefault, retired, insertErr := store.InsertPaymentMethod(
			context.Background(),
			"cus_attach_finalize_race",
			webhook.InsertPaymentMethodParams{
				StripePaymentMethodID: "pm_attach_finalize_race",
				Brand:                 "visa",
				Last4:                 "4242",
				ExpMonth:              12,
				ExpYear:               2099,
			},
		)
		done <- result{found: found, becameDefault: becameDefault, retired: retired, err: insertErr}
	}()
	select {
	case got := <-done:
		t.Fatalf("attach bypassed in-progress finalization: %+v", got)
	case <-time.After(150 * time.Millisecond):
	}
	require.NoError(t, finalizer.Commit(ctx))
	select {
	case got := <-done:
		require.NoError(t, got.err)
		require.True(t, got.found)
		require.True(t, got.retired)
		require.False(t, got.becameDefault)
	case <-time.After(5 * time.Second):
		t.Fatal("attach did not resume after finalization committed")
	}

	var count int
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT count(*) FROM ms_billing.payment_methods_mirror
		WHERE stripe_payment_method_id = 'pm_attach_finalize_race'`,
	).Scan(&count))
	require.Zero(t, count)
}

func TestPgxStore_SoftDeletePaymentMethod_FoundAndIdempotent(t *testing.T) {
	pool := testutil.NewTestDB(t)
	store := webhook.NewStore(pool)
	accountID := seedAccount(t, pool, "cus_del")
	seedActivePM(t, pool, accountID, "pm_del_1", 12, 2099)

	found, err := store.SoftDeletePaymentMethod(context.Background(), "pm_del_1")
	require.NoError(t, err)
	require.True(t, found)

	// Second call: row is already soft-deleted → found=false (idempotent).
	found, err = store.SoftDeletePaymentMethod(context.Background(), "pm_del_1")
	require.NoError(t, err)
	require.False(t, found, "soft-delete on already-deleted row is a no-op")

	var hasDeleted bool
	err = pool.QueryRow(context.Background(),
		`SELECT deleted_at IS NOT NULL FROM ms_billing.payment_methods_mirror WHERE stripe_payment_method_id = $1`,
		"pm_del_1").Scan(&hasDeleted)
	require.NoError(t, err)
	require.True(t, hasDeleted)
}

func TestPgxStore_SetDefaultPaymentMethod_FlipsOneAndUnflipsRest(t *testing.T) {
	pool := testutil.NewTestDB(t)
	store := webhook.NewStore(pool)
	accountID := seedAccount(t, pool, "cus_def")
	seedActivePM(t, pool, accountID, "pm_def_a", 12, 2099)
	seedActivePM(t, pool, accountID, "pm_def_b", 6, 2099)
	seedActivePM(t, pool, accountID, "pm_def_c", 1, 2099)

	require.NoError(t, store.SetDefaultPaymentMethod(context.Background(), "cus_def", "pm_def_b"))

	defaults := readDefaultFlags(t, pool, accountID)
	require.False(t, defaults["pm_def_a"])
	require.True(t, defaults["pm_def_b"])
	require.False(t, defaults["pm_def_c"])

	// Flip default to pm_def_c.
	require.NoError(t, store.SetDefaultPaymentMethod(context.Background(), "cus_def", "pm_def_c"))

	defaults = readDefaultFlags(t, pool, accountID)
	require.False(t, defaults["pm_def_a"])
	require.False(t, defaults["pm_def_b"], "previous default should flip back to false")
	require.True(t, defaults["pm_def_c"])

	// Clear default everywhere.
	require.NoError(t, store.SetDefaultPaymentMethod(context.Background(), "cus_def", ""))

	defaults = readDefaultFlags(t, pool, accountID)
	require.False(t, defaults["pm_def_a"])
	require.False(t, defaults["pm_def_b"])
	require.False(t, defaults["pm_def_c"])
}

func TestPgxStore_ResolvePendingAddCardRequest_CompletedThenDuplicate(t *testing.T) {
	// Add-card lifecycle, two passes:
	//   1. Fresh card → resolver flips request to 'completed' and points
	//      payment_method_id at the just-mirrored row.
	//   2. Same card re-attached (new pm_* id, same fingerprint) → resolver
	//      flips the second request to 'duplicate', points payment_method_id
	//      at the EXISTING surviving row, and soft-deletes the just-mirrored
	//      duplicate row so the UI shows one card per real-world fingerprint.
	pool := testutil.NewTestDB(t)
	store := webhook.NewStore(pool)
	accountID := seedAccount(t, pool, "cus_dup")
	ctx := context.Background()

	// First pass: mirror a fresh card and resolve a pending request.
	firstReqID := seedPendingAddCardRequest(t, pool, accountID, "pm_dup_1")
	insertMirrorWithFingerprint(t, pool, accountID, "pm_dup_1", "fp_abc")
	require.NoError(t, store.ResolvePendingAddCardRequest(ctx, "pm_dup_1"))

	status, pmID, deleted := readAddCardRequest(t, pool, firstReqID)
	require.Equal(t, "completed", status)
	require.NotNil(t, pmID, "completed request should point at the mirrored row")
	require.False(t, deleted, "the first card's mirror row stays active")

	// Second pass: same fingerprint, fresh pm_* id (Stripe's normal
	// re-attach behavior) → duplicate.
	secondReqID := seedPendingAddCardRequest(t, pool, accountID, "pm_dup_2")
	insertMirrorWithFingerprint(t, pool, accountID, "pm_dup_2", "fp_abc")
	require.NoError(t, store.ResolvePendingAddCardRequest(ctx, "pm_dup_2"))

	status, pmID, deleted = readAddCardRequest(t, pool, secondReqID)
	require.Equal(t, "duplicate", status)
	require.NotNil(t, pmID, "duplicate request still resolves to a PM (the surviving one)")

	// The just-mirrored pm_dup_2 row should be soft-deleted.
	require.True(t, isMirrorRowDeleted(t, pool, "pm_dup_2"))
	require.False(t, isMirrorRowDeleted(t, pool, "pm_dup_1"),
		"the pre-existing card should survive unchanged")

	// And ListPaymentMethods (the UI query) sees exactly one row for this account.
	var activeCount int
	err := pool.QueryRow(ctx,
		`SELECT COUNT(*) FROM ms_billing.payment_methods_mirror
		 WHERE account_id = $1 AND deleted_at IS NULL`, accountID).Scan(&activeCount)
	require.NoError(t, err)
	require.Equal(t, 1, activeCount)
}

func TestPgxStore_ResolvePendingAddCardRequest_NoMirrorRow_IsNoOp(t *testing.T) {
	// Event ordering: setup_intent.succeeded can arrive before
	// payment_method.attached. The resolver should bail out cleanly when
	// the mirror row isn't there yet — the partner handler resolves later.
	pool := testutil.NewTestDB(t)
	store := webhook.NewStore(pool)
	accountID := seedAccount(t, pool, "cus_noop")
	reqID := seedPendingAddCardRequest(t, pool, accountID, "pm_noop")

	require.NoError(t, store.ResolvePendingAddCardRequest(context.Background(), "pm_noop"))

	status, _, _ := readAddCardRequest(t, pool, reqID)
	require.Equal(t, "pending", status, "request stays pending when mirror row absent")
}

func TestPgxStore_ResolvePendingAddCardRequest_NullFingerprint_NeverDeduplicates(t *testing.T) {
	// Legacy rows pre-migration 005 have fingerprint=NULL. A new card
	// inserted alongside one of those must always resolve to 'completed'
	// — NULL fingerprints don't collide with anything.
	pool := testutil.NewTestDB(t)
	store := webhook.NewStore(pool)
	accountID := seedAccount(t, pool, "cus_legacy")
	ctx := context.Background()

	// Legacy mirror row, no fingerprint.
	seedActivePM(t, pool, accountID, "pm_legacy", 12, 2099)

	// New attach with a real fingerprint.
	reqID := seedPendingAddCardRequest(t, pool, accountID, "pm_new")
	insertMirrorWithFingerprint(t, pool, accountID, "pm_new", "fp_real")
	require.NoError(t, store.ResolvePendingAddCardRequest(ctx, "pm_new"))

	status, _, _ := readAddCardRequest(t, pool, reqID)
	require.Equal(t, "completed", status)
}

// --- ApplyInvoiceStatus reconciliation ------------------------------------

func TestPgxStore_ApplyInvoiceStatus_ForwardTransitions(t *testing.T) {
	// draft → open → paid: each forward step lands; amount_paid is recorded on
	// paid.
	pool := testutil.NewTestDB(t)
	store := webhook.NewStore(pool)
	accountID := seedAccount(t, pool, "cus_inv_fwd")
	seedInvoice(t, pool, accountID, "in_fwd", "draft", 0, 1200)
	ctx := context.Background()

	found, err := store.ApplyInvoiceStatus(ctx, webhook.ApplyInvoiceStatusParams{
		StripeInvoiceID: "in_fwd", Status: "open", AmountPaidCents: 0, AmountDueCents: 1200,
	})
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, "open", readInvoiceStatus(t, pool, "in_fwd"))

	found, err = store.ApplyInvoiceStatus(ctx, webhook.ApplyInvoiceStatusParams{
		StripeInvoiceID: "in_fwd", Status: "paid", AmountPaidCents: 1200, AmountDueCents: 1200,
	})
	require.NoError(t, err)
	require.True(t, found)
	status, paid := readInvoiceStatusAndPaid(t, pool, "in_fwd")
	require.Equal(t, "paid", status)
	require.Equal(t, int64(1200), paid)
}

func TestPgxStore_ApplyInvoiceStatus_OutOfOrder_LateFinalizedDoesNotRegressPaid(t *testing.T) {
	// Stripe delivers out of order: invoice.paid lands first, then a late
	// invoice.finalized (open). The monotonic guard must keep the row 'paid'.
	pool := testutil.NewTestDB(t)
	store := webhook.NewStore(pool)
	accountID := seedAccount(t, pool, "cus_inv_ooo")
	seedInvoice(t, pool, accountID, "in_ooo", "open", 0, 1200)
	ctx := context.Background()

	found, err := store.ApplyInvoiceStatus(ctx, webhook.ApplyInvoiceStatusParams{
		StripeInvoiceID: "in_ooo", Status: "paid", AmountPaidCents: 1200, AmountDueCents: 1200,
	})
	require.NoError(t, err)
	require.True(t, found)

	// Late finalized (open) arrives — must be rejected by the guard.
	found, err = store.ApplyInvoiceStatus(ctx, webhook.ApplyInvoiceStatusParams{
		StripeInvoiceID: "in_ooo", Status: "open", AmountPaidCents: 0, AmountDueCents: 1200,
	})
	require.NoError(t, err)
	require.False(t, found, "a late finalized must NOT regress a paid row")

	status, paid := readInvoiceStatusAndPaid(t, pool, "in_ooo")
	require.Equal(t, "paid", status, "row stays paid")
	require.Equal(t, int64(1200), paid, "amount_paid is not zeroed by the stale event")
}

func TestPgxStore_ApplyInvoiceStatus_TerminalNotOverwritten(t *testing.T) {
	// paid is terminal; a void event (also terminal, equal rank but different
	// status) must not overwrite it — terminal-once-set holds.
	pool := testutil.NewTestDB(t)
	store := webhook.NewStore(pool)
	accountID := seedAccount(t, pool, "cus_inv_term")
	seedInvoice(t, pool, accountID, "in_term", "paid", 1200, 1200)
	ctx := context.Background()

	found, err := store.ApplyInvoiceStatus(ctx, webhook.ApplyInvoiceStatusParams{
		StripeInvoiceID: "in_term", Status: "void", AmountPaidCents: 0, AmountDueCents: 1200,
	})
	require.NoError(t, err)
	require.False(t, found, "void must not overwrite a paid invoice")
	require.Equal(t, "paid", readInvoiceStatus(t, pool, "in_term"))
}

func TestPgxStore_ApplyInvoiceStatus_IdempotentReplay(t *testing.T) {
	// A replayed invoice.paid (same status) is allowed through to refresh the
	// amounts idempotently — the row stays paid with the same values.
	pool := testutil.NewTestDB(t)
	store := webhook.NewStore(pool)
	accountID := seedAccount(t, pool, "cus_inv_replay")
	seedInvoice(t, pool, accountID, "in_replay", "paid", 1200, 1200)
	ctx := context.Background()

	found, err := store.ApplyInvoiceStatus(ctx, webhook.ApplyInvoiceStatusParams{
		StripeInvoiceID: "in_replay", Status: "paid", AmountPaidCents: 1200, AmountDueCents: 1200,
	})
	require.NoError(t, err)
	require.True(t, found, "identical re-apply is allowed (idempotent amount refresh)")
	require.Equal(t, "paid", readInvoiceStatus(t, pool, "in_replay"))
}

func TestPgxStore_ApplyInvoiceStatus_PresentmentEnrichment_SetNeverCleared(t *testing.T) {
	// Migration 026 semantics: the presentment fields land when an event
	// carries them (finalized) and are NEVER cleared by a later event whose
	// payload omits them — COALESCE(NULLIF(new,''), old) keeps the stored
	// values while the status still advances.
	pool := testutil.NewTestDB(t)
	store := webhook.NewStore(pool)
	accountID := seedAccount(t, pool, "cus_inv_pres")
	seedInvoice(t, pool, accountID, "in_pres", "draft", 0, 1200)
	ctx := context.Background()

	// Pre-finalization event: no presentment fields → columns stay NULL.
	found, err := store.ApplyInvoiceStatus(ctx, webhook.ApplyInvoiceStatusParams{
		StripeInvoiceID: "in_pres", Status: "draft", AmountPaidCents: 0, AmountDueCents: 1200,
	})
	require.NoError(t, err)
	require.True(t, found)
	number, hostedURL, pdf := readInvoicePresentment(t, pool, "in_pres")
	require.Empty(t, number)
	require.Empty(t, hostedURL)
	require.Empty(t, pdf)

	// Finalized: Stripe assigned the fields → they enrich the mirror.
	found, err = store.ApplyInvoiceStatus(ctx, webhook.ApplyInvoiceStatusParams{
		StripeInvoiceID: "in_pres", Status: "open", AmountPaidCents: 0, AmountDueCents: 1200,
		Number:           "813C8918-0001",
		HostedInvoiceURL: "https://invoice.stripe.com/i/in_pres",
		InvoicePDF:       "https://pay.stripe.com/invoice/in_pres/pdf",
	})
	require.NoError(t, err)
	require.True(t, found)
	number, hostedURL, pdf = readInvoicePresentment(t, pool, "in_pres")
	require.Equal(t, "813C8918-0001", number)
	require.Equal(t, "https://invoice.stripe.com/i/in_pres", hostedURL)
	require.Equal(t, "https://pay.stripe.com/invoice/in_pres/pdf", pdf)

	// A later event with EMPTY presentment fields advances the status but
	// must not un-enrich the row.
	found, err = store.ApplyInvoiceStatus(ctx, webhook.ApplyInvoiceStatusParams{
		StripeInvoiceID: "in_pres", Status: "paid", AmountPaidCents: 1200, AmountDueCents: 1200,
	})
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, "paid", readInvoiceStatus(t, pool, "in_pres"))
	number, hostedURL, pdf = readInvoicePresentment(t, pool, "in_pres")
	require.Equal(t, "813C8918-0001", number, "empty payload value must not clear number")
	require.Equal(t, "https://invoice.stripe.com/i/in_pres", hostedURL, "empty payload value must not clear hosted_invoice_url")
	require.Equal(t, "https://pay.stripe.com/invoice/in_pres/pdf", pdf, "empty payload value must not clear invoice_pdf")
}

func TestPgxStore_ApplyInvoiceStatus_UnknownInvoiceID_NoOp(t *testing.T) {
	// An event for an invoice the charge spine never mirrored → 0 rows,
	// found=false, no crash.
	pool := testutil.NewTestDB(t)
	store := webhook.NewStore(pool)

	found, err := store.ApplyInvoiceStatus(context.Background(), webhook.ApplyInvoiceStatusParams{
		StripeInvoiceID: "in_does_not_exist", Status: "paid", AmountPaidCents: 1200, AmountDueCents: 1200,
	})
	require.NoError(t, err)
	require.False(t, found, "unknown stripe_invoice_id is a safe no-op")
}

// --- HasUnpaidInvoice (delinquency signal) ---------------------------------

func TestPgxStore_HasUnpaidInvoice_DerivesFromInvoiceState(t *testing.T) {
	pool := testutil.NewTestDB(t)
	store := billing.NewStore(pool)
	accountID := seedAccount(t, pool, "cus_delinq")
	ctx := context.Background()

	// No invoices → not delinquent.
	has, err := store.HasUnpaidInvoice(ctx, accountID)
	require.NoError(t, err)
	require.False(t, has)

	// A paid invoice → still not delinquent.
	seedInvoice(t, pool, accountID, "in_paid", "paid", 1200, 1200)
	has, err = store.HasUnpaidInvoice(ctx, accountID)
	require.NoError(t, err)
	require.False(t, has, "paid invoice is clean")

	// A draft invoice → not delinquent (no collection attempt yet).
	seedInvoice(t, pool, accountID, "in_draft", "draft", 0, 500)
	has, err = store.HasUnpaidInvoice(ctx, accountID)
	require.NoError(t, err)
	require.False(t, has, "draft is excluded — not finalized")

	// An open invoice (payment_failed leaves it open) → delinquent.
	seedInvoice(t, pool, accountID, "in_open", "open", 0, 800)
	has, err = store.HasUnpaidInvoice(ctx, accountID)
	require.NoError(t, err)
	require.True(t, has, "an open invoice surfaces the delinquency signal")
}

func TestPgxStore_HasUnpaidInvoice_PaymentFailedThenPaid_ClearsSignal(t *testing.T) {
	// payment_failed leaves the invoice 'open' (delinquent); a later paid
	// flips it to 'paid' and the signal clears.
	pool := testutil.NewTestDB(t)
	billingStore := billing.NewStore(pool)
	webhookStore := webhook.NewStore(pool)
	accountID := seedAccount(t, pool, "cus_recover")
	seedInvoice(t, pool, accountID, "in_recover", "open", 0, 1200)
	ctx := context.Background()

	has, err := billingStore.HasUnpaidInvoice(ctx, accountID)
	require.NoError(t, err)
	require.True(t, has, "open invoice → delinquent")

	// Subsequent invoice.paid reconciles via the webhook store.
	found, err := webhookStore.ApplyInvoiceStatus(ctx, webhook.ApplyInvoiceStatusParams{
		StripeInvoiceID: "in_recover", Status: "paid", AmountPaidCents: 1200, AmountDueCents: 1200,
	})
	require.NoError(t, err)
	require.True(t, found)

	has, err = billingStore.HasUnpaidInvoice(ctx, accountID)
	require.NoError(t, err)
	require.False(t, has, "paid clears the delinquency signal")
}

func TestPgxStore_ApplyInvoiceStatus_VoidClearsDelinquencySignal(t *testing.T) {
	// An admin voids a finalized invoice (open → void), or Stripe voids one on
	// subscription cancellation. invoice.voided MUST flip the mirror to 'void'
	// so HasUnpaidInvoice stops reporting the account delinquent — the debt was
	// forgiven and there is no other recovery path (the event is ACKed 200).
	pool := testutil.NewTestDB(t)
	webhookStore := webhook.NewStore(pool)
	billingStore := billing.NewStore(pool)
	accountID := seedAccount(t, pool, "cus_inv_void")
	seedInvoice(t, pool, accountID, "in_void", "open", 0, 1200)
	ctx := context.Background()

	has, err := billingStore.HasUnpaidInvoice(ctx, accountID)
	require.NoError(t, err)
	require.True(t, has, "open invoice → delinquent")

	found, err := webhookStore.ApplyInvoiceStatus(ctx, webhook.ApplyInvoiceStatusParams{
		StripeInvoiceID: "in_void", Status: "void", AmountPaidCents: 0, AmountDueCents: 1200,
	})
	require.NoError(t, err)
	require.True(t, found, "void must advance an open invoice (open<terminal)")
	require.Equal(t, "void", readInvoiceStatus(t, pool, "in_void"))

	has, err = billingStore.HasUnpaidInvoice(ctx, accountID)
	require.NoError(t, err)
	require.False(t, has, "void clears the delinquency signal")
}

func TestPgxStore_ApplyInvoiceStatus_UncollectibleKeepsDelinquencySignal(t *testing.T) {
	// Stripe gives up collecting (open → uncollectible). The mirror must record
	// the precise terminal state, and the delinquency signal stays TRUE because
	// the account still owes the money.
	pool := testutil.NewTestDB(t)
	webhookStore := webhook.NewStore(pool)
	billingStore := billing.NewStore(pool)
	accountID := seedAccount(t, pool, "cus_inv_unc")
	seedInvoice(t, pool, accountID, "in_unc", "open", 0, 1200)
	ctx := context.Background()

	found, err := webhookStore.ApplyInvoiceStatus(ctx, webhook.ApplyInvoiceStatusParams{
		StripeInvoiceID: "in_unc", Status: "uncollectible", AmountPaidCents: 0, AmountDueCents: 1200,
	})
	require.NoError(t, err)
	require.True(t, found, "uncollectible must advance an open invoice (open<terminal)")
	require.Equal(t, "uncollectible", readInvoiceStatus(t, pool, "in_unc"))

	has, err := billingStore.HasUnpaidInvoice(ctx, accountID)
	require.NoError(t, err)
	require.True(t, has, "uncollectible keeps the delinquency signal (account still owes)")
}

// --- helpers --------------------------------------------------------------

func seedInvoice(t *testing.T, pool *pgxpool.Pool, accountID uuid.UUID, stripeInvoiceID, status string, amountPaid, amountDue int64) {
	t.Helper()
	_, err := pool.Exec(context.Background(),
		`INSERT INTO ms_billing.invoices
		   (account_id, charge_funding_account_id, charge_funding_generation,
		    stripe_invoice_id, status, amount_paid, amount_due, currency)
		 SELECT $1, auth.funding_account_id, auth.generation,
		        $2, $3, $4, $5, 'usd'
		 FROM ms_billing.account_funding_authorizations auth
		 WHERE auth.account_id = $1`,
		accountID, stripeInvoiceID, status, amountPaid, amountDue,
	)
	require.NoError(t, err)
}

func readInvoiceStatus(t *testing.T, pool *pgxpool.Pool, stripeInvoiceID string) string {
	t.Helper()
	var status string
	require.NoError(t, pool.QueryRow(context.Background(),
		`SELECT status FROM ms_billing.invoices WHERE stripe_invoice_id = $1`,
		stripeInvoiceID).Scan(&status))
	return status
}

// readInvoicePresentment reads the migration-026 presentment columns,
// COALESCEd to "" so NULL and empty assert identically.
func readInvoicePresentment(t *testing.T, pool *pgxpool.Pool, stripeInvoiceID string) (number, hostedURL, pdf string) {
	t.Helper()
	require.NoError(t, pool.QueryRow(context.Background(),
		`SELECT COALESCE(number, ''), COALESCE(hosted_invoice_url, ''), COALESCE(invoice_pdf, '')
		 FROM ms_billing.invoices WHERE stripe_invoice_id = $1`,
		stripeInvoiceID).Scan(&number, &hostedURL, &pdf))
	return number, hostedURL, pdf
}

func readInvoiceStatusAndPaid(t *testing.T, pool *pgxpool.Pool, stripeInvoiceID string) (string, int64) {
	t.Helper()
	var status string
	var paid int64
	require.NoError(t, pool.QueryRow(context.Background(),
		`SELECT status, amount_paid FROM ms_billing.invoices WHERE stripe_invoice_id = $1`,
		stripeInvoiceID).Scan(&status, &paid))
	return status, paid
}

func seedAccount(t *testing.T, pool *pgxpool.Pool, stripeCustomerID string) uuid.UUID {
	t.Helper()
	accountID := uuid.New()
	_, err := pool.Exec(context.Background(),
		`INSERT INTO ms_billing.accounts (id, owner_kind, owner_user_id, stripe_customer_id)
		 VALUES ($1, 'user', $2, $3)`,
		accountID, uuid.New(), stripeCustomerID,
	)
	require.NoError(t, err)
	return accountID
}

func seedActivePM(t *testing.T, pool *pgxpool.Pool, accountID uuid.UUID, pmID string, expMonth, expYear int) {
	t.Helper()
	_, err := pool.Exec(context.Background(),
		`INSERT INTO ms_billing.payment_methods_mirror
		   (account_id, stripe_payment_method_id, brand, last4, exp_month, exp_year, is_default)
		 VALUES ($1, $2, 'visa', '4242', $3, $4, false)`,
		accountID, pmID, expMonth, expYear,
	)
	require.NoError(t, err)
}

func insertMirrorWithFingerprint(t *testing.T, pool *pgxpool.Pool, accountID uuid.UUID, pmID, fingerprint string) {
	t.Helper()
	_, err := pool.Exec(context.Background(),
		`INSERT INTO ms_billing.payment_methods_mirror
		   (account_id, stripe_payment_method_id, brand, last4, exp_month, exp_year, is_default, fingerprint)
		 VALUES ($1, $2, 'visa', '4242', 12, 2099, false, $3)`,
		accountID, pmID, fingerprint,
	)
	require.NoError(t, err)
}

func seedPendingAddCardRequest(t *testing.T, pool *pgxpool.Pool, accountID uuid.UUID, stripePMID string) uuid.UUID {
	t.Helper()
	reqID := uuid.New()
	_, err := pool.Exec(context.Background(),
		`INSERT INTO ms_billing.add_card_requests (id, account_id, stripe_pm_id, status)
		 VALUES ($1, $2, $3, 'pending')`,
		reqID, accountID, stripePMID,
	)
	require.NoError(t, err)
	return reqID
}

func readAddCardRequest(t *testing.T, pool *pgxpool.Pool, reqID uuid.UUID) (status string, paymentMethodID *uuid.UUID, mirrorDeleted bool) {
	t.Helper()
	var pmID *uuid.UUID
	require.NoError(t, pool.QueryRow(context.Background(),
		`SELECT status::text, payment_method_id FROM ms_billing.add_card_requests WHERE id = $1`,
		reqID).Scan(&status, &pmID))
	if pmID != nil {
		var del bool
		require.NoError(t, pool.QueryRow(context.Background(),
			`SELECT deleted_at IS NOT NULL FROM ms_billing.payment_methods_mirror WHERE id = $1`,
			*pmID).Scan(&del))
		mirrorDeleted = del
	}
	return status, pmID, mirrorDeleted
}

func isMirrorRowDeleted(t *testing.T, pool *pgxpool.Pool, stripePMID string) bool {
	t.Helper()
	var deleted bool
	require.NoError(t, pool.QueryRow(context.Background(),
		`SELECT deleted_at IS NOT NULL FROM ms_billing.payment_methods_mirror WHERE stripe_payment_method_id = $1`,
		stripePMID).Scan(&deleted))
	return deleted
}

func readDefaultFlags(t *testing.T, pool *pgxpool.Pool, accountID uuid.UUID) map[string]bool {
	t.Helper()
	rows, err := pool.Query(context.Background(),
		`SELECT stripe_payment_method_id, is_default FROM ms_billing.payment_methods_mirror
		 WHERE account_id = $1 AND deleted_at IS NULL`,
		accountID,
	)
	require.NoError(t, err)
	defer rows.Close()
	out := map[string]bool{}
	for rows.Next() {
		var pmID string
		var def bool
		require.NoError(t, rows.Scan(&pmID, &def))
		out[pmID] = def
	}
	require.NoError(t, rows.Err())
	return out
}

// TestPgxStore_RelaxCollectionOnPaidInvoice verifies the risk-graded RELAX
// driver (PR #9): a paid invoice re-trusts a PREPAID account back to arrears, but
// ONLY when no open/uncollectible invoice remains. It never touches an arrears
// account and is a no-op while delinquency persists.
func TestPgxStore_RelaxCollectionOnPaidInvoice(t *testing.T) {
	pool := testutil.NewTestDB(t)
	store := webhook.NewStore(pool)
	ctx := context.Background()

	accountID := seedAccount(t, pool, "cus_relax")
	// Tighten the account to prepaid (the state a relax must reverse).
	_, err := pool.Exec(ctx,
		`UPDATE ms_billing.accounts SET usage_billing_mode = 'prepaid' WHERE id = $1`, accountID.String())
	require.NoError(t, err)

	// The just-paid invoice + a SECOND still-open invoice: relax must NOT fire
	// while another invoice is unpaid.
	seedInvoice(t, pool, accountID, "in_relax_paid", "paid", 1200, 1200)
	seedInvoice(t, pool, accountID, "in_relax_open", "open", 0, 800)

	relaxed, err := store.RelaxCollectionOnPaidInvoice(ctx, "in_relax_paid")
	require.NoError(t, err)
	require.False(t, relaxed, "still delinquent (another open invoice) → no relax")
	require.Equal(t, "prepaid", readMode(t, pool, accountID))

	// Pay off the remaining invoice; now the relax fires.
	_, err = pool.Exec(ctx,
		`UPDATE ms_billing.invoices SET status = 'paid' WHERE stripe_invoice_id = $1`, "in_relax_open")
	require.NoError(t, err)

	relaxed, err = store.RelaxCollectionOnPaidInvoice(ctx, "in_relax_paid")
	require.NoError(t, err)
	require.True(t, relaxed, "no remaining delinquency → relax prepaid → arrears")
	require.Equal(t, "arrears", readMode(t, pool, accountID))

	// Idempotent: a second relax on an already-arrears account is a no-op.
	relaxed, err = store.RelaxCollectionOnPaidInvoice(ctx, "in_relax_paid")
	require.NoError(t, err)
	require.False(t, relaxed, "already arrears → no-op")

	// Unknown invoice id → safe no-op.
	relaxed, err = store.RelaxCollectionOnPaidInvoice(ctx, "in_does_not_exist")
	require.NoError(t, err)
	require.False(t, relaxed)
}

func readMode(t *testing.T, pool *pgxpool.Pool, accountID uuid.UUID) string {
	t.Helper()
	var mode string
	require.NoError(t, pool.QueryRow(context.Background(),
		`SELECT usage_billing_mode::text FROM ms_billing.accounts WHERE id = $1`,
		accountID.String()).Scan(&mode))
	return mode
}

// --- service-block failure latch + read-time streak derivation (migration 039) ---

// seedInvoiceAt inserts an invoice with an explicit created_at + ever_failed so
// the read-time streak derivation (ordered on created_at) can be exercised
// deterministically without relying on wall-clock insert order.
func seedInvoiceAt(t *testing.T, pool *pgxpool.Pool, accountID uuid.UUID, stripeInvoiceID, status string, everFailed bool, createdAt string) {
	t.Helper()
	_, err := pool.Exec(context.Background(),
		`INSERT INTO ms_billing.invoices
		   (account_id, charge_funding_account_id, charge_funding_generation,
		    stripe_invoice_id, status, amount_due, currency, ever_failed, created_at)
		 SELECT $1, auth.funding_account_id, auth.generation,
		        $2, $3, 1200, 'usd', $4, $5::timestamptz
		 FROM ms_billing.account_funding_authorizations auth
		 WHERE auth.account_id = $1`,
		accountID, stripeInvoiceID, status, everFailed, createdAt,
	)
	require.NoError(t, err)
}

func TestPgxStore_MarkInvoiceFailed_LatchesEverFailed(t *testing.T) {
	// The set-only latch flips ever_failed and is an idempotent no-op on repeat /
	// on an un-mirrored invoice.
	pool := testutil.NewTestDB(t)
	store := webhook.NewStore(pool)
	accountID := seedAccount(t, pool, "cus_latch")
	ctx := context.Background()
	seedInvoice(t, pool, accountID, "in_A", "open", 0, 1200)

	require.NoError(t, store.MarkInvoiceFailed(ctx, "in_A"))
	var everFailed bool
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT ever_failed FROM ms_billing.invoices WHERE stripe_invoice_id = 'in_A'`).Scan(&everFailed))
	require.True(t, everFailed)

	require.NoError(t, store.MarkInvoiceFailed(ctx, "in_A"), "repeat is a no-op")
	require.NoError(t, store.MarkInvoiceFailed(ctx, "in_orphan"), "un-mirrored invoice is a no-op")
}

func TestPgxStore_ServiceBlockSignals_StreakDerivation_IsDeliveryOrderImmune(t *testing.T) {
	// The failed-charge streak is DERIVED from (ever_failed OR uncollectible) +
	// created_at relative to the most-recent paid invoice — so it depends only on
	// the immutable facts, never on webhook delivery order. Timeline: A failed
	// (t1), B PAID (t2), C failed (t3). Only C is after the last paid, so the
	// streak is 1 regardless of the order these rows/latches landed.
	pool := testutil.NewTestDB(t)
	bstore := billing.NewStore(pool)
	accountID := seedAccount(t, pool, "cus_deriv")
	ctx := context.Background()

	seedInvoiceAt(t, pool, accountID, "in_A", "open", true, "2026-01-01T00:00:00Z")
	seedInvoiceAt(t, pool, accountID, "in_B", "paid", false, "2026-02-01T00:00:00Z")
	seedInvoiceAt(t, pool, accountID, "in_C", "open", true, "2026-03-01T00:00:00Z")

	sig, err := bstore.ServiceBlockSignals(ctx, accountID)
	require.NoError(t, err)
	require.Equal(t, 1, sig.FailedChargeStreak, "only failures after the last paid invoice count")

	// Two distinct failures after the last paid reach the block threshold.
	seedInvoiceAt(t, pool, accountID, "in_D", "uncollectible", false, "2026-04-01T00:00:00Z")
	sig, err = bstore.ServiceBlockSignals(ctx, accountID)
	require.NoError(t, err)
	require.Equal(t, 2, sig.FailedChargeStreak)

	// A newer paid invoice moves the cutoff forward and clears the streak (the
	// auto-cure) — no counter to reset.
	seedInvoiceAt(t, pool, accountID, "in_E", "paid", false, "2026-05-01T00:00:00Z")
	sig, err = bstore.ServiceBlockSignals(ctx, accountID)
	require.NoError(t, err)
	require.Equal(t, 0, sig.FailedChargeStreak, "a later paid invoice cures the streak")
}

func TestPgxStore_ServiceBlockSignals_ReflectsCardAndFirstCharge(t *testing.T) {
	// The one-shot read reflects the usable non-fraud card count (fraud-blocked
	// excluded) and the earliest real charge's status.
	pool := testutil.NewTestDB(t)
	bstore := billing.NewStore(pool)
	accountID := seedAccount(t, pool, "cus_sig")
	ctx := context.Background()

	sig, err := bstore.ServiceBlockSignals(ctx, accountID)
	require.NoError(t, err)
	require.Equal(t, 0, sig.UsableCardCount)
	require.Equal(t, 0, sig.FailedChargeStreak)
	require.Equal(t, "", sig.FirstChargeStatus)

	_, err = pool.Exec(ctx,
		`INSERT INTO ms_billing.payment_methods_mirror
		   (account_id, stripe_payment_method_id, brand, last4, exp_month, exp_year)
		 VALUES ($1, 'pm_good', 'visa', '4242', 12, 2999)`, accountID)
	require.NoError(t, err)
	_, err = pool.Exec(ctx,
		`INSERT INTO ms_billing.payment_methods_mirror
		   (account_id, stripe_payment_method_id, brand, last4, exp_month, exp_year, fraud_blocked)
		 VALUES ($1, 'pm_fraud', 'visa', '0002', 12, 2999, true)`, accountID)
	require.NoError(t, err)
	seedInvoice(t, pool, accountID, "in_first", "uncollectible", 0, 1200)

	sig, err = bstore.ServiceBlockSignals(ctx, accountID)
	require.NoError(t, err)
	require.Equal(t, 1, sig.UsableCardCount, "fraud-blocked card is excluded from the usable count")
	require.Equal(t, "uncollectible", sig.FirstChargeStatus)
}

func TestPgxStore_ApplyInvoiceStatus_UncollectibleToPaid_Cures(t *testing.T) {
	// M1: paying an uncollectible invoice (customer settles on the hosted page)
	// must land — paid outranks uncollectible — so the delinquency/block clears.
	pool := testutil.NewTestDB(t)
	store := webhook.NewStore(pool)
	accountID := seedAccount(t, pool, "cus_cure")
	seedInvoice(t, pool, accountID, "in_unc", "uncollectible", 0, 1200)
	ctx := context.Background()

	found, err := store.ApplyInvoiceStatus(ctx, webhook.ApplyInvoiceStatusParams{
		StripeInvoiceID: "in_unc", Status: "paid", AmountPaidCents: 1200, AmountDueCents: 1200,
	})
	require.NoError(t, err)
	require.True(t, found, "uncollectible→paid must be accepted")
	require.Equal(t, "paid", readInvoiceStatus(t, pool, "in_unc"))

	// And a stray late uncollectible after paid must NOT regress it.
	found, err = store.ApplyInvoiceStatus(ctx, webhook.ApplyInvoiceStatusParams{
		StripeInvoiceID: "in_unc", Status: "uncollectible", AmountPaidCents: 0, AmountDueCents: 1200,
	})
	require.NoError(t, err)
	require.False(t, found, "a late uncollectible must not overwrite paid")
	require.Equal(t, "paid", readInvoiceStatus(t, pool, "in_unc"))
}

// --- fraud flag (migration 038) + gate exclusion -------------------------

func seedCardFP(t *testing.T, pool *pgxpool.Pool, accountID uuid.UUID, pmID, fingerprint string) {
	t.Helper()
	var fp any
	if fingerprint != "" {
		fp = fingerprint
	}
	_, err := pool.Exec(context.Background(),
		`INSERT INTO ms_billing.payment_methods_mirror
		   (account_id, stripe_payment_method_id, brand, last4, exp_month, exp_year, fingerprint)
		 VALUES ($1, $2, 'visa', '4242', 12, 2999, $3)`,
		accountID, pmID, fp,
	)
	require.NoError(t, err)
}

func readFraud(t *testing.T, pool *pgxpool.Pool, pmID string) (blocked bool, reason string, flaggedSet bool) {
	t.Helper()
	require.NoError(t, pool.QueryRow(context.Background(),
		`SELECT fraud_blocked, COALESCE(fraud_reason, ''), fraud_flagged_at IS NOT NULL
		   FROM ms_billing.payment_methods_mirror WHERE stripe_payment_method_id = $1`,
		pmID).Scan(&blocked, &reason, &flaggedSet))
	return
}

func TestPgxStore_FlagPaymentMethodFraud_ByFingerprint_FlipsAndExcludesFromGate(t *testing.T) {
	pool := testutil.NewTestDB(t)
	store := webhook.NewStore(pool)
	bstore := billing.NewStore(pool)
	accountID := seedAccount(t, pool, "cus_fraud")
	seedCardFP(t, pool, accountID, "pm_fraud", "fp_x")
	ctx := context.Background()

	// Before: the card counts as usable.
	sig, err := bstore.ServiceBlockSignals(ctx, accountID)
	require.NoError(t, err)
	require.Equal(t, 1, sig.UsableCardCount)

	found, err := store.FlagPaymentMethodFraud(ctx, "cus_fraud", "fp_x", "", "dispute")
	require.NoError(t, err)
	require.True(t, found)

	blocked, reason, flaggedSet := readFraud(t, pool, "pm_fraud")
	require.True(t, blocked)
	require.Equal(t, "dispute", reason)
	require.True(t, flaggedSet, "fraud_flagged_at must be stamped")

	// After: the gate no longer counts the flagged card.
	sig, err = bstore.ServiceBlockSignals(ctx, accountID)
	require.NoError(t, err)
	require.Equal(t, 0, sig.UsableCardCount, "fraud-blocked card is excluded from the gate")

	// Idempotent replay: already flagged → 0 rows.
	found, err = store.FlagPaymentMethodFraud(ctx, "cus_fraud", "fp_x", "", "dispute")
	require.NoError(t, err)
	require.False(t, found)
}

func TestPgxStore_FlagPaymentMethodFraud_AllRowsSameFingerprint_AccountScoped(t *testing.T) {
	pool := testutil.NewTestDB(t)
	store := webhook.NewStore(pool)
	ctx := context.Background()

	// Same physical card (fp_shared) added twice on account A (pre-collapse), and
	// once on a DIFFERENT account B.
	accountA := seedAccount(t, pool, "cus_A")
	seedCardFP(t, pool, accountA, "pm_A1", "fp_shared")
	seedCardFP(t, pool, accountA, "pm_A2", "fp_shared")
	accountB := seedAccount(t, pool, "cus_B")
	seedCardFP(t, pool, accountB, "pm_B1", "fp_shared")

	found, err := store.FlagPaymentMethodFraud(ctx, "cus_A", "fp_shared", "", "dispute")
	require.NoError(t, err)
	require.True(t, found)

	// Both of A's copies flip; B's card with the same fingerprint is untouched.
	for _, pm := range []string{"pm_A1", "pm_A2"} {
		blocked, _, _ := readFraud(t, pool, pm)
		require.True(t, blocked, pm+" (account A) must be flagged")
	}
	blockedB, _, _ := readFraud(t, pool, "pm_B1")
	require.False(t, blockedB, "the same fingerprint on a DIFFERENT account must not be flagged")
}

func TestPgxStore_FlagPaymentMethodFraud_FingerprintFallbackToPMID(t *testing.T) {
	// A legacy card with a NULL fingerprint: resolution falls back to the pm id.
	pool := testutil.NewTestDB(t)
	store := webhook.NewStore(pool)
	accountID := seedAccount(t, pool, "cus_legacy")
	seedCardFP(t, pool, accountID, "pm_legacy", "") // NULL fingerprint
	ctx := context.Background()

	found, err := store.FlagPaymentMethodFraud(ctx, "cus_legacy", "", "pm_legacy", "early_fraud_warning")
	require.NoError(t, err)
	require.True(t, found)
	blocked, reason, _ := readFraud(t, pool, "pm_legacy")
	require.True(t, blocked)
	require.Equal(t, "early_fraud_warning", reason)
}

func TestPgxStore_FlagPaymentMethodFraud_UnknownCard_Drift(t *testing.T) {
	pool := testutil.NewTestDB(t)
	store := webhook.NewStore(pool)
	_ = seedAccount(t, pool, "cus_none")
	ctx := context.Background()

	found, err := store.FlagPaymentMethodFraud(ctx, "cus_none", "fp_absent", "", "dispute")
	require.NoError(t, err)
	require.False(t, found, "no matching mirror row → drift no-op")
}

func TestPgxStore_FlagPaymentMethodFraud_NullFingerprintRow_MatchedByPMID(t *testing.T) {
	// A legacy/wallet mirror row with a NULL fingerprint, but the disputed charge
	// DID carry a fingerprint — the row must still be flagged via its exact pm id
	// (the additive pm-id arm), not silently missed (F2).
	pool := testutil.NewTestDB(t)
	store := webhook.NewStore(pool)
	accountID := seedAccount(t, pool, "cus_wallet")
	seedCardFP(t, pool, accountID, "pm_wallet", "") // NULL fingerprint
	ctx := context.Background()

	found, err := store.FlagPaymentMethodFraud(ctx, "cus_wallet", "fp_charge", "pm_wallet", "dispute")
	require.NoError(t, err)
	require.True(t, found, "a NULL-fingerprint row must be flagged by exact pm id even when the charge carries a fingerprint")
	blocked, _, _ := readFraud(t, pool, "pm_wallet")
	require.True(t, blocked)
}
