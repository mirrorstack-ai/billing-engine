//go:build integration

package autotopup_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/mirrorstack-ai/billing-engine/internal/shared/testutil"
)

func TestMigration049_ExactConstraintAndIndexShapesAndMoneyAuditDeleteSemantics(t *testing.T) {
	pool := testutil.NewTestDB(t)
	ctx := context.Background()

	expectedConstraints := map[string]struct {
		validated  bool
		deferrable bool
		kind       string
		definition string
	}{
		"credit_auto_topup_configs_payment_method_fkey": {
			validated:  true,
			deferrable: true,
			kind:       "f",
			definition: "FOREIGN KEY (payment_method_id) REFERENCES ms_billing.payment_methods_mirror(id) DEFERRABLE",
		},
		"credit_ledger_attempt_payment_method_fkey": {
			validated:  true,
			deferrable: true,
			kind:       "f",
			definition: "FOREIGN KEY (attempt_payment_method_id) REFERENCES ms_billing.payment_methods_mirror(id) DEFERRABLE",
		},
		"credit_ledger_auto_topup_attempt_fields_check": {
			validated: true,
			kind:      "c",
			definition: "CHECK ((((type = 'auto_topup'::text) AND (attempt_payment_method_id IS NOT NULL) " +
				"AND (NULLIF(btrim(attempt_stripe_payment_method_id), ''::text) IS NOT NULL) " +
				"AND (NULLIF(btrim(attempt_stripe_customer_id), ''::text) IS NOT NULL) " +
				"AND (attempt_expires_at IS NOT NULL)) OR ((type = 'purchase'::text) " +
				"AND (attempt_payment_method_id IS NULL) " +
				"AND (attempt_stripe_payment_method_id IS NULL) " +
				"AND (attempt_expires_at IS NULL) AND (failure_code IS NULL)) OR " +
				"((type <> ALL (ARRAY['auto_topup'::text, 'purchase'::text])) " +
				"AND (attempt_payment_method_id IS NULL) " +
				"AND (attempt_stripe_payment_method_id IS NULL) " +
				"AND (attempt_stripe_customer_id IS NULL) AND (attempt_expires_at IS NULL) " +
				"AND (failure_code IS NULL))))",
		},
		"credit_ledger_auto_topup_failure_state_check": {
			validated: true,
			kind:      "c",
			definition: "CHECK ((((type = 'auto_topup'::text) AND (status = 'failed'::text) " +
				"AND (NULLIF(btrim(failure_code), ''::text) IS NOT NULL)) OR " +
				"((NOT ((type = 'auto_topup'::text) AND (status = 'failed'::text))) " +
				"AND (failure_code IS NULL))))",
		},
		"credit_ledger_auto_topup_attempt_window_check": {
			validated: true,
			kind:      "c",
			definition: "CHECK (((type <> 'auto_topup'::text) OR " +
				"((attempt_expires_at > created_at) " +
				"AND (attempt_expires_at <= (created_at + '00:10:00'::interval)))))",
		},
	}
	for name, want := range expectedConstraints {
		var (
			validated  bool
			deferrable bool
			kind       string
			definition string
		)
		require.NoError(t, pool.QueryRow(ctx,
			`SELECT convalidated, condeferrable, contype::text,
			        pg_get_constraintdef(oid, false)
			   FROM pg_constraint
			  WHERE connamespace='ms_billing'::regnamespace AND conname=$1`,
			name,
		).Scan(&validated, &deferrable, &kind, &definition), name)
		require.Equal(t, want.validated, validated, name)
		require.Equal(t, want.deferrable, deferrable, name)
		require.Equal(t, want.kind, kind, name)
		require.Equal(t, want.definition, definition, name)
	}

	var (
		indexUnique            bool
		indexValid             bool
		indexReady             bool
		indexLive              bool
		indexKeyAttributeCount int16
		indexAttributeCount    int16
		indexAccessMethod      string
		indexKeyColumn         string
		indexPredicate         string
	)
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT catalog_index.indisunique,
		        catalog_index.indisvalid,
		        catalog_index.indisready,
		        catalog_index.indislive,
		        catalog_index.indnkeyatts,
		        catalog_index.indnatts,
		        access_method.amname,
		        pg_get_indexdef(catalog_index.indexrelid, 1, true),
		        pg_get_expr(
		            catalog_index.indpred,
		            catalog_index.indrelid,
		            false
		        )
		   FROM pg_index AS catalog_index
		   JOIN pg_class AS index_relation
		     ON index_relation.oid = catalog_index.indexrelid
		   JOIN pg_am AS access_method
		     ON access_method.oid = index_relation.relam
		  WHERE catalog_index.indexrelid =
		        'ms_billing.credit_ledger_auto_topup_pending_uidx'::regclass`,
	).Scan(
		&indexUnique,
		&indexValid,
		&indexReady,
		&indexLive,
		&indexKeyAttributeCount,
		&indexAttributeCount,
		&indexAccessMethod,
		&indexKeyColumn,
		&indexPredicate,
	))
	require.True(t, indexUnique)
	require.True(t, indexValid)
	require.True(t, indexReady)
	require.True(t, indexLive)
	require.Equal(t, int16(1), indexKeyAttributeCount)
	require.Equal(t, int16(1), indexAttributeCount)
	require.Equal(t, "btree", indexAccessMethod)
	require.Equal(t, "account_id", indexKeyColumn)
	require.Equal(
		t,
		"((type = 'auto_topup'::text) AND (status = 'pending'::text))",
		indexPredicate,
	)

	t.Run("config prevents direct payment method hard delete", func(t *testing.T) {
		accountID := seedAutoAccount(t, pool)
		paymentMethodID := seedAutoPaymentMethod(t, pool, accountID, false)
		seedAutoConfig(t, pool, accountID, paymentMethodID, 0, true)

		_, err := pool.Exec(ctx,
			`DELETE FROM ms_billing.payment_methods_mirror WHERE id=$1`,
			paymentMethodID,
		)
		require.ErrorContains(t, err, "credit_auto_topup_configs_payment_method_fkey")
	})

	t.Run("ledger independently prevents direct payment method hard delete", func(t *testing.T) {
		accountID := seedAutoAccount(t, pool)
		paymentMethodID := seedAutoPaymentMethod(t, pool, accountID, false)
		insertValidAutoTopUp(t, pool, accountID, paymentMethodID, "pending")

		_, err := pool.Exec(ctx,
			`DELETE FROM ms_billing.payment_methods_mirror WHERE id=$1`,
			paymentMethodID,
		)
		require.ErrorContains(t, err, "credit_ledger_attempt_payment_method_fkey")
	})

	t.Run("account hard delete retains established cascade behavior", func(t *testing.T) {
		accountID := seedAutoAccount(t, pool)
		paymentMethodID := seedAutoPaymentMethod(t, pool, accountID, false)
		seedAutoConfig(t, pool, accountID, paymentMethodID, 0, true)
		insertValidAutoTopUp(t, pool, accountID, paymentMethodID, "pending")

		_, err := pool.Exec(ctx, `DELETE FROM ms_billing.accounts WHERE id=$1`, accountID)
		require.NoError(t, err)

		for _, table := range []string{
			"accounts", "payment_methods_mirror", "credit_auto_topup_configs", "credit_ledger",
		} {
			var count int
			require.NoError(t, pool.QueryRow(ctx,
				`SELECT count(*) FROM ms_billing.`+table+` WHERE `+
					map[string]string{
						"accounts":                  "id",
						"payment_methods_mirror":    "account_id",
						"credit_auto_topup_configs": "account_id",
						"credit_ledger":             "account_id",
					}[table]+`=$1`,
				accountID,
			).Scan(&count))
			require.Zero(t, count, table)
		}
	})
}

func TestMigration049_AttemptChecksRejectImpossibleStates(t *testing.T) {
	pool := testutil.NewTestDB(t)
	ctx := context.Background()
	accountID := seedAutoAccount(t, pool)
	paymentMethodID := seedAutoPaymentMethod(t, pool, accountID, false)
	createdAt := time.Date(2026, time.July, 25, 10, 0, 0, 0, time.UTC)

	insert := func(
		typ, status string,
		attemptPM any,
		stripePM any,
		stripeCustomer any,
		expiresAt any,
		failureCode any,
	) error {
		_, err := pool.Exec(ctx,
			`INSERT INTO ms_billing.credit_ledger
			   (account_id, amount_micros, type, status, balance_after_micros, actor,
			    idempotency_key, attempt_payment_method_id,
			    attempt_stripe_payment_method_id, attempt_stripe_customer_id,
			    attempt_expires_at, failure_code, created_at)
			 VALUES ($1, 5000000, $2, $3, 5000000, 'system', $4, $5, $6, $7, $8, $9, $10)`,
			accountID, typ, status, "constraint:"+uuid.NewString(),
			attemptPM, stripePM, stripeCustomer, expiresAt, failureCode, createdAt,
		)
		return err
	}

	err := insert("auto_topup", "pending", nil, nil, nil, nil, nil)
	require.ErrorContains(t, err, "credit_ledger_auto_topup_attempt_fields_check")

	err = insert(
		"grant", "settled", paymentMethodID, "pm_x", "cus_x",
		createdAt.Add(time.Minute), nil,
	)
	require.ErrorContains(t, err, "credit_ledger_auto_topup_attempt_fields_check")

	err = insert(
		"auto_topup", "pending", paymentMethodID, "pm_x", "cus_x",
		createdAt.Add(10*time.Minute), "declined",
	)
	require.ErrorContains(t, err, "credit_ledger_auto_topup_failure_state_check")

	err = insert(
		"auto_topup", "failed", paymentMethodID, "pm_x", "cus_x",
		createdAt.Add(10*time.Minute), nil,
	)
	require.ErrorContains(t, err, "credit_ledger_auto_topup_failure_state_check")

	for _, expiresAt := range []time.Time{
		createdAt,
		createdAt.Add(10*time.Minute + time.Microsecond),
	} {
		err = insert(
			"auto_topup", "pending", paymentMethodID, "pm_x", "cus_x",
			expiresAt, nil,
		)
		require.ErrorContains(t, err, "credit_ledger_auto_topup_attempt_window_check")
	}

	require.NoError(t, insert(
		"auto_topup", "pending", paymentMethodID, "pm_x", "cus_x",
		createdAt.Add(10*time.Minute), nil,
	))
	err = insert(
		"auto_topup", "pending", paymentMethodID, "pm_x", "cus_x",
		createdAt.Add(10*time.Minute), nil,
	)
	require.ErrorContains(t, err, "credit_ledger_auto_topup_pending_uidx")
}

func TestMigration049_DownUpRoundTrip(t *testing.T) {
	pool := testutil.NewTestDB(t)
	ctx := context.Background()

	_, err := pool.Exec(ctx, readMigration049(t, "049_credit_auto_topup_attempts.down.sql"))
	require.NoError(t, err)
	var paymentMethodType string
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT data_type
		   FROM information_schema.columns
		  WHERE table_schema='ms_billing'
		    AND table_name='credit_auto_topup_configs'
		    AND column_name='payment_method_id'`,
	).Scan(&paymentMethodType))
	require.Equal(t, "text", paymentMethodType)
	var attemptColumnExists bool
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT EXISTS (
		    SELECT 1 FROM information_schema.columns
		     WHERE table_schema='ms_billing'
		       AND table_name='credit_ledger'
		       AND column_name='attempt_payment_method_id'
		 )`,
	).Scan(&attemptColumnExists))
	require.False(t, attemptColumnExists)

	_, err = pool.Exec(ctx, readMigration049(t, "049_credit_auto_topup_attempts.up.sql"))
	require.NoError(t, err)
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT data_type
		   FROM information_schema.columns
		  WHERE table_schema='ms_billing'
		    AND table_name='credit_auto_topup_configs'
		    AND column_name='payment_method_id'`,
	).Scan(&paymentMethodType))
	require.Equal(t, "uuid", paymentMethodType)
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT EXISTS (
		    SELECT 1 FROM information_schema.columns
		     WHERE table_schema='ms_billing'
		       AND table_name='credit_ledger'
		       AND column_name='attempt_payment_method_id'
		 )`,
	).Scan(&attemptColumnExists))
	require.True(t, attemptColumnExists)
}

func TestMigration049_DownRefusesToDiscardPopulatedMoneyAudit(t *testing.T) {
	pool := testutil.NewTestDB(t)
	ctx := context.Background()
	accountID := seedAutoAccount(t, pool)
	paymentMethodID := seedAutoPaymentMethod(t, pool, accountID, false)
	attemptID := insertValidAutoTopUp(
		t,
		pool,
		accountID,
		paymentMethodID,
		"failed",
	)

	_, err := pool.Exec(ctx, readMigration049(t, "049_credit_auto_topup_attempts.down.sql"))

	require.ErrorContains(t, err, "rollback refused")
	var (
		status           string
		frozenStripePM   string
		frozenCustomerID string
		failureCode      string
	)
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT status, attempt_stripe_payment_method_id,
		        attempt_stripe_customer_id, failure_code
		   FROM ms_billing.credit_ledger
		  WHERE id=$1`,
		attemptID,
	).Scan(&status, &frozenStripePM, &frozenCustomerID, &failureCode))
	require.Equal(t, "failed", status)
	require.Equal(t, "pm_"+paymentMethodID.String(), frozenStripePM)
	require.Equal(t, "cus_"+accountID.String(), frozenCustomerID)
	require.Equal(t, "card_declined", failureCode)
}

func insertValidAutoTopUp(
	t *testing.T,
	pool *pgxpool.Pool,
	accountID, paymentMethodID uuid.UUID,
	status string,
) uuid.UUID {
	t.Helper()
	attemptID := uuid.New()
	createdAt := time.Date(2026, time.July, 25, 11, 0, 0, 0, time.UTC)
	var failureCode any
	if status == "failed" {
		failureCode = "card_declined"
	}
	_, err := pool.Exec(context.Background(),
		`INSERT INTO ms_billing.credit_ledger
		   (id, account_id, amount_micros, type, status, balance_after_micros,
		    actor, idempotency_key, attempt_payment_method_id,
		    attempt_stripe_payment_method_id, attempt_stripe_customer_id,
		    attempt_expires_at, failure_code, created_at)
		 VALUES ($1, $2, 5000000, 'auto_topup', $3, 0, 'system', $4, $5,
		         $6, $7, $8, $9, $10)`,
		attemptID,
		accountID,
		status,
		"auto:"+attemptID.String(),
		paymentMethodID,
		"pm_"+paymentMethodID.String(),
		"cus_"+accountID.String(),
		createdAt.Add(10*time.Minute),
		failureCode,
		createdAt,
	)
	require.NoError(t, err)
	return attemptID
}

func readMigration049(t *testing.T, name string) string {
	t.Helper()
	dir, err := os.Getwd()
	require.NoError(t, err)
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			break
		}
		parent := filepath.Dir(dir)
		require.NotEqual(t, dir, parent, "go.mod not found above %s", dir)
		dir = parent
	}
	body, err := os.ReadFile(filepath.Join(dir, "migrations", "billing", name))
	require.NoError(t, err)
	return string(body)
}
