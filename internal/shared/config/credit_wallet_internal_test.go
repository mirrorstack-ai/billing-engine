package config

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/require"
)

type fakeCreditWalletSchemaExecutor struct {
	errs            []error
	queries         []string
	baseContract    [3]bool
	baseErr         error
	runtimeContract creditAutoTopUpSchemaContract
	runtimeErr      error
}

func (f *fakeCreditWalletSchemaExecutor) Exec(_ context.Context, query string, _ ...any) (pgconn.CommandTag, error) {
	f.queries = append(f.queries, query)
	if len(f.errs) == 0 {
		return pgconn.CommandTag{}, nil
	}
	err := f.errs[0]
	f.errs = f.errs[1:]
	return pgconn.CommandTag{}, err
}

func (f *fakeCreditWalletSchemaExecutor) QueryRow(_ context.Context, query string, _ ...any) pgx.Row {
	f.queries = append(f.queries, query)
	if strings.Contains(query, "WITH schema_relations AS") {
		return fakeCreditRecoveryBaseRow{
			contract: f.baseContract,
			err:      f.baseErr,
		}
	}
	return fakeCreditRuntimeSchemaRow{
		contract: f.runtimeContract,
		err:      f.runtimeErr,
	}
}

type fakeCreditRecoveryBaseRow struct {
	contract [3]bool
	err      error
}

func (r fakeCreditRecoveryBaseRow) Scan(destinations ...any) error {
	if r.err != nil {
		return r.err
	}
	if len(destinations) != len(r.contract) {
		return fmt.Errorf(
			"scan destinations = %d, want %d",
			len(destinations),
			len(r.contract),
		)
	}
	for i, destination := range destinations {
		value, ok := destination.(*bool)
		if !ok {
			return fmt.Errorf(
				"scan destination %d has type %T, want *bool",
				i,
				destination,
			)
		}
		*value = r.contract[i]
	}
	return nil
}

type fakeCreditRuntimeSchemaRow struct {
	contract creditAutoTopUpSchemaContract
	err      error
}

func (r fakeCreditRuntimeSchemaRow) Scan(destinations ...any) error {
	if r.err != nil {
		return r.err
	}
	values := []bool{
		r.contract.configPaymentMethodColumn,
		r.contract.attemptPaymentMethodColumn,
		r.contract.attemptStripePMColumn,
		r.contract.attemptStripeCustomerColumn,
		r.contract.attemptExpiresColumn,
		r.contract.failureCodeColumn,
		r.contract.configPaymentMethodForeignKey,
		r.contract.attemptPaymentMethodForeignKey,
		r.contract.attemptFieldsCheck,
		r.contract.failureStateCheck,
		r.contract.attemptWindowCheck,
		r.contract.pendingAttemptUniqueIndex,
	}
	if len(destinations) != len(values) {
		return fmt.Errorf("scan destinations = %d, want %d", len(destinations), len(values))
	}
	for i, destination := range destinations {
		value, ok := destination.(*bool)
		if !ok {
			return fmt.Errorf("scan destination %d has type %T, want *bool", i, destination)
		}
		*value = values[i]
	}
	return nil
}

func completeCreditAutoTopUpSchemaContract() creditAutoTopUpSchemaContract {
	return creditAutoTopUpSchemaContract{
		configPaymentMethodColumn:      true,
		attemptPaymentMethodColumn:     true,
		attemptStripePMColumn:          true,
		attemptStripeCustomerColumn:    true,
		attemptExpiresColumn:           true,
		failureCodeColumn:              true,
		configPaymentMethodForeignKey:  true,
		attemptPaymentMethodForeignKey: true,
		attemptFieldsCheck:             true,
		failureStateCheck:              true,
		attemptWindowCheck:             true,
		pendingAttemptUniqueIndex:      true,
	}
}

func TestCreditWalletSchemaReadyClassifiesProbeErrors(t *testing.T) {
	boom := errors.New("database unavailable")
	tests := []struct {
		name      string
		errs      []error
		wantReady bool
		wantErr   error
		wantCalls int
	}{
		{name: "all objects readable", wantReady: true, wantCalls: 2},
		{name: "accounts table absent", errs: []error{&pgconn.PgError{Code: "42P01"}}, wantCalls: 1},
		{name: "billing_mode absent", errs: []error{&pgconn.PgError{Code: "42703"}}, wantCalls: 1},
		{name: "credit ledger absent", errs: []error{nil, &pgconn.PgError{Code: "42P01"}}, wantCalls: 2},
		{name: "first probe other error", errs: []error{boom}, wantErr: boom, wantCalls: 1},
		{name: "second probe undefined column is an error", errs: []error{nil, &pgconn.PgError{Code: "42703"}}, wantCalls: 2},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			fake := &fakeCreditWalletSchemaExecutor{errs: append([]error(nil), tc.errs...)}
			ready, err := creditWalletSchemaReady(context.Background(), fake)
			require.Equal(t, tc.wantReady, ready)
			if tc.wantErr != nil {
				require.ErrorIs(t, err, tc.wantErr)
			} else if tc.name == "second probe undefined column is an error" {
				require.Error(t, err)
				var pg *pgconn.PgError
				require.ErrorAs(t, err, &pg)
				require.Equal(t, "42703", pg.Code)
			} else {
				require.NoError(t, err)
			}
			require.Len(t, fake.queries, tc.wantCalls)
		})
	}
}

func TestCreditAutoTopUpSchemaReadyRequiresEveryCapability(t *testing.T) {
	previousLogger := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(io.Discard, nil)))
	t.Cleanup(func() {
		slog.SetDefault(previousLogger)
	})

	tests := []struct {
		name   string
		remove func(*creditAutoTopUpSchemaContract)
	}{
		{
			name: "config payment method nullable UUID column",
			remove: func(contract *creditAutoTopUpSchemaContract) {
				contract.configPaymentMethodColumn = false
			},
		},
		{
			name: "attempt payment method nullable UUID column",
			remove: func(contract *creditAutoTopUpSchemaContract) {
				contract.attemptPaymentMethodColumn = false
			},
		},
		{
			name: "attempt Stripe payment method nullable text column",
			remove: func(contract *creditAutoTopUpSchemaContract) {
				contract.attemptStripePMColumn = false
			},
		},
		{
			name: "attempt Stripe customer nullable text column",
			remove: func(contract *creditAutoTopUpSchemaContract) {
				contract.attemptStripeCustomerColumn = false
			},
		},
		{
			name: "attempt expiry nullable timestamptz column",
			remove: func(contract *creditAutoTopUpSchemaContract) {
				contract.attemptExpiresColumn = false
			},
		},
		{
			name: "failure code nullable text column",
			remove: func(contract *creditAutoTopUpSchemaContract) {
				contract.failureCodeColumn = false
			},
		},
		{
			name: "config payment method named validated foreign key",
			remove: func(contract *creditAutoTopUpSchemaContract) {
				contract.configPaymentMethodForeignKey = false
			},
		},
		{
			name: "attempt payment method named validated foreign key",
			remove: func(contract *creditAutoTopUpSchemaContract) {
				contract.attemptPaymentMethodForeignKey = false
			},
		},
		{
			name: "attempt fields named validated check",
			remove: func(contract *creditAutoTopUpSchemaContract) {
				contract.attemptFieldsCheck = false
			},
		},
		{
			name: "failure state named validated check",
			remove: func(contract *creditAutoTopUpSchemaContract) {
				contract.failureStateCheck = false
			},
		},
		{
			name: "attempt window named validated check",
			remove: func(contract *creditAutoTopUpSchemaContract) {
				contract.attemptWindowCheck = false
			},
		},
		{
			name: "valid unique pending attempt index",
			remove: func(contract *creditAutoTopUpSchemaContract) {
				contract.pendingAttemptUniqueIndex = false
			},
		},
	}

	full := completeCreditAutoTopUpSchemaContract()
	fake := &fakeCreditWalletSchemaExecutor{runtimeContract: full}
	ready, err := creditAutoTopUpSchemaReady(context.Background(), fake)
	require.NoError(t, err)
	require.True(t, ready)
	require.Len(t, fake.queries, 1)

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			contract := completeCreditAutoTopUpSchemaContract()
			tc.remove(&contract)
			fake := &fakeCreditWalletSchemaExecutor{runtimeContract: contract}
			ready, err := creditAutoTopUpSchemaReady(context.Background(), fake)
			require.NoError(t, err)
			require.False(t, ready)
			require.Len(t, fake.queries, 1)
		})
	}
}

func TestCreditAutoTopUpSchemaReadyPropagatesCatalogError(t *testing.T) {
	boom := errors.New("database unavailable")
	fake := &fakeCreditWalletSchemaExecutor{runtimeErr: boom}

	ready, err := creditAutoTopUpSchemaReady(context.Background(), fake)

	require.False(t, ready)
	require.ErrorIs(t, err, boom)
	require.Len(t, fake.queries, 1)
}

func TestCreditAutoTopUpSchemaReadyLogsEveryMissingCapability(t *testing.T) {
	var logs bytes.Buffer
	previousLogger := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&logs, nil)))
	t.Cleanup(func() {
		slog.SetDefault(previousLogger)
	})

	contract := completeCreditAutoTopUpSchemaContract()
	contract.configPaymentMethodColumn = false
	contract.attemptWindowCheck = false
	fake := &fakeCreditWalletSchemaExecutor{runtimeContract: contract}

	ready, err := creditAutoTopUpSchemaReady(context.Background(), fake)

	require.NoError(t, err)
	require.False(t, ready)
	require.Contains(t, logs.String(), "missing_or_incompatible_capabilities")
	require.Contains(
		t,
		logs.String(),
		"ms_billing.credit_auto_topup_configs.payment_method_id (nullable uuid column)",
	)
	require.Contains(
		t,
		logs.String(),
		"credit_ledger_auto_topup_attempt_window_check (validated check constraint)",
	)
	require.NotContains(
		t,
		logs.String(),
		"credit_ledger_auto_topup_pending_uidx (valid unique index)",
	)
}

func TestCreditAutoTopUpSchemaContractProbeUsesStableCatalogCapabilities(t *testing.T) {
	for _, fragment := range []string{
		"column_name = 'payment_method_id'",
		"column_name = 'attempt_payment_method_id'",
		"column_name = 'attempt_stripe_payment_method_id'",
		"column_name = 'attempt_stripe_customer_id'",
		"column_name = 'attempt_expires_at'",
		"column_name = 'failure_code'",
		"type_id = 'uuid'::regtype",
		"type_id = 'text'::regtype",
		"type_id = 'timestamptz'::regtype",
		"AND NOT not_null",
		"credit_auto_topup_configs_payment_method_fkey",
		"credit_ledger_attempt_payment_method_fkey",
		"credit_ledger_auto_topup_attempt_fields_check",
		"credit_ledger_auto_topup_failure_state_check",
		"credit_ledger_auto_topup_attempt_window_check",
		"credit_ledger_auto_topup_pending_uidx",
		"constraint_type = 'f'",
		"constraint_type = 'c'",
		"AND validated",
		"AND is_unique",
		"AND is_valid",
		"AND is_ready",
		"AND is_live",
	} {
		require.Truef(
			t,
			strings.Contains(creditAutoTopUpSchemaContractProbe, fragment),
			"probe missing migration-049 catalog capability %q",
			fragment,
		)
	}

	for _, unstableFragment := range []string{
		"pg_get_constraintdef",
		"pg_get_indexdef",
		"pg_get_expr",
		"key_column",
		"predicate =",
		"definition =",
	} {
		require.NotContains(t, creditAutoTopUpSchemaContractProbe, unstableFragment)
	}
}

func TestCreditRecoverySchemaReadyUsesCatalogOnlyAndRequiresBothMigrations(t *testing.T) {
	full := completeCreditAutoTopUpSchemaContract()
	fake := &fakeCreditWalletSchemaExecutor{
		baseContract:    [3]bool{true, true, true},
		runtimeContract: full,
	}

	ready, err := creditRecoverySchemaReady(context.Background(), fake)

	require.NoError(t, err)
	require.True(t, ready)
	require.Len(t, fake.queries, 2)
	for _, query := range fake.queries {
		normalized := strings.ToLower(query)
		require.Contains(t, normalized, "pg_catalog")
		require.NotContains(t, normalized, "from ms_billing.")
		require.NotContains(t, normalized, "join ms_billing.")
	}

	for index := range 3 {
		t.Run(fmt.Sprintf("missing base capability %d", index), func(t *testing.T) {
			base := [3]bool{true, true, true}
			base[index] = false
			fake := &fakeCreditWalletSchemaExecutor{
				baseContract:    base,
				runtimeContract: full,
			}
			ready, err := creditRecoverySchemaReady(
				context.Background(),
				fake,
			)
			require.NoError(t, err)
			require.False(t, ready)
			require.Len(t, fake.queries, 1)
			require.NotContains(
				t,
				strings.ToLower(fake.queries[0]),
				"from ms_billing.",
			)
		})
	}
}

func TestCreditRecoverySchemaReadyPropagatesCatalogErrors(t *testing.T) {
	boom := errors.New("catalog unavailable")
	fake := &fakeCreditWalletSchemaExecutor{baseErr: boom}

	ready, err := creditRecoverySchemaReady(context.Background(), fake)

	require.False(t, ready)
	require.ErrorIs(t, err, boom)
	require.Len(t, fake.queries, 1)
}
