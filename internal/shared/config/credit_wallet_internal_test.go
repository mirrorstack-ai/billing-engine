package config

import (
	"context"
	"errors"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/require"
)

type fakeCreditWalletSchemaExecutor struct {
	errs    []error
	queries []string
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
