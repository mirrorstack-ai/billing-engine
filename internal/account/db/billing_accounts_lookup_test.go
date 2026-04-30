package db

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// fakeRow drives Scan in unit tests. cols is the slice of values that
// will be assigned in order to the destination pointers.
type fakeRow struct {
	err  error
	cols []any
}

func (r *fakeRow) Scan(dest ...any) error {
	if r.err != nil {
		return r.err
	}
	if len(dest) != len(r.cols) {
		return errors.New("fakeRow: dest length mismatch")
	}
	for i, d := range dest {
		switch p := d.(type) {
		case *uuid.UUID:
			*p = r.cols[i].(uuid.UUID)
		case *string:
			*p = r.cols[i].(string)
		default:
			return errors.New("fakeRow: unsupported dest type")
		}
	}
	return nil
}

type fakeQuerier struct {
	row *fakeRow
}

func (f *fakeQuerier) QueryRow(_ context.Context, _ string, _ ...any) pgx.Row {
	return f.row
}

func (f *fakeQuerier) Exec(_ context.Context, _ string, _ ...any) (pgconn.CommandTag, error) {
	return pgconn.CommandTag{}, nil
}

func TestGetBillingAccountByID_Found(t *testing.T) {
	id := uuid.New()
	q := &fakeQuerier{row: &fakeRow{cols: []any{id, "cus_test"}}}
	got, err := NewBillingAccountLookupQueries(q).GetBillingAccountByID(context.Background(), id)
	if err != nil {
		t.Fatalf("err = %v, want nil", err)
	}
	if got.ID != id || got.StripeCustomerID != "cus_test" {
		t.Fatalf("got = %+v", got)
	}
}

func TestGetBillingAccountByID_NotFound(t *testing.T) {
	q := &fakeQuerier{row: &fakeRow{err: pgx.ErrNoRows}}
	_, err := NewBillingAccountLookupQueries(q).GetBillingAccountByID(context.Background(), uuid.New())
	if !errors.Is(err, ErrBillingAccountNotFound) {
		t.Fatalf("err = %v, want ErrBillingAccountNotFound", err)
	}
}

func TestGetBillingAccountByID_DBError(t *testing.T) {
	want := errors.New("connection refused")
	q := &fakeQuerier{row: &fakeRow{err: want}}
	_, err := NewBillingAccountLookupQueries(q).GetBillingAccountByID(context.Background(), uuid.New())
	if err == nil || errors.Is(err, ErrBillingAccountNotFound) {
		t.Fatalf("err = %v, want wrapped non-not-found error", err)
	}
}
