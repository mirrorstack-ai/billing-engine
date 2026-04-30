package db

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// fakeRow lets us drive Scan deterministically from a test.
type fakeRow struct {
	scan func(dst ...any) error
}

func (r fakeRow) Scan(dst ...any) error { return r.scan(dst...) }

type fakeDB struct {
	queryRow func(ctx context.Context, sql string, args ...any) pgx.Row
	exec     func(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
}

func (f *fakeDB) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	return f.queryRow(ctx, sql, args...)
}

func (f *fakeDB) Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	return f.exec(ctx, sql, args...)
}

func TestGetByOwner_NotFound_ReturnsSentinel(t *testing.T) {
	db := &fakeDB{
		queryRow: func(_ context.Context, _ string, _ ...any) pgx.Row {
			return fakeRow{scan: func(_ ...any) error { return pgx.ErrNoRows }}
		},
	}
	b := NewBillingAccounts(db)

	_, err := b.GetByOwner(context.Background(), "user", uuid.New())
	if !errors.Is(err, ErrBillingAccountNotFound) {
		t.Fatalf("expected ErrBillingAccountNotFound; got %v", err)
	}
}

func TestGetByOwner_Found_PopulatesStruct(t *testing.T) {
	wantID := uuid.New()
	wantOwner := uuid.New()
	db := &fakeDB{
		queryRow: func(_ context.Context, _ string, _ ...any) pgx.Row {
			return fakeRow{scan: func(dst ...any) error {
				*(dst[0].(*uuid.UUID)) = wantID
				*(dst[1].(*string)) = "user"
				*(dst[2].(*uuid.UUID)) = wantOwner
				*(dst[3].(*string)) = "cus_123"
				*(dst[4].(*string)) = "USD"
				return nil
			}}
		},
	}
	b := NewBillingAccounts(db)

	got, err := b.GetByOwner(context.Background(), "user", wantOwner)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if got.ID != wantID || got.OwnerID != wantOwner || got.StripeCustomerID != "cus_123" || got.Currency != "USD" {
		t.Fatalf("unexpected row: %+v", got)
	}
}

func TestGetByOwner_OtherError_Wrapped(t *testing.T) {
	boom := errors.New("boom")
	db := &fakeDB{
		queryRow: func(_ context.Context, _ string, _ ...any) pgx.Row {
			return fakeRow{scan: func(_ ...any) error { return boom }}
		},
	}
	b := NewBillingAccounts(db)

	_, err := b.GetByOwner(context.Background(), "user", uuid.New())
	if err == nil || errors.Is(err, ErrBillingAccountNotFound) {
		t.Fatalf("expected wrapped error, got %v", err)
	}
	if !errors.Is(err, boom) {
		t.Fatalf("expected to wrap underlying error; got %v", err)
	}
}

func TestInsert_ReturnsGeneratedID(t *testing.T) {
	wantID := uuid.New()
	db := &fakeDB{
		queryRow: func(_ context.Context, _ string, _ ...any) pgx.Row {
			return fakeRow{scan: func(dst ...any) error {
				*(dst[0].(*uuid.UUID)) = wantID
				return nil
			}}
		},
	}
	b := NewBillingAccounts(db)

	got, err := b.Insert(context.Background(), "org", uuid.New(), "cus_999", "EUR")
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if got != wantID {
		t.Fatalf("expected %s, got %s", wantID, got)
	}
}

func TestInsert_ScanError_Wrapped(t *testing.T) {
	boom := errors.New("conflict")
	db := &fakeDB{
		queryRow: func(_ context.Context, _ string, _ ...any) pgx.Row {
			return fakeRow{scan: func(_ ...any) error { return boom }}
		},
	}
	b := NewBillingAccounts(db)

	_, err := b.Insert(context.Background(), "user", uuid.New(), "cus_999", "USD")
	if err == nil || !errors.Is(err, boom) {
		t.Fatalf("expected to wrap underlying error; got %v", err)
	}
}
