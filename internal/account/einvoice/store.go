package einvoice

import (
	"context"
	"errors"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/mirrorstack-ai/billing-engine/internal/account/db"
)

// Store is the persistence interface Service depends on.
type Store interface {
	// GetProfile resolves the stored profile, or (zero, false) when the
	// account has never set one — a normal "no profile yet" outcome, not an
	// error.
	GetProfile(ctx context.Context, accountID uuid.UUID) (Profile, bool, error)

	// PutProfile upserts the whole profile, keyed on account_id, and returns
	// the row as persisted.
	PutProfile(ctx context.Context, p Profile) (Profile, error)
}

// NewStore returns a Store backed by the given pgxpool.
func NewStore(pool *pgxpool.Pool) Store {
	return &pgxStore{q: db.New(pool)}
}

type pgxStore struct {
	q *db.Queries
}

func (s *pgxStore) GetProfile(ctx context.Context, accountID uuid.UUID) (Profile, bool, error) {
	row, err := s.q.GetBuyerInvoiceProfile(ctx, accountID.String())
	if errors.Is(err, pgx.ErrNoRows) {
		return Profile{}, false, nil
	}
	if err != nil {
		return Profile{}, false, err
	}
	p, err := profileFromRow(row)
	if err != nil {
		return Profile{}, false, err
	}
	return p, true, nil
}

func (s *pgxStore) PutProfile(ctx context.Context, p Profile) (Profile, error) {
	row, err := s.q.PutBuyerInvoiceProfile(ctx, db.PutBuyerInvoiceProfileParams{
		AccountID:    p.AccountID.String(),
		Kind:         string(p.Kind),
		TaxID:        textOrNull(p.TaxID),
		CompanyName:  textOrNull(p.CompanyName),
		Address:      textOrNull(p.Address),
		Email:        textOrNull(p.Email),
		CarrierType:  textOrNull(string(p.CarrierType)),
		CarrierNum:   textOrNull(p.CarrierNum),
		DonationCode: textOrNull(p.DonationCode),
	})
	if err != nil {
		return Profile{}, err
	}
	return profileFromRow(row)
}

func profileFromRow(row db.MsBillingBuyerInvoiceProfile) (Profile, error) {
	accountID, err := uuid.Parse(row.AccountID)
	if err != nil {
		return Profile{}, err
	}
	return Profile{
		AccountID:    accountID,
		Kind:         Kind(row.Kind),
		TaxID:        row.TaxID.String,
		CompanyName:  row.CompanyName.String,
		Address:      row.Address.String,
		Email:        row.Email.String,
		CarrierType:  CarrierType(row.CarrierType.String),
		CarrierNum:   row.CarrierNum.String,
		DonationCode: row.DonationCode.String,
	}, nil
}

func textOrNull(v string) pgtype.Text {
	return pgtype.Text{String: v, Valid: v != ""}
}
