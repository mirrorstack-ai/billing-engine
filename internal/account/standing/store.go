package standing

import (
	"context"
	"errors"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/mirrorstack-ai/billing-engine/internal/account/db"
)

// NewStore returns the OwnerResolver backed by the generated standing
// queries (internal/account/db/queries/standing.sql).
func NewStore(pool *pgxpool.Pool) OwnerResolver {
	return &pgxStore{q: db.New(pool)}
}

type pgxStore struct {
	q *db.Queries
}

func (s *pgxStore) OwnerByStripeCustomer(ctx context.Context, stripeCustomerID string) (Owner, bool, error) {
	row, err := s.q.AccountOwnerByStripeCustomer(ctx, pgtype.Text{String: stripeCustomerID, Valid: true})
	return ownerRowFound(row.OwnerUserID, row.OwnerOrgID, err)
}

func (s *pgxStore) OwnerByStripeInvoice(ctx context.Context, stripeInvoiceID string) (Owner, bool, error) {
	row, err := s.q.AccountOwnerByStripeInvoice(ctx, stripeInvoiceID)
	return ownerRowFound(row.OwnerUserID, row.OwnerOrgID, err)
}

func (s *pgxStore) OwnerByStripePaymentMethod(ctx context.Context, stripePaymentMethodID string) (Owner, bool, error) {
	row, err := s.q.AccountOwnerByStripePaymentMethod(ctx, stripePaymentMethodID)
	return ownerRowFound(row.OwnerUserID, row.OwnerOrgID, err)
}

// ownerRowFound decodes the shared (owner_user_id, owner_org_id, error) row
// shape: ErrNoRows → not found (drift, a normal skip), else the one non-NULL
// owner column (the accounts polymorphic-owner CHECK guarantees exactly one).
func ownerRowFound(userID, orgID pgtype.UUID, err error) (Owner, bool, error) {
	if errors.Is(err, pgx.ErrNoRows) {
		return Owner{}, false, nil
	}
	if err != nil {
		return Owner{}, false, err
	}
	var o Owner
	if userID.Valid {
		o.UserID = uuid.UUID(userID.Bytes)
	}
	if orgID.Valid {
		o.OrgID = uuid.UUID(orgID.Bytes)
	}
	return o, true, nil
}
