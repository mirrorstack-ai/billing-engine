package einvoice

import (
	"context"
	"log/slog"

	"github.com/google/uuid"

	"github.com/mirrorstack-ai/billing-engine/internal/account/billing"
)

// Service implements the GetBuyerInvoiceProfile / PutBuyerInvoiceProfile RPCs
// (billing-engine#285). Both are platform CONTROL-PLANE calls — internal
// secret, api-platform proxies them (mirrorstack-ai/api-platform#1016). Pure
// data: Service never calls a vendor and never issues anything (#286, #287).
type Service struct {
	store Store
}

// NewService wires a Service. store is required.
func NewService(store Store) *Service {
	return &Service{store: store}
}

// GetProfile reads a billing account's buyer invoice profile. Exists=false
// with the documented default (KindPersonal, no carrier, no donation code) is
// a normal outcome when the account has never set one, not an error.
func (s *Service) GetProfile(ctx context.Context, req GetProfileRequest) (*GetProfileResponse, error) {
	if req.AccountID == uuid.Nil {
		return nil, billing.InvalidInput("account_id required")
	}
	p, found, err := s.store.GetProfile(ctx, req.AccountID)
	if err != nil {
		return nil, billing.Internal("get buyer invoice profile", err)
	}
	if !found {
		return &GetProfileResponse{Exists: false, Profile: Profile{AccountID: req.AccountID, Kind: KindPersonal}}, nil
	}
	return &GetProfileResponse{Exists: true, Profile: p}, nil
}

// PutProfile validates and replaces a billing account's buyer invoice
// profile in full. Validate mirrors migration 093's CHECK constraints so a
// bad profile is refused here with a readable message, not just a database
// error.
//
// The log line carries ONLY the account id and the outcome shape (kind,
// whether a carrier or donation code is set) — never tax_id, company_name,
// address, email, carrier_num or donation_code. Those are personal/business
// data (D526) and belong in the database, not CloudWatch.
func (s *Service) PutProfile(ctx context.Context, req PutProfileRequest) (*PutProfileResponse, error) {
	if req.AccountID == uuid.Nil {
		return nil, billing.InvalidInput("account_id required")
	}
	p := Profile{
		AccountID:    req.AccountID,
		Kind:         req.Kind,
		TaxID:        req.TaxID,
		CompanyName:  req.CompanyName,
		Address:      req.Address,
		Email:        req.Email,
		CarrierType:  req.CarrierType,
		CarrierNum:   req.CarrierNum,
		DonationCode: req.DonationCode,
	}
	if err := Validate(p); err != nil {
		return nil, err
	}
	stored, err := s.store.PutProfile(ctx, p)
	if err != nil {
		return nil, billing.Internal("put buyer invoice profile", err)
	}
	slog.InfoContext(ctx, "buyer invoice profile updated",
		"account_id", stored.AccountID,
		"kind", stored.Kind,
		"has_carrier", stored.CarrierType != "",
		"has_donation_code", stored.DonationCode != "",
	)
	return &PutProfileResponse{Profile: stored}, nil
}
