package invoice

import (
	"context"
	"fmt"

	"github.com/google/uuid"
)

// PaymentFactOrigin is where a payment fact came from — the input
// ResolveSeller uses to decide which seller a charge belongs to. A caller
// never names the seller directly (fix direction #4): it names the origin
// and the org id it already resolved, and this package derives the seller
// from that.
type PaymentFactOrigin string

const (
	// OriginDistributorSale: a distributor selling under its own 統編.
	// Resolves to that distributor's seller row.
	OriginDistributorSale PaymentFactOrigin = "distributor_sale"
	// OriginPlatformFee: a MirrorStack platform fee. Resolves to the
	// first-party Taiwan org (mirrorstack-tw).
	OriginPlatformFee PaymentFactOrigin = "platform_fee"
	// OriginAppSale: a customer-app sale, resolving to the app owner org.
	// Not implemented yet (fix direction #4: "later") — ResolveSeller
	// returns ErrSellerResolutionNotImplemented for it.
	OriginAppSale PaymentFactOrigin = "app_sale"
	// OriginUSEntityCharge: a charge by the US C-corp. Resolves to
	// FactNotApplicable — no Taiwan seller applies.
	OriginUSEntityCharge PaymentFactOrigin = "us_entity_charge"
)

// PaymentFact is the minimal input ResolveSeller needs. It stands in for
// billing-engine#313's payment-fact intake, which does not exist in this
// tree yet; this issue must work without it landing first, so the shape here
// is deliberately small — Origin plus the org id api-platform already
// resolved, nothing else.
type PaymentFact struct {
	Origin PaymentFactOrigin
	// OrgID is the org this fact's charge belongs to, as api-platform
	// resolved it. Ignored for OriginUSEntityCharge, which resolves to no
	// seller regardless of OrgID.
	OrgID uuid.UUID
}

// ResolveSeller derives a seller from a payment fact's origin, never from a
// caller-supplied seller id. Returns the resolved seller plus its current
// FactStatus, or (zero Seller, FactNotApplicable, nil) when no Taiwan
// invoice applies at all.
func (s *Service) ResolveSeller(ctx context.Context, fact PaymentFact) (Seller, FactStatus, error) {
	switch fact.Origin {
	case OriginUSEntityCharge:
		return Seller{}, FactNotApplicable, nil

	case OriginPlatformFee:
		return s.resolveByKindOrg(ctx, SellerKindPlatformCompany, fact.OrgID)

	case OriginDistributorSale:
		return s.resolveByKindOrg(ctx, SellerKindDistributorOrg, fact.OrgID)

	case OriginAppSale:
		return Seller{}, "", ErrSellerResolutionNotImplemented

	default:
		return Seller{}, "", fmt.Errorf("invoice: unknown payment fact origin %q", fact.Origin)
	}
}

func (s *Service) resolveByKindOrg(ctx context.Context, kind SellerKind, orgID uuid.UUID) (Seller, FactStatus, error) {
	seller, found, err := s.store.GetSellerByKindOrg(ctx, kind, orgID)
	if err != nil {
		return Seller{}, "", err
	}
	if !found {
		return Seller{}, "", ErrSellerNotFound
	}
	account, hasAccount, err := s.store.GetLatestProviderAccount(ctx, seller.ID)
	if err != nil {
		return Seller{}, "", err
	}
	hasActiveAccount := hasAccount && account.Status == KeyStatusActive
	return seller, seller.FactStatus(account, hasActiveAccount), nil
}
