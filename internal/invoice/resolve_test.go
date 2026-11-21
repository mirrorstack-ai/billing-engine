package invoice

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"
)

func TestResolveSeller_USEntityChargeIsNotApplicable(t *testing.T) {
	svc, _, _ := newTestService()
	_, status, err := svc.ResolveSeller(context.Background(), PaymentFact{Origin: OriginUSEntityCharge, OrgID: uuid.New()})
	if err != nil {
		t.Fatalf("ResolveSeller: %v", err)
	}
	if status != FactNotApplicable {
		t.Errorf("status = %v, want not_applicable", status)
	}
}

func TestResolveSeller_DistributorSale(t *testing.T) {
	svc, _, _ := newTestService()
	ctx := context.Background()
	orgID := uuid.New()
	seller := mustPutDistributor(t, svc, orgID, ModeSandbox)

	resolved, status, err := svc.ResolveSeller(ctx, PaymentFact{Origin: OriginDistributorSale, OrgID: orgID})
	if err != nil {
		t.Fatalf("ResolveSeller: %v", err)
	}
	if resolved.ID != seller.ID {
		t.Errorf("resolved seller %v, want %v", resolved.ID, seller.ID)
	}
	// Registered, tax_invoice, but no provider account configured yet.
	if status != FactDeferred {
		t.Errorf("status = %v, want deferred", status)
	}
}

func TestResolveSeller_PlatformFeeUsesPlatformCompanyKind(t *testing.T) {
	svc, _, _ := newTestService()
	ctx := context.Background()
	orgID := uuid.New()
	seller, err := svc.PutInvoiceSellerIdentity(ctx, Actor{ID: "op", Kind: ActorKindOperator}, "mirrorstack-tw setup",
		SellerIdentityInput{
			Kind: SellerKindPlatformCompany, OrgID: orgID, LegalName: "MirrorStack TW",
			RegistrationStatus: RegistrationUnregistered, DocumentKind: DocumentKindTaxInvoice,
			Payer: PayerPlatform, Mode: ModeSandbox,
		})
	if err != nil {
		t.Fatalf("PutInvoiceSellerIdentity: %v", err)
	}

	resolved, status, err := svc.ResolveSeller(ctx, PaymentFact{Origin: OriginPlatformFee, OrgID: orgID})
	if err != nil {
		t.Fatalf("ResolveSeller: %v", err)
	}
	if resolved.ID != seller.ID {
		t.Errorf("resolved seller %v, want %v", resolved.ID, seller.ID)
	}
	if status != FactDeferred {
		t.Errorf("status = %v, want deferred (mirrorstack-tw stays unregistered per D504)", status)
	}
}

func TestResolveSeller_AppSaleNotImplemented(t *testing.T) {
	svc, _, _ := newTestService()
	_, _, err := svc.ResolveSeller(context.Background(), PaymentFact{Origin: OriginAppSale, OrgID: uuid.New()})
	if !errors.Is(err, ErrSellerResolutionNotImplemented) {
		t.Fatalf("got %v, want ErrSellerResolutionNotImplemented", err)
	}
}

func TestResolveSeller_UnresolvedDistributorIsNotFound(t *testing.T) {
	svc, _, _ := newTestService()
	_, _, err := svc.ResolveSeller(context.Background(), PaymentFact{Origin: OriginDistributorSale, OrgID: uuid.New()})
	if !errors.Is(err, ErrSellerNotFound) {
		t.Fatalf("got %v, want ErrSellerNotFound", err)
	}
}
