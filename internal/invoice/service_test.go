package invoice

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/google/uuid"
)

func newTestService() (*Service, *memStore, *FakeBroker) {
	store := newMemStore()
	broker := NewFakeBroker()
	return NewService(store, broker), store, broker
}

func mustPutDistributor(t *testing.T, svc *Service, orgID uuid.UUID, mode Mode) Seller {
	t.Helper()
	seller, err := svc.PutInvoiceSellerIdentity(context.Background(),
		Actor{ID: "owner-1", Kind: ActorKindOrgOwner}, "initial setup",
		SellerIdentityInput{
			Kind:               SellerKindDistributorOrg,
			OrgID:              orgID,
			LegalName:          "Test Distributor Co",
			TaxID:              "53212539",
			RegistrationStatus: RegistrationRegistered,
			DocumentKind:       DocumentKindTaxInvoice,
			Payer:              PayerPlatform,
			Mode:               mode,
		})
	if err != nil {
		t.Fatalf("PutInvoiceSellerIdentity: %v", err)
	}
	return seller
}

func TestPutInvoiceSellerIdentity_ActorKindEnforced(t *testing.T) {
	svc, _, _ := newTestService()
	ctx := context.Background()

	// Platform company requires an operator actor.
	_, err := svc.PutInvoiceSellerIdentity(ctx, Actor{ID: "u1", Kind: ActorKindOrgOwner}, "setup",
		SellerIdentityInput{
			Kind: SellerKindPlatformCompany, OrgID: uuid.New(), LegalName: "MirrorStack TW",
			RegistrationStatus: RegistrationUnregistered, DocumentKind: DocumentKindTaxInvoice,
			Payer: PayerPlatform, Mode: ModeSandbox,
		})
	if err == nil {
		t.Fatal("expected an error when an org_owner actor sets platform_company identity")
	}

	// Distributor requires an org_owner actor.
	_, err = svc.PutInvoiceSellerIdentity(ctx, Actor{ID: "op1", Kind: ActorKindOperator}, "setup",
		SellerIdentityInput{
			Kind: SellerKindDistributorOrg, OrgID: uuid.New(), LegalName: "Distributor",
			RegistrationStatus: RegistrationUnregistered, DocumentKind: DocumentKindTaxInvoice,
			Payer: PayerPlatform, Mode: ModeSandbox,
		})
	if err == nil {
		t.Fatal("expected an error when an operator actor sets distributor_org identity")
	}
}

func TestPutInvoiceSellerIdentity_TaxIDValidation(t *testing.T) {
	svc, _, _ := newTestService()
	ctx := context.Background()
	actor := Actor{ID: "owner-1", Kind: ActorKindOrgOwner}

	_, err := svc.PutInvoiceSellerIdentity(ctx, actor, "setup", SellerIdentityInput{
		Kind: SellerKindDistributorOrg, OrgID: uuid.New(), LegalName: "D1",
		RegistrationStatus: RegistrationRegistered, DocumentKind: DocumentKindTaxInvoice,
		Payer: PayerPlatform, Mode: ModeSandbox,
	})
	if !errors.Is(err, ErrRegisteredRequiresTaxID) {
		t.Fatalf("got %v, want ErrRegisteredRequiresTaxID", err)
	}

	_, err = svc.PutInvoiceSellerIdentity(ctx, actor, "setup", SellerIdentityInput{
		Kind: SellerKindDistributorOrg, OrgID: uuid.New(), LegalName: "D2", TaxID: "53212530",
		RegistrationStatus: RegistrationRegistered, DocumentKind: DocumentKindTaxInvoice,
		Payer: PayerPlatform, Mode: ModeSandbox,
	})
	if !errors.Is(err, ErrInvalidTaxID) {
		t.Fatalf("got %v, want ErrInvalidTaxID", err)
	}
}

// Acceptance: "A seller without keys reads unset and its facts end deferred."
func TestGetInvoiceSellerSettings_UnsetReadsDeferred(t *testing.T) {
	svc, _, _ := newTestService()
	ctx := context.Background()
	seller := mustPutDistributor(t, svc, uuid.New(), ModeSandbox)

	settings, err := svc.GetInvoiceSellerSettings(ctx, seller.ID)
	if err != nil {
		t.Fatalf("GetInvoiceSellerSettings: %v", err)
	}
	if settings.Status != KeyStatusUnset {
		t.Errorf("Status = %v, want unset", settings.Status)
	}
	if settings.Configured {
		t.Errorf("Configured = true, want false")
	}
	if settings.FactStatus != FactDeferred {
		t.Errorf("FactStatus = %v, want deferred", settings.FactStatus)
	}
}

// Acceptance: "document_kind receipt or none resolves not_applicable."
func TestFactStatus_ReceiptOrNoneIsNotApplicable(t *testing.T) {
	for _, dk := range []DocumentKind{DocumentKindReceipt, DocumentKindNone} {
		svc, _, _ := newTestService()
		ctx := context.Background()
		seller, err := svc.PutInvoiceSellerIdentity(ctx, Actor{ID: "u", Kind: ActorKindOrgOwner}, "setup",
			SellerIdentityInput{
				Kind: SellerKindDistributorOrg, OrgID: uuid.New(), LegalName: "D",
				TaxID: "53212539", RegistrationStatus: RegistrationRegistered,
				DocumentKind: dk, Payer: PayerPlatform, Mode: ModeSandbox,
			})
		if err != nil {
			t.Fatalf("PutInvoiceSellerIdentity: %v", err)
		}
		settings, err := svc.GetInvoiceSellerSettings(ctx, seller.ID)
		if err != nil {
			t.Fatalf("GetInvoiceSellerSettings: %v", err)
		}
		if settings.FactStatus != FactNotApplicable {
			t.Errorf("document_kind=%v: FactStatus = %v, want not_applicable", dk, settings.FactStatus)
		}
	}
}

// Acceptance: "An unregistered seller resolves deferred."
func TestFactStatus_UnregisteredIsDeferred(t *testing.T) {
	svc, _, _ := newTestService()
	ctx := context.Background()
	seller, err := svc.PutInvoiceSellerIdentity(ctx, Actor{ID: "op", Kind: ActorKindOperator}, "mirrorstack-tw setup",
		SellerIdentityInput{
			Kind: SellerKindPlatformCompany, OrgID: uuid.New(), LegalName: "MirrorStack TW",
			RegistrationStatus: RegistrationUnregistered, DocumentKind: DocumentKindTaxInvoice,
			Payer: PayerPlatform, Mode: ModeSandbox,
		})
	if err != nil {
		t.Fatalf("PutInvoiceSellerIdentity: %v", err)
	}
	settings, err := svc.GetInvoiceSellerSettings(ctx, seller.ID)
	if err != nil {
		t.Fatalf("GetInvoiceSellerSettings: %v", err)
	}
	if settings.FactStatus != FactDeferred {
		t.Errorf("FactStatus = %v, want deferred", settings.FactStatus)
	}
}

func canaryPayload() SealedPayload {
	var buf [12]byte
	_, _ = rand.Read(buf[:])
	return SealedPayload([]byte(`{"apiKey":"canary-` + hex.EncodeToString(buf[:]) + `"}`))
}

func TestPutInvoiceProviderKeys_HappyPathAndNoKeyLeak(t *testing.T) {
	svc, store, broker := newTestService()
	ctx := context.Background()
	seller := mustPutDistributor(t, svc, uuid.New(), ModeSandbox)
	payload := canaryPayload()

	account, err := svc.PutInvoiceProviderKeys(ctx, Actor{ID: "owner-1", Kind: ActorKindOrgOwner}, "initial key setup",
		ProviderKeysInput{
			SellerID: seller.ID, Provider: ProviderSystemlead, CredentialMode: CredentialModeSellerKeys,
			Environment: EnvironmentStage, Payload: payload,
		})
	if err != nil {
		t.Fatalf("PutInvoiceProviderKeys: %v", err)
	}
	if account.Status != KeyStatusActive {
		t.Errorf("Status = %v, want active", account.Status)
	}
	if account.SecretRef == "" {
		t.Error("SecretRef is empty")
	}

	// The canary must appear nowhere outside the broker's own sealed store.
	dump := fmt.Sprintf("%#v", account)
	if strings.Contains(dump, "canary-") {
		t.Fatalf("provider account leaks the sealed payload: %s", dump)
	}
	for _, entry := range store.audit {
		if strings.Contains(fmt.Sprintf("%#v", entry), "canary-") {
			t.Fatalf("audit entry leaks the sealed payload: %#v", entry)
		}
	}
	for _, a := range store.accounts {
		if strings.Contains(a.SecretRef, "canary-") {
			t.Fatalf("stored secret_ref leaks the sealed payload: %s", a.SecretRef)
		}
	}
	_ = broker // sealed internally; not inspected here by design
}

func TestPutInvoiceProviderKeys_OperatorCannotEnterKeys(t *testing.T) {
	svc, _, _ := newTestService()
	ctx := context.Background()
	seller := mustPutDistributor(t, svc, uuid.New(), ModeSandbox)

	_, err := svc.PutInvoiceProviderKeys(ctx, Actor{ID: "op1", Kind: ActorKindOperator}, "trying to set keys",
		ProviderKeysInput{
			SellerID: seller.ID, Provider: ProviderSystemlead, CredentialMode: CredentialModeSellerKeys,
			Environment: EnvironmentStage, Payload: canaryPayload(),
		})
	if !errors.Is(err, ErrOperatorCannotEnterKeys) {
		t.Fatalf("got %v, want ErrOperatorCannotEnterKeys", err)
	}
}

// Acceptance: "A seller cannot have two active provider accounts."
func TestPutInvoiceProviderKeys_DuplicateActiveProviderRefused(t *testing.T) {
	svc, _, _ := newTestService()
	ctx := context.Background()
	seller := mustPutDistributor(t, svc, uuid.New(), ModeSandbox)
	ownerActor := Actor{ID: "owner-1", Kind: ActorKindOrgOwner}

	_, err := svc.PutInvoiceProviderKeys(ctx, ownerActor, "first provider",
		ProviderKeysInput{SellerID: seller.ID, Provider: ProviderSystemlead, CredentialMode: CredentialModeSellerKeys,
			Environment: EnvironmentStage, Payload: canaryPayload()})
	if err != nil {
		t.Fatalf("first PutInvoiceProviderKeys: %v", err)
	}

	_, err = svc.PutInvoiceProviderKeys(ctx, ownerActor, "switching provider",
		ProviderKeysInput{SellerID: seller.ID, Provider: ProviderECPay, CredentialMode: CredentialModeSellerKeys,
			Environment: EnvironmentStage, Payload: canaryPayload()})
	if !errors.Is(err, ErrDuplicateActiveProvider) {
		t.Fatalf("got %v, want ErrDuplicateActiveProvider", err)
	}

	// Rotating the SAME provider's key is not a duplicate — it is a rotation.
	rotated, err := svc.PutInvoiceProviderKeys(ctx, ownerActor, "key rotation",
		ProviderKeysInput{SellerID: seller.ID, Provider: ProviderSystemlead, CredentialMode: CredentialModeSellerKeys,
			Environment: EnvironmentStage, Payload: canaryPayload()})
	if err != nil {
		t.Fatalf("rotation PutInvoiceProviderKeys: %v", err)
	}
	if rotated.KeyVersion != 1 {
		// FakeBroker always seals fresh at version 1; a real broker would
		// bump it. The point here is that rotation succeeds rather than
		// erroring as a duplicate.
		t.Logf("rotated.KeyVersion = %d", rotated.KeyVersion)
	}
}

// Acceptance: "a revoked seller refuses to issue with a typed error."
func TestRevokeInvoiceProviderKeys_RefusesIssueAfterRevoke(t *testing.T) {
	svc, store, _ := newTestService()
	ctx := context.Background()
	seller := mustPutDistributor(t, svc, uuid.New(), ModeSandbox)
	ownerActor := Actor{ID: "owner-1", Kind: ActorKindOrgOwner}

	account, err := svc.PutInvoiceProviderKeys(ctx, ownerActor, "setup",
		ProviderKeysInput{SellerID: seller.ID, Provider: ProviderSystemlead, CredentialMode: CredentialModeSellerKeys,
			Environment: EnvironmentStage, Payload: canaryPayload()})
	if err != nil {
		t.Fatalf("PutInvoiceProviderKeys: %v", err)
	}

	revoked, err := svc.RevokeInvoiceProviderKeys(ctx, Actor{ID: "op1", Kind: ActorKindOperator}, "compromise suspected", seller.ID)
	if err != nil {
		t.Fatalf("RevokeInvoiceProviderKeys: %v", err)
	}
	if revoked.Status != KeyStatusRevoked {
		t.Fatalf("Status = %v, want revoked", revoked.Status)
	}
	if revoked.ID != account.ID {
		t.Fatalf("revoked a different account than the one created")
	}

	settings, err := svc.GetInvoiceSellerSettings(ctx, seller.ID)
	if err != nil {
		t.Fatalf("GetInvoiceSellerSettings: %v", err)
	}
	if settings.Status != KeyStatusRevoked {
		t.Errorf("settings.Status = %v, want revoked", settings.Status)
	}
	if settings.Configured {
		t.Errorf("Configured = true after revoke, want false")
	}

	latest, hasLatest, err := store.GetLatestProviderAccount(ctx, seller.ID)
	if err != nil {
		t.Fatalf("GetLatestProviderAccount: %v", err)
	}
	if err := EnsureCanIssue(latest, hasLatest); !errors.Is(err, ErrSellerRevoked) {
		t.Fatalf("EnsureCanIssue after revoke = %v, want ErrSellerRevoked", err)
	}

	// Revoking again with nothing active is a typed "not configured" error.
	if _, err := svc.RevokeInvoiceProviderKeys(ctx, ownerActor, "already gone", seller.ID); !errors.Is(err, ErrNotConfigured) {
		t.Fatalf("second revoke: got %v, want ErrNotConfigured", err)
	}
}

func TestEnsureCanIssue_NeverConfigured(t *testing.T) {
	if err := EnsureCanIssue(ProviderAccount{}, false); !errors.Is(err, ErrNotConfigured) {
		t.Fatalf("got %v, want ErrNotConfigured", err)
	}
}

func TestVerifyInvoiceCredentials_SuccessAndRateLimit(t *testing.T) {
	svc, _, _ := newTestService()
	ctx := context.Background()
	seller := mustPutDistributor(t, svc, uuid.New(), ModeSandbox)
	ownerActor := Actor{ID: "owner-1", Kind: ActorKindOrgOwner}

	if _, err := svc.PutInvoiceProviderKeys(ctx, ownerActor, "setup",
		ProviderKeysInput{SellerID: seller.ID, Provider: ProviderSystemlead, CredentialMode: CredentialModeSellerKeys,
			Environment: EnvironmentStage, Payload: canaryPayload()}); err != nil {
		t.Fatalf("PutInvoiceProviderKeys: %v", err)
	}

	for i := 0; i < DefaultVerifyRateLimit; i++ {
		result, err := svc.VerifyInvoiceCredentials(ctx, ownerActor, "routine check", seller.ID)
		if err != nil {
			t.Fatalf("VerifyInvoiceCredentials attempt %d: %v", i, err)
		}
		if result != VerifyResultSuccess {
			t.Fatalf("attempt %d: result = %v, want success", i, result)
		}
	}

	if _, err := svc.VerifyInvoiceCredentials(ctx, ownerActor, "one too many", seller.ID); !errors.Is(err, ErrVerifyRateLimited) {
		t.Fatalf("got %v, want ErrVerifyRateLimited", err)
	}
}

func TestVerifyInvoiceCredentials_NotConfigured(t *testing.T) {
	svc, _, _ := newTestService()
	ctx := context.Background()
	seller := mustPutDistributor(t, svc, uuid.New(), ModeSandbox)

	_, err := svc.VerifyInvoiceCredentials(ctx, Actor{ID: "owner-1", Kind: ActorKindOrgOwner}, "check", seller.ID)
	if !errors.Is(err, ErrNotConfigured) {
		t.Fatalf("got %v, want ErrNotConfigured", err)
	}
}

// Acceptance: "A mode switch with pending jobs is refused."
func TestPutInvoiceSellerIdentity_ModeSwitchRefusedWithPendingJobs(t *testing.T) {
	svc, store, _ := newTestService()
	ctx := context.Background()
	orgID := uuid.New()
	seller := mustPutDistributor(t, svc, orgID, ModeSandbox)
	store.pendingJobsBySr[seller.ID] = 1

	_, err := svc.PutInvoiceSellerIdentity(ctx, Actor{ID: "owner-1", Kind: ActorKindOrgOwner}, "switch to live",
		SellerIdentityInput{
			Kind: SellerKindDistributorOrg, OrgID: orgID, LegalName: "Test Distributor Co",
			TaxID: "53212539", RegistrationStatus: RegistrationRegistered,
			DocumentKind: DocumentKindTaxInvoice, Payer: PayerPlatform, Mode: ModeLive,
		})
	if !errors.Is(err, ErrModeSwitchWithPendingJobs) {
		t.Fatalf("got %v, want ErrModeSwitchWithPendingJobs", err)
	}

	// Clearing the pending jobs allows the same switch through.
	store.pendingJobsBySr[seller.ID] = 0
	updated, err := svc.PutInvoiceSellerIdentity(ctx, Actor{ID: "owner-1", Kind: ActorKindOrgOwner}, "switch to live",
		SellerIdentityInput{
			Kind: SellerKindDistributorOrg, OrgID: orgID, LegalName: "Test Distributor Co",
			TaxID: "53212539", RegistrationStatus: RegistrationRegistered,
			DocumentKind: DocumentKindTaxInvoice, Payer: PayerPlatform, Mode: ModeLive,
		})
	if err != nil {
		t.Fatalf("PutInvoiceSellerIdentity after clearing pending jobs: %v", err)
	}
	if updated.Mode != ModeLive {
		t.Errorf("Mode = %v, want live", updated.Mode)
	}
}
