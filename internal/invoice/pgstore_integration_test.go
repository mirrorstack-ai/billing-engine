//go:build integration

package invoice

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/mirrorstack-ai/billing-engine/internal/shared/testutil"
)

func newIntegrationStore(t *testing.T) (Store, *pgxpool.Pool) {
	t.Helper()
	pool := testutil.NewTestDB(t)
	return NewPostgresStore(pool), pool
}

func TestPostgresStore_SellerIdentityUpsert(t *testing.T) {
	store, _ := newIntegrationStore(t)
	ctx := context.Background()
	orgID := uuid.New()

	seller, err := store.UpsertSellerIdentity(ctx, SellerIdentityWrite{
		Kind: SellerKindDistributorOrg, OrgID: orgID, LegalName: "Distributor A",
		TaxID: "53212539", RegistrationStatus: RegistrationRegistered,
		DocumentKind: DocumentKindTaxInvoice, Payer: PayerPlatform, Mode: ModeSandbox,
	})
	if err != nil {
		t.Fatalf("UpsertSellerIdentity: %v", err)
	}

	// Upserting the same (kind, org_id) corrects the row rather than
	// creating a second one.
	updated, err := store.UpsertSellerIdentity(ctx, SellerIdentityWrite{
		Kind: SellerKindDistributorOrg, OrgID: orgID, LegalName: "Distributor A Renamed",
		TaxID: "53212539", RegistrationStatus: RegistrationRegistered,
		DocumentKind: DocumentKindTaxInvoice, Payer: PayerSeller, Mode: ModeSandbox,
	})
	if err != nil {
		t.Fatalf("UpsertSellerIdentity (correction): %v", err)
	}
	if updated.ID != seller.ID {
		t.Fatalf("correction created a new row: %v != %v", updated.ID, seller.ID)
	}
	if updated.LegalName != "Distributor A Renamed" || updated.Payer != PayerSeller {
		t.Fatalf("correction did not apply: %+v", updated)
	}

	fetched, found, err := store.GetSeller(ctx, seller.ID)
	if err != nil || !found {
		t.Fatalf("GetSeller: found=%v err=%v", found, err)
	}
	if fetched.TaxID != "53212539" {
		t.Fatalf("TaxID round-trip failed: %q", fetched.TaxID)
	}
}

// Exercises the DB-level invariant from migration 093: at most one ACTIVE
// provider account per seller, enforced by a partial unique index rather
// than by service-layer discipline alone.
func TestPostgresStore_OneActiveProviderAccountPerSeller(t *testing.T) {
	store, _ := newIntegrationStore(t)
	ctx := context.Background()

	seller, err := store.UpsertSellerIdentity(ctx, SellerIdentityWrite{
		Kind: SellerKindDistributorOrg, OrgID: uuid.New(), LegalName: "D",
		TaxID: "53212539", RegistrationStatus: RegistrationRegistered,
		DocumentKind: DocumentKindTaxInvoice, Payer: PayerPlatform, Mode: ModeSandbox,
	})
	if err != nil {
		t.Fatalf("UpsertSellerIdentity: %v", err)
	}

	_, err = store.CreateProviderAccount(ctx, ProviderAccountWrite{
		Provider: ProviderSystemlead, Scope: ScopeSeller, SellerID: seller.ID,
		CredentialMode: CredentialModeSellerKeys, Environment: EnvironmentStage,
		SecretRef: "ref-1", KeyVersion: 1, Status: KeyStatusActive,
	})
	if err != nil {
		t.Fatalf("first CreateProviderAccount: %v", err)
	}

	_, err = store.CreateProviderAccount(ctx, ProviderAccountWrite{
		Provider: ProviderECPay, Scope: ScopeSeller, SellerID: seller.ID,
		CredentialMode: CredentialModeSellerKeys, Environment: EnvironmentStage,
		SecretRef: "ref-2", KeyVersion: 1, Status: KeyStatusActive,
	})
	if err == nil {
		t.Fatal("expected the unique partial index to reject a second active account for the same seller")
	}
}

func TestPostgresStore_AuditIsAppendOnly(t *testing.T) {
	store, pool := newIntegrationStore(t)
	ctx := context.Background()

	seller, err := store.UpsertSellerIdentity(ctx, SellerIdentityWrite{
		Kind: SellerKindDistributorOrg, OrgID: uuid.New(), LegalName: "D",
		RegistrationStatus: RegistrationUnregistered, DocumentKind: DocumentKindTaxInvoice,
		Payer: PayerPlatform, Mode: ModeSandbox,
	})
	if err != nil {
		t.Fatalf("UpsertSellerIdentity: %v", err)
	}
	if err := store.AppendAudit(ctx, AuditEntry{
		SellerID: seller.ID, Verb: VerbPutInvoiceSellerIdentity,
		Actor: Actor{ID: "owner-1", Kind: ActorKindOrgOwner}, Reason: "setup",
		OccurredAt: time.Now(),
	}); err != nil {
		t.Fatalf("AppendAudit: %v", err)
	}

	_, err = pool.Exec(ctx, `UPDATE ms_billing.invoice_credential_audit SET reason = 'tampered' WHERE seller_id = $1`, seller.ID)
	if err == nil {
		t.Fatal("expected the append-only trigger to reject an UPDATE")
	}

	_, err = pool.Exec(ctx, `DELETE FROM ms_billing.invoice_credential_audit WHERE seller_id = $1`, seller.ID)
	if err == nil {
		t.Fatal("expected the append-only trigger to reject a DELETE")
	}
}

func TestPostgresStore_RegisteredRequiresTaxID(t *testing.T) {
	store, _ := newIntegrationStore(t)
	ctx := context.Background()

	_, err := store.UpsertSellerIdentity(ctx, SellerIdentityWrite{
		Kind: SellerKindDistributorOrg, OrgID: uuid.New(), LegalName: "D",
		RegistrationStatus: RegistrationRegistered, DocumentKind: DocumentKindTaxInvoice,
		Payer: PayerPlatform, Mode: ModeSandbox,
		// TaxID deliberately empty.
	})
	if err == nil {
		t.Fatal("expected the CHECK constraint to reject registered without a tax id")
	}
}
