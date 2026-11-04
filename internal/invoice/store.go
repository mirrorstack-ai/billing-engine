package invoice

import (
	"context"
	"time"

	"github.com/google/uuid"
)

// SellerIdentityWrite is the input to Store.UpsertSellerIdentity. It is not
// named *Request: internal argument bundles in this package deliberately
// avoid that suffix, which internal/architecture.ScanRequestFields treats as
// "arrives on the wire" — this is an internal call between Service and
// Store, not a caller-facing type.
type SellerIdentityWrite struct {
	Kind               SellerKind
	OrgID              uuid.UUID
	LegalName          string
	TaxID              string // empty when RegistrationStatus is unregistered
	RegistrationStatus RegistrationStatus
	DocumentKind       DocumentKind
	Payer              Payer
	Mode               Mode
}

// ProviderAccountWrite is the input to Store.CreateProviderAccount.
type ProviderAccountWrite struct {
	Provider           Provider
	Scope              Scope
	SellerID           uuid.UUID
	CredentialMode     CredentialMode
	BoundSubMerchantID string
	Environment        Environment
	SecretRef          string
	KeyVersion         int
	Status             KeyStatus
}

// AuditEntry is one append-only row of ms_billing.invoice_credential_audit.
type AuditEntry struct {
	SellerID          uuid.UUID
	ProviderAccountID *uuid.UUID
	Verb              string
	Actor             Actor
	Reason            Reason
	OccurredAt        time.Time
}

// Store is the persistence port Service writes through. The production
// implementation (NewPostgresStore) talks to ms_billing; tests use an
// in-memory fake.
type Store interface {
	UpsertSellerIdentity(ctx context.Context, w SellerIdentityWrite) (Seller, error)
	GetSeller(ctx context.Context, sellerID uuid.UUID) (Seller, bool, error)
	GetSellerByKindOrg(ctx context.Context, kind SellerKind, orgID uuid.UUID) (Seller, bool, error)

	GetActiveProviderAccount(ctx context.Context, sellerID uuid.UUID) (ProviderAccount, bool, error)
	// GetLatestProviderAccount returns the seller's most recently created
	// provider account regardless of status (active or revoked), so a
	// revoked account can still be reported as 'revoked' rather than
	// indistinguishable from "never configured". A seller can have more
	// than one row over time — PutInvoiceProviderKeys after a revoke
	// creates a new row rather than reusing the old one.
	GetLatestProviderAccount(ctx context.Context, sellerID uuid.UUID) (ProviderAccount, bool, error)
	GetProviderAccount(ctx context.Context, accountID uuid.UUID) (ProviderAccount, bool, error)
	CreateProviderAccount(ctx context.Context, w ProviderAccountWrite) (ProviderAccount, error)
	RotateProviderAccountSecret(ctx context.Context, accountID uuid.UUID, secretRef string, keyVersion int, env Environment, mode CredentialMode, boundSubMerchantID string) (ProviderAccount, error)
	RevokeProviderAccount(ctx context.Context, accountID uuid.UUID) (ProviderAccount, error)
	RecordVerifyResult(ctx context.Context, accountID uuid.UUID, result VerifyResult, at time.Time) error

	// CountPendingInvoiceJobs is a seam for billing-engine#287's invoice-job
	// table, which does not exist in this tree yet. The Postgres
	// implementation returns 0 unconditionally until #287 lands — documented
	// there, not hidden, so the mode-switch guard in Service degrades to
	// "nothing pending" rather than failing closed on a table that isn't
	// built.
	CountPendingInvoiceJobs(ctx context.Context, sellerID uuid.UUID) (int, error)

	AppendAudit(ctx context.Context, entry AuditEntry) error
	CountRecentVerifyAttempts(ctx context.Context, sellerID uuid.UUID, since time.Time) (int, error)
}
