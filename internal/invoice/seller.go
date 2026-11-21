package invoice

import (
	"time"

	"github.com/google/uuid"
)

// Seller is one row of ms_billing.invoice_sellers.
type Seller struct {
	ID                 uuid.UUID
	Kind               SellerKind
	OrgID              uuid.UUID
	LegalName          string
	TaxID              string // empty when unregistered
	RegistrationStatus RegistrationStatus
	DocumentKind       DocumentKind
	Payer              Payer
	Mode               Mode
	CreatedAt          time.Time
	UpdatedAt          time.Time
}

// ProviderAccount is one row of ms_billing.invoice_provider_accounts. It
// never carries key material — SecretRef is an opaque pointer into the
// sealing broker (see broker.go).
type ProviderAccount struct {
	ID                 uuid.UUID
	Provider           Provider
	Scope              Scope
	SellerID           uuid.UUID // zero when Scope == ScopePlatform
	CredentialMode     CredentialMode
	BoundSubMerchantID string // non-secret; set only for a seller's platform-mode binding
	Environment        Environment
	EligibilityState   string
	SecretRef          string
	KeyVersion         int
	Status             KeyStatus
	LastVerifyResult   *VerifyResult
	LastVerifyAt       *time.Time
	CreatedAt          time.Time
	UpdatedAt          time.Time
}

// FactStatus reports whether a seller's invoicing facts are actionable,
// given whatever provider account it currently resolves to (fix direction
// acceptance criteria):
//
//   - document_kind receipt or none always resolves not_applicable,
//     regardless of registration or key configuration.
//   - an unregistered seller resolves deferred.
//   - a registered seller with no active provider account resolves
//     deferred — a seller "without keys reads unset and its facts end
//     deferred".
//   - otherwise, applicable.
func (s Seller) FactStatus(account ProviderAccount, hasActiveAccount bool) FactStatus {
	if s.DocumentKind == DocumentKindReceipt || s.DocumentKind == DocumentKindNone {
		return FactNotApplicable
	}
	if s.RegistrationStatus == RegistrationUnregistered {
		return FactDeferred
	}
	if !hasActiveAccount || account.Status != KeyStatusActive {
		return FactDeferred
	}
	return FactApplicable
}

// SellerSettings is the shape GetInvoiceSellerSettings returns: provider,
// document kind, registration status, payer, mode, whether a credential is
// configured, key version, and the last verify result. Deliberately no field
// carries key bytes or a hint such as a last-four — see
// no_plaintext_secrets_test.go, which asserts that structurally.
type SellerSettings struct {
	SellerID           uuid.UUID
	Kind               SellerKind
	LegalName          string
	RegistrationStatus RegistrationStatus
	DocumentKind       DocumentKind
	Payer              Payer
	Mode               Mode
	FactStatus         FactStatus

	Provider         Provider // zero value if no provider account exists yet
	Configured       bool     // true only when Status == KeyStatusActive
	KeyVersion       int
	Status           KeyStatus
	LastVerifyResult *VerifyResult
	LastVerifyAt     *time.Time
}
