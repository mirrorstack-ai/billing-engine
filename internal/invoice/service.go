package invoice

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
)

// Audit verbs, matching the CHECK constraint on
// ms_billing.invoice_credential_audit.verb (migration 093).
const (
	VerbPutInvoiceSellerIdentity  = "PutInvoiceSellerIdentity"
	VerbPutInvoiceProviderKeys    = "PutInvoiceProviderKeys"
	VerbRevokeInvoiceProviderKeys = "RevokeInvoiceProviderKeys"
	VerbVerifyInvoiceCredentials  = "VerifyInvoiceCredentials"
)

// DefaultVerifyRateLimit and DefaultVerifyRateWindow are fix direction #3's
// "rate-limited per seller (default 6 an hour)".
const (
	DefaultVerifyRateLimit  = 6
	DefaultVerifyRateWindow = time.Hour
)

// Service implements the fixed verbs: PutInvoiceSellerIdentity,
// PutInvoiceProviderKeys, RevokeInvoiceProviderKeys,
// GetInvoiceSellerSettings, VerifyInvoiceCredentials, and ResolveSeller.
//
// It never holds a vendor credential beyond the single call into Broker.Seal
// — SealedPayload is handed in by the caller and not retained by Service
// after that call returns.
type Service struct {
	store  Store
	broker Broker

	// now is overridable for tests; defaults to time.Now.
	now func() time.Time

	verifyRateLimit  int
	verifyRateWindow time.Duration
}

// NewService returns a Service wired against store and broker, with the
// default verify rate limit (6/hour).
func NewService(store Store, broker Broker) *Service {
	return &Service{
		store:            store,
		broker:           broker,
		now:              time.Now,
		verifyRateLimit:  DefaultVerifyRateLimit,
		verifyRateWindow: DefaultVerifyRateWindow,
	}
}

// SellerIdentityInput is PutInvoiceSellerIdentity's input. Kept distinct from
// SellerIdentityWrite (the Store-facing type) so a validation failure (bad
// tax id, registered without one) is caught before anything reaches Store.
type SellerIdentityInput struct {
	Kind               SellerKind
	OrgID              uuid.UUID
	LegalName          string
	TaxID              string
	RegistrationStatus RegistrationStatus
	DocumentKind       DocumentKind
	Payer              Payer
	Mode               Mode
}

// PutInvoiceSellerIdentity creates or corrects one seller's identity facts.
// D525 pattern, generalized to identity: the platform's own seller
// (SellerKindPlatformCompany) is entered by an operator, since it has no org
// owner to act as one; every other kind requires an org_owner actor.
func (s *Service) PutInvoiceSellerIdentity(ctx context.Context, actor Actor, reason Reason, in SellerIdentityInput) (Seller, error) {
	if !SellerKindInCatalog(in.Kind) {
		return Seller{}, fmt.Errorf("invoice: unknown seller kind %q", in.Kind)
	}
	wantKind := ActorKindOrgOwner
	if in.Kind == SellerKindPlatformCompany {
		wantKind = ActorKindOperator
	}
	if actor.Kind != wantKind {
		return Seller{}, fmt.Errorf("invoice: %s identity requires a %s actor, got %s", in.Kind, wantKind, actor.Kind)
	}

	if in.RegistrationStatus == RegistrationRegistered {
		if in.TaxID == "" {
			return Seller{}, ErrRegisteredRequiresTaxID
		}
		if !ValidateTaxID(in.TaxID) {
			return Seller{}, ErrInvalidTaxID
		}
	}
	if in.TaxID != "" && !ValidateTaxID(in.TaxID) {
		return Seller{}, ErrInvalidTaxID
	}

	existing, hadExisting, err := s.store.GetSellerByKindOrg(ctx, in.Kind, in.OrgID)
	if err != nil {
		return Seller{}, err
	}
	if hadExisting && existing.Mode != in.Mode {
		pending, err := s.store.CountPendingInvoiceJobs(ctx, existing.ID)
		if err != nil {
			return Seller{}, err
		}
		if pending > 0 {
			return Seller{}, ErrModeSwitchWithPendingJobs
		}
	}

	seller, err := s.store.UpsertSellerIdentity(ctx, SellerIdentityWrite{
		Kind:               in.Kind,
		OrgID:              in.OrgID,
		LegalName:          in.LegalName,
		TaxID:              in.TaxID,
		RegistrationStatus: in.RegistrationStatus,
		DocumentKind:       in.DocumentKind,
		Payer:              in.Payer,
		Mode:               in.Mode,
	})
	if err != nil {
		return Seller{}, err
	}

	if err := s.store.AppendAudit(ctx, AuditEntry{
		SellerID:   seller.ID,
		Verb:       VerbPutInvoiceSellerIdentity,
		Actor:      actor,
		Reason:     reason,
		OccurredAt: s.now(),
	}); err != nil {
		return Seller{}, err
	}
	return seller, nil
}

// ProviderKeysInput is PutInvoiceProviderKeys's input. Payload is discarded
// by Service immediately after the Broker.Seal call — it is never logged,
// never stored on ProviderAccount, and never passed to Store.
type ProviderKeysInput struct {
	SellerID           uuid.UUID
	Provider           Provider
	CredentialMode     CredentialMode
	Environment        Environment
	BoundSubMerchantID string
	Payload            SealedPayload
}

// PutInvoiceProviderKeys seals a vendor credential and either activates a
// new provider account or rotates the key on an existing one for the same
// provider. Fix direction #5: a seller has exactly one active provider
// account; switching to a DIFFERENT provider while one is already active is
// refused (ErrDuplicateActiveProvider) — the caller must
// RevokeInvoiceProviderKeys first, making the switch an explicit two-step
// migration rather than an implicit failover.
func (s *Service) PutInvoiceProviderKeys(ctx context.Context, actor Actor, reason Reason, in ProviderKeysInput) (ProviderAccount, error) {
	if actor.Kind != ActorKindOrgOwner {
		return ProviderAccount{}, ErrOperatorCannotEnterKeys
	}
	if !ProviderInCatalog(in.Provider) {
		return ProviderAccount{}, fmt.Errorf("invoice: unknown provider %q", in.Provider)
	}
	seller, found, err := s.store.GetSeller(ctx, in.SellerID)
	if err != nil {
		return ProviderAccount{}, err
	}
	if !found {
		return ProviderAccount{}, ErrSellerNotFound
	}
	_ = seller

	existing, hasExisting, err := s.store.GetActiveProviderAccount(ctx, in.SellerID)
	if err != nil {
		return ProviderAccount{}, err
	}
	if hasExisting && existing.Provider != in.Provider {
		return ProviderAccount{}, ErrDuplicateActiveProvider
	}

	secretRef, keyVersion, err := s.broker.Seal(ctx, in.Provider, in.Payload)
	if err != nil {
		return ProviderAccount{}, err
	}

	var account ProviderAccount
	if hasExisting {
		account, err = s.store.RotateProviderAccountSecret(ctx, existing.ID, secretRef, keyVersion,
			in.Environment, in.CredentialMode, in.BoundSubMerchantID)
	} else {
		account, err = s.store.CreateProviderAccount(ctx, ProviderAccountWrite{
			Provider:           in.Provider,
			Scope:              ScopeSeller,
			SellerID:           in.SellerID,
			CredentialMode:     in.CredentialMode,
			BoundSubMerchantID: in.BoundSubMerchantID,
			Environment:        in.Environment,
			SecretRef:          secretRef,
			KeyVersion:         keyVersion,
			Status:             KeyStatusActive,
		})
	}
	if err != nil {
		return ProviderAccount{}, err
	}

	accountID := account.ID
	if err := s.store.AppendAudit(ctx, AuditEntry{
		SellerID:          in.SellerID,
		ProviderAccountID: &accountID,
		Verb:              VerbPutInvoiceProviderKeys,
		Actor:             actor,
		Reason:            reason,
		OccurredAt:        s.now(),
	}); err != nil {
		return ProviderAccount{}, err
	}
	return account, nil
}

// RevokeInvoiceProviderKeys revokes the seller's active provider account
// through the broker and marks it revoked. Either actor kind may revoke
// (D525: an operator may revoke but never enter or read a key).
func (s *Service) RevokeInvoiceProviderKeys(ctx context.Context, actor Actor, reason Reason, sellerID uuid.UUID) (ProviderAccount, error) {
	existing, hasExisting, err := s.store.GetActiveProviderAccount(ctx, sellerID)
	if err != nil {
		return ProviderAccount{}, err
	}
	if !hasExisting {
		return ProviderAccount{}, ErrNotConfigured
	}
	if err := s.broker.Revoke(ctx, existing.SecretRef); err != nil {
		return ProviderAccount{}, err
	}
	account, err := s.store.RevokeProviderAccount(ctx, existing.ID)
	if err != nil {
		return ProviderAccount{}, err
	}

	accountID := account.ID
	if err := s.store.AppendAudit(ctx, AuditEntry{
		SellerID:          sellerID,
		ProviderAccountID: &accountID,
		Verb:              VerbRevokeInvoiceProviderKeys,
		Actor:             actor,
		Reason:            reason,
		OccurredAt:        s.now(),
	}); err != nil {
		return ProviderAccount{}, err
	}
	return account, nil
}

// GetInvoiceSellerSettings reads back a seller's non-secret settings. Never
// returns key bytes or a hint — see SellerSettings's doc and
// no_plaintext_secrets_test.go.
func (s *Service) GetInvoiceSellerSettings(ctx context.Context, sellerID uuid.UUID) (SellerSettings, error) {
	seller, found, err := s.store.GetSeller(ctx, sellerID)
	if err != nil {
		return SellerSettings{}, err
	}
	if !found {
		return SellerSettings{}, ErrSellerNotFound
	}
	account, hasAccount, err := s.store.GetLatestProviderAccount(ctx, sellerID)
	if err != nil {
		return SellerSettings{}, err
	}
	hasActiveAccount := hasAccount && account.Status == KeyStatusActive

	settings := SellerSettings{
		SellerID:           seller.ID,
		Kind:               seller.Kind,
		LegalName:          seller.LegalName,
		RegistrationStatus: seller.RegistrationStatus,
		DocumentKind:       seller.DocumentKind,
		Payer:              seller.Payer,
		Mode:               seller.Mode,
		FactStatus:         seller.FactStatus(account, hasActiveAccount),
		Status:             KeyStatusUnset,
	}
	if hasAccount {
		settings.Provider = account.Provider
		settings.Configured = account.Status == KeyStatusActive
		settings.KeyVersion = account.KeyVersion
		settings.Status = account.Status
		settings.LastVerifyResult = account.LastVerifyResult
		settings.LastVerifyAt = account.LastVerifyAt
	}
	return settings, nil
}

// VerifyInvoiceCredentials makes the adapter's cheapest read-only signed
// call through the broker and classifies the result, without this process
// ever seeing the opened key. Rate-limited per seller (fix direction #3).
func (s *Service) VerifyInvoiceCredentials(ctx context.Context, actor Actor, reason Reason, sellerID uuid.UUID) (VerifyResult, error) {
	since := s.now().Add(-s.verifyRateWindow)
	count, err := s.store.CountRecentVerifyAttempts(ctx, sellerID, since)
	if err != nil {
		return "", err
	}
	if count >= s.verifyRateLimit {
		return "", ErrVerifyRateLimited
	}

	account, hasAccount, err := s.store.GetActiveProviderAccount(ctx, sellerID)
	if err != nil {
		return "", err
	}
	if !hasAccount {
		return "", ErrNotConfigured
	}

	result, err := s.broker.Verify(ctx, account.Provider, account.SecretRef, account.KeyVersion)
	if err != nil {
		return "", err
	}

	now := s.now()
	if err := s.store.RecordVerifyResult(ctx, account.ID, result, now); err != nil {
		return "", err
	}

	accountID := account.ID
	if err := s.store.AppendAudit(ctx, AuditEntry{
		SellerID:          sellerID,
		ProviderAccountID: &accountID,
		Verb:              VerbVerifyInvoiceCredentials,
		Actor:             actor,
		Reason:            reason,
		OccurredAt:        now,
	}); err != nil {
		return "", err
	}
	return result, nil
}

// EnsureCanIssue reports whether a seller may issue against account, which
// must come from Store.GetLatestProviderAccount (not GetActiveProviderAccount
// — that query cannot return a revoked row, which is exactly the case this
// function needs to distinguish from "never configured"). A revoked account
// refuses with ErrSellerRevoked; anything else short of active refuses with
// ErrNotConfigured. Exported for billing-engine#287's issuance path to call
// before ever attempting a vendor call.
func EnsureCanIssue(account ProviderAccount, hasAccount bool) error {
	if !hasAccount {
		return ErrNotConfigured
	}
	switch account.Status {
	case KeyStatusActive:
		return nil
	case KeyStatusRevoked:
		return ErrSellerRevoked
	default:
		return ErrNotConfigured
	}
}
