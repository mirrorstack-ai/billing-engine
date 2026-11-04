package invoice

import (
	"context"
	"time"

	"github.com/google/uuid"
)

// memStore is an in-memory Store for unit tests. It does not reproduce every
// constraint the real schema enforces (migration 093's integration test does
// that against real Postgres) — it exists so Service's business logic can be
// tested without Docker.
type memStore struct {
	sellers         map[uuid.UUID]Seller
	accounts        map[uuid.UUID]ProviderAccount
	accountsBySlr   map[uuid.UUID][]uuid.UUID // sellerID -> account ids, insertion order
	audit           []AuditEntry
	pendingJobsBySr map[uuid.UUID]int
}

func newMemStore() *memStore {
	return &memStore{
		sellers:         make(map[uuid.UUID]Seller),
		accounts:        make(map[uuid.UUID]ProviderAccount),
		accountsBySlr:   make(map[uuid.UUID][]uuid.UUID),
		pendingJobsBySr: make(map[uuid.UUID]int),
	}
}

func (m *memStore) UpsertSellerIdentity(_ context.Context, w SellerIdentityWrite) (Seller, error) {
	for _, existing := range m.sellers {
		if existing.Kind == w.Kind && existing.OrgID == w.OrgID {
			existing.LegalName = w.LegalName
			existing.TaxID = w.TaxID
			existing.RegistrationStatus = w.RegistrationStatus
			existing.DocumentKind = w.DocumentKind
			existing.Payer = w.Payer
			existing.Mode = w.Mode
			existing.UpdatedAt = time.Now()
			m.sellers[existing.ID] = existing
			return existing, nil
		}
	}
	now := time.Now()
	seller := Seller{
		ID:                 uuid.New(),
		Kind:               w.Kind,
		OrgID:              w.OrgID,
		LegalName:          w.LegalName,
		TaxID:              w.TaxID,
		RegistrationStatus: w.RegistrationStatus,
		DocumentKind:       w.DocumentKind,
		Payer:              w.Payer,
		Mode:               w.Mode,
		CreatedAt:          now,
		UpdatedAt:          now,
	}
	m.sellers[seller.ID] = seller
	return seller, nil
}

func (m *memStore) GetSeller(_ context.Context, sellerID uuid.UUID) (Seller, bool, error) {
	s, ok := m.sellers[sellerID]
	return s, ok, nil
}

func (m *memStore) GetSellerByKindOrg(_ context.Context, kind SellerKind, orgID uuid.UUID) (Seller, bool, error) {
	for _, s := range m.sellers {
		if s.Kind == kind && s.OrgID == orgID {
			return s, true, nil
		}
	}
	return Seller{}, false, nil
}

func (m *memStore) GetActiveProviderAccount(_ context.Context, sellerID uuid.UUID) (ProviderAccount, bool, error) {
	ids := m.accountsBySlr[sellerID]
	for i := len(ids) - 1; i >= 0; i-- {
		a := m.accounts[ids[i]]
		if a.Status == KeyStatusActive {
			return a, true, nil
		}
	}
	return ProviderAccount{}, false, nil
}

func (m *memStore) GetLatestProviderAccount(_ context.Context, sellerID uuid.UUID) (ProviderAccount, bool, error) {
	ids := m.accountsBySlr[sellerID]
	if len(ids) == 0 {
		return ProviderAccount{}, false, nil
	}
	return m.accounts[ids[len(ids)-1]], true, nil
}

func (m *memStore) GetProviderAccount(_ context.Context, accountID uuid.UUID) (ProviderAccount, bool, error) {
	a, ok := m.accounts[accountID]
	return a, ok, nil
}

func (m *memStore) CreateProviderAccount(_ context.Context, w ProviderAccountWrite) (ProviderAccount, error) {
	now := time.Now()
	a := ProviderAccount{
		ID:                 uuid.New(),
		Provider:           w.Provider,
		Scope:              w.Scope,
		SellerID:           w.SellerID,
		CredentialMode:     w.CredentialMode,
		BoundSubMerchantID: w.BoundSubMerchantID,
		Environment:        w.Environment,
		EligibilityState:   "eligible",
		SecretRef:          w.SecretRef,
		KeyVersion:         w.KeyVersion,
		Status:             w.Status,
		CreatedAt:          now,
		UpdatedAt:          now,
	}
	m.accounts[a.ID] = a
	m.accountsBySlr[w.SellerID] = append(m.accountsBySlr[w.SellerID], a.ID)
	return a, nil
}

func (m *memStore) RotateProviderAccountSecret(_ context.Context, accountID uuid.UUID, secretRef string, keyVersion int, env Environment, mode CredentialMode, boundSubMerchantID string) (ProviderAccount, error) {
	a := m.accounts[accountID]
	a.SecretRef = secretRef
	a.KeyVersion = keyVersion
	a.Environment = env
	a.CredentialMode = mode
	a.BoundSubMerchantID = boundSubMerchantID
	a.Status = KeyStatusActive
	a.UpdatedAt = time.Now()
	m.accounts[accountID] = a
	return a, nil
}

func (m *memStore) RevokeProviderAccount(_ context.Context, accountID uuid.UUID) (ProviderAccount, error) {
	a := m.accounts[accountID]
	a.Status = KeyStatusRevoked
	a.UpdatedAt = time.Now()
	m.accounts[accountID] = a
	return a, nil
}

func (m *memStore) RecordVerifyResult(_ context.Context, accountID uuid.UUID, result VerifyResult, at time.Time) error {
	a := m.accounts[accountID]
	a.LastVerifyResult = &result
	a.LastVerifyAt = &at
	m.accounts[accountID] = a
	return nil
}

func (m *memStore) CountPendingInvoiceJobs(_ context.Context, sellerID uuid.UUID) (int, error) {
	return m.pendingJobsBySr[sellerID], nil
}

func (m *memStore) AppendAudit(_ context.Context, entry AuditEntry) error {
	m.audit = append(m.audit, entry)
	return nil
}

func (m *memStore) CountRecentVerifyAttempts(_ context.Context, sellerID uuid.UUID, since time.Time) (int, error) {
	n := 0
	for _, e := range m.audit {
		if e.SellerID == sellerID && e.Verb == VerbVerifyInvoiceCredentials && e.OccurredAt.After(since) {
			n++
		}
	}
	return n, nil
}
