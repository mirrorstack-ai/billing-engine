package invoice

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// NewPostgresStore returns a Store backed by pool, talking only to
// ms_billing.invoice_sellers, ms_billing.invoice_provider_accounts, and
// ms_billing.invoice_credential_audit (migration 093). It never reads or
// writes any other schema — org id is treated as an opaque value passed in
// by the caller, same as everywhere else in this engine.
func NewPostgresStore(pool *pgxpool.Pool) Store {
	if pool == nil {
		panic("invoice.NewPostgresStore: pool must not be nil")
	}
	return &pgStore{pool: pool}
}

type pgStore struct {
	pool *pgxpool.Pool
}

func (s *pgStore) UpsertSellerIdentity(ctx context.Context, w SellerIdentityWrite) (Seller, error) {
	var taxID *string
	if w.TaxID != "" {
		taxID = &w.TaxID
	}
	row := s.pool.QueryRow(ctx, `
		INSERT INTO ms_billing.invoice_sellers
			(kind, org_id, legal_name, tax_id, registration_status, document_kind, payer, mode)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
		ON CONFLICT (kind, org_id) DO UPDATE SET
			legal_name           = EXCLUDED.legal_name,
			tax_id               = EXCLUDED.tax_id,
			registration_status  = EXCLUDED.registration_status,
			document_kind        = EXCLUDED.document_kind,
			payer                = EXCLUDED.payer,
			mode                 = EXCLUDED.mode
		RETURNING id, kind, org_id, legal_name, COALESCE(tax_id, ''), registration_status,
			document_kind, payer, mode, created_at, updated_at`,
		string(w.Kind), w.OrgID, w.LegalName, taxID, string(w.RegistrationStatus),
		string(w.DocumentKind), string(w.Payer), string(w.Mode))
	return scanSeller(row)
}

func (s *pgStore) GetSeller(ctx context.Context, sellerID uuid.UUID) (Seller, bool, error) {
	row := s.pool.QueryRow(ctx, `
		SELECT id, kind, org_id, legal_name, COALESCE(tax_id, ''), registration_status,
			document_kind, payer, mode, created_at, updated_at
		FROM ms_billing.invoice_sellers WHERE id = $1`, sellerID)
	return scanSellerFound(row)
}

func (s *pgStore) GetSellerByKindOrg(ctx context.Context, kind SellerKind, orgID uuid.UUID) (Seller, bool, error) {
	row := s.pool.QueryRow(ctx, `
		SELECT id, kind, org_id, legal_name, COALESCE(tax_id, ''), registration_status,
			document_kind, payer, mode, created_at, updated_at
		FROM ms_billing.invoice_sellers WHERE kind = $1 AND org_id = $2`, string(kind), orgID)
	return scanSellerFound(row)
}

func scanSeller(row pgx.Row) (Seller, error) {
	var s Seller
	var kind, regStatus, docKind, payer, mode string
	if err := row.Scan(&s.ID, &kind, &s.OrgID, &s.LegalName, &s.TaxID, &regStatus,
		&docKind, &payer, &mode, &s.CreatedAt, &s.UpdatedAt); err != nil {
		return Seller{}, err
	}
	s.Kind = SellerKind(kind)
	s.RegistrationStatus = RegistrationStatus(regStatus)
	s.DocumentKind = DocumentKind(docKind)
	s.Payer = Payer(payer)
	s.Mode = Mode(mode)
	return s, nil
}

func scanSellerFound(row pgx.Row) (Seller, bool, error) {
	s, err := scanSeller(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return Seller{}, false, nil
	}
	if err != nil {
		return Seller{}, false, err
	}
	return s, true, nil
}

func (s *pgStore) GetActiveProviderAccount(ctx context.Context, sellerID uuid.UUID) (ProviderAccount, bool, error) {
	row := s.pool.QueryRow(ctx, providerAccountSelect+`
		WHERE seller_id = $1 AND scope = 'seller' AND status = 'active'`, sellerID)
	return scanProviderAccountFound(row)
}

func (s *pgStore) GetLatestProviderAccount(ctx context.Context, sellerID uuid.UUID) (ProviderAccount, bool, error) {
	row := s.pool.QueryRow(ctx, providerAccountSelect+`
		WHERE seller_id = $1 AND scope = 'seller'
		ORDER BY created_at DESC LIMIT 1`, sellerID)
	return scanProviderAccountFound(row)
}

func (s *pgStore) GetProviderAccount(ctx context.Context, accountID uuid.UUID) (ProviderAccount, bool, error) {
	row := s.pool.QueryRow(ctx, providerAccountSelect+` WHERE id = $1`, accountID)
	return scanProviderAccountFound(row)
}

const providerAccountSelect = `
	SELECT id, provider, scope, COALESCE(seller_id, '00000000-0000-0000-0000-000000000000'),
		credential_mode, COALESCE(bound_sub_merchant_id, ''), environment, eligibility_state,
		COALESCE(secret_ref, ''), key_version, status, last_verify_result, last_verify_at,
		created_at, updated_at
	FROM ms_billing.invoice_provider_accounts`

func scanProviderAccount(row pgx.Row) (ProviderAccount, error) {
	var a ProviderAccount
	var provider, scope, credMode, environment, status string
	var lastResult *string
	if err := row.Scan(&a.ID, &provider, &scope, &a.SellerID, &credMode, &a.BoundSubMerchantID,
		&environment, &a.EligibilityState, &a.SecretRef, &a.KeyVersion, &status,
		&lastResult, &a.LastVerifyAt, &a.CreatedAt, &a.UpdatedAt); err != nil {
		return ProviderAccount{}, err
	}
	a.Provider = Provider(provider)
	a.Scope = Scope(scope)
	a.CredentialMode = CredentialMode(credMode)
	a.Environment = Environment(environment)
	a.Status = KeyStatus(status)
	if lastResult != nil {
		v := VerifyResult(*lastResult)
		a.LastVerifyResult = &v
	}
	return a, nil
}

func scanProviderAccountFound(row pgx.Row) (ProviderAccount, bool, error) {
	a, err := scanProviderAccount(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return ProviderAccount{}, false, nil
	}
	if err != nil {
		return ProviderAccount{}, false, err
	}
	return a, true, nil
}

func (s *pgStore) CreateProviderAccount(ctx context.Context, w ProviderAccountWrite) (ProviderAccount, error) {
	var sellerID *uuid.UUID
	if w.Scope == ScopeSeller {
		sellerID = &w.SellerID
	}
	var boundSubMerchant *string
	if w.BoundSubMerchantID != "" {
		boundSubMerchant = &w.BoundSubMerchantID
	}
	var secretRef *string
	if w.SecretRef != "" {
		secretRef = &w.SecretRef
	}
	row := s.pool.QueryRow(ctx, `
		INSERT INTO ms_billing.invoice_provider_accounts
			(provider, scope, seller_id, credential_mode, bound_sub_merchant_id,
			 environment, secret_ref, key_version, status)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
		RETURNING id, provider, scope, COALESCE(seller_id, '00000000-0000-0000-0000-000000000000'),
			credential_mode, COALESCE(bound_sub_merchant_id, ''), environment, eligibility_state,
			COALESCE(secret_ref, ''), key_version, status, last_verify_result, last_verify_at,
			created_at, updated_at`,
		string(w.Provider), string(w.Scope), sellerID, string(w.CredentialMode), boundSubMerchant,
		string(w.Environment), secretRef, w.KeyVersion, string(w.Status))
	return scanProviderAccount(row)
}

func (s *pgStore) RotateProviderAccountSecret(ctx context.Context, accountID uuid.UUID, secretRef string, keyVersion int, env Environment, mode CredentialMode, boundSubMerchantID string) (ProviderAccount, error) {
	var boundSubMerchant *string
	if boundSubMerchantID != "" {
		boundSubMerchant = &boundSubMerchantID
	}
	row := s.pool.QueryRow(ctx, `
		UPDATE ms_billing.invoice_provider_accounts
		SET secret_ref = $2, key_version = $3, environment = $4, credential_mode = $5,
			bound_sub_merchant_id = $6, status = 'active'
		WHERE id = $1
		RETURNING id, provider, scope, COALESCE(seller_id, '00000000-0000-0000-0000-000000000000'),
			credential_mode, COALESCE(bound_sub_merchant_id, ''), environment, eligibility_state,
			COALESCE(secret_ref, ''), key_version, status, last_verify_result, last_verify_at,
			created_at, updated_at`,
		accountID, secretRef, keyVersion, string(env), string(mode), boundSubMerchant)
	return scanProviderAccount(row)
}

func (s *pgStore) RevokeProviderAccount(ctx context.Context, accountID uuid.UUID) (ProviderAccount, error) {
	row := s.pool.QueryRow(ctx, `
		UPDATE ms_billing.invoice_provider_accounts
		SET status = 'revoked'
		WHERE id = $1
		RETURNING id, provider, scope, COALESCE(seller_id, '00000000-0000-0000-0000-000000000000'),
			credential_mode, COALESCE(bound_sub_merchant_id, ''), environment, eligibility_state,
			COALESCE(secret_ref, ''), key_version, status, last_verify_result, last_verify_at,
			created_at, updated_at`,
		accountID)
	return scanProviderAccount(row)
}

func (s *pgStore) RecordVerifyResult(ctx context.Context, accountID uuid.UUID, result VerifyResult, at time.Time) error {
	_, err := s.pool.Exec(ctx, `
		UPDATE ms_billing.invoice_provider_accounts
		SET last_verify_result = $2, last_verify_at = $3
		WHERE id = $1`, accountID, string(result), at)
	return err
}

// CountPendingInvoiceJobs always returns 0: billing-engine#287's invoice-job
// table does not exist in this tree yet. See the Store interface doc.
func (s *pgStore) CountPendingInvoiceJobs(ctx context.Context, sellerID uuid.UUID) (int, error) {
	return 0, nil
}

func (s *pgStore) AppendAudit(ctx context.Context, entry AuditEntry) error {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO ms_billing.invoice_credential_audit
			(seller_id, provider_account_id, verb, actor, actor_kind, reason, occurred_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7)`,
		entry.SellerID, entry.ProviderAccountID, entry.Verb, entry.Actor.ID,
		string(entry.Actor.Kind), string(entry.Reason), entry.OccurredAt)
	return err
}

func (s *pgStore) CountRecentVerifyAttempts(ctx context.Context, sellerID uuid.UUID, since time.Time) (int, error) {
	var n int
	err := s.pool.QueryRow(ctx, `
		SELECT COUNT(*) FROM ms_billing.invoice_credential_audit
		WHERE seller_id = $1 AND verb = 'VerifyInvoiceCredentials' AND occurred_at > $2`,
		sellerID, since).Scan(&n)
	return n, err
}
