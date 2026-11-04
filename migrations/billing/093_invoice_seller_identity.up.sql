-- Migration 093 — invoice seller identity, provider accounts, sealed
-- credentials (core-v2#1850, billing-engine#312).
--
-- WHY: nothing today says who the seller of an invoice is. charge_intents
-- has no seller column (docs/DESIGN.md:2004-2022; migration 062 added the
-- rail and deliberately left out a merchant-account policy) and this issue
-- does not change that — it holds the invoice side only: identity, vendor
-- account, credentials. D533: each seller issues under its own 統編 and its
-- own 字軌; MirrorStack operates the system. D551: e首發票 first, ECPay
-- second, both behind one port. Seller-keys mode now; platform mode later,
-- shaped so it needs no migration of its own (see invoice_provider_accounts
-- below).
--
-- Two tables:
--   invoice_sellers          — who the seller is, and the facts that decide
--                               whether/how it can issue.
--   invoice_provider_accounts — which vendor account (if any) a seller issues
--                               through, and a reference to its sealed
--                               credential. The secret itself is never here —
--                               secret_ref is an opaque pointer into the
--                               sealing broker (billing-engine#292); this
--                               repo's process never holds the plaintext key.
--   invoice_credential_audit — append-only: every identity/key write, who
--                               did it, under what actor_kind, and why
--                               (D525 pattern).
--
-- Deliberately does NOT reuse ms_billing.invoices — that table mirrors
-- Stripe (D271) and has nothing to do with a Taiwan tax document.

-- ---------------------------------------------------------------------------
-- invoice_sellers
-- ---------------------------------------------------------------------------
--
-- org_id is a soft FK, same convention as accounts.owner_org_id (001_init):
-- this engine cannot read the organizations schema (billing-engine#278), so
-- api-platform passes the org id on every call and nothing here joins against
-- ms_account.
--
-- UNIQUE(kind, org_id): PutInvoiceSellerIdentity is an upsert keyed on this
-- pair, so calling it again for the same org is a correction, not a second
-- seller. platform_company has exactly one live row in practice (mirrorstack-
-- tw) but the constraint is not narrowed to "at most one platform_company
-- row" — a second Taiwan entity existing later is not this migration's call
-- to foreclose.
CREATE TABLE IF NOT EXISTS ms_billing.invoice_sellers (
    id                   UUID PRIMARY KEY DEFAULT gen_random_uuid(),

    kind                 TEXT NOT NULL
                         CHECK (kind IN ('platform_company', 'distributor_org', 'app_owner_org')),
    org_id               UUID NOT NULL,   -- soft FK to ms_account.orgs.id

    legal_name           TEXT NOT NULL CHECK (legal_name <> ''),

    -- 統一編號. NULL until registered — an unregistered seller (mirrorstack-tw
    -- today, D504) has none to store. The checksum itself is validated in Go
    -- (internal/invoice/taxid.go) rather than in SQL: the algorithm has a
    -- branch (the digit-7 exception) that is painful to express as a single
    -- CHECK and easy to get silently wrong there.
    tax_id               TEXT NULL CHECK (tax_id IS NULL OR tax_id ~ '^[0-9]{8}$'),

    registration_status  TEXT NOT NULL DEFAULT 'unregistered'
                         CHECK (registration_status IN ('unregistered', 'registered')),
    CONSTRAINT invoice_sellers_registered_has_tax_id
        CHECK (registration_status <> 'registered' OR tax_id IS NOT NULL),

    -- A seller-level fact from the tax-bureau determination, never a
    -- per-sale or per-customer choice. 'receipt' and 'none' both mean this
    -- seller resolves not_applicable for invoicing (see Go FactStatus).
    document_kind        TEXT NOT NULL
                         CHECK (document_kind IN ('tax_invoice', 'receipt', 'none')),

    -- D544: MirrorStack pays the vendor by default; a per-seller switch to
    -- seller-pays is the fallback. This column only records who pays the
    -- vendor — it charges the seller nothing by itself (billing-engine#314
    -- wires the allowance/overage effect).
    payer                TEXT NOT NULL DEFAULT 'platform'
                         CHECK (payer IN ('platform', 'seller')),

    -- Copied onto every invoice issued for this seller and immutable there.
    -- A mode switch here is refused by the service layer while the seller
    -- has pending invoice jobs (not modeled in THIS migration — that table
    -- is billing-engine#287 — so the refusal lives in Go against a seam
    -- that defaults to "nothing pending" until #287 lands).
    mode                 TEXT NOT NULL DEFAULT 'sandbox'
                         CHECK (mode IN ('sandbox', 'live')),

    created_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at           TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT invoice_sellers_kind_org_unique UNIQUE (kind, org_id)
);

COMMENT ON COLUMN ms_billing.invoice_sellers.document_kind IS
    'Seller-level tax-bureau determination: tax_invoice | receipt | none. '
    'receipt and none both resolve not_applicable for invoicing regardless '
    'of registration_status or key configuration.';

CREATE TRIGGER invoice_sellers_set_updated_at
BEFORE UPDATE ON ms_billing.invoice_sellers
FOR EACH ROW
EXECUTE FUNCTION ms_billing.set_updated_at();

-- ---------------------------------------------------------------------------
-- invoice_provider_accounts
-- ---------------------------------------------------------------------------
--
-- scope + seller_id together carry BOTH credential modes without a later
-- migration:
--   scope = 'seller'   — this row belongs to exactly one seller.
--                         credential_mode = 'seller_keys' is that seller's
--                         own vendor account; credential_mode = 'platform'
--                         is that seller's BINDING into the shared platform
--                         account (bound_sub_merchant_id is its sub-merchant
--                         id under that account; non-secret).
--   scope = 'platform' — the shared account itself (e.g. ECPay PlatformID +
--                         platform keys). seller_id is NULL: it is not one
--                         seller's row. Exactly one active row per
--                         (provider, environment).
--
-- secret_ref is the ONLY credential-shaped column: an opaque reference into
-- the sealing broker (billing-engine#292). Never a key, never a hint. A row
-- with status != 'unset' always has a secret_ref — the converse is not
-- required, so a revoke can keep it for audit trail without re-sealing.
CREATE TABLE IF NOT EXISTS ms_billing.invoice_provider_accounts (
    id                     UUID PRIMARY KEY DEFAULT gen_random_uuid(),

    provider               TEXT NOT NULL
                           CHECK (provider IN ('systemlead', 'ecpay', 'fake')),

    scope                  TEXT NOT NULL CHECK (scope IN ('seller', 'platform')),
    seller_id              UUID NULL
                           REFERENCES ms_billing.invoice_sellers(id) ON DELETE CASCADE,
    CONSTRAINT invoice_provider_accounts_scope_seller_pairing
        CHECK ((scope = 'seller') = (seller_id IS NOT NULL)),

    credential_mode        TEXT NOT NULL
                           CHECK (credential_mode IN ('seller_keys', 'platform')),
    CONSTRAINT invoice_provider_accounts_seller_keys_is_seller_scoped
        CHECK (credential_mode <> 'seller_keys' OR scope = 'seller'),

    -- Non-secret. Only meaningful for a seller's binding into a shared
    -- platform account.
    bound_sub_merchant_id  TEXT NULL,
    CONSTRAINT invoice_provider_accounts_binding_is_platform_mode
        CHECK (bound_sub_merchant_id IS NULL
               OR (scope = 'seller' AND credential_mode = 'platform')),

    environment            TEXT NOT NULL CHECK (environment IN ('stage', 'production')),

    -- No CHECK on the value, same reasoning as selected_rail in 062: there is
    -- no closed vendor-eligibility vocabulary (ECPay's 中華民國境內營利登記
    -- requirement produces different codes than a future vendor's rules
    -- would), and a column nothing enforces structurally is better left open
    -- than given a fake-closed CHECK that a later vendor silently violates.
    eligibility_state      TEXT NOT NULL DEFAULT 'eligible',

    secret_ref             TEXT NULL,
    key_version            INT NOT NULL DEFAULT 0 CHECK (key_version >= 0),

    status                 TEXT NOT NULL DEFAULT 'unset'
                           CHECK (status IN ('unset', 'active', 'revoked')),
    CONSTRAINT invoice_provider_accounts_configured_has_secret_ref
        CHECK (status = 'unset' OR secret_ref IS NOT NULL),

    last_verify_result     TEXT NULL
                           CHECK (last_verify_result IS NULL
                                  OR last_verify_result IN ('success', 'failure', 'not_verifiable')),
    last_verify_at         TIMESTAMPTZ NULL,

    created_at             TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at             TIMESTAMPTZ NOT NULL DEFAULT now()
);

COMMENT ON COLUMN ms_billing.invoice_provider_accounts.secret_ref IS
    'Opaque reference into the sealing broker. Never the key itself and '
    'never a hint such as a last-four. The engine process holds this ref, '
    'not the credential.';

-- "A seller has exactly one active provider account" (fix direction #5) as a
-- database constraint, same taste as intent_settlement_claims's PK in 054:
-- a second active account is an integrity violation, not a race the service
-- layer has to win. Switching provider is therefore necessarily a two-step
-- migration (revoke, then put) rather than an implicit failover.
CREATE UNIQUE INDEX IF NOT EXISTS invoice_provider_accounts_one_active_per_seller
    ON ms_billing.invoice_provider_accounts (seller_id)
    WHERE status = 'active' AND scope = 'seller';

-- Mirror invariant for the shared platform account: at most one active
-- platform-scope account per provider per environment.
CREATE UNIQUE INDEX IF NOT EXISTS invoice_provider_accounts_one_active_platform
    ON ms_billing.invoice_provider_accounts (provider, environment)
    WHERE status = 'active' AND scope = 'platform';

CREATE INDEX IF NOT EXISTS invoice_provider_accounts_seller_idx
    ON ms_billing.invoice_provider_accounts (seller_id) WHERE seller_id IS NOT NULL;

CREATE TRIGGER invoice_provider_accounts_set_updated_at
BEFORE UPDATE ON ms_billing.invoice_provider_accounts
FOR EACH ROW
EXECUTE FUNCTION ms_billing.set_updated_at();

-- ---------------------------------------------------------------------------
-- invoice_credential_audit — append-only (fix direction #8)
-- ---------------------------------------------------------------------------
--
-- Every identity or key write appends here: actor, actor_kind, reason, time.
-- Never the key material itself — verb + actor + reason is the whole point;
-- an operator can revoke but, per D525, never reads or enters keys, so this
-- table never carries anything a key-reading operator could misuse.
CREATE TABLE IF NOT EXISTS ms_billing.invoice_credential_audit (
    id                  BIGSERIAL PRIMARY KEY,

    seller_id           UUID NOT NULL
                        REFERENCES ms_billing.invoice_sellers(id) ON DELETE CASCADE,
    provider_account_id UUID NULL
                        REFERENCES ms_billing.invoice_provider_accounts(id) ON DELETE SET NULL,

    verb                TEXT NOT NULL
                        CHECK (verb IN (
                            'PutInvoiceSellerIdentity',
                            'PutInvoiceProviderKeys',
                            'RevokeInvoiceProviderKeys',
                            'VerifyInvoiceCredentials'
                        )),

    actor               TEXT NOT NULL CHECK (actor <> ''),
    actor_kind          TEXT NOT NULL CHECK (actor_kind IN ('operator', 'org_owner')),
    reason              TEXT NOT NULL CHECK (reason <> ''),

    occurred_at         TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS invoice_credential_audit_seller_idx
    ON ms_billing.invoice_credential_audit (seller_id, occurred_at DESC);

-- Rate-limiting VerifyInvoiceCredentials (fix direction #3: default 6/hour)
-- reads this index: COUNT(*) WHERE seller_id = $1 AND verb =
-- 'VerifyInvoiceCredentials' AND occurred_at > now() - interval '1 hour'.
CREATE INDEX IF NOT EXISTS invoice_credential_audit_seller_verb_idx
    ON ms_billing.invoice_credential_audit (seller_id, verb, occurred_at DESC);

-- Append-only, enforced by the database rather than by convention — same
-- shape as charge_intents_reject_sealed_update in 054. An audit log a buggy
-- caller can edit after the fact is not an audit log.
CREATE OR REPLACE FUNCTION ms_billing.invoice_credential_audit_reject_mutation()
RETURNS TRIGGER AS $$
BEGIN
    RAISE EXCEPTION
        'invoice_credential_audit is append-only: no UPDATE or DELETE';
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER invoice_credential_audit_append_only
    BEFORE UPDATE OR DELETE ON ms_billing.invoice_credential_audit
    FOR EACH ROW EXECUTE FUNCTION ms_billing.invoice_credential_audit_reject_mutation();
