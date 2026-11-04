-- Migration 093 — the buyer invoice profile (billing-engine#285, D530-D551).
--
-- Taiwan e-invoice data, rail-neutral: 統編 (company tax id), carrier, 愛心碼
-- donation code. One row per billing account — never per payment rail. The
-- policy this table encodes is docs/adr/0001-taiwan-einvoice-platform-service.md;
-- EInvoicePolicyRevision (internal/account/einvoice) is the revision string
-- every later invoice row (#287) and payment fact (#313) stamps. This
-- migration is DATA ONLY — no issuance, no provider call, sandbox only until
-- the Taiwan company exists (D504, D520).
--
-- ms_billing.accounts(id) is a REAL FK (same schema, unlike the soft
-- ms_account FKs elsewhere in this tree) — ON DELETE CASCADE so the profile
-- never outlives the account it describes (data-handling rule: it is
-- deleted with the account unless an issued invoice already copied it,
-- which #287's snapshot makes true once that table exists).
--
-- taiwan_tax_id_checksum_valid implements the published 財政部 統一編號
-- check-digit algorithm (weights 1,2,1,2,1,2,4,1; sum each product's own
-- digit-sum; valid at total%10=0, OR at the documented 7th-digit-is-7
-- exception, (total+1)%10=0). Researched default (D526) — written here as a
-- CHECK, not just a Go-side validator, so a bad 統編 cannot reach the table
-- through any future write path, ORM, or psql session, not only through the
-- one Go package that happens to call it first today.
CREATE OR REPLACE FUNCTION ms_billing.taiwan_tax_id_checksum_valid(tax_id TEXT)
RETURNS BOOLEAN
LANGUAGE sql
IMMUTABLE
AS $$
    SELECT CASE
        WHEN tax_id !~ '^[0-9]{8}$' THEN false
        ELSE (
            WITH d AS (
                SELECT (substring(tax_id FROM n FOR 1))::int AS digit,
                       (ARRAY[1,2,1,2,1,2,4,1])[n] AS weight,
                       n
                FROM generate_series(1, 8) AS n
            ),
            p AS (
                SELECT digit, n, digit * weight AS product
                FROM d
            ),
            s AS (
                SELECT sum(product / 10 + product % 10) AS total,
                       max(digit) FILTER (WHERE n = 7) AS seventh_digit
                FROM p
            )
            SELECT (total % 10 = 0) OR (seventh_digit = 7 AND (total + 1) % 10 = 0)
            FROM s
        )
    END;
$$;

CREATE TABLE IF NOT EXISTS ms_billing.buyer_invoice_profiles (
    account_id     UUID PRIMARY KEY REFERENCES ms_billing.accounts(id) ON DELETE CASCADE,

    -- Document-recipient kind. NOT the seller's document kind (tax_invoice /
    -- receipt / none, billing-engine#312) — that is a seller-level fact this
    -- table does not hold. This is only whether the invoice names a person
    -- or a company.
    kind           TEXT NOT NULL,

    tax_id         TEXT NULL,
    company_name   TEXT NULL,
    address        TEXT NULL,
    email          TEXT NULL,

    -- Neutral enum; each vendor adapter (billing-engine#286) maps it to its
    -- own wire codes.
    carrier_type   TEXT NULL,
    carrier_num    TEXT NULL,

    -- 愛心碼: routes the invoice to a donation recipient instead of the buyer.
    donation_code  TEXT NULL,

    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT buyer_invoice_profiles_kind_known
        CHECK (kind IN ('personal', 'company')),

    -- company: a valid checksummed 統編 and a name; never a carrier or
    -- donation code (those route a PERSONAL invoice; a company invoice goes
    -- to the company under its own tax id, D547).
    CONSTRAINT buyer_invoice_profiles_company_shape
        CHECK (
            kind <> 'company'
            OR (
                tax_id IS NOT NULL
                AND ms_billing.taiwan_tax_id_checksum_valid(tax_id)
                AND company_name IS NOT NULL
                AND btrim(company_name) <> ''
                AND carrier_type IS NULL
                AND carrier_num IS NULL
                AND donation_code IS NULL
            )
        ),

    -- personal: never a 統編 (only a company row may carry one).
    CONSTRAINT buyer_invoice_profiles_personal_no_tax_id
        CHECK (kind <> 'personal' OR tax_id IS NULL),

    -- A carrier routes the invoice copy to the buyer; a donation code routes
    -- it to a charity. Both name a different recipient for the same
    -- document, so at most one may be set (MOF rule; every vendor enforces
    -- this too).
    CONSTRAINT buyer_invoice_profiles_carrier_donation_exclusive
        CHECK (carrier_type IS NULL OR donation_code IS NULL),

    CONSTRAINT buyer_invoice_profiles_carrier_type_known
        CHECK (carrier_type IS NULL OR carrier_type IN ('mobile_barcode', 'citizen_cert', 'vendor_member')),

    CONSTRAINT buyer_invoice_profiles_carrier_num_required
        CHECK (carrier_type IS NULL OR btrim(coalesce(carrier_num, '')) <> ''),

    CONSTRAINT buyer_invoice_profiles_carrier_num_absent_without_type
        CHECK (carrier_type IS NOT NULL OR carrier_num IS NULL),

    -- Mobile-barcode shape per the vendors' public docs (ezPay manual v1.2.1;
    -- a "1.2.2" referenced in older internal text is UNVERIFIED): '/' plus 7
    -- characters of 0-9 A-Z + - . . citizen_cert and vendor_member formats
    -- are vendor-specific and UNVERIFIED here; the adapter (#286) does that
    -- shape check before issuance.
    CONSTRAINT buyer_invoice_profiles_mobile_barcode_shape
        CHECK (carrier_type IS DISTINCT FROM 'mobile_barcode' OR carrier_num ~ '^/[0-9A-Z+.-]{7}$'),

    -- 愛心碼: 3 to 7 digits (researched default, D526).
    CONSTRAINT buyer_invoice_profiles_donation_code_shape
        CHECK (donation_code IS NULL OR donation_code ~ '^[0-9]{3,7}$')
);

CREATE TRIGGER buyer_invoice_profiles_set_updated_at
BEFORE UPDATE ON ms_billing.buyer_invoice_profiles
FOR EACH ROW
EXECUTE FUNCTION ms_billing.set_updated_at();

COMMENT ON TABLE ms_billing.buyer_invoice_profiles IS
    'Rail-neutral buyer invoice profile (統編, carrier, 愛心碼), one per billing account. Snapshotted (copied, not referenced) onto each payment fact and invoice row when those tables exist (#313, #287) so an edit here never changes an issued invoice.';
COMMENT ON COLUMN ms_billing.buyer_invoice_profiles.kind IS
    'personal or company — who the invoice document names. Never the seller''s tax-bureau document kind (#312).';
COMMENT ON COLUMN ms_billing.buyer_invoice_profiles.carrier_type IS
    'Neutral enum (mobile_barcode | citizen_cert | vendor_member); each vendor adapter (#286) maps it to its own codes.';
