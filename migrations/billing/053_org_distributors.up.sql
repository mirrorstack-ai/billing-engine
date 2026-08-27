-- Migration 053 — org distributor links (B2B2B distributor mode)
--
-- Records that customer org C is distributed by org B. This is ATTRIBUTION,
-- and it is deliberately NOT stored on ms_billing.org_billing_designations
-- for two independent reasons:
--
--  1. That table "picks only the FUNDING INSTRUMENT" (migration 041's own
--     words). Its sponsor_account_id feeds
--     `COALESCE(d.sponsor_account_id, a.id) AS funding_account_id`, so
--     anything written there DECIDES WHO PAYS. A customer that signed up
--     through a distributor's white-label domain must not thereby make the
--     distributor liable for its bill; attribution and payment are separate
--     facts and must stay separately settable.
--
--  2. org_billing_designations.funding is NOT NULL. A customer org is bound
--     to its distributor at REGISTRATION (via ?org= or the distributor's org
--     custom domain), long before it has chosen any funding instrument, so a
--     designation row may not exist yet. A link that could only be written
--     alongside a funding choice could never be written at the moment the
--     relationship is actually established.
--
-- REPLACES A DEAD PATH, not a live one. The pre-053 distributor queries
-- (credit_wallet.sql) joined the designation's sponsor_account_id and required
-- `distributor.owner_kind = 'org'`, but the ONLY writer of sponsor_account_id
-- (cycle.SetOrgDesignation) sources it from AccountIDByUser, which is
-- `WHERE owner_kind = 'user'`. The join could never match — the file said so
-- itself ("personal sponsorship never passes this join"). No production row
-- has ever satisfied it, so repointing those queries onto this table cannot
-- change any existing behaviour. See billing-engine#134.

CREATE TABLE IF NOT EXISTS ms_billing.org_distributors (
    -- The CUSTOMER org (C). PK: an org has at most ONE distributor. Re-binding
    -- is an UPDATE, never a second row, so "who distributes C" is a single
    -- unambiguous fact and the reverse lookup can never fan out.
    customer_org_id    UUID PRIMARY KEY,  -- soft FK ms_organizations.orgs.id

    -- The DISTRIBUTOR org (B). Soft FK, same as customer_org_id: ms_billing
    -- never writes outside its own schema and holds no FK into ms_organizations.
    distributor_org_id UUID NOT NULL,

    -- Provenance of the link, for audit. 'registration' = derived from the
    -- org context the customer signed up through (?org= or an org custom
    -- domain); 'manual' = set explicitly by an operator or the distributor.
    source             TEXT NOT NULL DEFAULT 'manual'
                       CHECK (source IN ('registration', 'manual')),

    created_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at         TIMESTAMPTZ NOT NULL DEFAULT now(),

    -- An org cannot distribute itself: that would make every self-owned org
    -- its own distributor and let the authority lookups grant an org
    -- distributor powers over its own wallet.
    CONSTRAINT org_distributors_not_self CHECK (customer_org_id <> distributor_org_id)
);

-- Reverse lookup: every customer of one distributor. Backs
-- ListDistributorCustomerSnapshots and the is_distributor derivation
-- (an org IS a distributor iff it appears here at least once).
CREATE INDEX IF NOT EXISTS org_distributors_distributor_idx
    ON ms_billing.org_distributors (distributor_org_id, customer_org_id);

COMMENT ON TABLE ms_billing.org_distributors IS
    'B2B2B distributor attribution: customer org C is distributed by org B. '
    'Separate from org_billing_designations because that table decides who '
    'PAYS (sponsor_account_id feeds funding_account_id) and requires a '
    'funding choice, while this link is established at registration and must '
    'never by itself move liability.';

COMMENT ON COLUMN ms_billing.org_distributors.source IS
    'registration = derived from the ?org= / org-custom-domain context the '
    'customer signed up through; manual = set explicitly.';
