-- Migration 001 — bootstrap the ms_billing schema and the polymorphic
-- billing-entity table.
--
-- Matches the design in mirrorstack-docs PR #5
-- (db/ms_billing/{README,tables,migrations}.md).
--
-- Schema:        ms_billing
-- Table:         accounts
-- Polymorphism:  owner_kind in ('user', 'org'); paired-but-exclusive
--                owner_user_id / owner_org_id columns; CHECK enforces
--                exactly-one. owner_org_id is forward-declared — the
--                ms_account.orgs table doesn't ship until Stage 2 of
--                the org-layer rollout.

CREATE SCHEMA IF NOT EXISTS ms_billing;

CREATE TABLE IF NOT EXISTS ms_billing.accounts (
    id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),

    -- Discriminator + paired-and-exclusive owner FK columns. Explicit
    -- discriminator (vs deriving from "which column is NULL") gives
    -- queries an indexed predicate and leaves room for a future
    -- 'service_account' kind without restructuring.
    owner_kind          TEXT NOT NULL CHECK (owner_kind IN ('user', 'org')),
    owner_user_id       UUID NULL,    -- soft FK to ms_account.users.id
    owner_org_id        UUID NULL,    -- soft FK to ms_account.orgs.id (forward-declared)

    -- Stripe identity anchor. NULL until first paid action triggers
    -- a Stripe Customer create. UNIQUE so webhook lookups by Customer
    -- ID are deterministic.
    stripe_customer_id  TEXT NULL UNIQUE,

    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now(),

    -- Exactly-one-owner. Captures three invariants in one expression:
    -- a populated FK column matches owner_kind; only one is populated;
    -- there is no "neither set" state.
    CONSTRAINT accounts_owner_check CHECK (
        (owner_kind = 'user' AND owner_user_id IS NOT NULL AND owner_org_id IS NULL)
        OR
        (owner_kind = 'org'  AND owner_org_id  IS NOT NULL AND owner_user_id IS NULL)
    )
);

-- Partial indexes: each row sets exactly one of the two FK columns;
-- a non-partial index would carry NULLs that never serve a query.
CREATE INDEX IF NOT EXISTS accounts_owner_user_idx
    ON ms_billing.accounts (owner_user_id) WHERE owner_user_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS accounts_owner_org_idx
    ON ms_billing.accounts (owner_org_id)  WHERE owner_org_id  IS NOT NULL;

-- Auto-maintained updated_at, matching the ms_account convention.
CREATE OR REPLACE FUNCTION ms_billing.set_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER accounts_set_updated_at
BEFORE UPDATE ON ms_billing.accounts
FOR EACH ROW
EXECUTE FUNCTION ms_billing.set_updated_at();
