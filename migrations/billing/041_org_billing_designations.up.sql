-- Migration 041 — org billing designations + org-owned roster support
-- (org-billing W0 substrate, workspace docs-temp/org-billing/design.md D1).
--
-- One org = ONE ms_billing.accounts row (owner_kind = 'org', schema-ready
-- since 001), created lazily at the org's first funding designation. The
-- designation picks only the FUNDING INSTRUMENT for the org account's
-- invoices — attribution (periods, aggregates, roster, overage pool, budgets,
-- collection state, invoice mirror) always lives on the org account itself:
--
--   funding = 'sponsor' — invoices charge the SPONSOR's Stripe customer /
--   default PM. The sponsor is always the acting org owner/admin's OWN
--   personal account (sponsor_account_id / sponsor_user_id), never another
--   member's wallet. Designating activates the org account immediately
--   (a usable instrument exists), so its ADR-0006 anchor = designation day.
--
--   funding = 'org' — the org account charges its own Stripe customer /
--   card. The designation row may exist BEFORE the card binds; account
--   resolution stays gated on accounts.activated_at (stamped by the
--   payment_method.attached webhook), so the org remains unbilled until the
--   bind completes — the pointer never flips to an unfunded account.
--
-- disclosed_backlog_micros records the pre-designation unbilled-backlog
-- estimate shown to (and confirmed by) the designating user before the
-- RepointOrgUsage sweep folds those events into the account's first open
-- period (org-billing decision 1, 2026-07-06).

CREATE TABLE IF NOT EXISTS ms_billing.org_billing_designations (
    org_id                   UUID PRIMARY KEY,  -- soft FK ms_organizations.orgs.id
    funding                  TEXT NOT NULL CHECK (funding IN ('sponsor', 'org')),
    -- ON DELETE CASCADE: a deleted sponsor account takes its sponsorship with
    -- it — the org drops to unbilled (same terminal state as a self-revoke;
    -- the funding-shape CHECK forbids a dangling sponsor pair).
    sponsor_account_id       UUID NULL REFERENCES ms_billing.accounts(id) ON DELETE CASCADE,
    sponsor_user_id          UUID NULL,         -- soft FK ms_account.users.id
    disclosed_backlog_micros BIGINT NOT NULL DEFAULT 0 CHECK (disclosed_backlog_micros >= 0),
    updated_by               UUID NOT NULL,     -- acting org owner/admin (soft FK users)
    created_at               TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at               TIMESTAMPTZ NOT NULL DEFAULT now(),

    -- sponsor funding carries the sponsor pair; org funding carries neither.
    CONSTRAINT org_designations_funding_check CHECK (
        (funding = 'sponsor' AND sponsor_account_id IS NOT NULL AND sponsor_user_id IS NOT NULL)
        OR
        (funding = 'org' AND sponsor_account_id IS NULL AND sponsor_user_id IS NULL)
    )
);

-- FK covering index (the CASCADE's delete-side scan; also "which orgs does
-- this user sponsor" — the sponsor-lifecycle guard's read).
CREATE INDEX IF NOT EXISTS org_billing_designations_sponsor_account_idx
    ON ms_billing.org_billing_designations (sponsor_account_id)
    WHERE sponsor_account_id IS NOT NULL;

CREATE TRIGGER org_billing_designations_set_updated_at
BEFORE UPDATE ON ms_billing.org_billing_designations
FOR EACH ROW
EXECUTE FUNCTION ms_billing.set_updated_at();

-- Org-owned roster rows. An org app may register BEFORE its org designates
-- funding: the row then carries owner_org_id with a NULL account_id — an
-- UNBILLED roster row (no base fee, no overage timers, excluded from every
-- charge sweep). The RepointOrgUsage sweep attaches it (account_id
-- backfilled, timers synthesized fresh) once the org's funded account
-- resolves. owner_org_id is stamped on EVERY org-owned registration — funded
-- or not — because the repoint sweep scopes NULL-account usage_events to the
-- org through it.
ALTER TABLE ms_billing.apps
    ALTER COLUMN account_id DROP NOT NULL;

ALTER TABLE ms_billing.apps
    ADD COLUMN IF NOT EXISTS owner_org_id UUID NULL;

-- A NULL account is legal ONLY for an org-owned row awaiting designation.
ALTER TABLE ms_billing.apps
    ADD CONSTRAINT apps_unbilled_only_org_check
    CHECK (account_id IS NOT NULL OR owner_org_id IS NOT NULL);

CREATE INDEX IF NOT EXISTS apps_owner_org_idx
    ON ms_billing.apps (owner_org_id) WHERE owner_org_id IS NOT NULL;

-- Backfill audit trail for the RepointOrgUsage sweep. The rollup windows
-- events by recorded_at, so a pre-designation event older than the account's
-- current open window would never fall inside ANY future window — the sweep
-- therefore CLAMPS such an event's recorded_at to the open window's start
-- ("backfilled events bill in the first period that closes after
-- designation", org-billing decision 1) and preserves the original instant
-- here. NULL = never repointed/clamped (every ordinary event).
ALTER TABLE ms_billing.usage_events
    ADD COLUMN IF NOT EXISTS repointed_from TIMESTAMPTZ NULL;
