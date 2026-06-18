-- Migration 013 — developer-settlement ledger (the stub revenue-share record).
--
-- Milestone D, PR #5 (aggregation + settlement). One row per (period,
-- module): what a third-party developer is OWED for that module's metered
-- income in a billing period, after the platform takes its margin-share.
-- This makes "platform keeps everything" no longer the implicit state — the
-- ledger accrues the developer's cut even though actual PAYOUT (Stripe
-- Connect / transfers) is DEFERRED to the B2B2B milestone (design §8
-- "Developer payout").
--
-- ============================================================
-- SETTLEMENT MODEL — READ BEFORE TOUCHING THIS TABLE
-- ============================================================
-- developer_owed = (income - infra) - platform_take
--   income            = Σ usage_aggregates.charged_micros for the module's
--                       metrics in the period (the customer-billed amount).
--   infra             = the developer's COGS (platform-measured infra cost).
--                       ALWAYS 0 in v1 — platform-infra COGS lands with PR
--                       #10; no infra metric is ingested yet.
--   margin_share      = the PLATFORM's take rate, from module_visibility:
--                         published → 15%  (the module is in the catalog)
--                         private   → 30%  (the module is unlisted)
--                       DEFAULT private (30%) when the visibility is unknown,
--                       so the platform never UNDER-collects on a lagging
--                       publish (design §4 Axis 3 / §7-B).
--   platform_take     = round_half_up(margin_share × (income - infra)).
--   developer_owed    = (income - infra) - platform_take.
--
-- margin_share_class is the SAME developer-settlement enum carried on
-- module_visibility (migration 009/010). It is the developer-side dimension
-- and NEVER multiplies the customer charge — the customer always pays the
-- per-metric customer price (design §4 Axis 3). This ledger settles OFF the
-- customer's bill.
--
-- Money is micro-dollar BIGINT, round-half-up deterministically; never float.
-- ============================================================
--
-- IDEMPOTENT per (period, module): the settlement rollup re-runs upsert the
-- same row (ON CONFLICT) rather than duplicating it, mirroring the
-- usage_aggregates idempotency.
--
-- developer_id is NULLABLE: billing-engine has NO module→developer sync yet
-- (it never learns which developer owns a module), so the row is keyed on
-- module_id and developer_id is backfilled when that sync exists. infra_micros
-- defaults 0 until platform-infra COGS lands (PR #10). status defaults
-- 'accrued' — payout is deferred to B2B2B, so v1 only accrues ledger rows.
--
-- Born clean at slot 013 (011/012 are RESERVED for PR #6 invoices/
-- billing_runs; the runner applies *.up.sql in filename order with no
-- gap-checking, so the reserved gap is intentional).
--
-- Spec: mirrorstack-docs/db/ms_billing/tables.md#developer_settlements

CREATE TABLE IF NOT EXISTS ms_billing.developer_settlements (
    id                 UUID PRIMARY KEY DEFAULT gen_random_uuid(),

    period_id          UUID NOT NULL REFERENCES ms_billing.billing_periods(id) ON DELETE CASCADE,

    -- Denormalized from the period for per-account settlement scans. Soft
    -- semantics: the account the metered income was billed to.
    account_id         UUID NOT NULL,

    -- Soft FK to the platform module id (ms_applications). The settlement
    -- grain together with period_id.
    module_id          UUID NOT NULL,

    -- The third-party developer owed this settlement. NULLABLE: there is no
    -- module→developer sync in billing-engine yet, so the row is keyed on
    -- module_id and developer_id is backfilled when that sync lands.
    developer_id       UUID NULL,

    -- income = Σ charged_micros for the module's metrics in the period.
    income_micros      BIGINT NOT NULL CHECK (income_micros >= 0),

    -- infra = developer COGS (platform-measured). ALWAYS 0 until PR #10.
    infra_micros       BIGINT NOT NULL DEFAULT 0 CHECK (infra_micros >= 0),

    -- Snapshotted margin-share class so a later visibility flip never
    -- rewrites a settled period.
    margin_share_class ms_billing.margin_share_class NOT NULL,

    -- platform_take = round_half_up(margin_share × (income - infra)).
    platform_take_micros  BIGINT NOT NULL CHECK (platform_take_micros >= 0),

    -- developer_owed = (income - infra) - platform_take.
    developer_owed_micros BIGINT NOT NULL CHECK (developer_owed_micros >= 0),

    -- accrued (ledger-only, v1) → paid (payout sent, B2B2B) → void.
    status             TEXT NOT NULL DEFAULT 'accrued'
                       CHECK (status IN ('accrued', 'paid', 'void')),

    created_at         TIMESTAMPTZ NOT NULL DEFAULT now(),

    -- One settlement per (period, module); a rollup re-run upserts the same
    -- row rather than duplicating it.
    CONSTRAINT developer_settlements_period_module_key
        UNIQUE (period_id, module_id)
);

-- Per-account settlement scan (and the FK-style parent lookup on account).
CREATE INDEX IF NOT EXISTS developer_settlements_account_idx
    ON ms_billing.developer_settlements (account_id);
