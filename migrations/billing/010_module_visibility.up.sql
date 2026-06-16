-- Migration 010 — per-module developer margin-share dimension.
--
-- Milestone D, Axis 3. Mirrors a module's published/private visibility,
-- fed by an internal SetModuleVisibility RPC on publish/unpublish. Its
-- ONLY use is the DEVELOPER margin-share at settlement:
--   published → platform takes 15% of (income - infra)
--   private   → platform takes 30% of (income - infra)
-- This is settled OFF the customer's bill. It is NEVER a customer
-- markup: the customer is always charged the flat 1.2x regardless of a
-- module's visibility (design §4 Axis 3, §7-B unknown-visibility note).
--
-- DEFAULT-PRIVATE on unknown: when the visibility mirror lags a publish,
-- default to private (30% platform take) so the platform never
-- UNDER-collects its cut; recompute on the next SetModuleVisibility,
-- which must fire on every publish. The settlement recompute itself
-- lands with the developer-settlement rollup in PR #5; this PR ships the
-- mirror table + its upsert only.
--
-- Spec: mirrorstack-docs/db/ms_billing/tables.md#module_visibility

CREATE TABLE IF NOT EXISTS ms_billing.module_visibility (
    -- Soft FK to the platform module id (ms_applications). One row per
    -- module; PK is the module id itself.
    module_id  UUID PRIMARY KEY,

    -- private (30% take) / published (15% take). DEFAULT private = the
    -- safer "never under-collect" state when a publish hasn't synced.
    visibility ms_billing.margin_share_class NOT NULL DEFAULT 'private',

    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Auto-maintained updated_at, matching the ms_billing convention.
CREATE TRIGGER module_visibility_set_updated_at
BEFORE UPDATE ON ms_billing.module_visibility
FOR EACH ROW
EXECUTE FUNCTION ms_billing.set_updated_at();
