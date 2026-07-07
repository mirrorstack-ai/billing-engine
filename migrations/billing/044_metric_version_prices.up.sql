-- Migration 044 — per-VERSION immutable price snapshot + reproducibility
-- window columns (usage-time pricing, Phase 1).
--
-- SUPERSEDES the exploration in fix/peak-multiversion-overcharge (#58, on
-- hold): #58 collapsed module_version out of the peak/time_weighted rollup
-- to stop a Σ-per-version double-COUNT. That is correct but coarse — the
-- deeper bug is that price itself is resolved version-BLIND: metric_definitions
-- is UNIQUE(module_id, metric), no version, no effective-date, so a module
-- publishing a new version at a new price RETROACTIVELY re-bills usage that
-- already accrued under the OLD version at the NEW rate (the in-place
-- UpsertMetricDefinition destroys the old price the instant a new one lands).
-- See docs-temp/usage-time-pricing/design.md for the full model.
--
-- THE FIX: bind price to WHEN the usage happened, not to whatever the
-- catalog says NOW.
--
-- 1) ms_billing.metric_version_prices — an immutable, INSERT-ONLY price
--    snapshot per (module_id, metric, module_version), written ONCE at
--    version publish (api-platform's SetMetricVersionPrices control-plane
--    call, mirroring SetMetricDefinitions' manifest sync). Resolved
--    VERSION-FIRST at rollup (LookupMetricVersionPrice), with the existing
--    metric_definitions catalog row as the fallback for module_version=''
--    events (pre-stamping) or any version with no snapshot for whatever
--    legacy reason. There is deliberately NO UPDATE path: the service layer
--    upserts with ON CONFLICT DO NOTHING, so a mid-period re-price of a
--    LATER version can never retroactively change an EARLIER version's
--    already-billed rate — publishing v0.1.3 at $0.05 leaves v0.1.2's
--    already-snapshotted $0.02 untouched forever.
--
-- 2) ms_billing.usage_aggregates.active_seconds / period_days — nullable
--    audit/reproducibility columns snapshotting the per-version ACTIVE
--    WINDOW (the LEAD-segmented duration a version's samples covered) and
--    the period length in days, so a closed invoice can re-derive the exact
--    per-version window-prorated charge (freeze-on-close, migration 009
--    invariant) without re-deriving it from usage_events. NULL for the
--    additive kinds (count/sum) — proration never applies to them; they stay
--    a plain per-version SUM, unprorated and unchanged by this migration.
--
-- Spec: docs-temp/usage-time-pricing/design.md; mirrorstack-docs/db/ms_billing/
--       tables.md (metric_version_prices, usage_aggregates.active_seconds /
--       period_days) — companion doc update tracked alongside this PR.

-- 1) The per-version immutable price snapshot. No updated_at, no UPDATE
--    trigger: this table is insert-only by design (see header) — the ONLY
--    write path is an upsert with ON CONFLICT (module_id, metric,
--    module_version) DO NOTHING, so a duplicate publish of the same version
--    is a no-op, never an overwrite.
CREATE TABLE IF NOT EXISTS ms_billing.metric_version_prices (
    id                UUID PRIMARY KEY DEFAULT gen_random_uuid(),

    -- Soft FK to the platform module id (ms_applications), matching
    -- metric_definitions.module_id (migration 006).
    module_id         UUID NOT NULL,
    metric            TEXT NOT NULL,
    module_version    TEXT NOT NULL,

    -- The customer price in effect for this (module, metric, version) at
    -- the instant it was published. Micro-dollars, never float. NOT NULL:
    -- a row exists only to price a version (an unpriced version simply has
    -- no row and falls back to the metric_definitions catalog).
    unit_price_micros BIGINT NOT NULL CHECK (unit_price_micros >= 0),

    created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT metric_version_prices_module_metric_version_key
        UNIQUE (module_id, metric, module_version)
);

-- 2) Reproducibility snapshot columns on usage_aggregates. NULL for
--    count/sum rows (proration never applies — see header); populated for
--    peak/time_weighted rows by the reworked RollupPeakKind /
--    RollupTimeWeightedKind. Matches the NUMERIC convention billable_quantity
--    already uses on this table (migration 009); nullable (no DEFAULT 0,
--    unlike billable_quantity) because "not applicable" and "zero" are
--    distinct here — a genuinely zero-length window is real data, absence
--    (additive kinds) is not.
ALTER TABLE ms_billing.usage_aggregates
    ADD COLUMN active_seconds NUMERIC NULL CHECK (active_seconds IS NULL OR active_seconds >= 0),
    ADD COLUMN period_days    NUMERIC NULL CHECK (period_days    IS NULL OR period_days    >= 0);
