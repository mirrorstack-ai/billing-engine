-- Migration 096 — the usage source tag holds the four values of the live
-- api-platform contract (billing-engine#368). ADDITIVE ONLY: migration 095 is not
-- edited, no row changes, no column changes.
--
-- api-platform tags every infra usage call with the JSON field `source`:
-- platform_sidebar and member (the console agent, api-platform#1313), module, and
-- sandbox_trial (the capped authoring-sandbox trial, migration 095). Migration 095
-- allowed only sandbox_trial, so every sidebar and member AI event was refused by
-- the CHECK: not recorded, not charged. platform_sidebar, member and module are
-- ORDINARY usage — chargeable, priced live at rollup, counted by the AI budgets,
-- never dev_served and never carrying a run id or a stamped price. Only
-- sandbox_trial keeps the shape CHECKs of 095 (usage_events_sandbox_trial_shape,
-- usage_events_run_id_needs_source), which are untouched here.
--
-- A closed set stays closed: a new source is a migration, not a producer string.
-- Added NOT VALID (no scan while the constraint is being created), then VALIDATEd:
-- every stored row is NULL or sandbox_trial, so the validation can only pass.
ALTER TABLE ms_billing.usage_events
    DROP CONSTRAINT IF EXISTS usage_events_source_known;
ALTER TABLE ms_billing.usage_events
    ADD CONSTRAINT usage_events_source_known
        CHECK (source IS NULL OR source IN ('platform_sidebar', 'member', 'sandbox_trial', 'module')) NOT VALID;
ALTER TABLE ms_billing.usage_events
    VALIDATE CONSTRAINT usage_events_source_known;

COMMENT ON COLUMN ms_billing.usage_events.source IS
    'Migrations 095/096: NULL (no source) or one of platform_sidebar, member, module (ordinary usage: chargeable, priced live at rollup, counted by the AI budgets exactly like an event with no source) or sandbox_trial (a capped trial run MirrorStack pays for, D441: recorded and priced, never charged — the CHECK forces dev_served, so every charging filter excludes it, and only the sandbox budget counts it).';
