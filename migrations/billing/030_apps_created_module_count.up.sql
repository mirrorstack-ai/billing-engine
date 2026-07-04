-- Migration 030 — freeze the module count the creation-proration charge prices
-- (review finding, creation-grace PR #46).
--
-- ms_billing.apps.module_count is a LIVE snapshot: SyncAppModules overwrites it
-- on every install/uninstall. Under creation grace (migration 029) the
-- creation-proration charge no longer fires synchronously at RegisterApp — it
-- is delayed until the app survives GraceDays and the sweep gets to it, which
-- can be days after creation. A customer is free to install/uninstall modules
-- via SyncAppModules during that window, so by the time the sweep reads
-- module_count it may no longer be the count that actually applied on the
-- historical creation-period days being priced — the charge would retroactively
-- apply a tier that never existed on those days.
--
-- created_module_count freezes the count AT REGISTRATION (RegisterApp's INSERT
-- — the same instant that stamps created_at) and is NEVER touched again: it is
-- absent from every SyncAppModules write path. The creation-proration charge
-- (ChargeCreationProration) prices its historical window off THIS column;
-- module_count remains exactly what it was — the LIVE count the boundary
-- advance leg (and the display read for all FUTURE periods) uses.
--
-- Backfill: for rows that predate this migration, the live module_count is the
-- best available approximation of what applied at creation (no per-day history
-- exists to reconstruct it precisely) — acceptable because every pre-migration
-- row not yet proration-charged, if any, is still within its grace/pending
-- window and its module_count has had comparatively little time to drift.

ALTER TABLE ms_billing.apps
    ADD COLUMN created_module_count INT NULL CHECK (created_module_count >= 0);

UPDATE ms_billing.apps
   SET created_module_count = module_count
 WHERE created_module_count IS NULL;

ALTER TABLE ms_billing.apps
    ALTER COLUMN created_module_count SET NOT NULL;

COMMENT ON COLUMN ms_billing.apps.created_module_count IS
    'Module count frozen at RegisterApp time (immutable — SyncAppModules never '
    'writes this column). ChargeCreationProration prices the creation-period '
    'window off THIS value, never the live module_count.';
