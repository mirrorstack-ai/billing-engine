-- Migration 037 — freeze the app display name in the billing mirror.
--
-- ms_billing.apps was an existence mirror keyed on app_id with NO human name
-- (billing-engine deliberately held no app names — the display name was
-- resolved downstream from the live app registry by app_id). That breaks the
-- moment an app is DELETED: it vanishes from the registry, so its historical
-- bill rows can no longer resolve a name and render as "unknown app".
--
-- Freezing the name here makes the bill self-contained (the same posture as
-- created_module_count / app_base_snapshots): RegisterApp stamps the initial
-- name, SyncAppModules updates it while the app is live, and it is NEVER
-- cleared on delete — so a deleted app's bill still shows its last-known name.
--
-- Nullable: pre-migration rows and any RegisterApp caller that omits the name
-- stay NULL (surfaced as empty → the frontend's existing registry fallback).

ALTER TABLE ms_billing.apps
    ADD COLUMN name TEXT NULL;

COMMENT ON COLUMN ms_billing.apps.name IS
    'App display name, frozen from RegisterApp''s payload and updated by SyncAppModules while the app is live (gated on deleted_at IS NULL); NEVER cleared on delete — this is what lets a deleted app''s historical bill still show its name. NULL for pre-037 rows / callers that omit it.';
