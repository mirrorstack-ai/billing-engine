-- Down for 030 — drop the frozen creation-time module-count column. Reverting
-- returns ChargeCreationProration to pricing off the live module_count (the
-- pre-030, retroactive-tier-drift posture).

ALTER TABLE ms_billing.apps
    DROP COLUMN IF EXISTS created_module_count;
