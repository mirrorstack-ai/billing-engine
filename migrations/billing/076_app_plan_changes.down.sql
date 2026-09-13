-- 076 down: drop the plan-change ledger (its indexes go with it) and restore
-- the migration-031 wording of the skip marker's comment.
DROP TABLE IF EXISTS ms_billing.app_plan_changes;

COMMENT ON COLUMN ms_billing.apps.proration_skipped_at IS
    'Set once (never unset) when ChargeCreationProration determines the '
    'account only activated at/after this app''s anchored creation period had '
    'already closed — a would-be retroactive catch-up charge (D1d). The app '
    'is permanently excluded from the proration sweep from then on.';
