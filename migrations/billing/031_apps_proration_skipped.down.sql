-- Down for 031 — drop the permanent-skip marker and restore the migration-029
-- partial index predicate (deleted_at IS NULL AND proration_invoice_id IS
-- NULL only).

DROP INDEX IF EXISTS ms_billing.apps_pending_proration_idx;

CREATE INDEX IF NOT EXISTS apps_pending_proration_idx
    ON ms_billing.apps (created_at)
    WHERE deleted_at IS NULL AND proration_invoice_id IS NULL;

ALTER TABLE ms_billing.apps
    DROP COLUMN IF EXISTS proration_skipped_at;
