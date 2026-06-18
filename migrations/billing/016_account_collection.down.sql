-- Revert migration 016.

-- Restore the billing_runs status CHECK to its pre-016 vocabulary (drop
-- 'skipped_prepaid' + 'skipped_ceiling'). Any rows in those states must be
-- reconciled before a down-migration in a live DB; in dev the table is empty.
-- IF EXISTS keeps the down idempotent if the up only partially applied (the
-- constraint was never re-added).
ALTER TABLE ms_billing.billing_runs
    DROP CONSTRAINT IF EXISTS billing_runs_status_check;
ALTER TABLE ms_billing.billing_runs
    ADD CONSTRAINT billing_runs_status_check
        CHECK (status IN ('pending', 'invoiced', 'skipped_no_pm', 'failed'));

-- Drop the account collection columns, then the enum type (no column references
-- it once dropped).
ALTER TABLE ms_billing.accounts
    DROP COLUMN IF EXISTS spend_ceiling_micros,
    DROP COLUMN IF EXISTS credit_limit_micros,
    DROP COLUMN IF EXISTS usage_billing_mode;

DROP TYPE IF EXISTS ms_billing.usage_billing_mode;
