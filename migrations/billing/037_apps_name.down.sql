-- 037 down: drop the frozen app-name column.
ALTER TABLE ms_billing.apps
    DROP COLUMN IF EXISTS name;
