-- 075 down: drop the per-app plan column (its CHECK constraint goes with it).
ALTER TABLE ms_billing.apps
    DROP COLUMN IF EXISTS plan;
