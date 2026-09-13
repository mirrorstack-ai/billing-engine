-- 077 down: drop the member count (its CHECK constraint goes with it).
ALTER TABLE ms_billing.apps
    DROP COLUMN IF EXISTS member_count;
