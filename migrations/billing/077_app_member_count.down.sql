-- 077 down: drop the member-count history, the member count and the created
-- plan (their CHECK constraints go with them).
DROP TABLE IF EXISTS ms_billing.app_member_counts;
ALTER TABLE ms_billing.apps
    DROP COLUMN IF EXISTS member_count;
ALTER TABLE ms_billing.apps
    DROP COLUMN IF EXISTS created_plan;
