-- 083 down: drop the proposal marker.
ALTER TABLE ms_billing.billing_runs
    DROP COLUMN IF EXISTS proposal_attempted_at;
