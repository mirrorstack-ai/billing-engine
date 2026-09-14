-- 082 down: drop the bind-failure reason.
ALTER TABLE ms_billing.add_card_requests
    DROP COLUMN IF EXISTS failure_code;
