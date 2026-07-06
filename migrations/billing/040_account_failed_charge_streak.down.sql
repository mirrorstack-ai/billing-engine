-- 040 down: drop the consecutive failed-charge streak column.
ALTER TABLE ms_billing.accounts
    DROP COLUMN IF EXISTS failed_charge_streak;
