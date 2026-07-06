-- 038 down: drop the fraud-flag columns from the payment-method mirror.
ALTER TABLE ms_billing.payment_methods_mirror
    DROP COLUMN IF EXISTS fraud_blocked,
    DROP COLUMN IF EXISTS fraud_reason,
    DROP COLUMN IF EXISTS fraud_flagged_at;
