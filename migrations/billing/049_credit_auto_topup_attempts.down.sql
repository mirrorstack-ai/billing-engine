-- Migration 049 down — remove execution metadata, retaining all ledger rows.
--
-- This is schema-only rollback support for non-production rehearsal. A live
-- money ledger is never rolled back by automation.

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM ms_billing.credit_ledger
        WHERE type = 'auto_topup'
    ) THEN
        RAISE EXCEPTION
            'migration 049 rollback refused: auto_topup ledger audit rows exist';
    END IF;
END
$$;

DROP INDEX IF EXISTS ms_billing.credit_ledger_auto_topup_recent_idx;
DROP INDEX IF EXISTS ms_billing.credit_ledger_auto_topup_pending_uidx;

ALTER TABLE ms_billing.credit_ledger
    DROP CONSTRAINT IF EXISTS credit_ledger_auto_topup_attempt_fields_check,
    DROP CONSTRAINT IF EXISTS credit_ledger_auto_topup_failure_state_check,
    DROP CONSTRAINT IF EXISTS credit_ledger_auto_topup_attempt_window_check,
    DROP CONSTRAINT IF EXISTS credit_ledger_attempt_payment_method_fkey,
    DROP COLUMN IF EXISTS failure_code,
    DROP COLUMN IF EXISTS attempt_expires_at,
    DROP COLUMN IF EXISTS attempt_stripe_customer_id,
    DROP COLUMN IF EXISTS attempt_stripe_payment_method_id,
    DROP COLUMN IF EXISTS attempt_payment_method_id;

ALTER TABLE ms_billing.credit_auto_topup_configs
    DROP CONSTRAINT IF EXISTS credit_auto_topup_configs_payment_method_fkey;

ALTER TABLE ms_billing.credit_auto_topup_configs
    ALTER COLUMN payment_method_id TYPE TEXT
    USING payment_method_id::text;
