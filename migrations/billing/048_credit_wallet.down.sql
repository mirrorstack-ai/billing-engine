-- Migration 048 (down) — remove the universal credit wallet.

DROP TRIGGER IF EXISTS credit_auto_topup_configs_set_updated_at
    ON ms_billing.credit_auto_topup_configs;

DROP TABLE IF EXISTS ms_billing.credit_auto_topup_configs;
DROP TABLE IF EXISTS ms_billing.credit_ledger;

ALTER TABLE ms_billing.accounts
    DROP CONSTRAINT IF EXISTS accounts_billing_mode_check,
    DROP COLUMN IF EXISTS billing_mode;
