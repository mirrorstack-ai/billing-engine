DROP INDEX IF EXISTS ms_billing.pmm_account_fingerprint_active_idx;
ALTER TABLE ms_billing.payment_methods_mirror DROP COLUMN IF EXISTS fingerprint;
