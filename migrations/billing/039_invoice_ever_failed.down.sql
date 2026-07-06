-- 039 down: drop the sticky per-invoice failure marker.
ALTER TABLE ms_billing.invoices
    DROP COLUMN IF EXISTS ever_failed;
