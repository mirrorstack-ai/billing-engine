-- Down migration 034 — drop the large auto-collect disclosure columns. Both are
-- pure additive columns (the flag frozen per-invoice, the threshold per-account),
-- so dropping them fully reverts the migration; up/down/up round-trips cleanly.

ALTER TABLE ms_billing.invoices
    DROP COLUMN IF EXISTS is_large_auto_collect;

ALTER TABLE ms_billing.accounts
    DROP COLUMN IF EXISTS auto_collect_threshold_micros;
