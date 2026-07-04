-- Down for 030 — drop the account-wide pooled overage timer + snapshot ledger.
-- Rolling back reverts overage to whatever the code expects: the display read
-- treats a missing snapshot as "no pooled overage charged" and a missing
-- overage_since column as never-over. Money already charged stays in
-- invoices/billing_runs (unaffected). Drop the table first, then the column.

DROP TABLE IF EXISTS ms_billing.account_overage_snapshots;

ALTER TABLE ms_billing.accounts
    DROP COLUMN IF EXISTS overage_since;
