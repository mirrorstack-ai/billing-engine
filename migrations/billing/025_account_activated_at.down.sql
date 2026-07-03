-- Down migration 025 — drop the billing-period anchor column. Dropping the
-- column reverts BOTH the ADD COLUMN and the one-time backfill (the frozen
-- anchor values live only in this column), so up/down/up round-trips cleanly.

ALTER TABLE ms_billing.accounts
    DROP COLUMN IF EXISTS activated_at;
