-- Down migration for 012_billing_runs.
--
-- Drops the billing_runs table; its FK index and the UNIQUE constraint drop
-- implicitly with the table.

DROP TABLE IF EXISTS ms_billing.billing_runs;
