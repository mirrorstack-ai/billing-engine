-- Unqualified index name (an index drops from its table's schema;
-- a schema-prefixed DROP INDEX names the index, not its schema).
DROP INDEX IF EXISTS billing_periods_account_idx;
DROP TABLE IF EXISTS ms_billing.billing_periods;
DROP TYPE IF EXISTS ms_billing.billing_period_status;
