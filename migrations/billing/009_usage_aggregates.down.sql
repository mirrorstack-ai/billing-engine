-- Unqualified index names (an index drops from its table's schema).
DROP INDEX IF EXISTS usage_aggregates_account_idx;
DROP INDEX IF EXISTS usage_aggregates_period_idx;
DROP TABLE IF EXISTS ms_billing.usage_aggregates;
-- margin_share_class is also referenced by module_visibility (010); its
-- down migration runs first (higher number), so by the time 009 down
-- runs no table references the type and it is safe to drop.
DROP TYPE IF EXISTS ms_billing.margin_share_class;
