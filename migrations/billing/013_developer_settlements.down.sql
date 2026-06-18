-- Unqualified index name (an index drops from its table's schema).
DROP INDEX IF EXISTS developer_settlements_account_idx;
DROP TABLE IF EXISTS ms_billing.developer_settlements;
-- margin_share_class is NOT dropped here: it is owned by migration 009
-- (where usage_aggregates first references it) and still referenced by
-- module_visibility (010). This table only consumes the existing type.
