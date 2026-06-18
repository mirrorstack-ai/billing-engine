-- Explicit trigger drop before the table for tear-down clarity (Postgres drops
-- it implicitly on DROP TABLE, but being explicit is defensive against schema
-- inspection tools and matches the born-clean migration style).
DROP TRIGGER IF EXISTS budgets_set_updated_at ON ms_billing.budgets;
DROP TABLE IF EXISTS ms_billing.budgets;
DROP TYPE IF EXISTS ms_billing.budget_scope;
