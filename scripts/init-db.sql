-- Run once after docker compose up to create schemas and apply migrations.
-- Usage: psql -h localhost -U mirrorstack -d mirrorstack -f scripts/init-db.sql

-- Apply billing migrations in order:
\i migrations/billing/001_init.up.sql
\i migrations/billing/002_payment_methods_mirror.up.sql
\i migrations/billing/003_webhook_events_processed.up.sql
\i migrations/billing/004_add_card_requests.up.sql
\i migrations/billing/005_payment_methods_fingerprint.up.sql
\i migrations/billing/006_metric_definitions.up.sql
\i migrations/billing/007_usage_events.up.sql
\i migrations/billing/008_billing_periods.up.sql
\i migrations/billing/009_usage_aggregates.up.sql
\i migrations/billing/010_module_visibility.up.sql
-- 011–013 are RESERVED for the meter charge-chain PRs
-- (invoices / billing_runs / developer_settlements) and not in the tree yet;
-- budgets (014–015) only read usage_events × metric_definitions and reference
-- accounts, so they apply cleanly over the gap. See migrations/billing/README.md.
\i migrations/billing/014_budgets.up.sql
\i migrations/billing/015_budget_alerts.up.sql
