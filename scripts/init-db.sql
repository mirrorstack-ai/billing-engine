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
-- 011–012: the Stripe charge chain (invoices / billing_runs, PR #6).
\i migrations/billing/011_invoices.up.sql
\i migrations/billing/012_billing_runs.up.sql
-- 013: developer-settlement ledger (PR #5).
\i migrations/billing/013_developer_settlements.up.sql
\i migrations/billing/014_budgets.up.sql
\i migrations/billing/015_budget_alerts.up.sql
