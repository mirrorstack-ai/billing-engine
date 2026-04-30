-- Run once after docker compose up to create schemas and apply migrations.
-- Usage: psql -h localhost -U mirrorstack -d mirrorstack -f scripts/init-db.sql

CREATE SCHEMA IF NOT EXISTS ms_billing_account;

-- Apply account migrations in order:
\i migrations/account/001_billing_accounts.up.sql
\i migrations/account/002_subscriptions.up.sql
\i migrations/account/003_subscription_items.up.sql
\i migrations/account/004_invoices_mirror.up.sql
\i migrations/account/005_payment_methods_mirror.up.sql
\i migrations/account/006_webhook_events_processed.up.sql
