-- Run once after docker compose up to create schemas and apply migrations.
-- Usage: psql -h localhost -U mirrorstack -d mirrorstack -f scripts/init-db.sql

-- Apply billing migrations in order:
\i migrations/billing/001_init.up.sql
\i migrations/billing/002_payment_methods_mirror.up.sql
\i migrations/billing/003_webhook_events_processed.up.sql
