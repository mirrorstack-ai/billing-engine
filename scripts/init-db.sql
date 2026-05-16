-- Run once after docker compose up to create schemas and apply migrations.
-- Usage: psql -h localhost -U mirrorstack -d mirrorstack -f scripts/init-db.sql

-- Apply billing migrations in order:
\i migrations/billing/001_init.up.sql
