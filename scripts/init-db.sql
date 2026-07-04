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
-- 016: risk-graded collection fields on accounts + billing_runs status (PR #9).
\i migrations/billing/016_account_collection.up.sql
-- 017: platform-infra metric catalog seed (Plane 1, PR #10a).
\i migrations/billing/017_platform_infra_metrics.up.sql
-- 018: AI model price catalog (Plane 1, PR #10b).
\i migrations/billing/018_ai_model_prices.up.sql
-- 019–022: infra catalog hygiene + P1 seed + display groups + compute-alias drop.
\i migrations/billing/019_infra_catalog_hygiene.up.sql
\i migrations/billing/020_p1_infra_catalog_seed.up.sql
\i migrations/billing/021_metric_display_groups.up.sql
\i migrations/billing/022_drop_compute_alias.up.sql
-- 023: module_version usage dimension.
\i migrations/billing/023_usage_module_version.up.sql
-- 024: production billing_svc grants (NOTICE-skips locally — no billing_svc role in dev).
\i migrations/billing/024_billing_svc_grants.up.sql
-- 025: account activation timestamp (was missing here — pre-existing drift).
\i migrations/billing/025_account_activated_at.up.sql
-- 026: Stripe invoice presentment mirror columns (webhook write path requires them).
\i migrations/billing/026_invoice_presentment.up.sql
