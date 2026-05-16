-- Migration 003 — Stripe webhook idempotency record.
--
-- Maintained by billing-engine/cmd/account-webhook. Every incoming
-- event_id is inserted before any side-effects; the PRIMARY KEY
-- collision short-circuits duplicate processing on Stripe retries.
--
-- event_id is TEXT (Stripe format `evt_<base32>`), not UUID.
-- 90-day operational purge; Stripe doesn't retry past 3 days, so
-- older records are debug-only.
--
-- Spec: mirrorstack-docs/db/ms_billing/tables.md#webhook_events_processed
--       mirrorstack-docs/api/billing/account-webhook.md#idempotency

CREATE TABLE IF NOT EXISTS ms_billing.webhook_events_processed (
    event_id     TEXT PRIMARY KEY,
    event_type   TEXT NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Supports the diagnostic "latest event of type X" query and the
-- retention purge job:
--   DELETE FROM ms_billing.webhook_events_processed
--   WHERE processed_at < now() - INTERVAL '90 days';
CREATE INDEX IF NOT EXISTS wep_type_time_idx
    ON ms_billing.webhook_events_processed (event_type, processed_at);
