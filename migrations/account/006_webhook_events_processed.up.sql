-- Idempotency table for Stripe webhook delivery. Stripe may redeliver the
-- same event; the handler inserts the event id here inside the same
-- transaction as the side effect. A duplicate insert collides on the primary
-- key and the handler short-circuits.
--
-- Intentionally has no FK — it must remain writable even if the row that the
-- event refers to has been deleted, and lookups should never cascade.
CREATE TABLE IF NOT EXISTS ms_billing_account.billing_webhook_events_processed (
    stripe_event_id TEXT PRIMARY KEY,
    processed_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);
