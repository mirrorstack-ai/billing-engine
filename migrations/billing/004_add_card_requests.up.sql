-- Migration 004 — add-card request log.
--
-- Tracks individual "add a payment method" attempts initiated via
-- StartAddPaymentMethod. The frontend polls FinishAddPaymentMethod
-- by request_id; the webhook (setup_intent.succeeded +
-- payment_method.attached) resolves the row when Stripe confirms the
-- card has been attached.
--
-- This replaces the previous client-side polling of GetPaymentMethods
-- with set-difference logic: the frontend now correlates against a
-- single durable identifier instead of diffing the user's card list.
--
-- Spec: mirrorstack-docs/api/billing/account-api.md#startaddpaymentmethod
--
-- Operational TTL: setup intents expire after 24h; rows older than
-- 24h with status='pending' can be purged by a future cleanup job
-- (no FK from elsewhere; safe to drop).

CREATE TYPE ms_billing.add_card_request_status AS ENUM (
    'pending',
    'completed',
    'duplicate',
    'failed'
);

CREATE TABLE IF NOT EXISTS ms_billing.add_card_requests (
    id                UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    account_id        UUID NOT NULL REFERENCES ms_billing.accounts(id) ON DELETE CASCADE,
    setup_intent_id   TEXT NULL,
    stripe_pm_id      TEXT NULL,
    status            ms_billing.add_card_request_status NOT NULL DEFAULT 'pending',
    payment_method_id UUID NULL REFERENCES ms_billing.payment_methods_mirror(id) ON DELETE SET NULL,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    resolved_at       TIMESTAMPTZ NULL
);

-- Webhook lookup path: setup_intent.succeeded → request row.
-- Partial index on still-pending rows keeps it tiny (resolved rows
-- accumulate but the index only covers the working set).
CREATE INDEX IF NOT EXISTS acr_setup_intent_pending_idx
    ON ms_billing.add_card_requests (setup_intent_id)
    WHERE status = 'pending';

-- payment_method.attached fallback: when setup_intent.succeeded
-- has already stamped stripe_pm_id but couldn't resolve (no mirror
-- row yet), the attached handler matches by stripe_pm_id.
CREATE INDEX IF NOT EXISTS acr_stripe_pm_pending_idx
    ON ms_billing.add_card_requests (stripe_pm_id)
    WHERE status = 'pending';

-- Index the account_id FK: GetAddCardRequest filters on (id, account_id)
-- and a future TTL sweep / per-account listing scans by account_id. The
-- ON DELETE CASCADE FK also benefits from an index on the referencing
-- column to avoid a seq scan on parent-row deletes.
CREATE INDEX IF NOT EXISTS acr_account_id_idx
    ON ms_billing.add_card_requests (account_id);
