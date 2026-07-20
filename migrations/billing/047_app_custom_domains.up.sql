-- Migration 047 — custom-domain billing mirror.
--
-- One row represents one activation of a custom hostname. Billing starts at
-- activated_at, stops prospectively at removed_at, and has no free allowance
-- or grace window. The charge-tracking columns make the activation-period
-- sweep idempotent and crash-recoverable in the same way as the per-module
-- timer sweep; steady-state boundary charges derive solely from the live
-- activation interval and deliberately do not depend on charge_resolved.

CREATE TABLE IF NOT EXISTS ms_billing.app_custom_domains (
    -- Stable activation-charge identity. Deterministic Stripe idempotency keys
    -- and the ms_charge_ref metadata anchor derive from this id.
    id                     UUID PRIMARY KEY DEFAULT gen_random_uuid(),

    account_id             UUID NOT NULL REFERENCES ms_billing.accounts(id) ON DELETE CASCADE,
    app_id                 UUID NOT NULL REFERENCES ms_billing.apps(app_id) ON DELETE CASCADE,
    hostname               TEXT NOT NULL,

    -- The custom domain becomes billable at activation (not attachment).
    activated_at           TIMESTAMPTZ NOT NULL,

    -- NULL while live. Removal is prospective: an already-charged period is
    -- never credited, while future boundary counts exclude the row.
    removed_at             TIMESTAMPTZ NULL,

    -- Durable activation-period charge state. charge_attempted_at is stamped
    -- before the first Stripe call so a retry can recover by ms_charge_ref even
    -- after Stripe's idempotency-key retention window. charge_resolved is the
    -- terminal guard for either charged or D1d-forgiven activation periods.
    charge_attempted_at     TIMESTAMPTZ NULL,
    charged_at              TIMESTAMPTZ NULL,
    charge_resolved         BOOLEAN NOT NULL DEFAULT false,
    charge_invoice_id       TEXT NULL,
    charge_invoice_item_id  TEXT NULL,

    created_at              TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- A hostname can have at most one live activation. A removed historical row
-- remains for audit and does not prevent a later re-activation.
CREATE UNIQUE INDEX IF NOT EXISTS app_custom_domains_live_hostname_uidx
    ON ms_billing.app_custom_domains (hostname)
    WHERE removed_at IS NULL;

-- Activation-period sweep work list. Resolved and removed rows drop out of the
-- index; activated_at is the zero-length-grace eligibility cutoff.
CREATE INDEX IF NOT EXISTS app_custom_domains_sweep_idx
    ON ms_billing.app_custom_domains (activated_at, id)
    WHERE removed_at IS NULL AND charge_resolved = false;
