-- Migration 002 — local mirror of Stripe payment methods.
--
-- Maintained by billing-engine/cmd/account-webhook on
-- payment_method.attached / payment_method.detached events.
-- Read by Ensure (hot-path gate; partial idx covers the query)
-- and GetPaymentMethods (UI).
--
-- Soft-delete on detach (`deleted_at`) preserves audit + reconciliation
-- + reattachment paths. 90-day operational purge clears stale rows.
--
-- Spec: mirrorstack-docs/db/ms_billing/tables.md#payment_methods_mirror

CREATE TABLE IF NOT EXISTS ms_billing.payment_methods_mirror (
    id                       UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    account_id               UUID NOT NULL REFERENCES ms_billing.accounts(id) ON DELETE CASCADE,
    stripe_payment_method_id TEXT NOT NULL UNIQUE,
    brand                    TEXT NOT NULL,
    last4                    TEXT NOT NULL,
    exp_month                INT  NOT NULL CHECK (exp_month BETWEEN 1 AND 12),
    exp_year                 INT  NOT NULL CHECK (exp_year >= 1970),
    is_default               BOOLEAN NOT NULL DEFAULT false,
    attached_at              TIMESTAMPTZ NOT NULL DEFAULT now(),
    deleted_at               TIMESTAMPTZ NULL
);

CREATE INDEX IF NOT EXISTS pmm_account_active_idx
    ON ms_billing.payment_methods_mirror (account_id)
    WHERE deleted_at IS NULL;
