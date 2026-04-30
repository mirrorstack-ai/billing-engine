-- Local mirror of Stripe invoices. Stripe is the source of truth; this table
-- exists to power read paths (history page, dashboards) without round-tripping
-- to Stripe on every request. `total` is stored in the smallest currency unit
-- (cents for USD/EUR, no fractional unit for TWD — Stripe still uses integer).
CREATE TABLE IF NOT EXISTS ms_billing_account.billing_invoices (
    id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    billing_account_id  UUID NOT NULL REFERENCES ms_billing_account.billing_accounts(id) ON DELETE CASCADE,
    stripe_invoice_id   TEXT NOT NULL UNIQUE,
    status              TEXT NOT NULL,
    total               BIGINT NOT NULL,
    currency            TEXT NOT NULL,
    hosted_invoice_url  TEXT NOT NULL DEFAULT '',
    period_start        TIMESTAMPTZ NOT NULL,
    period_end          TIMESTAMPTZ NOT NULL,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_billing_invoices_account ON ms_billing_account.billing_invoices (billing_account_id);
CREATE INDEX idx_billing_invoices_status ON ms_billing_account.billing_invoices (status);
