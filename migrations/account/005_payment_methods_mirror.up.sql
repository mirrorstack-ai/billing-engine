-- Local mirror of Stripe payment methods. Only non-sensitive display fields
-- are stored (brand + last4 + expiry). Full PAN/CVC never touch this DB —
-- Stripe holds those.
CREATE TABLE IF NOT EXISTS ms_billing_account.billing_payment_methods (
    id                          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    billing_account_id          UUID NOT NULL REFERENCES ms_billing_account.billing_accounts(id) ON DELETE CASCADE,
    stripe_payment_method_id    TEXT NOT NULL UNIQUE,
    brand                       TEXT NOT NULL,
    last4                       TEXT NOT NULL,
    exp_month                   INT NOT NULL,
    exp_year                    INT NOT NULL,
    is_default                  BOOLEAN NOT NULL DEFAULT false,
    created_at                  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_billing_payment_methods_account ON ms_billing_account.billing_payment_methods (billing_account_id);
CREATE UNIQUE INDEX idx_billing_payment_methods_default
    ON ms_billing_account.billing_payment_methods (billing_account_id)
    WHERE is_default;
