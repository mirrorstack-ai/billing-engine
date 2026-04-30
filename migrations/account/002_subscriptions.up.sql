CREATE TABLE IF NOT EXISTS ms_billing_account.billing_subscriptions (
    id                      UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    billing_account_id      UUID NOT NULL REFERENCES ms_billing_account.billing_accounts(id) ON DELETE CASCADE,
    stripe_subscription_id  TEXT NOT NULL UNIQUE,
    status                  TEXT NOT NULL,
    current_period_start    TIMESTAMPTZ NOT NULL,
    current_period_end      TIMESTAMPTZ NOT NULL,
    cancel_at_period_end    BOOLEAN NOT NULL DEFAULT false,
    created_at              TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at              TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_billing_subscriptions_account ON ms_billing_account.billing_subscriptions (billing_account_id);
CREATE INDEX idx_billing_subscriptions_status ON ms_billing_account.billing_subscriptions (status);
