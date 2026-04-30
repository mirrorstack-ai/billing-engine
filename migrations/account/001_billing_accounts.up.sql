CREATE SCHEMA IF NOT EXISTS ms_billing_account;

CREATE TABLE IF NOT EXISTS ms_billing_account.billing_accounts (
    id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    owner_type          TEXT NOT NULL CHECK (owner_type IN ('user', 'org')),
    owner_id            UUID NOT NULL,
    stripe_customer_id  TEXT NOT NULL UNIQUE,
    currency            TEXT NOT NULL CHECK (currency IN ('USD', 'TWD', 'EUR')),
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (owner_type, owner_id)
);

CREATE INDEX idx_billing_accounts_owner ON ms_billing_account.billing_accounts (owner_type, owner_id);
