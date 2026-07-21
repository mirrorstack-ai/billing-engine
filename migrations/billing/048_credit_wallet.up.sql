-- Migration 048 — universal credit wallet.
--
-- credit_ledger is the durable, account-scoped money journal. Monetary rows
-- are never deleted or rewritten; the only in-place transition is the status /
-- presentment enrichment of a pending Stripe-backed purchase when its invoice
-- webhook settles or fails. Amounts are signed integer micro-dollars and the
-- authoritative posted balance is the sum of settled rows.
--
-- A draw may span several funding lots. Each negative draw row therefore
-- points at the positive source row it consumed through source_credit_id; a
-- final NULL-source row represents the deliberately unsecured portion allowed
-- by a credits-mode account's credit policy. This keeps the required ledger
-- append-only while making the expiring-grant -> non-expiring-grant /
-- preallocation -> purchased-credit consumption order auditable.

ALTER TABLE ms_billing.accounts
    ADD COLUMN billing_mode TEXT NOT NULL DEFAULT 'standard';

ALTER TABLE ms_billing.accounts
    ADD CONSTRAINT accounts_billing_mode_check
        CHECK (billing_mode IN ('standard', 'credits'));

COMMENT ON COLUMN ms_billing.accounts.billing_mode IS
    'Universal-wallet policy: standard applies wallet credit before ordinary '
    'collection; credits settles the full boundary through the wallet. Distinct '
    'from usage_billing_mode, which is the arrears/prepaid collection-risk state.';

-- accounts.credit_limit_micros already exists from migration 016. The wallet
-- configuration reuses that non-negative micro-dollar field; callers set the
-- credits-mode default explicitly, so the established standard-account default
-- and risk-collection behaviour remain unchanged.

CREATE TABLE IF NOT EXISTS ms_billing.credit_ledger (
    id                   UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    account_id           UUID NOT NULL REFERENCES ms_billing.accounts(id) ON DELETE CASCADE,

    amount_micros        BIGINT NOT NULL CHECK (amount_micros <> 0),
    type                 TEXT NOT NULL
                         CHECK (type IN (
                             'purchase', 'auto_topup', 'grant', 'preallocation',
                             'usage_draw', 'subscription_draw', 'refund', 'adjustment'
                         )),
    status               TEXT NOT NULL
                         CHECK (status IN ('pending', 'settled', 'failed', 'refunded')),
    balance_after_micros BIGINT NOT NULL,
    actor                TEXT NOT NULL
                         CHECK (actor IN ('self', 'distributor', 'system')),

    -- Globally unique when present, matching Stripe's client/server
    -- idempotency boundary. NULL is reserved for explicitly non-idempotent
    -- administrative adjustments.
    idempotency_key      TEXT NULL,

    -- Stripe-backed purchases attach their finalized invoice and hosted URL.
    -- stripe_invoice_id is additive to the public ledger shape and supplies the
    -- unambiguous webhook -> purchase relationship.
    stripe_invoice_id    TEXT NULL,
    receipt_url          TEXT NULL,

    -- Only positive grants expire. Purchased and auto-top-up credits never do.
    expires_at           TIMESTAMPTZ NULL,

    -- Draws and preallocations may be tied to the first-class billing period.
    period_id            UUID NULL REFERENCES ms_billing.billing_periods(id) ON DELETE SET NULL,

    -- A negative draw consumes at most one funding lot. Multi-lot boundary
    -- draws are represented by multiple rows; NULL is the credits-mode
    -- unsecured remainder.
    source_credit_id     UUID NULL REFERENCES ms_billing.credit_ledger(id) ON DELETE RESTRICT,

    created_at           TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT credit_ledger_expiry_check CHECK (
        expires_at IS NULL OR (type = 'grant' AND amount_micros > 0)
    ),
    CONSTRAINT credit_ledger_funding_sign_check CHECK (
        type NOT IN ('purchase', 'auto_topup', 'grant', 'preallocation')
        OR amount_micros > 0
    ),
    CONSTRAINT credit_ledger_draw_sign_check CHECK (
        type NOT IN ('usage_draw', 'subscription_draw')
        OR amount_micros < 0
    ),
    CONSTRAINT credit_ledger_source_check CHECK (
        source_credit_id IS NULL
        OR (
            source_credit_id <> id
            AND type IN ('usage_draw', 'subscription_draw')
            AND amount_micros < 0
        )
    )
);

-- Client/server idempotency keys and Stripe invoices each identify at most one
-- ledger row. NULL values remain exempt.
CREATE UNIQUE INDEX IF NOT EXISTS credit_ledger_idempotency_key_uidx
    ON ms_billing.credit_ledger (idempotency_key)
    WHERE idempotency_key IS NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS credit_ledger_stripe_invoice_uidx
    ON ms_billing.credit_ledger (stripe_invoice_id)
    WHERE stripe_invoice_id IS NOT NULL;

-- Stable newest-first keyset pagination and account balance scans.
CREATE INDEX IF NOT EXISTS credit_ledger_account_created_idx
    ON ms_billing.credit_ledger (account_id, created_at DESC, id DESC);

CREATE INDEX IF NOT EXISTS credit_ledger_account_settled_idx
    ON ms_billing.credit_ledger (account_id)
    WHERE status = 'settled';

-- Source allocation lookup / FK coverage and period-level draw recovery.
CREATE INDEX IF NOT EXISTS credit_ledger_source_credit_idx
    ON ms_billing.credit_ledger (source_credit_id)
    WHERE source_credit_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS credit_ledger_account_period_idx
    ON ms_billing.credit_ledger (account_id, period_id)
    WHERE period_id IS NOT NULL;

-- A retry can consume a given lot only once for an account-period draw. The
-- credits-mode unsecured remainder is likewise singular. Deterministic
-- idempotency_key values remain the cross-operation/global guard.
CREATE UNIQUE INDEX IF NOT EXISTS credit_ledger_period_source_draw_uidx
    ON ms_billing.credit_ledger (account_id, period_id, type, source_credit_id)
    WHERE period_id IS NOT NULL
      AND source_credit_id IS NOT NULL
      AND type IN ('usage_draw', 'subscription_draw');

CREATE UNIQUE INDEX IF NOT EXISTS credit_ledger_period_unsecured_draw_uidx
    ON ms_billing.credit_ledger (account_id, period_id, type)
    WHERE period_id IS NOT NULL
      AND source_credit_id IS NULL
      AND type IN ('usage_draw', 'subscription_draw');

-- Optional one-row-per-account auto-top-up configuration. A pending
-- auto_topup ledger row is the durable in-flight guard; last-attempt details
-- are derived from the newest such row, so no duplicate mutable state lives
-- here.
CREATE TABLE IF NOT EXISTS ms_billing.credit_auto_topup_configs (
    account_id        UUID PRIMARY KEY REFERENCES ms_billing.accounts(id) ON DELETE CASCADE,
    enabled           BOOLEAN NOT NULL DEFAULT false,
    threshold_micros  BIGINT NOT NULL DEFAULT 0 CHECK (threshold_micros >= 0),
    amount_micros     BIGINT NOT NULL DEFAULT 5000000
                      CHECK (amount_micros BETWEEN 5000000 AND 5000000000),
    payment_method_id TEXT NULL,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT credit_auto_topup_enabled_payment_method_check CHECK (
        NOT enabled OR payment_method_id IS NOT NULL
    )
);

CREATE TRIGGER credit_auto_topup_configs_set_updated_at
BEFORE UPDATE ON ms_billing.credit_auto_topup_configs
FOR EACH ROW
EXECUTE FUNCTION ms_billing.set_updated_at();

