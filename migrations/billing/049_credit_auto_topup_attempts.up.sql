-- Migration 049 — durable automatic credit top-up attempts.
--
-- Migration 048 intentionally shipped only the configuration and the generic
-- ledger vocabulary. This migration adds the attempt facts needed to execute
-- an off-session Stripe charge without ever consulting mutable configuration
-- after the attempt is created.

-- The configuration has always carried the local payment_methods_mirror UUID,
-- but migration 048 stored it as unconstrained text. Make that relationship
-- explicit so an enabled policy cannot point at an arbitrary string. Cards are
-- soft-deleted in normal operation. Deferrable NO ACTION rejects a direct hard
-- delete of a referenced card while still allowing the existing account-level
-- cascades to remove the account, config, card mirror, and ledger in one
-- statement regardless of internal cascade-trigger order.
ALTER TABLE ms_billing.credit_auto_topup_configs
    ALTER COLUMN payment_method_id TYPE UUID
    USING payment_method_id::uuid;

ALTER TABLE ms_billing.credit_auto_topup_configs
    ADD CONSTRAINT credit_auto_topup_configs_payment_method_fkey
        FOREIGN KEY (payment_method_id)
        REFERENCES ms_billing.payment_methods_mirror(id)
        ON DELETE NO ACTION
        DEFERRABLE INITIALLY IMMEDIATE;

-- These values are frozen when the pending ledger row is inserted. A later
-- policy edit, default-card change, or soft delete therefore cannot redirect
-- an already-authorized attempt to a different customer or card.
ALTER TABLE ms_billing.credit_ledger
    ADD COLUMN attempt_payment_method_id UUID NULL,
    ADD COLUMN attempt_stripe_payment_method_id TEXT NULL,
    ADD COLUMN attempt_stripe_customer_id TEXT NULL,
    ADD COLUMN attempt_expires_at TIMESTAMPTZ NULL,
    ADD COLUMN failure_code TEXT NULL;

-- Never cascade a payment-method hard delete into the money journal. This is
-- intentionally deferrable for the same account-delete reason as the config
-- FK above; ordinary/direct payment-method deletion remains rejected.
ALTER TABLE ms_billing.credit_ledger
    ADD CONSTRAINT credit_ledger_attempt_payment_method_fkey
        FOREIGN KEY (attempt_payment_method_id)
        REFERENCES ms_billing.payment_methods_mirror(id)
        ON DELETE NO ACTION
        DEFERRABLE INITIALLY IMMEDIATE;

COMMENT ON COLUMN ms_billing.credit_ledger.attempt_payment_method_id IS
    'Auto-top-up audit snapshot of the selected local payment-method UUID.';
COMMENT ON COLUMN ms_billing.credit_ledger.attempt_stripe_payment_method_id IS
    'Auto-top-up frozen Stripe payment method; never re-resolved from config.';
COMMENT ON COLUMN ms_billing.credit_ledger.attempt_stripe_customer_id IS
    'Auto-top-up frozen Stripe customer; must own the frozen payment method.';
COMMENT ON COLUMN ms_billing.credit_ledger.attempt_expires_at IS
    'End of the bounded in-flight grace. Expired attempts reconcile before retry.';
COMMENT ON COLUMN ms_billing.credit_ledger.failure_code IS
    'Stable terminal payment failure token; NULL while pending or after settlement.';

-- Attempt-only facts are complete on auto-top-up rows and absent everywhere
-- else. Empty Stripe identifiers are rejected as strongly as NULLs.
ALTER TABLE ms_billing.credit_ledger
    ADD CONSTRAINT credit_ledger_auto_topup_attempt_fields_check CHECK (
        (
            type = 'auto_topup'
            AND attempt_payment_method_id IS NOT NULL
            AND NULLIF(BTRIM(attempt_stripe_payment_method_id), '') IS NOT NULL
            AND NULLIF(BTRIM(attempt_stripe_customer_id), '') IS NOT NULL
            AND attempt_expires_at IS NOT NULL
        )
        OR (
            type <> 'auto_topup'
            AND attempt_payment_method_id IS NULL
            AND attempt_stripe_payment_method_id IS NULL
            AND attempt_stripe_customer_id IS NULL
            AND attempt_expires_at IS NULL
            AND failure_code IS NULL
        )
    );

-- A stable failure token exists if and only if an auto-top-up is terminally
-- failed. Paid-is-highest recovery clears it while moving failed -> settled.
ALTER TABLE ms_billing.credit_ledger
    ADD CONSTRAINT credit_ledger_auto_topup_failure_state_check CHECK (
        (
            type = 'auto_topup'
            AND status = 'failed'
            AND NULLIF(BTRIM(failure_code), '') IS NOT NULL
        )
        OR (
            NOT (type = 'auto_topup' AND status = 'failed')
            AND failure_code IS NULL
        )
    );

-- Every attempt gets a strictly positive, bounded recovery grace. The writer
-- supplies created_at and attempt_expires_at from the same captured instant;
-- no application/DB clock skew can silently extend the in-flight guard.
ALTER TABLE ms_billing.credit_ledger
    ADD CONSTRAINT credit_ledger_auto_topup_attempt_window_check CHECK (
        type <> 'auto_topup'
        OR (
            attempt_expires_at > created_at
            AND attempt_expires_at <= created_at + INTERVAL '10 minutes'
        )
    );

-- The account row is the transaction serialization boundary, while this
-- partial unique index is the relational backstop: exactly one durable attempt
-- may be genuinely in flight for an account. An expired row must first become
-- settled/failed before the next pending row can be inserted.
CREATE UNIQUE INDEX credit_ledger_auto_topup_pending_uidx
    ON ms_billing.credit_ledger (account_id)
    WHERE type = 'auto_topup' AND status = 'pending';

CREATE INDEX credit_ledger_auto_topup_recent_idx
    ON ms_billing.credit_ledger (account_id, created_at DESC, id DESC)
    WHERE type = 'auto_topup';
