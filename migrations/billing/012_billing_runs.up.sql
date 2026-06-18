-- Migration 012 — per-(account, period) charge-attempt idempotency.
--
-- Milestone D, PR #6 (Stripe charge + cycle binary). One row per attempt to
-- charge an account for a closed billing period. This is the FIRST of the two
-- idempotency layers protecting the charge spine (the second is deterministic
-- Stripe Idempotency-Keys, applied per Stripe call):
--
--   InsertBillingRun  inserts ON CONFLICT(account_id, period_start, period_end)
--                     DO NOTHING. The insert succeeding (a row returned) is the
--                     "first time" gate — only the first attempt for a window
--                     proceeds to charge. A re-run (cron retry, partial-failure
--                     resume) finds the row already present, the insert is a
--                     no-op, and the cycle skips re-charging.
--
-- Because cmd/billing-cycle runs on a request-scoped Lambda (no always-on
-- runtime), a batch can be interrupted mid-iteration; billing_runs makes the
-- batch RESUMABLE — completed accounts already have their run row, so a re-fire
-- charges only the accounts that hadn't completed (design §8 "Lambda is
-- request-scoped").
--
-- status lifecycle:
--   pending          run row inserted, charge not yet completed (transient)
--   invoiced         charge created + invoice mirrored (terminal success)
--   skipped_no_pm    no usable default payment method → usage RETAINED, the
--                    next cycle re-attempts (NOT a failure, NOT lost usage)
--   failed           charge errored after the PM gate (terminal; PR #7 webhook
--                    reconciliation + risk-graded retry build on this)
--
-- total_amount mirrors the charged total (NUMERIC cents) for the run, so the
-- run row alone answers "what did we bill this account for this period".
-- stripe_invoice_id is NULL until a charge is actually created (zero-arrears
-- and skipped_no_pm runs never touch Stripe).
--
-- Born clean at slot 012 (the second reserved charge-chain slot; the runner
-- applies *.up.sql in filename order with no gap-checking).
--
-- Spec: mirrorstack-docs/db/ms_billing/tables.md#billing_runs

CREATE TABLE IF NOT EXISTS ms_billing.billing_runs (
    id                 UUID PRIMARY KEY DEFAULT gen_random_uuid(),

    account_id         UUID NOT NULL REFERENCES ms_billing.accounts(id) ON DELETE CASCADE,

    period_start       TIMESTAMPTZ NOT NULL,
    period_end         TIMESTAMPTZ NOT NULL,

    status             TEXT NOT NULL DEFAULT 'pending'
                       CHECK (status IN ('pending', 'invoiced', 'skipped_no_pm', 'failed')),

    -- The Stripe invoice this run created, NULL until a charge actually happens
    -- (zero-arrears and skipped_no_pm runs never call Stripe).
    stripe_invoice_id  TEXT NULL,

    -- Charged total for the run (NUMERIC cents). 0 for zero-arrears / skipped.
    total_amount       NUMERIC NOT NULL DEFAULT 0 CHECK (total_amount >= 0),

    created_at         TIMESTAMPTZ NOT NULL DEFAULT now(),

    -- THE idempotency gate: one run per (account, period window). InsertBillingRun
    -- inserts ON CONFLICT DO NOTHING; the conflict (no row returned) means the
    -- window was already attempted, so the cycle does not re-charge.
    CONSTRAINT billing_runs_account_period_key
        UNIQUE (account_id, period_start, period_end)
);

-- FK index + per-account run history scan.
CREATE INDEX IF NOT EXISTS billing_runs_account_idx
    ON ms_billing.billing_runs (account_id);

-- At most one run per Stripe invoice. The UNIQUE(account, period) gate already
-- bounds runs to one per window, but this closes the gap directly: a charged
-- invoice id maps to exactly one run row (partial — pending/skipped/zero-arrears
-- runs hold NULL and are exempt).
CREATE UNIQUE INDEX IF NOT EXISTS billing_runs_stripe_invoice_key
    ON ms_billing.billing_runs (stripe_invoice_id)
    WHERE stripe_invoice_id IS NOT NULL;
