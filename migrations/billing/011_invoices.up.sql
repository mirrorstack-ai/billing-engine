-- Migration 011 — Stripe invoice mirror (the charged-state ledger).
--
-- Milestone D, PR #6 (Stripe charge + cycle binary). One row per Stripe
-- invoice billing-engine creates for an account's closed period: the local
-- mirror of the Stripe-side invoice so the platform can read charge state
-- without a Stripe round-trip. billing-engine is the SOLE writer (it owns the
-- Stripe credentials); api-platform never touches Stripe or this table.
--
-- The charge spine writes one mirror row at invoice-create time (status from
-- the freshly created Stripe invoice). Webhook reconciliation (invoice.created/
-- finalized/paid/payment_failed → ApplyInvoiceStatus, updating status +
-- amount_paid) is PR #7 — this PR ships the table + the create-time mirror only.
--
-- Money is NUMERIC (whole cents at the Stripe boundary — Stripe amounts are
-- integer minor units; micro-dollars are converted to cents round-half-up
-- before they reach Stripe, design §8). stripe_invoice_id is UNIQUE so a
-- webhook lookup by invoice id is deterministic and a re-mirror is idempotent.
--
-- Born clean at slot 011 (one of the two reserved charge-chain slots; the
-- runner applies *.up.sql in filename order with no gap-checking, so this
-- slots in before 013/014/015 by filename sort).
--
-- Spec: mirrorstack-docs/db/ms_billing/tables.md#invoices

CREATE TABLE IF NOT EXISTS ms_billing.invoices (
    id                 UUID PRIMARY KEY DEFAULT gen_random_uuid(),

    account_id         UUID NOT NULL REFERENCES ms_billing.accounts(id) ON DELETE CASCADE,

    -- Stripe identity anchor. UNIQUE so a webhook (PR #7) can look the row up
    -- by Stripe invoice id deterministically and a re-mirror upserts the same
    -- row rather than duplicating it.
    stripe_invoice_id  TEXT NOT NULL UNIQUE,

    -- Stripe invoice status mirrored verbatim (draft/open/paid/uncollectible/
    -- void). TEXT (not an ENUM) so a new Stripe status never needs a migration.
    status             TEXT NOT NULL,

    -- Stripe amounts are integer minor units (cents). Carried as NUMERIC so a
    -- currency with a different minor-unit scale stays representable; the charge
    -- spine writes whole cents.
    amount_due         NUMERIC NOT NULL DEFAULT 0 CHECK (amount_due  >= 0),
    amount_paid        NUMERIC NOT NULL DEFAULT 0 CHECK (amount_paid >= 0),
    currency           TEXT NOT NULL DEFAULT 'usd',

    -- The billing-period window this invoice covers (signup-day anniversary
    -- window), echoed for reconciliation + display. Nullable so a non-period
    -- (manual) invoice can mirror here later without a window.
    period_start       TIMESTAMPTZ NULL,
    period_end         TIMESTAMPTZ NULL,

    created_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at         TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- FK index + per-account invoice list.
CREATE INDEX IF NOT EXISTS invoices_account_idx
    ON ms_billing.invoices (account_id);

-- Auto-maintained updated_at (PR #7 webhook reconciliation updates status +
-- amount_paid in place), matching the 001 accounts convention.
CREATE TRIGGER invoices_set_updated_at
BEFORE UPDATE ON ms_billing.invoices
FOR EACH ROW
EXECUTE FUNCTION ms_billing.set_updated_at();
