-- Migration 088 — the TAX LINE on the invoice mirror (core-v2#250).
--
-- Every list price is NET (未稅); tax is a separate itemized line fixed at
-- invoice issuance. These columns record, on the mirror row, the
-- TaxDetermination the invoice's sealed ChargeIntent carried, so the invoice
-- history can show the line and reconciliation can compare it — without a
-- Stripe round-trip and without the engine computing anything here
-- (docs/DESIGN.md §7: adapters and mirrors never calculate tax).
--
--   * tax_amount        — the determination's amount, whole cents like
--                         amount_due (011). Zero is a real answer.
--   * tax_rate_bps      — the rate the rule row applied, in basis points.
--                         The determination carries no rate today, so every
--                         writer leaves it NULL until a TaxPolicyRevision
--                         supplies one.
--   * tax_jurisdiction  — the authority whose rule was applied.
--   * tax_rule_revision — the frozen rule set named in the sealed intent.
--   * tax_verification  — intent.TaxVerificationClass, the closed SEALABLE
--                         set (the zero value never seals, so never lands).
--   * tax_inclusive     — always false: list prices are net, so tax is never
--                         inside amount_due's line prices. The column exists so
--                         a reader states it rather than assumes it.
--
-- 🔴 NULL is UNKNOWN, not zero. A row mirrored before this migration — or by
-- a writer with no sealed intent behind the invoice — has NULL tax columns and
-- the read returns no tax object; a determined not_applicable row has
-- tax_amount = 0 and says why. The two must never read alike (INV-004), so
-- the determination is all-or-nothing (invoices_tax_determination_whole).
--
-- Writer: the charge spine's UpsertInvoice ONLY, first determination wins
-- (COALESCE on conflict). Webhook reconciliation (ApplyInvoiceStatus) does
-- not name these columns, so a Stripe event can never rewrite the sealed
-- determination.
--
-- Born clean: additive nullable columns plus a constant-default boolean (fast
-- default, no rewrite). No backfill — a historic invoice has no determination
-- to copy, and inventing one is exactly what NULL refuses.
--
-- Spec: mirrorstack-docs/db/ms_billing/tables.md#invoices

ALTER TABLE ms_billing.invoices
    ADD COLUMN IF NOT EXISTS tax_amount        NUMERIC NULL CHECK (tax_amount >= 0),
    ADD COLUMN IF NOT EXISTS tax_rate_bps      INTEGER NULL CHECK (tax_rate_bps >= 0),
    ADD COLUMN IF NOT EXISTS tax_jurisdiction  TEXT NULL,
    ADD COLUMN IF NOT EXISTS tax_rule_revision TEXT NULL,
    ADD COLUMN IF NOT EXISTS tax_verification  TEXT NULL
        CONSTRAINT invoices_tax_verification_class
        CHECK (tax_verification IN ('independently_reproducible', 'provider_attested', 'not_applicable')),
    ADD COLUMN IF NOT EXISTS tax_inclusive     BOOLEAN NOT NULL DEFAULT false
        CONSTRAINT invoices_tax_exclusive CHECK (NOT tax_inclusive);

ALTER TABLE ms_billing.invoices
    DROP CONSTRAINT IF EXISTS invoices_tax_determination_whole;
ALTER TABLE ms_billing.invoices
    ADD CONSTRAINT invoices_tax_determination_whole CHECK (
        (tax_verification IS NULL) = (tax_amount IS NULL)
        AND (tax_verification IS NULL) = (tax_jurisdiction IS NULL)
        AND (tax_verification IS NULL) = (tax_rule_revision IS NULL)
        AND (tax_rate_bps IS NULL OR tax_verification IS NOT NULL)
    );

COMMENT ON COLUMN ms_billing.invoices.tax_verification IS
    'How the sealed tax figure was established (intent.TaxVerificationClass). '
    'NULL = no determination recorded (pre-088 or no sealed intent) — unknown, never zero.';
