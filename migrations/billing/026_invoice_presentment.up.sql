-- Migration 026 — invoice PRESENTMENT columns (Stripe-assigned at finalization).
--
-- Account-billing read wave (ListInvoices). Adds the three customer-facing
-- fields Stripe mints when an invoice is FINALIZED, so the invoices mirror
-- (011) can serve the web-account invoice history without a Stripe round-trip:
--
--   * number             — Stripe's customer-facing invoice number
--                          (e.g. 813C8918-0001), immutable once assigned.
--   * hosted_invoice_url — the Stripe-hosted invoice page (view / pay online).
--   * invoice_pdf        — the direct PDF download link.
--
-- All three are NULL until a webhook event that carries them lands: Stripe
-- only assigns them at finalization, so a draft mirror row has none, and
-- HISTORIC rows mirrored before this migration stay NULL until a later
-- invoice.* event happens to re-deliver the invoice object. NULL is a normal
-- presentation state (the UI hides View / Download for such rows), NOT drift.
--
-- Writer: webhook reconciliation ONLY (ApplyInvoiceStatus), upsert-style — a
-- non-empty payload value lands, an empty one NEVER clears an already-enriched
-- column back to NULL (Stripe delivers at-least-once and unordered; a sparse
-- payload must not un-enrich the mirror). The charge spine's UpsertInvoice
-- deliberately does not touch them: it mirrors at invoice-CREATE time, before
-- Stripe has assigned any of the three.
--
-- Born clean: pure additive nullable TEXT columns (fast-default metadata
-- change, no table rewrite, no data lock). No backfill — unlike 025 there is
-- nothing local to backfill FROM; the values exist only on Stripe's side.
--
-- Spec: mirrorstack-docs/db/ms_billing/tables.md#invoices

ALTER TABLE ms_billing.invoices
    ADD COLUMN number             TEXT NULL,
    ADD COLUMN hosted_invoice_url TEXT NULL,
    ADD COLUMN invoice_pdf        TEXT NULL;

COMMENT ON COLUMN ms_billing.invoices.number IS
    'Stripe customer-facing invoice number, assigned at finalization and immutable afterwards. '
    'NULL until a webhook event carrying it lands (drafts + historic pre-026 rows).';

COMMENT ON COLUMN ms_billing.invoices.hosted_invoice_url IS
    'Stripe-hosted invoice page URL (view / pay online). Assigned at finalization; NULL = not yet delivered.';

COMMENT ON COLUMN ms_billing.invoices.invoice_pdf IS
    'Stripe invoice PDF download URL. Assigned at finalization; NULL = not yet delivered.';
