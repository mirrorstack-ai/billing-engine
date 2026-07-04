-- Down migration 026 — drop the invoice presentment columns. The values are
-- pure mirrors of Stripe-owned fields (re-derivable from any later invoice.*
-- webhook delivery), so dropping them loses no authoritative state and
-- up/down/up round-trips cleanly.

ALTER TABLE ms_billing.invoices
    DROP COLUMN IF EXISTS number,
    DROP COLUMN IF EXISTS hosted_invoice_url,
    DROP COLUMN IF EXISTS invoice_pdf;
