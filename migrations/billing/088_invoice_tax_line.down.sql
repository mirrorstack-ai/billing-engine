-- Down migration 088 — drop the invoice tax-line columns. Lossy for display
-- and reconciliation only: the authoritative determination stays sealed in
-- each ChargeIntent, nothing moves money on these columns, and up/down/up
-- round-trips cleanly (the dropped columns take their constraints with them).

ALTER TABLE ms_billing.invoices
    DROP CONSTRAINT IF EXISTS invoices_tax_determination_whole,
    DROP COLUMN IF EXISTS tax_inclusive,
    DROP COLUMN IF EXISTS tax_verification,
    DROP COLUMN IF EXISTS tax_rule_revision,
    DROP COLUMN IF EXISTS tax_jurisdiction,
    DROP COLUMN IF EXISTS tax_rate_bps,
    DROP COLUMN IF EXISTS tax_amount;
