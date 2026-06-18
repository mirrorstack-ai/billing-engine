-- Down migration for 011_invoices.
--
-- Drops the invoices table; its FK index and the BEFORE UPDATE trigger drop
-- implicitly with the table. The set_updated_at() function is owned by 001 and
-- still referenced by accounts (and 006/010 triggers), so it is NOT dropped
-- here.

DROP TABLE IF EXISTS ms_billing.invoices;
