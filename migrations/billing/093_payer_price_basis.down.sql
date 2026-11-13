-- Down 093 — remove ONLY what 093 up created: the two new tables and their
-- indexes. Nothing else in ms_billing is touched.
DROP TABLE IF EXISTS ms_billing.payer_price_basis_audit;
DROP TABLE IF EXISTS ms_billing.payer_price_basis;
