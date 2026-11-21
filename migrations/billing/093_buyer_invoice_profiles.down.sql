-- Down 093 — drop the buyer invoice profile table, its trigger and checksum
-- function. Reversible: nothing else references this table yet (#287/#313
-- are future migrations).
DROP TRIGGER IF EXISTS buyer_invoice_profiles_set_updated_at ON ms_billing.buyer_invoice_profiles;
DROP TABLE IF EXISTS ms_billing.buyer_invoice_profiles;
DROP FUNCTION IF EXISTS ms_billing.taiwan_tax_id_checksum_valid(TEXT);
