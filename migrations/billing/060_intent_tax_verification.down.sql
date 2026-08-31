-- Reverses 060. Dropping the column also drops its CHECK.
--
-- Reversing this is a canonical-encoding downgrade (v2 -> v1): any intent
-- sealed while it existed has a digest taken over these bytes, so a row that
-- survives the down migration can no longer reproduce its own digest. Safe
-- only while charge_intents is empty, which is the same condition under which
-- the up migration is safe.
ALTER TABLE ms_billing.charge_intents
    DROP CONSTRAINT IF EXISTS charge_intents_tax_verification_known;

ALTER TABLE ms_billing.charge_intents
    DROP COLUMN IF EXISTS tax_verification;
