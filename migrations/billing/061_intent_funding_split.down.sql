-- Reverses 061. Dropping the columns drops their CHECKs.
--
-- This is a canonical-encoding downgrade (v3 -> v2): any intent sealed while
-- these columns existed has a digest taken over them, so a surviving row could
-- no longer reproduce its own digest. Safe only while charge_intents is empty,
-- the same condition under which the up migration is safe.
ALTER TABLE ms_billing.charge_intents
    DROP CONSTRAINT IF EXISTS charge_intents_funding_balances,
    DROP CONSTRAINT IF EXISTS charge_intents_funding_non_negative;

ALTER TABLE ms_billing.charge_intents
    DROP COLUMN IF EXISTS provider_remainder_micros,
    DROP COLUMN IF EXISTS wallet_allocation_micros;
