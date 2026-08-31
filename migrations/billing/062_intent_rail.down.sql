-- Reverses 062. Dropping the columns drops the CHECK.
--
-- A canonical-encoding downgrade (v4 -> v3): an intent sealed while these
-- existed has a digest taken over them and could not reproduce it afterwards.
-- Safe only while charge_intents is empty.
ALTER TABLE ms_billing.charge_intents
    DROP CONSTRAINT IF EXISTS charge_intents_rail_stated;

ALTER TABLE ms_billing.charge_intents
    DROP COLUMN IF EXISTS routing_policy_revision,
    DROP COLUMN IF EXISTS selected_rail;
