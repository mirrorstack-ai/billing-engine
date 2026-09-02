-- Reverse 069. Dropping the column discards every provider link recorded since
-- it landed, which is irreversible — the reference exists nowhere else.
DROP INDEX IF EXISTS ms_billing.intent_settlement_claims_provider_reference_idx;

ALTER TABLE ms_billing.intent_settlement_claims
    DROP CONSTRAINT IF EXISTS intent_settlement_claims_succeeded_names_its_object;

ALTER TABLE ms_billing.intent_settlement_claims
    DROP COLUMN IF EXISTS provider_reference;
