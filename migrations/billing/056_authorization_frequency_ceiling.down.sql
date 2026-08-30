-- Restores migration 054's standing constraint exactly, then drops the
-- column. Order matters: the constraint references the column, so dropping
-- the column first would take the constraint with it and leave nothing to
-- restore if this file were re-run.

ALTER TABLE ms_billing.billing_authorizations
    DROP CONSTRAINT IF EXISTS billing_authorizations_standing_is_bounded;

ALTER TABLE ms_billing.billing_authorizations
    ADD CONSTRAINT billing_authorizations_standing_is_bounded
        CHECK (scope <> 'standing'
               OR (array_length(charge_kinds, 1) > 0
                   AND per_charge_ceiling_micros > 0));

ALTER TABLE ms_billing.billing_authorizations
    DROP CONSTRAINT IF EXISTS billing_authorizations_trigger_is_complete;

ALTER TABLE ms_billing.billing_authorizations
    DROP COLUMN IF EXISTS frequency_ceiling,
    DROP COLUMN IF EXISTS trigger_below_micros,
    DROP COLUMN IF EXISTS top_up_amount_micros;
