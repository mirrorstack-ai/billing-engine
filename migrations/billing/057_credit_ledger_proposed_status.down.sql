-- Refuses while any proposed row exists rather than stranding a sealed
-- obligation: rewriting one to a legacy status would either forgive it
-- ('settled' grants credit that was never paid for) or invite a second
-- collection ('pending' is resumable).
DO $$
DECLARE
    remaining BIGINT;
BEGIN
    SELECT count(*) INTO remaining
      FROM ms_billing.credit_ledger
     WHERE status = 'proposed';

    IF remaining > 0 THEN
        RAISE EXCEPTION
            'cannot revert 057: % credit_ledger rows are proposed; '
            'reverting would forgive or re-collect a sealed obligation',
            remaining;
    END IF;

    ALTER TABLE ms_billing.credit_ledger
        DROP CONSTRAINT IF EXISTS credit_ledger_proposed_has_reference;

    ALTER TABLE ms_billing.credit_ledger
        DROP COLUMN IF EXISTS proposed_reference;

    ALTER TABLE ms_billing.credit_ledger
        DROP CONSTRAINT IF EXISTS credit_ledger_status_check;

    ALTER TABLE ms_billing.credit_ledger
        ADD CONSTRAINT credit_ledger_status_check
            CHECK (status IN ('pending', 'settled', 'failed', 'refunded'));
END $$;
