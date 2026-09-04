-- Reverses 071.
--
-- 🔴 REFUSED WHILE THE LEDGER HOLDS A ROW. app_transfer_events is the
-- idempotency record: a replayed request_id answers from it instead of
-- transferring again. Drop it with rows in it and re-apply the up, and the
-- next retry of any of those request_ids — api-platform retries post-commit —
-- is a FRESH transfer that can re-key an app back to an account that already
-- paid to be rid of it. It is also the only record of which account a
-- transfer moved usage from, which an auditor cannot reconstruct from
-- usage_events once the rows carry the new account_id.
--
-- Same shape as 049/057/067: count, and refuse with the number rather than
-- delete on the operator's behalf. There is no "resolve the rows first" here —
-- a transfer that happened cannot be made not to have happened — so a
-- non-empty ledger means this migration is forward-only on this database.
DO $$
DECLARE
    n BIGINT;
BEGIN
    SELECT count(*) INTO n FROM ms_billing.app_transfer_events;
    IF n > 0 THEN
        RAISE EXCEPTION
            'refusing to drop ms_billing.app_transfer_events: % transfer(s) are recorded in it. Dropping the idempotency ledger would let a retried request_id transfer again, and would erase the only record of where each transfer moved usage from.', n;
    END IF;
END $$;

-- The forfeit stamps go with the ledger they point at. The guard above has
-- already proven no transfer is recorded, so no row can carry one: every
-- charge_forfeited_by / grace_forfeited_by is NULL here, and dropping the
-- column erases no resolution — the row stays resolved, it just stops saying
-- which transfer did it (which nothing did).
ALTER TABLE ms_billing.app_module_overage_timers
    DROP CONSTRAINT IF EXISTS app_module_overage_timers_forfeit_is_resolved_uncharged;
ALTER TABLE ms_billing.app_module_overage_timers
    DROP COLUMN IF EXISTS grace_forfeited_by;
ALTER TABLE ms_billing.app_custom_domains
    DROP CONSTRAINT IF EXISTS app_custom_domains_forfeit_is_resolved_uncharged;
ALTER TABLE ms_billing.app_custom_domains
    DROP COLUMN IF EXISTS charge_forfeited_by;

-- The triggers and the functions go with the table: leaving the split guard
-- behind would refuse writes on behalf of a ledger that no longer exists, and
-- the append-only function would be an orphan.
DROP TRIGGER IF EXISTS app_custom_domains_attribution_agrees ON ms_billing.app_custom_domains;
DROP TRIGGER IF EXISTS app_module_overage_timers_attribution_agrees ON ms_billing.app_module_overage_timers;
DROP TRIGGER IF EXISTS apps_attribution_agrees ON ms_billing.apps;
DROP FUNCTION IF EXISTS ms_billing.app_account_attribution_agrees();
DROP TRIGGER IF EXISTS app_transfer_events_append_only ON ms_billing.app_transfer_events;
DROP FUNCTION IF EXISTS ms_billing.app_transfer_events_reject_mutation();
DROP TABLE IF EXISTS ms_billing.app_transfer_events;
