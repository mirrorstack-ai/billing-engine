-- Down of migration 073 — dev_served.
--
-- 🔴 THIS DOWN IS LOSSY, IN TWO DIFFERENT WAYS. Read both before running it.
--
-- 1) IT DELETES AGGREGATE ROWS. The narrow (pre-073) uniqueness key cannot be
--    restored while a period holds two rows for one (app, module, metric,
--    model, module_version, aggregation_key) that differ only by dev_served —
--    CREATE UNIQUE INDEX would simply fail on the duplicate. So the
--    dev_served rows are DELETED first. Merging them into their charged
--    sibling is NOT an option: it would fold never-charged tunnel usage into
--    a billable line, which is the one outcome this whole feature exists to
--    prevent. Deleting them loses only DISPLAY data — no dev_served row has
--    ever contributed to an invoice, a settlement, a budget or a wallet
--    debit — but it is a real, unrecoverable loss of the developer's
--    "what would this have cost" record for those periods.
--
-- 2) IT DISARMS THE RULE ON THE RAW LEDGER. Dropping usage_events.dev_served
--    keeps every tunnel-served EVENT but forgets that it was one. Re-applying
--    073 afterwards brings the column back at its DEFAULT false, so those
--    events read as ordinary billable usage, and the next rollup of a still
--    open period will PRICE THEM INTO THE ARREARS AND CHARGE THEM. A rollback
--    that is going to be followed by a re-apply must therefore either be
--    confined to periods already invoiced, or have api-platform re-stamp the
--    affected events — there is no way to recover the flag from the ledger
--    itself.
--
-- Order mirrors the up in reverse: remove what the wide key permitted, restore
-- the narrow arbiter, then drop the columns.

DELETE FROM ms_billing.usage_aggregates WHERE dev_served;

-- Rebuild migration 055's arbiter (period, app, module, metric, model,
-- module_version, COALESCE(aggregation_key, '')) under its original name, then
-- drop the widened one — same build-before-drop ordering as the up, so the
-- table is never left without a uniqueness guard.
CREATE UNIQUE INDEX usage_aggregates_period_line_aggregation_key
    ON ms_billing.usage_aggregates (
        period_id, app_id, module_id, metric, model, module_version,
        COALESCE(aggregation_key, '')
    );

DROP INDEX IF EXISTS ms_billing.usage_aggregates_period_line_dev_served_key;

ALTER TABLE ms_billing.usage_aggregates
    DROP COLUMN IF EXISTS dev_served;

DROP INDEX IF EXISTS ms_billing.usage_events_dev_served_idx;

ALTER TABLE ms_billing.usage_events
    DROP COLUMN IF EXISTS dev_served;
