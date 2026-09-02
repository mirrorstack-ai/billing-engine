-- Reverses 066. Safe: nothing writes to this table, and a dropped grouping
-- makes every intent settle on its own invoice — which is option A, not a
-- corrupt state.

DROP TRIGGER IF EXISTS intent_groups_sealed ON ms_billing.intent_groups;
DROP FUNCTION IF EXISTS ms_billing.intent_groups_reject_mutation();
DROP TABLE IF EXISTS ms_billing.intent_groups;
