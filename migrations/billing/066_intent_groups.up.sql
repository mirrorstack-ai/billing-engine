-- Migration 066: which intents settle on one invoice.
--
-- 🔴 THIS SUPPORTS OPTION B OF BOUNDARY-KIND-DECISION.md, WHICH IS OPEN.
-- Nothing writes to this table yet. If A, C or D is chosen, it is dropped.
--
-- The period-boundary invoice is FOUR of §6's charge kinds and combined
-- proration is two, while a ChargeIntent carries ONE Kind — and migration
-- 054's header says why that matters: the kind "selects which rule of a
-- standing authorization applies: a caller that chose it could pick the
-- permission its charge happens to fit". So one intent per invoice weakens
-- the authorization control, and several intents per invoice needs something
-- durable to say which belong together.
--
-- # Why a side table and not a sealed field
--
-- A ChargeIntent is what the customer owes and under what terms. WHICH OTHER
-- CHARGES SHARE ITS INVOICE is not one of those things: two customers owed
-- identical amounts under identical terms hold identical documents whether
-- their charges were invoiced together or separately. If that were false, the
-- digest would be attesting to something the customer never agreed to.
--
-- So grouping is an EXECUTION concern, and it belongs beside
-- intent_settlement_claims rather than inside the seal. Three consequences,
-- all of them the reason for this shape:
--
--   * no canonical supersession, so this is not on the charge_intents = 0
--     clock and can be revised later at ordinary cost;
--   * no string convention to break — the grouping is STATED when a leg
--     proposes, not inferred from a shared source-reference prefix, which
--     would fail silently in the direction of splitting one charge into
--     several;
--   * an intent's digest is identical whether it is invoiced alone or with
--     three others, which is the property that makes option B reversible.
--
-- # What it does not do
--
-- It does not make a group atomic. That is the executor's job: claim every
-- intent in the group before collecting, and record every outcome after. A
-- row here is a statement about intent, not a lock.
--
-- A group that is never executed strands nothing. The rows are inert, and
-- PendingExecution already refuses to hand out a claimed or terminal intent.

CREATE TABLE IF NOT EXISTS ms_billing.intent_groups (
    -- One row per intent. An intent belongs to at most one group: a document
    -- that could settle on two invoices is a double charge waiting for a
    -- retry, and the primary key is what makes that unrepresentable rather
    -- than merely unlikely.
    intent_digest TEXT PRIMARY KEY REFERENCES ms_billing.charge_intents(digest),

    -- The group's identity, chosen by the leg that proposed the set. It is
    -- opaque here: the executor collects whatever shares a value, and
    -- stripeadapter derives its own provider idempotency key from the sorted
    -- SET of digests rather than from this string — so a leg that reused a
    -- group id by accident cannot make two different charges look like one
    -- retry at the provider.
    group_id      TEXT NOT NULL CHECK (group_id <> ''),

    created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS intent_groups_group_idx
    ON ms_billing.intent_groups (group_id);

-- Append-only, and for the same reason charge_intents is: a group that can be
-- edited after its intents were sealed is a group whose membership at
-- collection time is not the membership anybody agreed to. Correcting one
-- means superseding the intents, which is INV-003's answer to every other
-- "we need to change it" in this schema.
CREATE OR REPLACE FUNCTION ms_billing.intent_groups_reject_mutation()
RETURNS TRIGGER AS $$
BEGIN
    RAISE EXCEPTION
        'intent_groups is append-only: supersede the intents instead of regrouping them (INV-003)';
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS intent_groups_sealed ON ms_billing.intent_groups;
CREATE TRIGGER intent_groups_sealed
    BEFORE UPDATE OR DELETE ON ms_billing.intent_groups
    FOR EACH ROW EXECUTE FUNCTION ms_billing.intent_groups_reject_mutation();
