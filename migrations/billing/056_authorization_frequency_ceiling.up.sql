-- A standing authorization needs an ATTEMPT bound, not only amount bounds.
--
-- docs/DESIGN.md §6 names it for auto_topup: "per-attempt, frequency and
-- period ceilings". Migration 054 shipped the two amount ceilings and not
-- the count, so an authorization that permits $50 per attempt and $200 per
-- period also permits two hundred one-dollar attempts — inside every bound
-- it declares, and a runaway.
--
-- Numbered 056, not 055: 055 is claimed by the period-boundary cutover on
-- build/boundary, and two further unmerged branches each claim 053. Taking
-- the next slot past the highest CLAIMED one is what stops a merge order
-- deciding whether a migration applies.
--
-- One statement per concern, and the standing constraint is replaced rather
-- than added beside, so the table never carries two rules about the same
-- thing.

ALTER TABLE ms_billing.billing_authorizations
    ADD COLUMN IF NOT EXISTS frequency_ceiling INTEGER NOT NULL DEFAULT 0
        CHECK (frequency_ceiling >= 0);

COMMENT ON COLUMN ms_billing.billing_authorizations.frequency_ceiling IS
    'The most attempts this authorization permits in its period. A COUNT, not '
    'an amount: many small attempts stay inside both micro ceilings and are '
    'still a runaway. Zero means unbounded and is only legal for a one-time '
    'authorization, which covers exactly one document by construction.';

-- Existing standing rows predate the column and would violate the widened
-- constraint. There are none in any environment that has run 054 without an
-- intent proposer wired -- WithIntentProposer had no non-test caller until
-- 2026-08-30 -- but the backfill is written rather than assumed, because
-- "there should be no rows" is not a thing a migration may believe.
UPDATE ms_billing.billing_authorizations
   SET frequency_ceiling = 1
 WHERE scope = 'standing'
   AND frequency_ceiling = 0;

-- The balance trigger and amount rule, §6's other two bounds for auto_topup.
--
-- These must persist or LoadAuthorization silently drops them: the row would
-- reload with no amount rule, and an authorization with no rule permits any
-- amount inside its ceilings. A bound that disappears on reload is worse than
-- one that was never added.
ALTER TABLE ms_billing.billing_authorizations
    ADD COLUMN IF NOT EXISTS trigger_below_micros BIGINT NOT NULL DEFAULT 0
        CHECK (trigger_below_micros >= 0),
    ADD COLUMN IF NOT EXISTS top_up_amount_micros BIGINT NOT NULL DEFAULT 0
        CHECK (top_up_amount_micros >= 0);

COMMENT ON COLUMN ms_billing.billing_authorizations.top_up_amount_micros IS
    'The accepted top-up size. A ceiling says "no more than"; this says '
    '"exactly this". Zero means the authorization is not balance-triggered.';

-- Both halves or neither: a trigger with no amount rule permits any size once
-- the balance falls, and a rule with no trigger permits that size at any time.
-- Either alone is a different arrangement from the one the customer accepted.
-- §6's "provider and mandate": WHICH rail, and WHICH reusable mandate on it,
-- the customer accepted. An off-session standing authorization naming neither
-- authorises a charge against whatever instrument is on file later — which
-- survives the customer replacing their card, and is not what was accepted.
ALTER TABLE ms_billing.billing_authorizations
    ADD COLUMN IF NOT EXISTS provider TEXT NOT NULL DEFAULT '',
    ADD COLUMN IF NOT EXISTS mandate_reference TEXT NOT NULL DEFAULT '';

-- §6's notice "lead time": how long after delivery the customer must be left
-- before money moves. Stored in seconds because that is the unit the column
-- can CHECK; the Go side carries a time.Duration.
ALTER TABLE ms_billing.billing_authorizations
    ADD COLUMN IF NOT EXISTS notice_lead_seconds BIGINT NOT NULL DEFAULT 0
        CHECK (notice_lead_seconds >= 0);

UPDATE ms_billing.billing_authorizations
   SET notice_lead_seconds = 86400
 WHERE scope = 'standing'
   AND notice_lead_seconds = 0;

ALTER TABLE ms_billing.billing_authorizations
    DROP CONSTRAINT IF EXISTS billing_authorizations_instrument_is_complete;

ALTER TABLE ms_billing.billing_authorizations
    ADD CONSTRAINT billing_authorizations_instrument_is_complete
        CHECK ((provider = '') = (mandate_reference = ''));

ALTER TABLE ms_billing.billing_authorizations
    DROP CONSTRAINT IF EXISTS billing_authorizations_trigger_is_complete;

ALTER TABLE ms_billing.billing_authorizations
    ADD CONSTRAINT billing_authorizations_trigger_is_complete
        CHECK ((trigger_below_micros > 0) = (top_up_amount_micros > 0));

ALTER TABLE ms_billing.billing_authorizations
    DROP CONSTRAINT IF EXISTS billing_authorizations_standing_is_bounded;

ALTER TABLE ms_billing.billing_authorizations
    ADD CONSTRAINT billing_authorizations_standing_is_bounded
        CHECK (scope <> 'standing'
               OR (array_length(charge_kinds, 1) > 0
                   AND per_charge_ceiling_micros > 0
                   AND frequency_ceiling > 0
                   AND notice_lead_seconds > 0));

-- The notice DELIVERY instant.
--
-- predicate.ClauseNoticeWaitElapsed measured the wait against
-- eligibility_not_before alone — a timestamp whoever wrote the receipt chose.
-- Checking it against the authorization's accepted lead time needs the instant
-- the bytes actually arrived, and the table did not record it, so the executor
-- could not supply it even in principle.
ALTER TABLE ms_billing.notice_receipts
    ADD COLUMN IF NOT EXISTS delivered_at TIMESTAMPTZ;

COMMENT ON COLUMN ms_billing.notice_receipts.delivered_at IS
    'When the notice bytes arrived. The wait runs from here, and the accepted '
    'lead time is measured against it. NULL on rows written before migration '
    '056, which therefore cannot satisfy the wait -- the right answer, since '
    'nothing recorded when their clock started.';

-- The source-capacity reservation §6 requires for collect_receivable:
-- "a linked intent for the remaining amount only, under a new FundingPlan and
-- a source-capacity reservation".
--
-- Without it, two receivables can each claim the whole remainder of one intent
-- and both collect. The intent's own digest PRIMARY KEY does not stop that:
-- they are DIFFERENT documents, each individually valid, and INV-008's
-- one-settlement-per-intent guard sees two intents rather than one obligation
-- claimed twice.
--
-- reserved_micros is the running total claimed against a source. The CHECK is
-- cross-column, so the database itself refuses an over-reservation — a guard in
-- Go alone would be one racing process away from useless.
ALTER TABLE ms_billing.charge_intents
    ADD COLUMN IF NOT EXISTS reserved_micros BIGINT NOT NULL DEFAULT 0;

ALTER TABLE ms_billing.charge_intents
    DROP CONSTRAINT IF EXISTS charge_intents_reservation_within_total;

ALTER TABLE ms_billing.charge_intents
    ADD CONSTRAINT charge_intents_reservation_within_total
        CHECK (reserved_micros >= 0 AND reserved_micros <= total_micros);

-- One row per receivable, so a retry of the same receivable reserves once.
-- Without this the reservation is not idempotent and a replayed proposal eats
-- the remainder twice.
CREATE TABLE IF NOT EXISTS ms_billing.intent_receivable_links (
    receivable_digest TEXT PRIMARY KEY
        REFERENCES ms_billing.charge_intents(digest),
    source_digest     TEXT NOT NULL
        REFERENCES ms_billing.charge_intents(digest),
    reserved_micros   BIGINT NOT NULL CHECK (reserved_micros > 0),
    reserved_at       TIMESTAMPTZ NOT NULL DEFAULT now(),

    -- A receivable collecting itself is a loop, not a remainder.
    CONSTRAINT intent_receivable_links_not_self
        CHECK (receivable_digest <> source_digest)
);

CREATE INDEX IF NOT EXISTS intent_receivable_links_source
    ON ms_billing.intent_receivable_links (source_digest);
