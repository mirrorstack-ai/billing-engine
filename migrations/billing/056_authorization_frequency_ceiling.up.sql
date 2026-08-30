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
                   AND frequency_ceiling > 0));
