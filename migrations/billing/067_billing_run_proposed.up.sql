-- Migration 067: the boundary run status for a leg that PROPOSED instead of
-- collecting.
--
-- The period boundary is the last leg with no intent seam. When the cutover
-- flag arms it, RunBillingCycle derives the same amounts, seals them as two
-- intents (the closed period's usage arrears and the next period's
-- subscription) and returns without touching Stripe. That outcome needs its own
-- terminal status: it is not 'invoiced' — no invoice exists and no money moved —
-- and it is emphatically not 'failed'.
--
-- Recording it as either would corrupt the one measurement the legacy drop
-- depends on. scripts/legacy-drop-preconditions.sql asks production whether any
-- boundary run is still mid-flight, and a proposed run that reads as 'pending'
-- would block the drop forever while a proposed run that reads as 'invoiced'
-- would claim money was collected that never was.
--
-- status is plain TEXT + CHECK rather than a CREATE TYPE enum (migration 012),
-- so extending the vocabulary is a constraint swap. The CHECK is dropped and
-- re-added with the same names plus one; no existing row changes, and no row
-- can currently hold the new value because nothing writes it yet.
ALTER TABLE ms_billing.billing_runs
    DROP CONSTRAINT billing_runs_status_check;

ALTER TABLE ms_billing.billing_runs
    ADD CONSTRAINT billing_runs_status_check
        CHECK (status IN (
            'pending',
            'invoiced',
            'skipped_no_pm',
            'failed',
            'skipped_prepaid',
            'skipped_ceiling',
            -- The intent path: this run's charge was sealed as intents and
            -- nothing was collected. Terminal for this worker; the executor
            -- owns what happens next.
            'proposed'
        ));

COMMENT ON COLUMN ms_billing.billing_runs.status IS
    'Terminal outcome of one boundary run. ''proposed'' means the intent cutover was armed: the amounts were sealed as intents and no money moved, which is neither invoiced nor failed.';
