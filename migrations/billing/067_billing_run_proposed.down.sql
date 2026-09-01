-- Reverse 067. Any run recorded as 'proposed' must be resolved before the
-- vocabulary can narrow again, or the constraint would be re-added against rows
-- that violate it.
--
-- Deliberately no UPDATE here. Rewriting a proposed run to 'failed' or
-- 'invoiced' would assert something untrue about whether money moved, and this
-- file would be the thing that made the legacy-drop measurement wrong. A
-- migration that cannot run without lying should refuse.
DO $$
DECLARE
    n BIGINT;
BEGIN
    SELECT count(*) INTO n FROM ms_billing.billing_runs WHERE status = 'proposed';
    IF n > 0 THEN
        RAISE EXCEPTION
            'refusing to narrow the status vocabulary: % billing run(s) are ''proposed''. Resolve them deliberately first — rewriting them here would assert money moved, or failed to, when neither happened.', n;
    END IF;
END $$;

ALTER TABLE ms_billing.billing_runs
    DROP CONSTRAINT billing_runs_status_check;

ALTER TABLE ms_billing.billing_runs
    ADD CONSTRAINT billing_runs_status_check
        CHECK (status IN ('pending', 'invoiced', 'skipped_no_pm', 'failed', 'skipped_prepaid', 'skipped_ceiling'));
