-- 083: a DURABLE "this run was ever handed to the intent proposer" marker
-- (billing-engine#217, review round 2).
--
-- The stale-freeze reconciliation may re-freeze a boundary's frozen figure at
-- the live derivation only when nothing external holds the frozen figure: no
-- provider invoice and no sealed intent. `status = 'proposed'` cannot carry
-- the second half on its own — InsertBillingRun's reclaim resets status to
-- 'pending', so proposed → reclaimed → crash before ProposeGroup → reclaimed
-- again reads 'pending' while attempt A's sealed document is alive, and a
-- re-freeze there would seal a SECOND digest for one boundary.
--
-- proposal_attempted_at is stamped BEFORE ProposeGroup is called and never
-- cleared (COALESCE keeps the first instant): "maybe sealed" is treated as
-- sealed, which also closes the seal-then-crash-before-mark window. A run that
-- carries it is never re-frozen; the split's own guard decides it. Nullable,
-- expand-only, no backfill: every existing 'proposed' row is also recognised by
-- its status in the same predicate.
ALTER TABLE ms_billing.billing_runs
    ADD COLUMN IF NOT EXISTS proposal_attempted_at TIMESTAMPTZ NULL;

COMMENT ON COLUMN ms_billing.billing_runs.proposal_attempted_at IS
    'First instant this run was handed to the intent proposer (stamped BEFORE ProposeGroup, never cleared). A run carrying it may hold a sealed intent, so its frozen figure is never re-frozen (billing-engine#217).';
