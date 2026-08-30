-- 'proposed' for a credit-ledger entry whose charge was sealed as an intent
-- rather than collected.
--
-- This is the auto-top-up leg's terminal marker on the intent path. An
-- auto-top-up attempt IS a credit_ledger row, so unlike the cycle legs this
-- touches a SHARED status vocabulary rather than a leg-private table.
--
-- ✅ Measured before writing this: every credit_ledger status comparison in the
-- codebase is POSITIVE — `= 'settled'` for wallet balance, source allocation,
-- draw recovery and the app bill; `= 'pending'` and `= 'failed'` for
-- auto-top-up resume and retry. There is no `<>`, `!=` or `NOT IN` on ledger
-- status anywhere, in SQL, Go, or the partial indexes below. So a new value
-- cannot be swept into an existing set by a negative filter.
--
-- That is the opposite of billing_runs, where `status <> 'invoiced'` meant a
-- new 'proposed' silently joined the in-flight set and blocked org deletion
-- forever. The difference is entirely positive vs negative filtering.
--
-- Two consequences, both wanted:
--   * resume and retry (`= 'pending'` / `= 'failed'`) SKIP a proposed row, so
--     the legacy rail cannot resume an attempt the intent rail has taken;
--   * the wallet balance (`= 'settled'`) ignores it, because a proposed
--     top-up has granted no credit.
--
-- Numbered 057: 053 is claimed by two unmerged branches, 055 by the
-- period-boundary cutover, 056 by the authorization bounds. Taking the next
-- slot past the highest CLAIMED one is what stops merge order deciding whether
-- a migration applies.

ALTER TABLE ms_billing.credit_ledger
    DROP CONSTRAINT IF EXISTS credit_ledger_status_check;

ALTER TABLE ms_billing.credit_ledger
    ADD CONSTRAINT credit_ledger_status_check
        CHECK (status IN ('pending', 'settled', 'failed', 'refunded', 'proposed'));

-- The intent this entry was sealed as, written prefixed as 'intent:<digest>'
-- so nothing downstream can read a digest as a provider object id.
ALTER TABLE ms_billing.credit_ledger
    ADD COLUMN IF NOT EXISTS proposed_reference TEXT;

ALTER TABLE ms_billing.credit_ledger
    DROP CONSTRAINT IF EXISTS credit_ledger_proposed_has_reference;

-- A proposed row without its reference is a sealed obligation nobody can walk
-- to its document — the defect the custom-domain leg shipped with, where a
-- proposal was recorded using the no-charge forgiveness marker.
ALTER TABLE ms_billing.credit_ledger
    ADD CONSTRAINT credit_ledger_proposed_has_reference
        CHECK ((status = 'proposed') = (proposed_reference IS NOT NULL));
