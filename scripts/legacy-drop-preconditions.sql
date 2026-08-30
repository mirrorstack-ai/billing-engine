-- Legacy-drop preconditions — READ ONLY.
--
-- docs/DESIGN.md §11 step 8 is "migrate every caller to intents, then delete
-- the direct charge code and revoke the legacy provider credentials." Most of
-- that is code, and internal/architecture's checks cover the code half. This
-- file covers the half that is not code.
--
-- Several legacy money paths keep durable in-flight state, and deleting the
-- code that owns it strands a charge nobody can finish or prove. Whether that
-- state is empty is a question about the running database at the moment of the
-- drop, not about the tree. So it is asked here, against production, and the
-- answer is only good for as long as nothing new starts.
--
-- USAGE
--   psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -f scripts/legacy-drop-preconditions.sql
--
-- Every statement is a SELECT. There is no UPDATE, INSERT or DELETE in this
-- file and there must never be one: it is run against production by someone
-- deciding whether a deletion is safe, and a script that could change what it
-- is measuring is not a measurement.
--
-- Read the verdict column. Any BLOCKED row stops the corresponding deletion.

\pset border 2
\echo ''
\echo '=== legacy-drop preconditions (read-only) ==='
\echo ''

-- 1. The period-boundary collector (internal/account/cycle/charge.go).
--
-- frozen_charge_cents is the only record of the exact cents a crashed attempt
-- committed to. A run holding one while not yet 'invoiced' is mid-flight: the
-- amount is decided, the outcome is not, and only the current code knows how
-- to finish it.
SELECT
    'cycle/charge.go — boundary collector'                     AS deletion,
    count(*)                                                   AS blocking_rows,
    CASE WHEN count(*) = 0 THEN 'READY' ELSE 'BLOCKED' END     AS verdict,
    'billing_runs frozen mid-flight'                           AS what
FROM ms_billing.billing_runs
WHERE frozen_charge_cents IS NOT NULL
  AND status <> 'invoiced';

-- 2. The module-overage collector (internal/account/cycle/overage.go).
--
-- An armed-but-unresolved timer is a charge in flight. The arming query
-- matching zero rows means ALREADY SETTLED rather than missing, so the
-- distinction is unrecoverable once the code is gone.
SELECT
    'cycle/overage.go — module overage collector'              AS deletion,
    count(*)                                                   AS blocking_rows,
    CASE WHEN count(*) = 0 THEN 'READY' ELSE 'BLOCKED' END     AS verdict,
    'overage timers armed but unresolved'                      AS what
FROM ms_billing.app_module_overage_timers
WHERE charge_attempted_at IS NOT NULL
  AND grace_resolved = false;

-- 3. The custom-domain collector (internal/account/cycle/domain_charges.go).
SELECT
    'cycle/domain_charges.go — domain collector'               AS deletion,
    count(*)                                                   AS blocking_rows,
    CASE WHEN count(*) = 0 THEN 'READY' ELSE 'BLOCKED' END     AS verdict,
    'domain charges attempted but unresolved'                  AS what
FROM ms_billing.app_custom_domains
WHERE charge_attempted_at IS NOT NULL
  AND charge_resolved = false;

-- 4. The creation-proration collector (internal/account/cycle/proration.go).
--
-- The code condition matters as much as this one: proration and overage both
-- mint the shared "mod-overage-ii-<timer>" idempotency key, so dropping one
-- leg while the other still uses it removes the last double-charge guard.
SELECT
    'cycle/proration.go — proration collector'                 AS deletion,
    count(*)                                                   AS blocking_rows,
    CASE WHEN count(*) = 0 THEN 'READY' ELSE 'BLOCKED' END     AS verdict,
    'combined proration attempts unresolved'                   AS what
FROM ms_billing.app_combined_proration_attempts
WHERE resolved_at IS NULL;

-- 5. The credit-purchase executor (internal/account/creditpurchase/).
--
-- Every pending purchase carries a frozen, immutable funding claim. There is
-- no path to re-arm one after the code is gone.
SELECT
    'creditpurchase/ — the whole package'                      AS deletion,
    count(*)                                                   AS blocking_rows,
    CASE WHEN count(*) = 0 THEN 'READY' ELSE 'BLOCKED' END     AS verdict,
    'credit purchases still pending'                           AS what
FROM ms_billing.credit_ledger
WHERE type = 'purchase'
  AND status = 'pending';

-- 6. The auto-top-up executor (internal/account/autotopup/).
--
-- Pending means a charge whose outcome is unknown. The retry latch also keys
-- on a failed attempt's timestamp against the config's, so a drop must not
-- leave an account latched with no code able to clear it.
SELECT
    'autotopup/ — the whole package'                           AS deletion,
    count(*)                                                   AS blocking_rows,
    CASE WHEN count(*) = 0 THEN 'READY' ELSE 'BLOCKED' END     AS verdict,
    'auto top-ups still pending'                               AS what
FROM ms_billing.credit_ledger
WHERE type = 'auto_topup'
  AND status = 'pending';

-- 7. The unpaid-invoice collector (internal/account/billing/unpaid.go:223).
--
-- This is the one collecting call with no deterministic idempotency key, by
-- design, so its behaviour cannot be reproduced by replay. Every invoice it
-- would still be asked to collect must already be covered by a replacement
-- before the original can go.
SELECT
    'billing/unpaid.go — PayInvoice'                           AS deletion,
    count(*)                                                   AS blocking_rows,
    'REVIEW'                                                   AS verdict,
    'open collectible invoices needing a replacement intent'   AS what
-- The mirror stores Stripe's invoice status verbatim (011_invoices.up.sql:35):
-- draft / open / paid / uncollectible / void. 'open' is the collectible one.
-- There is no 'past_due' — that is a subscription status, not an invoice one,
-- and naming it here would have looked like extra coverage while matching
-- nothing.
FROM ms_billing.invoices
WHERE status = 'open';

\echo ''
\echo 'READY  = the durable state this deletion would strand is empty.'
\echo 'BLOCKED= something is mid-flight. Deleting now strands a charge nobody'
\echo '         can finish or prove.'
\echo 'REVIEW = a count, not a verdict. Each row needs a replacement intent'
\echo '         before the unkeyed collector can go.'
\echo ''
\echo 'A READY answer is only good while nothing new starts. Run this in the'
\echo 'same window as the deployment that removes the code, not the day before.'
\echo ''
