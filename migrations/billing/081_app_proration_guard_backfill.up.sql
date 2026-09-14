-- 081: arm the app's creation-proration guard for every combined attempt the
-- intent rail already resolved (billing-engine#208 round 3, review item 6).
--
-- MarkCombinedProrationProposed used to resolve the migration-050 header with
-- the sealed intent's reference (or the nothing-to-bill 'none:' reference)
-- and leave apps.proration_invoice_id NULL. AppsPendingProration selects on
-- that column alone, so every such app was re-selected by every sweep, and
-- ChargeCreationProration then refused it as "combined attempt disagrees with
-- its unarmed app guard" — an error per app per cycle, and its co-created
-- over-module timers deferred for as long as it lasted. The store now writes
-- both rows in one transaction; this brings the rows written before it up to
-- the same state. Only RESOLVED headers are touched, so the migration-050
-- trigger (which refuses the app write while a header is unresolved) is
-- satisfied, and only an unarmed, unskipped app is written (first-write-wins,
-- like SetAppProrationInvoice).
UPDATE ms_billing.apps AS a
SET proration_invoice_id = att.resolved_invoice_id
FROM ms_billing.app_combined_proration_attempts AS att
WHERE att.app_id = a.app_id
  AND att.resolved_at IS NOT NULL
  AND att.resolved_invoice_id IS NOT NULL
  AND a.proration_invoice_id IS NULL
  AND a.proration_skipped_at IS NULL;
