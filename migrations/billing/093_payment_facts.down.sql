-- Reverses 093.
--
-- 🔴 Dropping this table discards every rail-neutral payment fact recorded so
-- far — the only cross-rail record an invoice worker (#287) can issue off of.
-- Nothing here moves money, so the down is safe for billing; it is lossy for
-- invoicing reconciliation, same caveat as 087's.

DROP TRIGGER IF EXISTS payment_facts_append_only ON ms_billing.payment_facts;
DROP TABLE IF EXISTS ms_billing.payment_facts;
DROP FUNCTION IF EXISTS ms_billing.payment_facts_reject_mutation();
