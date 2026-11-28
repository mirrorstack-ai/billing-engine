-- Reverses 098.
--
-- Dropping the table resets both infra pullers to their first-run sweep (the
-- newest 3 closed hours). Nothing moves money on it; windows that were still
-- unconfirmed when it is dropped are no longer retried.

DROP TABLE IF EXISTS ms_billing.infra_sync_watermarks;
