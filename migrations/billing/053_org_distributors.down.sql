-- Down 053 — drop the org distributor links table.
--
-- The index goes with the table. Nothing outside ms_billing references it
-- (both columns are soft FKs), so the drop is self-contained.
--
-- Rolling back returns the distributor queries to their pre-053 shape, which
-- joined designation.sponsor_account_id — a join no production row has ever
-- satisfied (see the up migration and billing-engine#134). The rollback
-- therefore restores a dead path, and any links recorded here are lost.

DROP TABLE IF EXISTS ms_billing.org_distributors;
