-- Reverses 064.
--
-- 🔴 Dropping this table destroys evidence. It is safe only while the table is
-- empty, which it is in every environment today — no deployment holds an
-- evidence signing key, so nothing can write a record at all. Once a record
-- exists the down migration is the wrong tool: INV-014's own answer to a
-- record that should not have been written is a CORRECTION record, not a
-- deletion.

DROP TRIGGER IF EXISTS evidence_records_append_only ON ms_billing.evidence_records;
DROP FUNCTION IF EXISTS ms_billing.evidence_records_reject_mutation();
DROP TABLE IF EXISTS ms_billing.evidence_records;
