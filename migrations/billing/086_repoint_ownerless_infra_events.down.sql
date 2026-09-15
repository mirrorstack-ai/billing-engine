-- 086 down: nothing to undo. The up statement attached ownerless infra rows to
-- their app's account; there is no record of which rows it touched (a swept row
-- is indistinguishable from one the fixed ingest wrote), and detaching them
-- would recreate the T183 defect on purpose. A rollback of the CODE (the
-- record-time resolution) leaves these rows correctly attributed.
SELECT 1;
