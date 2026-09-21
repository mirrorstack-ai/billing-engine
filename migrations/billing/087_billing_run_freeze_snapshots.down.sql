-- Reverses 087.
--
-- 🔴 Dropping these tables discards every freeze snapshot — the only record of
-- which lines a frozen boundary figure was derived from. Nothing moves money on
-- them, so the down is safe for billing; it is lossy for reconciliation.

DROP TRIGGER IF EXISTS billing_run_freeze_snapshot_lines_sealed ON ms_billing.billing_run_freeze_snapshot_lines;
DROP TRIGGER IF EXISTS billing_run_freeze_snapshots_sealed ON ms_billing.billing_run_freeze_snapshots;
DROP TABLE IF EXISTS ms_billing.billing_run_freeze_snapshot_lines;
DROP TABLE IF EXISTS ms_billing.billing_run_freeze_snapshots;
DROP FUNCTION IF EXISTS ms_billing.billing_run_freeze_snapshots_reject_edit();
