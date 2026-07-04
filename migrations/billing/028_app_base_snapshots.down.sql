-- Down for 028 — drop the per-app-period base snapshots. Rolling back reverts
-- GetAppBill's base display to the live-count estimate for every period (the
-- pre-028 posture): the read path treats a missing row as "no snapshot" and
-- falls back, so no code change is required to survive a rollback window.
-- Charge history itself is unaffected (invoices/billing_runs keep the money
-- audit trail).

DROP TABLE IF EXISTS ms_billing.app_base_snapshots;
