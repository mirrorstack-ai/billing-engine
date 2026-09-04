-- Reverses 071. The triggers and the function go with the table: leaving the
-- guard behind would refuse writes on behalf of a ledger that no longer exists.
DROP TRIGGER IF EXISTS app_custom_domains_attribution_agrees ON ms_billing.app_custom_domains;
DROP TRIGGER IF EXISTS app_module_overage_timers_attribution_agrees ON ms_billing.app_module_overage_timers;
DROP TRIGGER IF EXISTS apps_attribution_agrees ON ms_billing.apps;
DROP FUNCTION IF EXISTS ms_billing.app_account_attribution_agrees();
DROP TABLE IF EXISTS ms_billing.app_transfer_events;
