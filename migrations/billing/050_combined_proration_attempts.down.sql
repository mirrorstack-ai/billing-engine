-- Down migration 050 — remove durable combined-proration ownership.

DROP TRIGGER IF EXISTS app_module_timers_unresolved_combined_proration_terminal_guard
    ON ms_billing.app_module_overage_timers;
DROP FUNCTION IF EXISTS ms_billing.guard_unresolved_combined_proration_timer_terminal();
DROP TRIGGER IF EXISTS apps_unresolved_combined_proration_terminal_guard
    ON ms_billing.apps;
DROP FUNCTION IF EXISTS ms_billing.guard_unresolved_combined_proration_app_terminal();
DROP TABLE IF EXISTS ms_billing.app_combined_proration_attempt_timers;
DROP TABLE IF EXISTS ms_billing.app_combined_proration_attempts;
