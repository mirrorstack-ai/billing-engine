DROP TRIGGER IF EXISTS add_card_requests_reject_retired_org_write
    ON ms_billing.add_card_requests;
DROP FUNCTION IF EXISTS ms_billing.guard_retired_org_add_card_request_write();

DROP TRIGGER IF EXISTS auto_top_up_reject_retired_org_write
    ON ms_billing.credit_auto_topup_configs;
DROP FUNCTION IF EXISTS ms_billing.guard_retired_org_auto_top_up_write();

DROP TRIGGER IF EXISTS payment_methods_reject_retired_org_write
    ON ms_billing.payment_methods_mirror;
DROP FUNCTION IF EXISTS ms_billing.guard_retired_org_payment_method_write();

DROP TRIGGER IF EXISTS credit_ledger_reject_retired_org_future_write
    ON ms_billing.credit_ledger;
DROP FUNCTION IF EXISTS ms_billing.guard_retired_org_credit_ledger_write();

DROP TRIGGER IF EXISTS combined_proration_reject_retired_org_write
    ON ms_billing.app_combined_proration_attempts;
DROP TRIGGER IF EXISTS billing_runs_reject_retired_org_write
    ON ms_billing.billing_runs;
DROP FUNCTION IF EXISTS ms_billing.guard_retired_org_account_write();

DROP TRIGGER IF EXISTS invoices_reject_retired_org_insert
    ON ms_billing.invoices;
DROP FUNCTION IF EXISTS ms_billing.guard_retired_org_invoice_write();

DROP TRIGGER IF EXISTS usage_events_reject_retired_org_insert
    ON ms_billing.usage_events;
DROP FUNCTION IF EXISTS ms_billing.guard_retired_org_usage_insert();

DROP TRIGGER IF EXISTS custom_domains_reject_retired_org_write
    ON ms_billing.app_custom_domains;
DROP FUNCTION IF EXISTS ms_billing.guard_org_domain_write();

DROP TRIGGER IF EXISTS module_timers_reject_retired_org_write
    ON ms_billing.app_module_overage_timers;
DROP FUNCTION IF EXISTS ms_billing.guard_org_timer_write();

DROP TRIGGER IF EXISTS apps_reject_retired_org_write ON ms_billing.apps;
DROP FUNCTION IF EXISTS ms_billing.guard_org_app_write();

DROP TRIGGER IF EXISTS accounts_reject_retired_org_reactivation
    ON ms_billing.accounts;
DROP FUNCTION IF EXISTS ms_billing.guard_org_account_reactivation();

DROP TRIGGER IF EXISTS org_designations_reject_retired
    ON ms_billing.org_billing_designations;
DROP FUNCTION IF EXISTS ms_billing.guard_org_designation_write();

DROP FUNCTION IF EXISTS ms_billing.assert_account_and_funding_billing_active(UUID, UUID);
DROP FUNCTION IF EXISTS ms_billing.lock_account_and_funding_lifecycle_shared(UUID, UUID);
DROP FUNCTION IF EXISTS ms_billing.account_and_funding_lifecycle_orgs(UUID, UUID);
DROP FUNCTION IF EXISTS ms_billing.assert_account_org_billing_active(UUID);
DROP FUNCTION IF EXISTS ms_billing.assert_funding_account_org_billing_active(UUID);
DROP FUNCTION IF EXISTS ms_billing.account_org_billing_active_after_shared_lock(UUID);
DROP FUNCTION IF EXISTS ms_billing.lock_account_org_billing_lifecycle_shared(UUID);
DROP FUNCTION IF EXISTS ms_billing.account_org_billing_lifecycle_orgs(UUID);
DROP FUNCTION IF EXISTS ms_billing.assert_org_billing_active_pair(UUID, UUID);
DROP FUNCTION IF EXISTS ms_billing.assert_org_billing_active(UUID);
DROP FUNCTION IF EXISTS ms_billing.assert_org_billing_active_many(UUID[]);
DROP FUNCTION IF EXISTS ms_billing.lock_org_billing_lifecycle_shared_many(UUID[]);

DROP TABLE IF EXISTS ms_billing.account_funding_authorizations;

DROP TRIGGER IF EXISTS org_deletion_retired_sponsorships_immutable
    ON ms_billing.org_deletion_retired_sponsorships;
DROP TABLE IF EXISTS ms_billing.org_deletion_retired_sponsorships;

DROP TRIGGER IF EXISTS org_deletion_finalizations_immutable
    ON ms_billing.org_deletion_finalizations;
DROP FUNCTION IF EXISTS ms_billing.reject_org_deletion_finalization_mutation();

DROP TABLE IF EXISTS ms_billing.org_deletion_finalizations;

DROP INDEX IF EXISTS ms_billing.credit_ledger_pending_purchase_funder_idx;

ALTER TABLE ms_billing.credit_ledger
    DROP CONSTRAINT IF EXISTS credit_ledger_purchase_funding_arm_check,
    DROP CONSTRAINT IF EXISTS credit_ledger_auto_topup_attempt_fields_check;

-- Migration 049 did not permit manual purchases to retain a Stripe customer.
-- Clear only the 052 snapshot before restoring that exact historical shape.
UPDATE ms_billing.credit_ledger
SET attempt_stripe_customer_id = NULL
WHERE type = 'purchase';

ALTER TABLE ms_billing.credit_ledger
    ADD CONSTRAINT credit_ledger_auto_topup_attempt_fields_check CHECK (
        (
            type = 'auto_topup'
            AND attempt_payment_method_id IS NOT NULL
            AND NULLIF(BTRIM(attempt_stripe_payment_method_id), '') IS NOT NULL
            AND NULLIF(BTRIM(attempt_stripe_customer_id), '') IS NOT NULL
            AND attempt_expires_at IS NOT NULL
        )
        OR (
            type <> 'auto_topup'
            AND attempt_payment_method_id IS NULL
            AND attempt_stripe_payment_method_id IS NULL
            AND attempt_stripe_customer_id IS NULL
            AND attempt_expires_at IS NULL
            AND failure_code IS NULL
        )
    );

ALTER TABLE ms_billing.credit_ledger
    DROP COLUMN IF EXISTS charge_funding_legacy_unresolved,
    DROP COLUMN IF EXISTS charge_funding_generation,
    DROP COLUMN IF EXISTS charge_funding_account_id;

ALTER TABLE ms_billing.invoices
    DROP CONSTRAINT IF EXISTS invoices_funding_provenance_check,
    DROP COLUMN IF EXISTS charge_funding_legacy_unresolved,
    DROP COLUMN IF EXISTS charge_funding_generation,
    DROP COLUMN IF EXISTS charge_funding_account_id;

ALTER TABLE ms_billing.app_module_overage_timers
    DROP CONSTRAINT IF EXISTS module_timers_attempt_funding_check,
    DROP CONSTRAINT IF EXISTS module_timers_funding_arm_check,
    DROP COLUMN IF EXISTS charge_funding_legacy_unresolved,
    DROP COLUMN IF EXISTS charge_funding_generation,
    DROP COLUMN IF EXISTS charge_funding_account_id;

ALTER TABLE ms_billing.app_custom_domains
    DROP CONSTRAINT IF EXISTS custom_domains_attempt_funding_check,
    DROP CONSTRAINT IF EXISTS custom_domains_funding_arm_check,
    DROP COLUMN IF EXISTS charge_funding_legacy_unresolved,
    DROP COLUMN IF EXISTS charge_funding_generation,
    DROP COLUMN IF EXISTS charge_funding_account_id;

ALTER TABLE ms_billing.app_combined_proration_attempts
    DROP CONSTRAINT IF EXISTS combined_proration_funding_arm_check,
    DROP COLUMN IF EXISTS charge_funding_legacy_unresolved,
    DROP COLUMN IF EXISTS charge_funding_generation,
    DROP COLUMN IF EXISTS charge_funding_account_id;

ALTER TABLE ms_billing.billing_runs
    DROP CONSTRAINT IF EXISTS billing_runs_frozen_funding_check,
    DROP CONSTRAINT IF EXISTS billing_runs_funding_arm_check,
    DROP COLUMN IF EXISTS charge_funding_legacy_unresolved,
    DROP COLUMN IF EXISTS charge_funding_generation,
    DROP COLUMN IF EXISTS charge_funding_account_id;

DROP TRIGGER IF EXISTS org_designations_sync_funding_authorization
    ON ms_billing.org_billing_designations;
DROP FUNCTION IF EXISTS ms_billing.sync_org_designation_funding_authorization();

DROP TRIGGER IF EXISTS accounts_sync_funding_authorization
    ON ms_billing.accounts;
DROP FUNCTION IF EXISTS ms_billing.sync_account_funding_authorization();
