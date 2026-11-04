-- Migration 093 down — reverse invoice seller identity, provider accounts,
-- sealed credentials. Pure structural rollback: nothing seeds data, so there
-- is nothing to un-insert, only tables/triggers/functions to drop in
-- dependency order.

DROP TRIGGER IF EXISTS invoice_credential_audit_append_only ON ms_billing.invoice_credential_audit;
DROP FUNCTION IF EXISTS ms_billing.invoice_credential_audit_reject_mutation();

DROP TABLE IF EXISTS ms_billing.invoice_credential_audit;

DROP TRIGGER IF EXISTS invoice_provider_accounts_set_updated_at ON ms_billing.invoice_provider_accounts;
DROP TABLE IF EXISTS ms_billing.invoice_provider_accounts;

DROP TRIGGER IF EXISTS invoice_sellers_set_updated_at ON ms_billing.invoice_sellers;
DROP TABLE IF EXISTS ms_billing.invoice_sellers;
