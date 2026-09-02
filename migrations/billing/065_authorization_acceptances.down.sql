-- Reverses 065.
--
-- 🔴 Dropping this table removes the only record that a customer was shown
-- their terms and answered. Safe only while it is empty, which it is in every
-- environment (billing_authorizations measured 0 on 2026-08-31, and nothing
-- issues a challenge yet).

DROP TRIGGER IF EXISTS authorization_acceptances_sealed ON ms_billing.authorization_acceptances;
DROP FUNCTION IF EXISTS ms_billing.authorization_acceptances_reject_edit();
DROP TABLE IF EXISTS ms_billing.authorization_acceptances;
