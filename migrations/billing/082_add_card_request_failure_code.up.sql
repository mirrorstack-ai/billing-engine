-- 082: record WHY a card bind failed (billing-engine#215).
--
-- cmd/account-webhook resolves add_card_requests on setup_intent.succeeded
-- (→ completed / duplicate) but never handled setup_intent.setup_failed: a
-- declined SetupIntent (card refused, 3DS failed, expired) left the row
-- 'pending' for ever and the console's status poll never learned the bind
-- failed — nor why. The 'failed' enum value already exists (migration 004;
-- org deletion uses it); this adds the reason.
--
-- failure_code is Stripe's last_setup_error.decline_code when the issuer gave
-- one (e.g. insufficient_funds, do_not_honor), else its error code (e.g.
-- card_declined, setup_intent_authentication_failure). A short machine token
-- the console maps to a message; never free text, never the raw message.
-- NULL on every non-failed row and on a 'failed' row written by a path that
-- has no Stripe reason (the org-deletion sweep).
ALTER TABLE ms_billing.add_card_requests
    ADD COLUMN IF NOT EXISTS failure_code TEXT NULL;

COMMENT ON COLUMN ms_billing.add_card_requests.failure_code IS
    'Stripe last_setup_error decline_code (else code) recorded by setup_intent.setup_failed; NULL unless the bind failed with a Stripe reason (billing-engine#215).';
