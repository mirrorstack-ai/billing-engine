-- Reverse of 054. Dropped in dependency order.
DROP TABLE IF EXISTS ms_billing.intent_settlement_claims;
DROP TABLE IF EXISTS ms_billing.notice_receipts;
DROP TABLE IF EXISTS ms_billing.billing_authorizations;
DROP TABLE IF EXISTS ms_billing.charge_intent_source_facts;
DROP TABLE IF EXISTS ms_billing.charge_intent_lines;
DROP TRIGGER IF EXISTS charge_intents_sealed ON ms_billing.charge_intents;
DROP FUNCTION IF EXISTS ms_billing.charge_intents_reject_sealed_update();
DROP TABLE IF EXISTS ms_billing.charge_intents;
