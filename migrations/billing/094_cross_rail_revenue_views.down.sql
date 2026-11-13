-- Reverse 094. Drops the cross-rail revenue views and their support objects.
-- Reporting-only and additive, so nothing else in the schema is touched.
DROP VIEW IF EXISTS ms_billing.rev_mrr_movements_monthly;
DROP VIEW IF EXISTS ms_billing.rev_mrr_monthly;
DROP VIEW IF EXISTS ms_billing.rev_credits_deferred;
DROP VIEW IF EXISTS ms_billing.rev_cash_collected_monthly;
DROP VIEW IF EXISTS ms_billing.rev_recognized_monthly;
DROP VIEW IF EXISTS ms_billing.rev_settled_lines;

DROP FUNCTION IF EXISTS ms_billing.rev_normalize_rail(TEXT);

DROP TABLE IF EXISTS ms_billing.rev_plan_price_reference;
DROP TABLE IF EXISTS ms_billing.rev_fx_rates;
