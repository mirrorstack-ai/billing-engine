-- 090: infra.db.gib_hours raw price 114 -> 137 (owner rule 2026-10-06,
-- core-v2#1758 WP13: every infra price the customer pays = actual infra COST x 1.2).
--
-- The owner's $0.10 per GiB-month is the COST basis, not the customer price:
--
--     0.10 USD / GiB-month / 730 h = 136.99 µ$ per GiB-hour  (raw cost) -> 137
--     137 x 1.2 (infra markup, cycle/money.go isReservedMetric) = 164.4 µ$
--                                                = $0.12 per GiB-month at 730 h
--
-- Migration 089 read $0.10 as the CUSTOMER price and seeded 114 raw; this is the
-- correction it anticipated ("one UPDATE of this row BEFORE the first invoice that
-- carries it"). Modules absorb the metric at 0 (ms.AbsorbInfra()), so no usage_events
-- row carries a charge for it and no invoice changes. The price lives only in
-- metric_definitions (the sentinel row 089 seeded); there is no per-version row.
--
-- Rerunnable: only a row still at 114 is touched, so a later finance UPDATE and a
-- second run are both left alone.
UPDATE ms_billing.metric_definitions
SET unit_price_micros = 137
WHERE module_id = '00000000-0000-0000-0000-000000000000'
  AND metric = 'infra.db.gib_hours'
  AND unit_price_micros = 114;
