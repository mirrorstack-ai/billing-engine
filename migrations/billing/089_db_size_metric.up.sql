-- 089: register the database-size meter infra.db.gib_hours (core-v2#1757,
-- billing-engine#237, rollout core-v2#1758 WP9; owner ruling 2026-10-02, price
-- $0.10 per GiB-month).
--
-- Until now nothing metered Postgres bytes: every module absorbs platform infra
-- at 0 (ms.AbsorbInfra()), so tables that only grow (credit_ledger, watch_events,
-- quiz_attempts, ad_clicks, ...) were the platform's cost and on no bill. This
-- row is the catalog half of the meter; cmd/infra-db-sync is the producer.
--
-- KIND is time_weighted, matching the registry case added with this migration
-- (internal/account/usage/infra.go). The producer emits the GiB STANDING at each
-- closed hour boundary and the rollup integrates it (value * seconds / 3600), so
-- billable_quantity is GiB-hours. The unit is therefore 'GiB-hour', like
-- infra.storage.gib_hours (migration 020).
--
-- 🔴 THE PRICE IS RAW COGS-STYLE, THE CUSTOMER PAYS 1.2x. Every infra.* row is
-- marked up 12/10 at rollup (cycle/money.go isReservedMetric), the same way
-- 078's 122,406 becomes 146,887 and 051's storage 37 becomes 44.4. The owner's
-- $0.10 per GiB-month is read as the CUSTOMER price, as every other price the
-- contracts quote (reconciliation row 3, 2026-10-05):
--
--     0.10 USD / GiB-month / 730 h = 136.99 µ$ per GiB-hour  (customer)
--     136.99 / 1.2                 = 114.16 µ$               (raw)  -> seed 114
--
-- 114 raw bills 136.8 µ$ = $0.0999 per GiB-month at 730 h. Seeding 137 would bill
-- $0.12. The basis is the open reconciliation row 14; if the owner meant 137 raw
-- it is one UPDATE of this row BEFORE the first invoice that carries it (and
-- never after: storage events price from the live row, billing-engine#219).
--
-- Modules still ABSORB it at 0 by default (the SDK expands ms.AbsorbInfra() to
-- price-0 overrides for every catalog metric), so seeding this row charges
-- nobody; a module opts in with ms.Meter("infra.db.gib_hours", ms.Price(n)), a
-- price-only override, exactly as for infra.storage.gib_hours. n is a raw price
-- too. display_group 'database' already exists in the metric_group enum (021).
--
-- Idempotent seed: ON CONFLICT DO NOTHING (the INITIAL value only; a finance
-- UPDATE is authoritative and a re-run must not undo it). Rerunnable.
INSERT INTO ms_billing.metric_definitions (
    module_id, metric, kind, unit, unit_price_micros, active, display_group
) VALUES
    ('00000000-0000-0000-0000-000000000000', 'infra.db.gib_hours', 'time_weighted', 'GiB-hour', 114, true, 'database')
ON CONFLICT (module_id, metric) DO NOTHING;
