-- Freeze snapshots (migration 087, billing-engine#218): the derivation a
-- boundary freeze was taken from, written once per run and sealed, so a
-- reclaim that re-derives a different figure can say WHICH line moved.
--
-- Nothing here moves money or feeds a money reader. The cycle writes the
-- snapshot best-effort right after a fresh freeze wins, and reads it only to
-- log the line-level drift.
--
--   InsertBillingRunFreezeSnapshot       the header, bound to the run's frozen cents
--   InsertBillingRunFreezeSnapshotLines  the collectable aggregate lines
--   BillingRunFreezeSnapshot             read the header back
--   BillingRunFreezeLineDrift            frozen lines vs live aggregates, changed only

-- InsertBillingRunFreezeSnapshot writes the header ONLY while the run still
-- holds the figure being described: the INSERT … SELECT matches the run row on
-- frozen_charge_cents, so a snapshot can never describe a freeze a concurrent
-- daemon won with a different figure, or one a re-freeze has since moved.
-- First-write-wins on run_id: 0 rows means another attempt already recorded
-- the first derivation (or the run no longer holds these cents), and the
-- caller writes no lines.
-- name: InsertBillingRunFreezeSnapshot :execrows
INSERT INTO ms_billing.billing_run_freeze_snapshots (
    run_id, frozen_cents, usage_charged_micros, allowance_micros, arrears_micros,
    advance_base_micros, advance_overage_micros, advance_domains_micros,
    members_micros, wallet_drawn_micros
)
SELECT r.id, @frozen_cents::bigint,
       @usage_charged_micros::bigint, @allowance_micros::bigint, @arrears_micros::bigint,
       @advance_base_micros::bigint, @advance_overage_micros::bigint, @advance_domains_micros::bigint,
       @members_micros::bigint, @wallet_drawn_micros::bigint
FROM ms_billing.billing_runs r
WHERE r.id = @run_id::uuid
  AND r.frozen_charge_cents = @frozen_cents::bigint
ON CONFLICT (run_id) DO NOTHING;

-- InsertBillingRunFreezeSnapshotLines copies the run's collectable aggregate
-- lines — the exact row set PeriodChargedTotal (cycle.sql) sums: this account,
-- this period window, dev_served = false — and returns what it copied, so the
-- caller can refuse a snapshot whose Σ is not the total the freeze was
-- derived from (a rollup that moved between the two reads).
-- name: InsertBillingRunFreezeSnapshotLines :one
WITH inserted AS (
    INSERT INTO ms_billing.billing_run_freeze_snapshot_lines (
        run_id, app_id, module_id, metric, model, module_version, kind,
        aggregation_key, billable_quantity, unit_price_micros,
        customer_markup_num, customer_markup_den,
        raw_cost_micros, charged_micros, active_seconds, period_days, rolled_up_at
    )
    SELECT r.id, ua.app_id, ua.module_id, ua.metric, ua.model, ua.module_version, ua.kind,
           ua.aggregation_key, ua.billable_quantity, ua.unit_price_micros,
           ua.customer_markup_num, ua.customer_markup_den,
           ua.raw_cost_micros, ua.charged_micros, ua.active_seconds, ua.period_days, ua.rolled_up_at
    FROM ms_billing.billing_runs r
    JOIN ms_billing.usage_aggregates ua ON ua.account_id = r.account_id
    JOIN ms_billing.billing_periods bp ON bp.id = ua.period_id
    WHERE r.id = @run_id::uuid
      AND bp.period_start = r.period_start
      AND bp.period_end   = r.period_end
      AND ua.dev_served   = false
    RETURNING charged_micros
)
SELECT COUNT(*)::bigint AS lines,
       COALESCE(SUM(charged_micros), 0)::bigint AS charged_micros
FROM inserted;

-- name: BillingRunFreezeSnapshot :one
SELECT frozen_cents, usage_charged_micros, allowance_micros, arrears_micros,
       advance_base_micros, advance_overage_micros, advance_domains_micros,
       members_micros, wallet_drawn_micros, snapshotted_at
FROM ms_billing.billing_run_freeze_snapshots
WHERE run_id = @run_id::uuid;

-- BillingRunFreezeLineDrift is the line-level diff between a run's frozen
-- derivation and the live aggregates (same row set as PeriodChargedTotal),
-- joined on the aggregate key. Only lines that differ come back: one present
-- on a single side, or one whose quantity, unit price, markup or charged
-- figure moved. Empty for a run with no snapshot — the caller reads the header
-- first to tell "no drift" from "nothing to compare against".
-- name: BillingRunFreezeLineDrift :many
WITH frozen AS (
    SELECT l.app_id, l.module_id, l.metric, l.model, l.module_version,
           COALESCE(l.aggregation_key, '') AS aggregation_key,
           l.billable_quantity, l.unit_price_micros,
           l.customer_markup_num, l.customer_markup_den, l.charged_micros
    FROM ms_billing.billing_run_freeze_snapshot_lines l
    WHERE l.run_id = @run_id::uuid
),
live AS (
    SELECT ua.app_id, ua.module_id, ua.metric, ua.model, ua.module_version,
           COALESCE(ua.aggregation_key, '') AS aggregation_key,
           ua.billable_quantity, ua.unit_price_micros,
           ua.customer_markup_num, ua.customer_markup_den, ua.charged_micros
    FROM ms_billing.billing_runs r
    JOIN ms_billing.usage_aggregates ua ON ua.account_id = r.account_id
    JOIN ms_billing.billing_periods bp ON bp.id = ua.period_id
    WHERE r.id = @run_id::uuid
      AND bp.period_start = r.period_start
      AND bp.period_end   = r.period_end
      AND ua.dev_served   = false
)
SELECT COALESCE(f.app_id, l.app_id)::uuid                   AS app_id,
       COALESCE(f.module_id, l.module_id)::uuid             AS module_id,
       COALESCE(f.metric, l.metric)::text                   AS metric,
       COALESCE(f.model, l.model)::text                     AS model,
       COALESCE(f.module_version, l.module_version)::text   AS module_version,
       COALESCE(f.aggregation_key, l.aggregation_key)::text AS aggregation_key,
       (f.app_id IS NOT NULL)::boolean                      AS in_frozen,
       (l.app_id IS NOT NULL)::boolean                      AS in_live,
       COALESCE(f.billable_quantity, 0)::text               AS frozen_quantity,
       COALESCE(l.billable_quantity, 0)::text               AS live_quantity,
       COALESCE(f.unit_price_micros, 0)::bigint             AS frozen_unit_price_micros,
       COALESCE(l.unit_price_micros, 0)::bigint             AS live_unit_price_micros,
       COALESCE(f.customer_markup_num, 0)::integer          AS frozen_markup_num,
       COALESCE(f.customer_markup_den, 0)::integer          AS frozen_markup_den,
       COALESCE(l.customer_markup_num, 0)::integer          AS live_markup_num,
       COALESCE(l.customer_markup_den, 0)::integer          AS live_markup_den,
       COALESCE(f.charged_micros, 0)::bigint                AS frozen_charged_micros,
       COALESCE(l.charged_micros, 0)::bigint                AS live_charged_micros
FROM frozen f
FULL OUTER JOIN live l
  ON  l.app_id          = f.app_id
  AND l.module_id       = f.module_id
  AND l.metric          = f.metric
  AND l.model           = f.model
  AND l.module_version  = f.module_version
  AND l.aggregation_key = f.aggregation_key
WHERE f.app_id IS NULL
   OR l.app_id IS NULL
   OR f.billable_quantity   <> l.billable_quantity
   OR f.unit_price_micros   <> l.unit_price_micros
   OR f.customer_markup_num <> l.customer_markup_num
   OR f.customer_markup_den <> l.customer_markup_den
   OR f.charged_micros      <> l.charged_micros
ORDER BY 3, 2, 1, 4, 5, 6;
