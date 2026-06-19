-- Queries backing internal/account/usage.pgxStore (the usage-metering
-- Store interface). All operate on the ms_billing schema.
--
-- Milestone D, PR #3 (storage + ingest, declaration-first). This file
-- ships only the ingest + manifest-sync + live-summary read path:
--   UpsertMetricDefinition     manifest-fed catalog upsert (kind/unit/price)
--   LookupMetricDefinition     REJECT undeclared metric + resolve kind/price
--   InsertUsageEvent           idempotent raw-event write
--   UpsertModuleVisibility     developer margin-share mirror
--   CurrentPeriodUsageSummary  live charged_micros per metric (fast path)
--
-- The live summary deliberately uses a calendar-month-to-date window
-- (currentPeriodWindow in service.go), NOT a billing_periods row: the
-- authoritative period anchor (signup-day anniversary) and its writer
-- belong to cmd/billing-cycle, so the OpenPeriodForAccount upsert ships
-- with its first caller in PR #5/#6 rather than as a dead query here.
-- Migration 008 still ships the billing_periods table now (the schema
-- lands clean ahead of its writer), but no query in PR #3 touches it.
--
-- DEFERRED to PR #5 (cmd/billing-cycle), where they'd be dead code here:
--   OpenPeriodForAccount (the billing_periods upsert / period anchor),
--   the per-kind rollup SELECTs (count/sum→SUM, peak→MAX,
--   time_weighted→∫v dt over ordered events) and UpsertUsageAggregate.
-- Those write the immutable snapshotted billable record; this PR has no
-- rollup, so adding them now would ship unused queries.

-- UpsertMetricDefinition syncs one (module, metric) row from the module
-- MANIFEST (declaration-first): api-platform calls SetMetricDefinitions on
-- install/publish with every metric the module declared via ms.Meter, and
-- this upsert keeps the catalog authoritative. Keyed (module_id, metric);
-- a re-sync updates kind/unit/price/active in place. unit_price_micros is
-- NULL for a metered-but-unpriced metric.
-- name: UpsertMetricDefinition :exec
INSERT INTO ms_billing.metric_definitions (
    module_id, metric, kind, unit, unit_price_micros, active
) VALUES (
    $1, $2, $3, $4, $5, $6
)
ON CONFLICT (module_id, metric)
DO UPDATE SET
    kind              = EXCLUDED.kind,
    unit              = EXCLUDED.unit,
    unit_price_micros = EXCLUDED.unit_price_micros,
    active            = EXCLUDED.active;

-- LookupMetricDefinition returns the authoritative kind + per-unit customer
-- price for a (module, metric). The ingest path uses it to REJECT an
-- undeclared metric (no row → INVALID_INPUT) and to snapshot `kind` onto
-- the usage_events row; an inactive metric is handled in Go.
-- name: LookupMetricDefinition :one
SELECT kind, unit, unit_price_micros, active
FROM ms_billing.metric_definitions
WHERE module_id = $1 AND metric = $2;

-- InsertUsageEvent writes one raw metered fact, idempotent on event_id.
-- ON CONFLICT(event_id) DO NOTHING makes an at-least-once retry of the
-- same SDK call (same minted eventId) a no-op. :execrows so the caller
-- can tell a fresh insert (1) from a deduped retry (0). account_id is
-- nullable (lazy account); kind is the declared kind resolved from
-- metric_definitions. model is the per-event AI pricing dimension
-- (migration 018) — NULL for every non-AI event.
-- name: InsertUsageEvent :execrows
INSERT INTO ms_billing.usage_events (
    event_id, account_id, app_id, module_id, metric, kind, value, recorded_at, model
) VALUES (
    $1, $2, $3, $4, $5, $6, $7, $8, $9
)
ON CONFLICT (event_id) DO NOTHING;

-- UpsertModuleVisibility records a module's published/private visibility
-- (developer margin-share dimension; NEVER a customer markup). Fired by
-- the SetModuleVisibility RPC on publish/unpublish.
-- name: UpsertModuleVisibility :exec
INSERT INTO ms_billing.module_visibility (module_id, visibility)
VALUES ($1, $2)
ON CONFLICT (module_id)
DO UPDATE SET visibility = EXCLUDED.visibility;

-- CurrentPeriodUsageSummary is the LIVE current-period fast path used by
-- GetUsageSummary before any rollup exists: it sums raw usage_events for
-- the account in [period_start, period_end) joined to metric_definitions
-- and projects raw_cost_micros per metric = quantity × unit_price_micros.
-- For custom metrics this declared price IS the customer charge (NO blanket
-- 1.2×); the 1.2× is platform-infra-only and not in this PR's scope.
--
-- This is the live aggregate-on-read summary only — NOT the immutable
-- billable record (that is usage_aggregates, written at rollup in PR #5).
-- All four kinds are summed here as a coarse running total; the exact
-- per-kind rollup (MAX for peak, integral for time_weighted) is a PR #5
-- concern and does not affect the customer-facing live estimate.
-- name: CurrentPeriodUsageSummary :many
SELECT
    e.metric                                            AS metric,
    e.kind                                              AS kind,
    COALESCE(SUM(e.value), 0)::numeric                  AS total_quantity,
    COALESCE(MAX(md.unit_price_micros), 0)::bigint      AS unit_price_micros,
    COALESCE(
        SUM(e.value * COALESCE(md.unit_price_micros, 0)),
        0
    )::numeric                                          AS raw_cost_micros
FROM ms_billing.usage_events e
LEFT JOIN ms_billing.metric_definitions md
    ON md.module_id = e.module_id AND md.metric = e.metric
WHERE e.account_id = $1
  AND e.recorded_at >= $2
  AND e.recorded_at <  $3
GROUP BY e.metric, e.kind
ORDER BY e.metric;
