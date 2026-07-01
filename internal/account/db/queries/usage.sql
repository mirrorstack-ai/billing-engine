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
-- (migration 018) — NULL for every non-AI event. module_version is the
-- per-event attribution dimension (migration 023, purely reporting — it
-- never affects price) — NULL for every event that carries no version.
-- name: InsertUsageEvent :execrows
INSERT INTO ms_billing.usage_events (
    event_id, account_id, app_id, module_id, metric, kind, value, recorded_at, model, module_version
) VALUES (
    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10
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
--
-- GROUP BY now includes e.module_id (widened from PR #3's (metric, kind)
-- only) so the row can carry the emitting module's id + visibility — a
-- consumer previously had to hardcode a 30% platform-take assumption because
-- it could not see the real value. visibility is the module_visibility
-- mirror (migration 010, developer margin-share dimension — NEVER a customer
-- markup); COALESCE to 'private' matches the settlement default (design
-- §7-B: never under-collect on a lagging publish) for a module with no
-- visibility row yet.
-- name: CurrentPeriodUsageSummary :many
SELECT
    e.module_id                                         AS module_id,
    e.metric                                            AS metric,
    e.kind                                              AS kind,
    COALESCE(SUM(e.value), 0)::numeric                  AS total_quantity,
    COALESCE(MAX(md.unit_price_micros), 0)::bigint      AS unit_price_micros,
    COALESCE(
        SUM(e.value * COALESCE(md.unit_price_micros, 0)),
        0
    )::numeric                                          AS raw_cost_micros,
    -- display_group is the §11 compaction taxonomy the frontend rolls up by.
    -- COALESCE to 'other' so an event whose catalog row is missing (LEFT JOIN
    -- miss) or not-yet-grouped still carries a valid group — the same defensive
    -- default the column itself uses. MAX() picks the single group per
    -- (module, metric, kind) group-by; a metric has exactly one display_group,
    -- so MAX is just "the group" (no aggregation ambiguity).
    COALESCE(MAX(md.display_group), 'other')::ms_billing.metric_group
                                                        AS display_group,
    -- MAX() picks the single visibility per module_id group-by (module_id is
    -- module_visibility's PRIMARY KEY, so at most one row per group).
    COALESCE(MAX(mv.visibility), 'private')::ms_billing.margin_share_class
                                                        AS visibility
FROM ms_billing.usage_events e
LEFT JOIN ms_billing.metric_definitions md
    ON md.module_id = e.module_id AND md.metric = e.metric
LEFT JOIN ms_billing.module_visibility mv
    ON mv.module_id = e.module_id
WHERE e.account_id = $1
  AND e.recorded_at >= $2
  AND e.recorded_at <  $3
GROUP BY e.module_id, e.metric, e.kind
ORDER BY e.metric;

-- UsageHistoryForAccount is the multi-month trend-chart read: it reads the
-- IMMUTABLE billable record (usage_aggregates, written by cmd/billing-cycle's
-- rollup — NOT usage_events) joined to its billing_periods row, for every
-- closed period whose period_start falls in the caller-supplied trailing
-- window [window_start, window_end). This table has never been read by any
-- RPC before GetUsageHistory; the live current-period estimate
-- (CurrentPeriodUsageSummary above) stays the only usage_events reader.
--
-- Grouped at (period, metric, kind) — the SAME granularity GetUsageSummary
-- exposes — by summing across every model (migration 018) and module_version
-- (migration 023) dimension an aggregate row may have split into: a trend
-- chart wants ONE number per metric per period, not one per (metric, model,
-- version) combination (that finer cut is GetVersionBreakdown's job, for the
-- current period only). A period with no usage_aggregates rows yet (rollup
-- hasn't run, or truly zero usage) simply contributes no rows — the caller
-- must not treat a missing month as an error.
-- name: UsageHistoryForAccount :many
SELECT
    bp.period_start                                     AS period_start,
    bp.period_end                                       AS period_end,
    ua.metric                                            AS metric,
    ua.kind                                              AS kind,
    COALESCE(SUM(ua.billable_quantity), 0)::numeric      AS total_quantity,
    COALESCE(MAX(ua.unit_price_micros), 0)::bigint       AS unit_price_micros,
    COALESCE(SUM(ua.raw_cost_micros), 0)::bigint         AS raw_cost_micros,
    COALESCE(SUM(ua.charged_micros), 0)::bigint          AS charged_micros,
    COALESCE(MAX(md.display_group), 'other')::ms_billing.metric_group
                                                         AS display_group
FROM ms_billing.usage_aggregates ua
JOIN ms_billing.billing_periods bp
    ON bp.id = ua.period_id
LEFT JOIN ms_billing.metric_definitions md
    ON md.module_id = ua.module_id AND md.metric = ua.metric
WHERE ua.account_id = $1
  AND bp.period_start >= $2
  AND bp.period_start <  $3
GROUP BY bp.period_start, bp.period_end, ua.metric, ua.kind
ORDER BY bp.period_start ASC, ua.metric ASC;

-- VersionBreakdownForAccount is the per-module_version cost/income breakdown
-- read for ONE period: it sums the immutable billable record
-- (usage_aggregates) grouped by module_version ACROSS every metric (and,
-- within a version, across every model split) so a consumer sees which
-- version of a module is driving cost. Summing billable_quantity across
-- metrics with different units (e.g. request count + byte-hours) is
-- dimensionally rough — it is offered as a secondary total alongside the
-- authoritative money columns (raw_cost_micros / charged_micros), which is
-- the actual "cost/income breakdown" this RPC exists for.
--
-- module_id_filter narrows to one module's versions when has_module_filter is
-- true; when false every one of the owner's modules is included (module_id_filter
-- is then ignored — any well-formed uuid is accepted as a placeholder).
-- name: VersionBreakdownForAccount :many
SELECT
    ua.module_version                                    AS module_version,
    COALESCE(SUM(ua.billable_quantity), 0)::numeric      AS total_quantity,
    COALESCE(SUM(ua.raw_cost_micros), 0)::bigint         AS raw_cost_micros,
    COALESCE(SUM(ua.charged_micros), 0)::bigint          AS charged_micros
FROM ms_billing.usage_aggregates ua
JOIN ms_billing.billing_periods bp
    ON bp.id = ua.period_id
WHERE ua.account_id = $1
  AND bp.period_start = $2
  AND ($3::boolean = false OR ua.module_id = $4)
GROUP BY ua.module_version
ORDER BY ua.module_version;
