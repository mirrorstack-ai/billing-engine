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
-- The live summary deliberately uses the account's live anchored-period window
-- (billingperiod.AnchoredPeriodWindow, keyed on the card-binding day — ADR
-- 0005), NOT a billing_periods row: the authoritative period rows and their
-- writer belong to cmd/billing-cycle, so the OpenPeriodForAccount upsert ships
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
    module_id, metric, kind, aggregation_key, unit, unit_price_micros, active
) VALUES (
    @module_id::uuid, @metric::text, @kind::ms_billing.metric_kind,
    sqlc.narg(aggregation_key)::text, @unit::text,
    sqlc.narg(unit_price_micros)::bigint, @active::boolean
)
ON CONFLICT (module_id, metric)
DO UPDATE SET
    kind              = EXCLUDED.kind,
    aggregation_key   = EXCLUDED.aggregation_key,
    unit              = EXCLUDED.unit,
    unit_price_micros = EXCLUDED.unit_price_micros,
    active            = EXCLUDED.active;

-- UpsertMetricVersionPrice writes ONE immutable per-(module, metric,
-- module_version) price snapshot (migration 044, usage-time-pricing Phase 1
-- — SetMetricVersionPrices, the control-plane RPC api-platform fires at
-- version PUBLISH). UNLIKE UpsertMetricDefinition (which DOES UPDATE — the
-- catalog is a live, mutable row), this is ON CONFLICT DO NOTHING: the table
-- has NO update path at all, by design. A duplicate publish of the exact
-- same (module_id, metric, module_version) is a no-op, never an overwrite —
-- this is what makes a mid-period re-price of a LATER version never
-- retroactively change an EARLIER version's already-billed price. :execrows
-- so the caller can tell a fresh snapshot (1) from an already-published,
-- unchanged one (0) — both are success, never an error.
-- name: UpsertMetricVersionPrice :execrows
INSERT INTO ms_billing.metric_version_prices (
    module_id, metric, module_version, unit_price_micros
) VALUES (
    $1, $2, $3, $4
)
ON CONFLICT (module_id, metric, module_version) DO NOTHING;

-- UpsertInfraPriceOverride writes ONE per-(module, metric) price OVERRIDE for a
-- reserved platform-infra metric (decision 19 §4.3): the ms.Meter("infra.X",
-- ms.Price(n)) override a module declared for a platform-MEASURED metric. It is
-- the control-plane twin of RecordInfraUsage's gate and the INVERSE of
-- UpsertMetricDefinition (which syncs a module's OWN custom metrics and rejects
-- reserved names) — the SetMetricDefinitions reserved-name guard therefore stays
-- intact; only this dedicated seam persists a reserved override.
--
-- PRICE-ONLY: the row carries the caller's unit_price_micros but INHERITS kind +
-- unit from the SENTINEL base catalog row (module_id = the all-zero platform-infra
-- sentinel, seeded by migrations 017/018/020) via INSERT ... SELECT — the caller
-- never supplies kind/unit (the platform owns infra-metric semantics). The written
-- row keys the REAL @module_id, so the app bill's dual-price resolution (decision
-- 19 §4.2) finds it by the event's real module_id while the sentinel row stays the
-- platform default. active is forced true (a declared override is live).
--
-- :execrows so the store can distinguish a real upsert (1) from a SELECT that
-- matched no sentinel row (0 → a seed drift the store surfaces as an error, rather
-- than a silent no-op). ON CONFLICT (module_id, metric) DO UPDATE keeps it
-- idempotent — a re-publish updates the price in place; kind/unit stay the
-- sentinel's, so the update is price-only (modeled on migration 018's
-- metric_model_prices, a secondary price layered over the same sentinel row).
-- name: UpsertInfraPriceOverride :execrows
INSERT INTO ms_billing.metric_definitions (
    module_id, metric, kind, aggregation_key, unit, unit_price_micros, active
)
SELECT
    @module_id::uuid,
    sentinel.metric,
    sentinel.kind,
    sentinel.aggregation_key,
    sentinel.unit,
    @unit_price_micros::bigint,
    true
FROM ms_billing.metric_definitions sentinel
WHERE sentinel.module_id = '00000000-0000-0000-0000-000000000000'
  AND sentinel.metric    = @metric::text
ON CONFLICT (module_id, metric)
DO UPDATE SET
    unit_price_micros = EXCLUDED.unit_price_micros,
    aggregation_key   = EXCLUDED.aggregation_key,
    active            = true;

-- AbsorbAllInfraPriceOverrides writes a price-0 override for EVERY active
-- platform-infra metric in one statement — the ms.AbsorbInfra() declaration, for
-- a module that bills its customer through its own meters and passes no platform
-- infrastructure cost through on top.
--
-- 🔴 THE SET IS THE SENTINEL CATALOG, NOT A LIST IN CODE. Enumerating "all infra
-- metrics" anywhere but here — in the SDK, in api-platform, in a Go slice beside
-- platformInfraKind — is a copy that drifts. It already did: the SDK's own
-- documented example named infra.compute.ms for a year after migration 019
-- renamed it and 022 deleted it, so the override it produced resolved to nothing.
-- Reading the sentinel rows means a metric seeded tomorrow is absorbed tomorrow,
-- with no republish and no second list to keep in step.
--
-- Same INSERT ... SELECT shape as UpsertInfraPriceOverride (kind + unit inherited
-- from the sentinel, price supplied), just unfiltered by metric and pinned to 0.
-- Runs BEFORE the per-metric upserts in the same transaction, so an explicit
-- ms.Meter("infra.X", ms.Price(n)) overwrites the 0 and wins — "absorb everything
-- except egress" is expressible. :execrows so the store can report the count.
-- name: AbsorbAllInfraPriceOverrides :execrows
INSERT INTO ms_billing.metric_definitions (
    module_id, metric, kind, aggregation_key, unit, unit_price_micros, active
)
SELECT
    @module_id::uuid,
    sentinel.metric,
    sentinel.kind,
    sentinel.aggregation_key,
    sentinel.unit,
    0,
    true
FROM ms_billing.metric_definitions sentinel
WHERE sentinel.module_id = '00000000-0000-0000-0000-000000000000'
  AND sentinel.active
ON CONFLICT (module_id, metric)
DO UPDATE SET
    unit_price_micros = 0,
    aggregation_key   = EXCLUDED.aggregation_key,
    active            = true;

-- DeleteInfraPriceOverridesNotIn makes a module's reserved-metric override set
-- AUTHORITATIVE: whatever the manifest no longer declares stops applying.
--
-- 🔴 WITHOUT THIS AN OVERRIDE IS WRITE-ONCE. The sync was upsert-only, so
-- deleting ms.Meter("infra.X", ms.Price(n)) from a module left the row in place
-- and the app owner kept being charged at a price the module no longer declares
-- — silently, forever, and now visibly: the app bill renders an overridden infra
-- line at that price, toned as a deliberate discount or markup.
--
-- Scoped three ways so it can only ever remove what this sync owns: to ONE
-- module_id (never the all-zero sentinel — the service rejects that caller), to
-- the RESERVED namespaces (a module's own custom metric_definitions rows are
-- SetMetricDefinitions' business and must survive untouched), and to metrics
-- absent from @keep. An empty @keep clears every override the module has, which
-- is exactly what removing the last declaration must do.
-- name: DeleteInfraPriceOverridesNotIn :execrows
DELETE FROM ms_billing.metric_definitions
WHERE module_id = @module_id::uuid
  AND (metric LIKE 'infra.%' OR metric LIKE 'platform.%')
  AND NOT (metric = ANY(@keep::text[]));

-- DeleteInfraPriceOverridesNotInOrActive is the ms.AbsorbInfra() counterpart of
-- DeleteInfraPriceOverridesNotIn: the module keeps a row for every ACTIVE
-- sentinel metric (AbsorbAllInfraPriceOverrides just wrote them) plus anything
-- explicitly named in @keep, and loses the rest.
--
-- The "rest" is not empty: a metric the sentinel catalog has since DEACTIVATED
-- still has this module's stale override row, and absorb-all skips it (it
-- selects `sentinel.active`). Leaving it would resurrect a price for a metric
-- the platform retired.
-- name: DeleteInfraPriceOverridesNotInOrActive :execrows
DELETE FROM ms_billing.metric_definitions t
WHERE t.module_id = @module_id::uuid
  AND (t.metric LIKE 'infra.%' OR t.metric LIKE 'platform.%')
  AND NOT (t.metric = ANY(@keep::text[]))
  AND NOT EXISTS (
      SELECT 1
      FROM ms_billing.metric_definitions sentinel
      WHERE sentinel.module_id = '00000000-0000-0000-0000-000000000000'
        AND sentinel.active
        AND sentinel.metric = t.metric
  );

-- LookupMetricDefinition returns the authoritative kind + per-unit customer
-- price for a (module, metric). The ingest path uses it to REJECT an
-- undeclared metric (no row → INVALID_INPUT) and to snapshot `kind` onto
-- the usage_events row; an inactive metric is handled in Go.
-- name: LookupMetricDefinition :one
SELECT kind, aggregation_key, unit, unit_price_micros, active
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
-- dev_served is the migration-073 tunnel flag: true when api-platform
-- authenticated the call with the module's LIVE TUNNEL SESSION secret rather
-- than its deployed credential. It is a property of the FACT and is stored
-- here so it survives to the aggregate unchanged; false (the wire default) is
-- ordinary chargeable usage. The ON CONFLICT (event_id) DO NOTHING idempotency
-- is UNTOUCHED — dev_served rides along on the insert and, exactly like every
-- other column, a deduped retry never rewrites it.
-- name: InsertUsageEvent :execrows
INSERT INTO ms_billing.usage_events (
    event_id, account_id, app_id, module_id, metric, kind, value, recorded_at,
    model, module_version, observation_version, subject, metadata, occurred_at,
    billable_at, aggregation_key, payload_fingerprint, occurrence_policy,
    dev_served
) VALUES (
    @event_id::text, sqlc.narg(account_id)::uuid, @app_id::uuid,
    @module_id::uuid, @metric::text, @kind::ms_billing.metric_kind,
    @value::numeric, @recorded_at::timestamptz, sqlc.narg(model)::text,
    sqlc.narg(module_version)::text, @observation_version::smallint,
    sqlc.narg(subject)::text, sqlc.narg(metadata)::json,
    sqlc.narg(occurred_at)::timestamptz, @billable_at::timestamptz,
    sqlc.narg(aggregation_key)::text,
    @payload_fingerprint::bytea, @occurrence_policy::text,
    @dev_served::boolean
)
ON CONFLICT (event_id) DO NOTHING;

-- UsageEventFingerprint resolves an event-id retry after an INSERT conflict.
-- A NULL fingerprint identifies a pre-055 historical row: it remains a legacy
-- idempotent success because its original authoritative owner cannot be
-- reconstructed safely enough to compare a new canonical payload.
-- name: UsageEventFingerprint :one
SELECT payload_fingerprint
FROM ms_billing.usage_events
WHERE event_id = $1;

-- UsageRejectionFingerprint extends event-id collision detection across a
-- previous policy rejection. The identical payload may become admissible later
-- (for example once a future timestamp enters tolerance), but the id can never
-- be rebound to different semantics.
-- name: UsageRejectionFingerprint :one
SELECT payload_fingerprint
FROM ms_billing.usage_observation_rejections
WHERE event_id = $1
ORDER BY rejected_at ASC
LIMIT 1;

-- LockUsageAccountActivation serializes v2 period derivation with the
-- first-card activation transition. The caller compares this authoritative
-- row state with the precomputed anchor and retries if activation won first.
-- Rollup takes the same row lock before its period barrier.
-- name: LockUsageAccountActivation :one
SELECT activated_at
FROM ms_billing.accounts
WHERE id = @account_id::uuid
FOR SHARE;

-- UsagePeriodClosed is evaluated while the caller holds the account-period
-- advisory transaction lock shared with rollup. `closing` is terminal for
-- observation intake even if a later payment attempt is retried.
-- name: UsagePeriodClosed :one
SELECT (
    EXISTS (
        SELECT 1
        FROM ms_billing.billing_periods p
        WHERE p.account_id = @account_id::uuid
          AND p.period_start = @period_start::timestamptz
          AND p.period_end = @period_end::timestamptz
          AND p.status IN ('closing', 'invoiced')
    )
    OR EXISTS (
        SELECT 1
        FROM ms_billing.billing_runs r
        WHERE r.account_id = @account_id::uuid
          AND r.period_start = @period_start::timestamptz
          AND r.period_end = @period_end::timestamptz
    )
    OR EXISTS (
        -- Defense in depth for any pre-055/manual row whose status repair was
        -- missed: a frozen aggregate is already an immutable rollup snapshot.
        SELECT 1
        FROM ms_billing.billing_periods p
        JOIN ms_billing.usage_aggregates aggregate ON aggregate.period_id = p.id
        WHERE p.account_id = @account_id::uuid
          AND p.period_start = @period_start::timestamptz
          AND p.period_end = @period_end::timestamptz
    )
)::boolean;

-- UsageSubjectPeak reads the current keyed maximum under the account-period
-- advisory lock. The ingest service uses only the positive increase as its
-- disposable-credit fast-path delta; rollup remains the authoritative bill.
-- name: UsageSubjectPeak :one
SELECT COALESCE(MAX(value), 0)::numeric
FROM ms_billing.usage_events
WHERE account_id = @account_id::uuid
  AND app_id = @app_id::uuid
  AND module_id = @module_id::uuid
  AND metric = @metric::text
  AND COALESCE(model, '') = @model::text
  AND COALESCE(module_version, '') = @module_version::text
  AND subject = @subject::text
  AND aggregation_key = 'subject'
  AND COALESCE(billable_at, recorded_at) >= @period_start::timestamptz
  AND COALESCE(billable_at, recorded_at) <  @period_end::timestamptz;

-- InsertUsageObservationRejection persists only bounded identifiers and the
-- canonical fingerprint. Diagnostic metadata is deliberately never stored on
-- a rejected row.
-- name: InsertUsageObservationRejection :execrows
INSERT INTO ms_billing.usage_observation_rejections (
    event_id, account_id, app_id, module_id, owner_user_id, owner_org_id,
    metric, subject, occurred_at, reason, payload_fingerprint
) VALUES (
    @event_id::text, sqlc.narg(account_id)::uuid, @app_id::uuid,
    @module_id::uuid, sqlc.narg(owner_user_id)::uuid,
    sqlc.narg(owner_org_id)::uuid, @metric::text, sqlc.narg(subject)::text,
    sqlc.narg(occurred_at)::timestamptz, @reason::text,
    @payload_fingerprint::bytea
)
ON CONFLICT (event_id, reason, payload_fingerprint) DO NOTHING;

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
-- dev_served (migration 073) is a GROUP BY dimension and a returned column, not
-- a filter: this summary is a DISPLAY read, and a developer testing a paid
-- meter has to be able to see what it would have cost. The consumer splits the
-- two sections on the flag; nothing here sums them together.
-- name: CurrentPeriodUsageSummary :many
WITH base_events AS (
    SELECT
        app_id, module_id, metric, kind, aggregation_key, subject, dev_served,
        COALESCE(model, '') AS model,
        COALESCE(module_version, '') AS module_version,
        value
    FROM ms_billing.usage_events
    WHERE account_id = $1
      AND COALESCE(billable_at, recorded_at) >= $2
      AND COALESCE(billable_at, recorded_at) <  $3
),
billable_events AS (
    -- Every legacy/non-keyed row keeps the exact coarse live behavior.
    SELECT app_id, module_id, metric, kind, model, module_version, dev_served, value AS billable_value
    FROM base_events
    WHERE aggregation_key IS DISTINCT FROM 'subject'
    UNION ALL
    -- Keyed peak is exact even before rollup: one MAX per authoritative
    -- subject inside the existing app/model/version bill-line dimensions.
    SELECT
        app_id, module_id, metric, kind, model, module_version, dev_served,
        MAX(value)::numeric AS billable_value
    FROM base_events
    WHERE aggregation_key = 'subject'
    GROUP BY app_id, module_id, metric, kind, model, module_version, dev_served, subject
)
SELECT
    e.module_id                                         AS module_id,
    e.metric                                            AS metric,
    e.kind                                              AS kind,
    e.dev_served                                        AS dev_served,
    COALESCE(SUM(e.billable_value), 0)::numeric         AS total_quantity,
    COALESCE(MAX(md.unit_price_micros), 0)::bigint      AS unit_price_micros,
    COALESCE(
        SUM(e.billable_value * COALESCE(md.unit_price_micros, 0)),
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
FROM billable_events e
LEFT JOIN ms_billing.metric_definitions md
    ON md.module_id = e.module_id AND md.metric = e.metric
LEFT JOIN ms_billing.module_visibility mv
    ON mv.module_id = e.module_id
GROUP BY e.module_id, e.metric, e.kind, e.dev_served
ORDER BY e.metric, e.dev_served;

-- UsageHistoryForAccount is the multi-month trend-chart read: it reads the
-- IMMUTABLE billable record (usage_aggregates, written by cmd/billing-cycle's
-- rollup — NOT usage_events) joined to its billing_periods row, for every
-- closed period whose period_start falls in the caller-supplied trailing
-- window [window_start, window_end). This table has never been read by any
-- RPC before GetUsageHistory; the live current-period estimate
-- (CurrentPeriodUsageSummary above) stays the only usage_events reader.
--
-- Grouped at (period, module, metric, kind) — the SAME granularity
-- GetUsageSummary exposes — by summing across every model (migration 018) and
-- module_version (migration 023) dimension an aggregate row may have split
-- into: a trend chart wants ONE number per module-metric per period, not one
-- per (metric, model, version) combination (that finer cut is
-- GetVersionBreakdown's job, for the current period only). module_id +
-- visibility ride along exactly as on CurrentPeriodUsageSummary so a consumer
-- can scope history to one module and apply the real margin-share class. A
-- period with no usage_aggregates rows yet (rollup hasn't run, or truly zero
-- usage) simply contributes no rows — the caller must not treat a missing
-- month as an error.
-- name: UsageHistoryForAccount :many
SELECT
    bp.period_start                                     AS period_start,
    bp.period_end                                       AS period_end,
    ua.module_id                                         AS module_id,
    ua.metric                                            AS metric,
    ua.kind                                              AS kind,
    COALESCE(SUM(ua.billable_quantity), 0)::numeric      AS total_quantity,
    COALESCE(MAX(ua.unit_price_micros), 0)::bigint       AS unit_price_micros,
    COALESCE(SUM(ua.raw_cost_micros), 0)::bigint         AS raw_cost_micros,
    COALESCE(SUM(ua.charged_micros), 0)::bigint          AS charged_micros,
    COALESCE(MAX(md.display_group), 'other')::ms_billing.metric_group
                                                         AS display_group,
    -- MAX() picks the single visibility per module_id group-by (module_id is
    -- module_visibility's PRIMARY KEY); COALESCE to 'private' is the
    -- settlement default (§7-B: never under-collect on a lagging publish).
    COALESCE(MAX(mv.visibility), 'private')::ms_billing.margin_share_class
                                                         AS visibility
FROM ms_billing.usage_aggregates ua
JOIN ms_billing.billing_periods bp
    ON bp.id = ua.period_id
LEFT JOIN ms_billing.metric_definitions md
    ON md.module_id = ua.module_id AND md.metric = ua.metric
LEFT JOIN ms_billing.module_visibility mv
    ON mv.module_id = ua.module_id
WHERE ua.account_id = $1
  AND bp.period_start >= $2
  AND bp.period_start <  $3
  -- dev_served rows are EXCLUDED (migration 073). Every period this reads is
  -- CLOSED and invoiced, so each of its numbers has an invoice to agree with;
  -- folding in usage that was never billed would make the trend chart disagree
  -- with the customer's own receipts, every month a developer used a tunnel.
  -- The current period's dev/deployed split is a live read
  -- (AppBillLines / AppUsageSummary), not a historical one.
  AND ua.dev_served = false
GROUP BY bp.period_start, bp.period_end, ua.module_id, ua.metric, ua.kind
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
  -- dev_served rows are EXCLUDED (migration 073): this is a per-version
  -- COST/INCOME breakdown, and a version's cost is what it actually cost.
  -- A tunnel-served row would raise a version's apparent spend by an amount
  -- that appears on no invoice.
  AND ua.dev_served = false
GROUP BY ua.module_version
ORDER BY ua.module_version;

-- AppUsageSummary is the APP-OWNER's bill for ONE app in the CURRENT period —
-- the read behind /apps/{appId}/settings/billing (what the app owner PAYS for
-- this one app). It gates on account_id (the payer) and app_id (the one app),
-- and returns one line per (module_id, metric, model, module_version) so the
-- billing UI can render per-module, per-version sub-lines. There is NO customer
-- markup by module visibility on this page: the app owner pays the module's
-- DECLARED unit_price_micros per metered unit (design: billing_markup_strategy —
-- the developer margin-share 30/15 by visibility is settled DEV-side, off this
-- bill, and is deliberately absent here — no visibility / markup columns).
--
-- Two sources, "rolled-up-else-live" (mirrors the CurrentPeriodUsageSummary
-- COALESCE-to-events fast path, extended to the app scope + the model /
-- module_version dimensions):
--
--   * rolled: once cmd/billing-cycle has rolled this app's current period into
--     the IMMUTABLE billable record (usage_aggregates, joined to its
--     billing_periods row by period_start), read the frozen
--     billable_quantity / unit_price_micros / charged_micros directly. This is
--     the SAME current-period aggregates read GetVersionBreakdown uses.
--   * live: when no rollup exists yet for this (account, app, period), estimate
--     LIVE from raw usage_events exactly like CurrentPeriodUsageSummary —
--     billable_quantity = SUM(value); unit_price_micros = the catalog price;
--     charged_micros = SUM(value × unit_price) with NO markup (custom-metric
--     charge == raw cost). model / module_version COALESCE to '' for an event
--     that carries neither (matching the rollup's own COALESCE keying).
--
-- The gate is all-or-nothing per (account, app, period): the rollup writes a
-- whole period at once, so `WHERE NOT EXISTS (SELECT 1 FROM rolled)` shows the
-- live estimate ONLY until the first aggregate row for this app+period lands,
-- then flips to the frozen record. charged_micros is projected ::numeric on
-- both branches (the aggregate's BIGINT widens losslessly) so the row shape is
-- uniform; the store rounds it half-up through the shared micros decoder.
--
-- OUT OF SCOPE (design docs 15): the single-price re-pricing correction and the
-- never-closed-period reconciliation between the live anchored-period window and
-- the authoritative billing_periods rows — this query uses the current price
-- model and the account's anchored window, matching every other current-period
-- reader (the window is resolved caller-side and passed in as [start, end)).
-- name: AppUsageSummary :many
WITH rolled AS (
    SELECT
        ua.module_id                                       AS module_id,
        ua.metric                                          AS metric,
        ua.kind                                            AS kind,
        ua.model                                           AS model,
        ua.module_version                                  AS module_version,
        -- dev_served (migration 073) is a LINE DIMENSION here, never a filter:
        -- the console renders tunnel-served usage as its own section, priced,
        -- with the money total taken from the non-dev lines only. It is part of
        -- the aggregate's idempotency key, so this GROUP BY still resolves one
        -- source row per output row.
        ua.dev_served                                      AS dev_served,
        COALESCE(SUM(ua.billable_quantity), 0)::numeric    AS billable_quantity,
        COALESCE(MAX(ua.unit_price_micros), 0)::bigint     AS unit_price_micros,
        COALESCE(SUM(ua.charged_micros), 0)::numeric       AS charged_micros,
        -- active_seconds / period_days (migration 044, usage-time-pricing
        -- Phase 1): the GROUP BY key here is already the exact idempotency
        -- key of one usage_aggregates row (module_id, metric, kind, model,
        -- module_version), so MAX over a singleton group is just "the
        -- value" — same pattern as unit_price_micros above. NULL when the
        -- row predates these columns or is an additive (count/sum) kind
        -- that legitimately never populates them (migration 044 header).
        MAX(ua.active_seconds)::numeric                    AS active_seconds,
        MAX(ua.period_days)::numeric                       AS period_days
    FROM ms_billing.usage_aggregates ua
    JOIN ms_billing.billing_periods bp
        ON bp.id = ua.period_id
    WHERE ua.account_id   = @account_id::uuid
      AND ua.app_id       = @app_id::uuid
      AND bp.period_start = @period_start::timestamptz
    GROUP BY ua.module_id, ua.metric, ua.kind, ua.model, ua.module_version, ua.dev_served
),
live_base AS (
    SELECT
        module_id, metric, kind, aggregation_key, subject, dev_served,
        COALESCE(model, '') AS model,
        COALESCE(module_version, '') AS module_version,
        value
    FROM ms_billing.usage_events
    WHERE account_id = @account_id::uuid
      AND app_id = @app_id::uuid
      AND COALESCE(billable_at, recorded_at) >= @period_start::timestamptz
      AND COALESCE(billable_at, recorded_at) <  @period_end::timestamptz
),
live_values AS (
    SELECT module_id, metric, kind, model, module_version, dev_served, value AS billable_value
    FROM live_base
    WHERE aggregation_key IS DISTINCT FROM 'subject'
    UNION ALL
    SELECT
        module_id, metric, kind, model, module_version, dev_served,
        MAX(value)::numeric AS billable_value
    FROM live_base
    WHERE aggregation_key = 'subject'
    GROUP BY module_id, metric, kind, model, module_version, dev_served, subject
),
live AS (
    SELECT
        e.module_id                                        AS module_id,
        e.metric                                           AS metric,
        e.kind                                             AS kind,
        e.model                                             AS model,
        e.module_version                                    AS module_version,
        e.dev_served                                        AS dev_served,
        COALESCE(SUM(e.billable_value), 0)::numeric         AS billable_quantity,
        COALESCE(MAX(md.unit_price_micros), 0)::bigint     AS unit_price_micros,
        COALESCE(
            SUM(e.billable_value * COALESCE(md.unit_price_micros, 0)),
            0
        )::numeric                                         AS charged_micros,
        -- The live (pre-rollup) estimate has no window-segmentation logic —
        -- that lives only in cmd/billing-cycle's rollup. Explicit NULL,
        -- type-matched to the rolled branch so the UNION ALL type-checks:
        -- before a period rolls up, the active window is simply unknown.
        NULL::numeric                                      AS active_seconds,
        NULL::numeric                                      AS period_days
    FROM live_values e
    LEFT JOIN ms_billing.metric_definitions md
        ON md.module_id = e.module_id AND md.metric = e.metric
    GROUP BY e.module_id, e.metric, e.kind, e.model, e.module_version, e.dev_served
)
SELECT
    module_id, metric, kind, model, module_version, dev_served,
    billable_quantity, unit_price_micros, charged_micros,
    active_seconds, period_days
FROM rolled
UNION ALL
SELECT
    module_id, metric, kind, model, module_version, dev_served,
    billable_quantity, unit_price_micros, charged_micros,
    active_seconds, period_days
FROM live
WHERE NOT EXISTS (SELECT 1 FROM rolled)
ORDER BY module_id, metric, model, module_version, dev_served;

-- AppBillLines is the FULL per-app bill's line source — the read behind
-- GetAppBill (the app-owner's 最終費用 bill for ONE app in ONE period). It is
-- AppUsageSummary widened into the whole pricing structure: it returns EVERY
-- usage line for the (account, app, period), BOTH the customer-facing module
-- usage (custom metrics, priced at the declared unit_price with NO markup) AND
-- the platform-infra plane (reserved infra.* / platform.* metrics, priced at the
-- 1.2× infra markup). The Go service (GetAppBill) partitions these by name:
--   * a NON-reserved line → 模組使用量 MODULE USAGE (its own displayed line),
--   * a reserved infra.* / platform.* line → folded into the 基礎設施
--     INFRASTRUCTURE total (its own summary line; NOT shown per-metric).
-- Splitting in Go (not two queries) keeps ONE rolled-up-else-live gate over ONE
-- scan, and lets the service derive the installed-module proxy (COUNT DISTINCT
-- non-reserved module_id) from the same rows.
--
-- Rolled-up-else-live, identical to AppUsageSummary (see its header): once the
-- period is rolled into usage_aggregates read the FROZEN billable record, else
-- estimate LIVE from raw usage_events. The ONE difference from AppUsageSummary is
-- the LIVE charged_micros: a reserved infra.* / platform.* line applies the
-- ×12/10 infra markup INLINE so the live infra estimate matches the rolled
-- usage_aggregates.charged_micros (the rollup snapshots that same 1.2× via
-- cycle.infraMarkupNum/Den + isReservedMetric — design §4 Axis 3). A custom
-- metric keeps the identity 1× (declared price, NO customer markup). The rolled
-- branch already carries the correct markup frozen in charged_micros, so it needs
-- no CASE. NUMERIC ×12/10 stays exact (÷10 terminates); the store rounds it
-- half-up ONCE per line through the shared micros decoder.
--
-- UNINSTALL-SAFE: like AppUsageSummary, this NEVER joins ms_applications /
-- module_install (which drops on uninstall) — it reads only the immutable
-- usage_aggregates / usage_events ledgers, so an uninstalled module's accrued
-- usage still bills + displays. The module DISPLAY NAME is resolved by the caller
-- from the module catalog (module_versions), never from an install row; this
-- query returns module_id only (like every other usage read here).
-- name: AppBillLines :many
WITH rolled AS (
    SELECT
        ua.module_id                                       AS module_id,
        ua.metric                                          AS metric,
        ua.kind                                            AS kind,
        ua.model                                           AS model,
        ua.module_version                                  AS module_version,
        -- dev_served (migration 073) is a LINE DIMENSION here, never a filter:
        -- the console renders tunnel-served usage as its own section, priced,
        -- with the money total taken from the non-dev lines only. It is part of
        -- the aggregate's idempotency key, so this GROUP BY still resolves one
        -- source row per output row.
        ua.dev_served                                      AS dev_served,
        COALESCE(SUM(ua.billable_quantity), 0)::numeric    AS billable_quantity,
        COALESCE(MAX(ua.unit_price_micros), 0)::bigint     AS unit_price_micros,
        COALESCE(SUM(ua.charged_micros), 0)::numeric       AS charged_micros,
        -- active_seconds / period_days (migration 044, usage-time-pricing
        -- Phase 1): the GROUP BY key here is already the exact idempotency
        -- key of one usage_aggregates row (module_id, metric, kind, model,
        -- module_version), so MAX over a singleton group is just "the
        -- value" — same pattern as unit_price_micros above. NULL when the
        -- row predates these columns or is an additive (count/sum) kind
        -- that legitimately never populates them (migration 044 header).
        MAX(ua.active_seconds)::numeric                    AS active_seconds,
        MAX(ua.period_days)::numeric                       AS period_days
    FROM ms_billing.usage_aggregates ua
    JOIN ms_billing.billing_periods bp
        ON bp.id = ua.period_id
    WHERE ua.account_id   = @account_id::uuid
      AND ua.app_id       = @app_id::uuid
      AND bp.period_start = @period_start::timestamptz
    GROUP BY ua.module_id, ua.metric, ua.kind, ua.model, ua.module_version, ua.dev_served
),
live_base AS (
    SELECT
        module_id, metric, kind, aggregation_key, subject, dev_served,
        COALESCE(model, '') AS model,
        COALESCE(module_version, '') AS module_version,
        value
    FROM ms_billing.usage_events
    WHERE account_id = @account_id::uuid
      AND app_id = @app_id::uuid
      AND COALESCE(billable_at, recorded_at) >= @period_start::timestamptz
      AND COALESCE(billable_at, recorded_at) <  @period_end::timestamptz
),
live_values AS (
    SELECT module_id, metric, kind, model, module_version, dev_served, value AS billable_value
    FROM live_base
    WHERE aggregation_key IS DISTINCT FROM 'subject'
    UNION ALL
    SELECT
        module_id, metric, kind, model, module_version, dev_served,
        MAX(value)::numeric AS billable_value
    FROM live_base
    WHERE aggregation_key = 'subject'
    GROUP BY module_id, metric, kind, model, module_version, dev_served, subject
),
live AS (
    SELECT
        e.module_id                                        AS module_id,
        e.metric                                           AS metric,
        e.kind                                             AS kind,
        e.model                                             AS model,
        e.module_version                                    AS module_version,
        e.dev_served                                        AS dev_served,
        COALESCE(SUM(e.billable_value), 0)::numeric         AS billable_quantity,
        COALESCE(MAX(md.unit_price_micros), 0)::bigint     AS unit_price_micros,
        -- Custom metric → qty × price (identity 1×). Reserved infra.* / platform.*
        -- → qty × price × 12/10 (the SAME 1.2× the rollup freezes; mirrors
        -- cycle.infraMarkupNum/Den). metric is a GROUP BY column, so the CASE is
        -- constant within each group.
        CASE
            WHEN e.metric LIKE 'infra.%' OR e.metric LIKE 'platform.%'
                THEN COALESCE(SUM(e.billable_value * COALESCE(md.unit_price_micros, 0)), 0) * 12 / 10
            ELSE COALESCE(SUM(e.billable_value * COALESCE(md.unit_price_micros, 0)), 0)
        END::numeric                                       AS charged_micros,
        -- The live (pre-rollup) estimate has no window-segmentation logic —
        -- that lives only in cmd/billing-cycle's rollup. Explicit NULL,
        -- type-matched to the rolled branch so the UNION ALL type-checks:
        -- before a period rolls up, the active window is simply unknown.
        NULL::numeric                                      AS active_seconds,
        NULL::numeric                                      AS period_days
    FROM live_values e
    LEFT JOIN ms_billing.metric_definitions md
        ON md.module_id = e.module_id AND md.metric = e.metric
    GROUP BY e.module_id, e.metric, e.kind, e.model, e.module_version, e.dev_served
)
SELECT
    module_id, metric, kind, model, module_version, dev_served,
    billable_quantity, unit_price_micros, charged_micros,
    active_seconds, period_days
FROM rolled
UNION ALL
SELECT
    module_id, metric, kind, model, module_version, dev_served,
    billable_quantity, unit_price_micros, charged_micros,
    active_seconds, period_days
FROM live
WHERE NOT EXISTS (SELECT 1 FROM rolled)
ORDER BY module_id, metric, model, module_version, dev_served;

-- AppInfraBillLines is the per-metric 基礎設施 (infrastructure) RESIDUAL breakdown
-- for the app-owner bill — the CATALOG-ANCHORED read behind GetAppBill's InfraLines.
-- It returns ONE row per ACTIVE platform-infra sentinel metric_definitions row (the
-- declared infra metrics under module_id = the all-zero sentinel), so EVERY declared
-- infra metric renders even when it has zero usage this period (the app-bill "show
-- all" lists declared-but-unused infra metrics at qty 0 · $0). The enumeration source
-- is the CATALOG (metric_definitions), NOT the usage ledger — that FROM
-- metric_definitions … LEFT JOIN usage is the whole reason a $0 metric still appears.
--
-- RESIDUAL scope (decision 19): the usage side is filtered to the SENTINEL-attributed
-- events only (module_id = the all-zero sentinel) — the genuinely-unattributable infra
-- (platform-agent AI tokens, CDN/egress, async producers that send Nil ModuleID). Infra
-- attributed to a REAL incurring module (module_id <> sentinel) is billed by the sibling
-- AppModuleInfraBillLines query and rendered inside that module's card. The two partition
-- the reserved namespace with no overlap, so the reconciliation invariant holds:
--   infra_total_micros == Σ(AppModuleInfraBillLines.charged) + Σ(AppInfraBillLines.charged).
--
-- The usage side mirrors AppBillLines' rolled-up-else-live gate, collapsed to ONE row
-- per metric (SUM across model / module_version) and filtered to the reserved infra.* /
-- platform.* namespaces AND the sentinel module_id:
--   * rolled: once the period is in usage_aggregates read the FROZEN
--     billable_quantity / charged_micros (the ×1.2 infra markup already
--     snapshotted at rollup).
--   * live: when no infra rollup exists yet, estimate LIVE from usage_events with
--     the ×12/10 infra markup applied INLINE — the SAME single 1.2× AppBillLines
--     applies (mirrors cycle.infraMarkupNum/Den), so it is charged exactly ONCE.
-- The gate is `WHERE NOT EXISTS (SELECT 1 FROM rolled)`, all-or-nothing over the
-- infra rows, identical in spirit to AppBillLines.
--
-- unit_price_micros in the SELECT is the RAW catalog COGS (metric_definitions,
-- pre-markup); charged_micros carries the post-markup line total. NUMERIC ×12/10
-- stays exact (÷10 terminates); the store rounds charged_micros half-up ONCE per
-- metric through the shared micros decoder (do NOT re-sum already-rounded values).
--
-- UNINSTALL-SAFE / ledger-only exactly like AppBillLines: never joins an install
-- table. account_id may be the all-zero UUID for a lazy (account-less) app — the
-- usage CTEs then match nothing (a lazy event carries NULL account_id, which never
-- equals the zero UUID) and every declared metric COALESCEs to qty 0 · $0.
-- name: AppInfraBillLines :many
WITH rolled AS (
    SELECT ua.metric                                       AS metric,
           COALESCE(SUM(ua.billable_quantity), 0)::numeric AS billable_quantity,
           COALESCE(SUM(ua.charged_micros), 0)::numeric    AS charged_micros
    FROM ms_billing.usage_aggregates ua
    JOIN ms_billing.billing_periods bp
        ON bp.id = ua.period_id
    WHERE ua.account_id   = @account_id::uuid
      AND ua.app_id       = @app_id::uuid
      AND bp.period_start = @period_start::timestamptz
      AND (ua.metric LIKE 'infra.%' OR ua.metric LIKE 'platform.%')
      AND ua.module_id = '00000000-0000-0000-0000-000000000000'
      -- dev_served EXCLUDED (migration 073). Reserved metrics are metered at
      -- the PLATFORM's own chokepoints (RecordInfraUsage, which never sets the
      -- flag), so this filter should match everything — it is a floor, not a
      -- behaviour: it means a stray dev_served infra row, however it got here,
      -- can never enter infra_total_micros, which is a reconciliation scalar
      -- the app bill's total is built from.
      AND ua.dev_served = false
    GROUP BY ua.metric
),
live AS (
    SELECT e.metric                                        AS metric,
           COALESCE(SUM(e.value), 0)::numeric              AS billable_quantity,
           (COALESCE(SUM(e.value * COALESCE(md.unit_price_micros, 0)), 0) * 12 / 10)::numeric
                                                           AS charged_micros
    FROM ms_billing.usage_events e
    LEFT JOIN ms_billing.metric_definitions md
        ON md.module_id = e.module_id AND md.metric = e.metric
    WHERE e.account_id  = @account_id::uuid
      AND e.app_id      = @app_id::uuid
      AND COALESCE(e.billable_at, e.recorded_at) >= @period_start::timestamptz
      AND COALESCE(e.billable_at, e.recorded_at) <  @period_end::timestamptz
      AND (e.metric LIKE 'infra.%' OR e.metric LIKE 'platform.%')
      AND e.module_id = '00000000-0000-0000-0000-000000000000'
      AND e.dev_served = false -- see the rolled branch above (migration 073)
    GROUP BY e.metric
),
usage AS (
    SELECT metric, billable_quantity, charged_micros FROM rolled
    UNION ALL
    SELECT metric, billable_quantity, charged_micros FROM live
    WHERE NOT EXISTS (SELECT 1 FROM rolled)
)
SELECT md.metric                                       AS metric,
       md.kind                                         AS kind,
       md.unit                                         AS unit,
       md.display_group                                AS display_group,
       COALESCE(md.unit_price_micros, 0)::bigint       AS unit_price_micros,
       COALESCE(u.billable_quantity, 0)::numeric       AS billable_quantity,
       COALESCE(u.charged_micros, 0)::numeric          AS charged_micros
FROM ms_billing.metric_definitions md
LEFT JOIN usage u ON u.metric = md.metric
WHERE md.module_id = '00000000-0000-0000-0000-000000000000'
  AND md.active
ORDER BY md.display_group, md.metric;

-- AppModuleInfraBillLines is the per-MODULE 基礎設施 (infrastructure) breakdown for
-- the app-owner bill — decision 19's dual-price, sentinel-fallback read behind
-- GetAppBill's ModuleInfraLines. Unlike the app-level RESIDUAL (AppInfraBillLines,
-- catalog-anchored, sentinel-attributed), this is USAGE-anchored and returns ONE row
-- per (module_id, module_version, metric) for reserved infra.* / platform.* usage
-- ATTRIBUTED to a REAL incurring module (module_id <> the all-zero sentinel) — so only
-- modules that actually incurred infra appear, grouped into their module card in the UI.
--
-- DUAL PRICE (the whole point). metric_definitions is joined TWICE on the same metric:
--   * md_def — the fixed SENTINEL row (module_id = the all-zero sentinel). AUTHORITATIVE
--     for kind / unit / display_group and for default_unit_price_micros (the raw
--     platform-default COGS). The override row is PRICE-ONLY, so semantics come from here.
--   * md_ovr — a LEFT JOIN on the event's REAL module_id (md_ovr.module_id = module_id
--     AND md_ovr.metric = metric). module_unit_price_micros is that per-module override
--     price, or NULL when the module has no override row. NULL is the UI's plain-vs-adjusted
--     switch — do NOT COALESCE it to the default on the wire (the store decodes it as a
--     nullable *int64; NULL ⇔ render the plain line at the default price).
--   * charged_micros = round_half_up(qty × COALESCE(override, default) × 12/10). Markup
--     (cycle.infraMarkupNum/Den = 12/10) applied EXACTLY ONCE inline; both unit prices are
--     raw pre-markup COGS. ms.Price(0) → override 0 → charged 0 (full absorb), no special case.
--
-- The resolution chain composes decision 15/19: (module,metric,version) → (module,metric)
-- → (SENTINEL,metric). The live branch resolves it inline via COALESCE(md_ovr, md_def); the
-- rolled branch reads the FROZEN charged_micros the rollup already snapshotted through the
-- Go MetricPriceMicros fallback (cycle/store.go), so both branches agree.
--
-- Rolled-up-else-live exactly like AppInfraBillLines (its own gate, scoped to the
-- non-sentinel partition): a whole period rolls at once, so `WHERE NOT EXISTS (SELECT 1
-- FROM rolled)` shows the live estimate only until this app+period's first aggregate lands.
-- UNINSTALL-SAFE / ledger-only: never joins an install table. The display prices
-- (md_def / md_ovr) are resolved in the OUTER query so they reflect the CURRENT catalog
-- (matching AppInfraBillLines, which also reads current unit_price for its display column).
-- name: AppModuleInfraBillLines :many
WITH rolled AS (
    SELECT ua.module_id                                    AS module_id,
           ua.module_version                               AS module_version,
           ua.metric                                       AS metric,
           COALESCE(SUM(ua.billable_quantity), 0)::numeric AS billable_quantity,
           COALESCE(SUM(ua.charged_micros), 0)::numeric    AS charged_micros
    FROM ms_billing.usage_aggregates ua
    JOIN ms_billing.billing_periods bp
        ON bp.id = ua.period_id
    WHERE ua.account_id   = @account_id::uuid
      AND ua.app_id       = @app_id::uuid
      AND bp.period_start = @period_start::timestamptz
      AND (ua.metric LIKE 'infra.%' OR ua.metric LIKE 'platform.%')
      AND ua.module_id <> '00000000-0000-0000-0000-000000000000'
      -- dev_served EXCLUDED (migration 073), same floor as AppInfraBillLines:
      -- Σ(this query) + Σ(AppInfraBillLines) IS infra_total_micros, so a stray
      -- flagged row here would break that reconciliation identity as well as
      -- charging a tunnel.
      AND ua.dev_served = false
    GROUP BY ua.module_id, ua.module_version, ua.metric
),
live AS (
    SELECT e.module_id                                     AS module_id,
           COALESCE(e.module_version, '')                  AS module_version,
           e.metric                                        AS metric,
           COALESCE(SUM(e.value), 0)::numeric              AS billable_quantity,
           -- COALESCE(override, default) is constant within each (module, metric)
           -- group, so the SUM reduces to price × Σ value; markup applied once.
           (COALESCE(SUM(e.value * COALESCE(md_ovr.unit_price_micros, md_def.unit_price_micros, 0)), 0) * 12 / 10)::numeric
                                                           AS charged_micros
    FROM ms_billing.usage_events e
    JOIN ms_billing.metric_definitions md_def
        ON md_def.module_id = '00000000-0000-0000-0000-000000000000'
       AND md_def.metric = e.metric
    LEFT JOIN ms_billing.metric_definitions md_ovr
        ON md_ovr.module_id = e.module_id AND md_ovr.metric = e.metric
    WHERE e.account_id  = @account_id::uuid
      AND e.app_id      = @app_id::uuid
      AND COALESCE(e.billable_at, e.recorded_at) >= @period_start::timestamptz
      AND COALESCE(e.billable_at, e.recorded_at) <  @period_end::timestamptz
      AND (e.metric LIKE 'infra.%' OR e.metric LIKE 'platform.%')
      AND e.module_id <> '00000000-0000-0000-0000-000000000000'
      AND e.dev_served = false -- see the rolled branch above (migration 073)
    GROUP BY e.module_id, COALESCE(e.module_version, ''), e.metric
),
usage AS (
    SELECT module_id, module_version, metric, billable_quantity, charged_micros FROM rolled
    UNION ALL
    SELECT module_id, module_version, metric, billable_quantity, charged_micros FROM live
    WHERE NOT EXISTS (SELECT 1 FROM rolled)
)
SELECT u.module_id                                     AS module_id,
       u.module_version                                AS module_version,
       u.metric                                        AS metric,
       md_def.kind                                     AS kind,
       md_def.unit                                     AS unit,
       md_def.display_group                            AS display_group,
       COALESCE(md_def.unit_price_micros, 0)::bigint   AS default_unit_price_micros,
       md_ovr.unit_price_micros                        AS module_unit_price_micros,
       u.billable_quantity                             AS billable_quantity,
       u.charged_micros                                AS charged_micros
FROM usage u
JOIN ms_billing.metric_definitions md_def
    ON md_def.module_id = '00000000-0000-0000-0000-000000000000'
   AND md_def.metric = u.metric
LEFT JOIN ms_billing.metric_definitions md_ovr
    ON md_ovr.module_id = u.module_id AND md_ovr.metric = u.metric
ORDER BY u.module_id, md_def.display_group, u.metric, u.module_version;

-- ListBillingPeriods lists an account's REAL billing_periods rows (the closed,
-- rolled/invoiced periods) newest-first — the source for the web's top-right 週期
-- (period) selector on the app bill. Each row carries is_current: true iff its
-- period_start equals the caller-supplied current anchored-period start
-- (@current_month_start = billingperiod.AnchoredPeriodWindow(now, anchorDay).start),
-- so a (rare) already-open current-period row is flagged rather than duplicated.
-- The service PREPENDS a synthetic current live period when no row is current yet
-- (the in-progress period has no billing_periods row until cmd/billing-cycle rolls
-- it up), so the selector always offers "current" + every past period.
--
-- ANCHOR (ADR 0005): billing_periods rows are card-binding-day anchored windows
-- (cmd/billing-cycle closes each account's just-ended AnchoredJustClosed period),
-- and @current_month_start is the matching anchored start — so the is_current
-- match holds for anchored (and, for a day-1 anchor, calendar-month) periods
-- alike. The parameter name is retained for wire compatibility.
-- name: ListBillingPeriods :many
SELECT
    bp.id                                                 AS id,
    bp.period_start                                       AS period_start,
    bp.period_end                                         AS period_end,
    (bp.period_start = @current_month_start::timestamptz) AS is_current
FROM ms_billing.billing_periods bp
WHERE bp.account_id = @account_id::uuid
ORDER BY bp.period_start DESC;

-- BillingPeriodWindow resolves ONE billing_periods row's [period_start,
-- period_end) window by its id, GATED on account_id so a caller can only address
-- its own periods (no cross-account period enumeration). GetAppBill uses it to
-- turn an optional period-id ref into the window the AppBillLines read runs over;
-- a past period resolves to a closed window whose usage_aggregates are frozen.
-- pgx.ErrNoRows (no such id for this account) → the service returns NOT_FOUND.
-- name: BillingPeriodWindow :one
SELECT period_start, period_end
FROM ms_billing.billing_periods
WHERE id = @period_id::uuid AND account_id = @account_id::uuid;

-- AppIDsWithUsage enumerates the DISTINCT app_ids that have ANY usage for the
-- account in ONE period — the usage half of GetAccountBill's app roster (the
-- other half is the ms_billing.apps mirror, MirroredAppIDsOverlappingWindow).
-- It is the app-ENUMERATION projection of AppBillLines' rolled-up-else-live
-- gate, faithful to its PER-APP semantics: GetAppBill's gate is all-or-nothing
-- per (account, app, period) — an app reads its FROZEN usage_aggregates rows
-- when it has any, else its LIVE usage_events — so an app belongs on the
-- account bill iff it appears in EITHER ledger for the window. Hence a plain
-- UNION (dedup) of both branches, NOT a NOT-EXISTS gate: an app whose events
-- arrived after the account's rollup (no aggregate rows of its own yet) still
-- enumerates here and still bills through GetAppBill's live branch.
--
-- The all-zero account-agent sentinel is deliberately excluded from BOTH ledger
-- branches: it is account-level spend, never an app roster/base-fee candidate.
--
-- UNINSTALL-SAFE / ledger-only like every bill read: never joins an install or
-- roster table (a deleted app's accrued usage still bills — D1e), and gated on
-- account_id so a caller only ever enumerates its own apps. ORDER BY app_id
-- (bytewise) for a deterministic scan; the service re-sorts after merging the
-- mirror half anyway.
-- name: AppIDsWithUsage :many
SELECT app_id FROM (
    SELECT ua.app_id AS app_id
    FROM ms_billing.usage_aggregates ua
    JOIN ms_billing.billing_periods bp
        ON bp.id = ua.period_id
    WHERE ua.account_id   = @account_id::uuid
      AND ua.app_id      <> '00000000-0000-0000-0000-000000000000'::uuid
      AND bp.period_start = @period_start::timestamptz
    UNION
    SELECT e.app_id AS app_id
    FROM ms_billing.usage_events e
    WHERE e.account_id  = @account_id::uuid
      AND e.app_id     <> '00000000-0000-0000-0000-000000000000'::uuid
      AND COALESCE(e.billable_at, e.recorded_at) >= @period_start::timestamptz
      AND COALESCE(e.billable_at, e.recorded_at) <  @period_end::timestamptz
) apps_with_usage
ORDER BY app_id;

-- AppOwnerOrg verifies that an unresolved org-owned event still has the roster
-- attribution marker the later self-healing attach sweep requires. A NULL
-- owner_org_id is returned as NULL and is not treated as a missing app row.
-- name: AppOwnerOrg :one
SELECT owner_org_id
FROM ms_billing.apps
WHERE app_id = $1
  AND deleted_at IS NULL
  AND NOT EXISTS (
      SELECT 1 FROM ms_billing.org_deletion_finalizations f
      WHERE f.org_id = ms_billing.apps.owner_org_id
  );
