-- Migration 021 — metric DISPLAY GROUP taxonomy (billing-display compaction).
--
-- Infra-metrics design (docs-temp/infra-metrics/design.md §11). The
-- /dev/module/<slug>/billing page today renders one row PER metric — a wall of
-- ~42 infra line items. §11 compacts that to ~7 GROUP rows (compute, database,
-- storage, network, ai, requests, platform_security) with click-to-disclose
-- members. The group→metric mapping is AUTHORITATIVE in billing-engine (this
-- migration): api-platform is a pure proxy and web-applications does the
-- rollup arithmetic — neither classifies. A frontend name-prefix mapping was
-- rejected (design §11): the group is a catalog attribute the API carries, so
-- adding/retiring a metric never needs a frontend edit.
--
-- WHY AN ENUM (not free TEXT): the seven groups are a CLOSED, platform-owned
-- taxonomy — a typo'd group string would silently land a metric in a phantom
-- group the frontend never renders. The enum makes an invalid group a write-time
-- error, mirroring metric_kind (migration 006). `other` is the explicit default
-- bucket: every CUSTOM (Plane-2 developer) metric and any infra metric not yet
-- mapped falls here, so the column is NOT NULL DEFAULT 'other' and no row is ever
-- ungrouped.
--
-- WHY ON metric_definitions (not a side-table): display_group is a per-metric
-- ATTRIBUTE with exactly one value per (module, metric) row — the same cardinality
-- as kind/unit. A side-table would add a join to every summary read for a single
-- enum column. The column lives on the catalog row the CurrentPeriodUsageSummary
-- query already LEFT JOINs, so the group falls out of the existing join for free.
--
-- BACKFILL is restricted to the infra.* rows that ACTUALLY EXIST in the catalog
-- at this migration's point in history (cumulative 017 + 018 + 019 + 020 seeds,
-- where 019 re-chartered infra.compute.ms → infra.compute.walltime.ms and kept
-- the old name as a deprecated alias — both grouped to compute below). The
-- design's full §11 table maps many more metric names, but their catalog rows
-- land in later seed PRs; each such row is tagged in the SAME migration that
-- seeds it (or via a follow-up UPDATE), exactly as kind/unit are. Tagging a
-- not-yet-seeded name here would be a dead UPDATE (0 rows). The mapping applied
-- below (per design §11):
--   compute : infra.compute.walltime.ms, infra.compute.ms (deprecated 019
--             transition alias — same compute group), infra.cron.count
--   network : infra.egress.bytes, infra.egress.api.bytes
--   ai      : infra.ai.input.tokens, infra.ai.output.tokens,
--             infra.ai.cache_write.tokens, infra.ai.cache_read.tokens,
--             infra.ai.requests
--   storage : infra.storage.put.count, infra.storage.list.count,
--             infra.storage.gib_hours
--   requests: infra.request.count, infra.mcp.tool_call.count,
--             infra.event.count, infra.event.bytes
-- (database / platform_security have no seeded rows yet; nothing to backfill.)
--
-- All seeded infra rows live under the all-zero platform-infra SENTINEL
-- module_id (migration 017); the UPDATEs are scoped to it so a custom developer
-- metric that happens to share an infra-looking name (it cannot — RecordUsage
-- rejects the infra. prefix from the SDK) is never touched. Every other catalog
-- row keeps the DEFAULT 'other'.
--
-- Spec: mirrorstack-docs/db/ms_billing/tables.md#metric_definitions

-- 1) The closed display-group taxonomy. Value order is the design §11 order;
--    `other` last as the catch-all default.
CREATE TYPE ms_billing.metric_group AS ENUM (
    'compute',
    'database',
    'storage',
    'network',
    'ai',
    'requests',
    'platform_security',
    'other'
);

-- 2) The per-metric display group. NOT NULL DEFAULT 'other' so every existing
--    row (and every future custom metric) is grouped without a backfill — only
--    the mapped infra.* rows are reclassified below.
ALTER TABLE ms_billing.metric_definitions
    ADD COLUMN display_group ms_billing.metric_group NOT NULL DEFAULT 'other';

-- 3) Backfill the seeded infra.* rows per design §11 (only rows that exist:
--    017 + 018 + 019 + 020). Scoped to the platform-infra sentinel module_id.
--
-- infra.compute.walltime.ms is the authoritative compute row (migration 019
-- re-chartered infra.compute.ms → infra.compute.walltime.ms); 019 also kept the
-- old infra.compute.ms name as a DEPRECATED transition alias that still prices
-- in-flight events. Both are the compute group so a transition-window event
-- never lands in 'other' — when 019's alias is dropped (PR #4) only the
-- walltime row remains, still compute.
UPDATE ms_billing.metric_definitions
SET display_group = 'compute'
WHERE module_id = '00000000-0000-0000-0000-000000000000'
  AND metric IN ('infra.compute.walltime.ms', 'infra.compute.ms', 'infra.cron.count');

-- infra.egress.bytes is retired/unpriced (019 re-priced it to 0 but did NOT
-- delete the row); it still EXISTS in the catalog, so tagging it 'network' is
-- correct (a residual transition-window event still rolls up to network).
UPDATE ms_billing.metric_definitions
SET display_group = 'network'
WHERE module_id = '00000000-0000-0000-0000-000000000000'
  AND metric IN ('infra.egress.bytes', 'infra.egress.api.bytes');

UPDATE ms_billing.metric_definitions
SET display_group = 'ai'
WHERE module_id = '00000000-0000-0000-0000-000000000000'
  AND metric IN (
      'infra.ai.input.tokens',
      'infra.ai.output.tokens',
      'infra.ai.cache_write.tokens',
      'infra.ai.cache_read.tokens',
      'infra.ai.requests'
  );

UPDATE ms_billing.metric_definitions
SET display_group = 'storage'
WHERE module_id = '00000000-0000-0000-0000-000000000000'
  AND metric IN ('infra.storage.put.count', 'infra.storage.list.count', 'infra.storage.gib_hours');

UPDATE ms_billing.metric_definitions
SET display_group = 'requests'
WHERE module_id = '00000000-0000-0000-0000-000000000000'
  AND metric IN (
      'infra.request.count',
      'infra.mcp.tool_call.count',
      'infra.event.count',
      'infra.event.bytes'
  );
