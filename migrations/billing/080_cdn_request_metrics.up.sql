-- 080: a deploy's Cloudflare requests and R2 reads, and the 'deploy' group
-- (core-v2#1412 Option B2, billing-engine#212; owner rulings 2026-09-13).
--
-- cdn-worker#58 (B1) writes blob4 = tier and double2 = one request on every
-- egress data point. cmd/infra-egress-sync now records two counts from them:
--
--   infra.cdn.request.count   every metered request at the edge (a Workers
--                             invocation for the deploy), all tiers;
--   infra.cdn.r2.read.count   the requests served from the R2 tier — each is
--                             an R2 class-B operation.
--
-- ===========================================================================
-- COST + UNIT MATH (Cloudflare list prices, Workers Paid / R2, 2026-09):
--   Workers requests $0.30 per million = 0.3 µ$/request → below 1 µ$ (rule 5,
--   migration 020 header) → billed PER 1,000 REQUESTS: producer value =
--   requests / 1000, COGS 300 µ$ per 1k.
--   R2 class-B reads $0.36 per million = 0.36 µ$/read → per 1,000: 360 µ$.
-- The owner ruled these are PRICING, not cost recovery (Cloudflare's Free
-- plan makes the real cost ~0), at the list price as raw COGS with the
-- standard 1.2× reserved-metric markup at rollup: 360 / 432 µ$ per 1k charged.
-- Cache hits and misses bill through the request count only; egress bytes
-- stay one key at one rate (078); Workers CPU time is excluded (ruling c).
-- ===========================================================================
--
-- RAW COGS is seeded here, NEVER pre-multiplied. KIND is platform-owned and
-- matches the platformInfraKind() registry cases added alongside
-- (internal/account/usage/infra.go): both → count.
--
-- Idempotent seed: ON CONFLICT DO NOTHING — the INITIAL value only; a finance
-- correction lands as a follow-up UPDATE migration, the established convention.
INSERT INTO ms_billing.metric_definitions (
    module_id, metric, kind, unit, unit_price_micros, active, display_group
) VALUES
    ('00000000-0000-0000-0000-000000000000', 'infra.cdn.request.count', 'count', 'per-1k-requests', 300, true, 'deploy'),
    ('00000000-0000-0000-0000-000000000000', 'infra.cdn.r2.read.count', 'count', 'per-1k-reads',    360, true, 'deploy')
ON CONFLICT (module_id, metric) DO NOTHING;

-- The existing deploy-side keys join the group: static CDN egress (078) and
-- the SSR family (045/046). infra.egress.bytes (retired, price 0) stays in
-- 'network': it is history, not a deploy line.
UPDATE ms_billing.metric_definitions
SET    display_group = 'deploy'
WHERE  module_id = '00000000-0000-0000-0000-000000000000'
  AND  metric IN (
      'infra.egress.cdn.bytes',
      'infra.compute.ssr.gb_seconds',
      'infra.compute.ssr.request.count',
      'infra.compute.ssr.egress.bytes'
  );
