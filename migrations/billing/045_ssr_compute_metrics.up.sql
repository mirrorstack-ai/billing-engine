-- Migration 045 — SSR compute metering catalog seed.
--
-- COSTS are the RAW AWS Lambda ARM64 COGS for ap-northeast-1 (apphost's SSR
-- Lambda fleet: aws.go:128 confirms ArchitectureArm64), NOT the customer
-- price — the flat reserved-metric x12/10 markup (cycle/money.go
-- isReservedMetric) applies exactly once at rollup, same as every other
-- infra.* row. Embedding the markup here would double-bill.
--
-- VERIFIED 2026-07-12 against AWS's own Pricing API (aws pricing
-- get-products --service-code AWSLambda, regionCode=ap-northeast-1,
-- effective 2026-07-01): Duration = $0.0000133334/GB-s (ARM64 Tier-1,
-- identical to us-east-1 — no Tokyo premium, an earlier draft of this
-- migration guessed one and was wrong), Requests = $0.0000002000/request
-- ($0.20/1M, flat). Both rates are real, not placeholder guesses like the
-- original infra.egress.bytes seed (017) that migration 019 had to retire.
--
-- Rule 5 (migration 020 "bill in the coarsest unit whose per-unit price >=
-- 1 µ$, producer emits pre-scaled"):
--   infra.compute.ssr.gb_seconds:    $0.0000133334/GB-s = 13.3334 µ$/GB-s
--     >= 1 µ$ already → bill PER GB-SECOND directly. Floor to 13 µ$/GB-s.
--     PRODUCER value = Σ(Duration_ms)/1000 * MemorySize_GB (see design doc
--     §1 / §2).
--   infra.compute.ssr.request.count: $0.20/1M = 0.2 µ$/request < 1 µ$ →
--     floors to 0 at the integer column → bill PER 1K REQUESTS: 0.2 x 1000
--     = 200 µ$/1k requests. PRODUCER value = invocations / 1000.0.
--
-- Effective customer price after the automatic 1.2x reserved-metric markup
-- (never stored, only ever computed at rollup): 13 x 1.2 = 15.6 µ$/GB-second,
-- 200 x 1.2 = 240 µ$/1k requests.
--
-- Seeded under the same all-zero platform-infra sentinel module_id as every
-- prior infra metric (migration 017 convention). ON CONFLICT DO NOTHING —
-- this is the INITIAL value only; a finance correction lands as a follow-up
-- UPDATE migration, never a re-seed (same convention as 019/020).
--
-- KIND is platform-owned and matches the platformInfraKind() registry cases
-- added alongside this migration (internal/account/usage/infra.go):
--   infra.compute.ssr.gb_seconds    -> sum
--   infra.compute.ssr.request.count -> count
--
-- Spec: docs-temp/app-hosting/ssr-metering-design.md (§1 / §4);
-- companion mirrorstack-docs/db/ms_billing/ update tracked alongside this PR.
INSERT INTO ms_billing.metric_definitions (
    module_id, metric, kind, unit, unit_price_micros, active
) VALUES
    ('00000000-0000-0000-0000-000000000000', 'infra.compute.ssr.gb_seconds',    'sum',   'GB-second',    13,  true),
    ('00000000-0000-0000-0000-000000000000', 'infra.compute.ssr.request.count', 'count', '1k requests', 200,  true)
ON CONFLICT (module_id, metric) DO NOTHING;
