-- Migration 020 — P1 PRODUCER-TARGET infra catalog seed (no new producer).
--
-- Infra-metrics design (docs-temp/infra-metrics/design.md §2.2/§2.4/§2.5/§2.7,
-- §3 rule 5, §6). Seeds the eight P1 platform-infra metrics under the all-zero
-- sentinel module_id so the downstream PRODUCER PRs (#5 dispatch ingress / #6
-- async / #7 storage) emit names that ALREADY exist in the catalog AND in the
-- platformInfraKind() registry (internal/account/usage/infra.go — extended in
-- this same PR). RecordInfraUsage rejects an unregistered reserved metric, and
-- the rollup loud-fails a reserved metric with no catalog row, so the seed row +
-- the registry case are a COUPLED PAIR — both ship here, the producer ships
-- later. NO producer, NO schema change, NO new metric semantics in this PR.
--
-- ===========================================================================
-- SUB-MICRO BIGINT FLOOR → COARSEST UNIT WITH price >= 1 µ$ (design §3 rule 5):
-- unit_price_micros is an integer BIGINT and MUST NOT widen to NUMERIC. Any
-- per-unit COGS < 1 µ$ truncates to 0 at the integer floor and SILENTLY bills
-- nothing. Rule 5: bill in the COARSEST unit whose per-unit price >= 1 µ$, and
-- the PRODUCER emits the value PRE-SCALED to that unit — exactly like the
-- infra.ai.* family (per-1k, value = tokens/1000, migration 018) and
-- infra.storage.gib_hours (value pre-scaled to GiB). For each metric below the
-- chosen unit + its integer price + the REQUIRED producer value scaling is the
-- CONTRACT consumed by PR #5/#6/#7 — it is restated per-row so the producer
-- author reads it at the seed.
--
-- The per-metric rule-5 decision:
--   infra.request.count        1.2  µ$/req      >= 1 → PER-REQUEST  (value = 1/req)
--   infra.mcp.tool_call.count  1.5  µ$/call     >= 1 → PER-CALL     (value = 1/call)
--   infra.cron.count           1    µ$/fire     >= 1 → PER-FIRE     (value = 1/fire)
--   infra.event.count          0.4  µ$/delivery <  1 → PER-1K       (value = n/1000)
--   infra.event.bytes          placeholder      see TODO(finance) → PER-GiB (value in GiB)
--   infra.egress.api.bytes     0.0000838 µ$/B   <  1 → PER-GiB      (value in GiB)
--   infra.storage.put.count    0.005 µ$/PUT     <  1 → PER-1K       (value = n/1000)
--   infra.storage.list.count   0.005 µ$/LIST    <  1 → PER-1K       (value = n/1000)
-- ===========================================================================
--
-- COSTS are the RAW PROVIDER LIST cost (COGS), NOT the customer price. The flat
-- reserved-metric ×12/10 markup is applied EXACTLY ONCE at rollup
-- (cycle/money.go via isReservedMetric — every infra.* matches the `infra.`
-- prefix). Embedding 1.2× in a seed row would double-bill. Same RAW-COGS
-- contract as migrations 017 and 018.
--
-- KIND is platform-owned (the platform owns infra-metric semantics) and matches
-- the platformInfraKind() registry extended in this PR:
--   infra.request.count        → count
--   infra.mcp.tool_call.count  → count
--   infra.cron.count           → count
--   infra.event.count          → count
--   infra.event.bytes          → sum
--   infra.egress.api.bytes     → sum
--   infra.storage.put.count    → count
--   infra.storage.list.count   → count
--
-- NAME/UNIT INCONSISTENCY FLAGGED, NOT FIXED HERE: design §3 rule 1 says the
-- name ends in its UNIT token, but rule 5 forces a coarser BILLING unit than the
-- name implies for the two `.bytes` metrics — infra.egress.api.bytes and
-- infra.event.bytes are NAMED in bytes but PRICED + emitted in GiB (mirrors how
-- infra.ai.input.tokens is named in tokens but priced/emitted per-1k). The doc
-- says "KEEP the doc metric name; do not rename here", so the name stays `.bytes`
-- and the GiB basis is made explicit in the unit string + this comment.
-- TODO(follow-up): reconcile the .bytes name vs the GiB billing unit (either a
-- name carrying the gib basis, or an explicit doc note that .bytes families bill
-- per-GiB) — a NAMING decision, out of scope for this seed PR.
--
-- Idempotent seeds: ON CONFLICT DO NOTHING (NOT DO UPDATE) so a finance edit to
-- a seeded COGS row survives a re-run / re-init — identical to 017 and 018. The
-- seed is the INITIAL value only; once a row exists the DB row wins.
--
-- Spec: mirrorstack-docs/db/ms_billing/tables.md (metric_definitions)
INSERT INTO ms_billing.metric_definitions (
    module_id, metric, kind, unit, unit_price_micros, active
) VALUES
    -- §2.7 Ingress. 1.2 µ$/req (APIGW $1/M + Lambda $0.2/M + routing) >= 1 µ$, so
    -- billable PER REQUEST. Seeded at the integer FLOOR of 1.2 = 1 (the BIGINT
    -- floor; finance pins the fractional rate). PRODUCER value = 1 per request.
    ('00000000-0000-0000-0000-000000000000', 'infra.request.count',       'count', 'request',       1,     true),

    -- §2.7 Ingress. 1.5 µ$/call (auth + routing) >= 1 µ$ → PER CALL. Seeded at
    -- floor(1.5) = 1 (finance pins). PRODUCER value = 1 per MCP tool call.
    ('00000000-0000-0000-0000-000000000000', 'infra.mcp.tool_call.count', 'count', 'call',          1,     true),

    -- §2.2 Async. 1 µ$/fire (EventBridge Scheduler) >= 1 µ$ → PER FIRE.
    -- PRODUCER value = 1 per scheduler fire (the tick only; triggered work bills
    -- separately as walltime.ms / task.*).
    ('00000000-0000-0000-0000-000000000000', 'infra.cron.count',          'count', 'fire',          1,     true),

    -- §2.2 Async. 0.4 µ$/delivery (bus hop/subscriber) < 1 µ$ → floors to 0
    -- per-delivery. RULE 5: bill PER 1K DELIVERIES → 0.4 × 1000 = 400 µ$/1k.
    -- PRODUCER value = deliveries / 1000.0 (one fanout delivery emits 0.001).
    ('00000000-0000-0000-0000-000000000000', 'infra.event.count',         'count', '1k deliveries', 400,   true),

    -- §2.2 Async. Doc cost is a "1 (internal bus, finance pins)" PLACEHOLDER the
    -- prompt flags as likely wrong ($1/byte = $1.07e9/GiB; $1/MB = $1.05e6/GiB —
    -- both implausible for any real messaging bus; AWS EventBridge data volume is
    -- ~$1/GB ≈ 1,000,000 µ$/GiB, SNS ~$0.50/GB). Rather than seed a wildly
    -- inflated placeholder that would OVERCHARGE if the TODO is forgotten, seed a
    -- conservative GiB-basis FLOOR (price = 1 µ$/GiB, >= 1 so no silent-zero) and
    -- make finance pin the real rate. GiB unit also sidesteps the per-byte floor.
    -- PRODUCER value = bytes / (1024^3) (value in GiB), same scaling as egress.
    -- TODO(finance) #26: real internal-bus egress is ~1,000,000 µ$/GiB ($1/GB,
    -- AWS EventBridge order of magnitude); this 1 µ$/GiB is an intentionally
    -- conservative placeholder — pin the real per-GiB rate here BEFORE the
    -- infra.event.bytes producer ships in PR #6. Tracked:
    -- github.com/mirrorstack-ai/billing-engine/issues/26.
    ('00000000-0000-0000-0000-000000000000', 'infra.event.bytes',         'sum',   'GiB',           1,     true),

    -- §2.5 Egress. 0.0000838 µ$/byte (S3/Lambda→internet) < 1 µ$ → floors to 0
    -- per-byte. RULE 5: bill PER GiB → 0.0000838 × 1024^3 ≈ 89,980, rounded to
    -- 90,000 µ$/GiB (≈$0.09/GiB, the documented S3/Lambda→internet rate; the
    -- 0.02% rounding is a defensible finance round-number). NAME stays
    -- `.bytes` (see header), unit + producer value are GiB.
    -- PRODUCER value = bytes / (1024^3) (value in GiB, float64).
    ('00000000-0000-0000-0000-000000000000', 'infra.egress.api.bytes',    'sum',   'GiB',           90000, true),

    -- §2.4 Object storage. $0.005/1k PUT = 0.005 µ$/PUT < 1 µ$ → floors to 0
    -- per-PUT. RULE 5: bill PER 1K PUTs → 5 µ$/1k. PRODUCER value = puts / 1000.0
    -- (one PUT/COPY at upload.Confirm emits 0.001).
    ('00000000-0000-0000-0000-000000000000', 'infra.storage.put.count',   'count', '1k requests',   5,     true),

    -- §2.4 Object storage. $0.005/1k LIST = 0.005 µ$/LIST < 1 µ$ → floors to 0
    -- per-LIST. RULE 5: bill PER 1K LISTs → 5 µ$/1k. PRODUCER value = lists /
    -- 1000.0 (one ListByPrefix continuation page emits 0.001).
    ('00000000-0000-0000-0000-000000000000', 'infra.storage.list.count',  'count', '1k requests',   5,     true)
ON CONFLICT (module_id, metric) DO NOTHING;
