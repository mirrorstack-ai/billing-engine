-- 078: charge static CDN egress at the catalog rate (owner ruling 2026-09-13,
-- via the ask/review session; billing-engine#210).
--
-- Migration 019 retired infra.egress.bytes to price 0 as an "unpriced
-- reporting parent" — its 1 µ$/byte seed was ~12,000× too high and the CDN
-- children it was to be split into never landed — so every static-file byte
-- cmd/infra-egress-sync records has been free. The owner's ruling is that CDN
-- egress is charged, at the rate the catalog already carries for the SSR hop
-- and for API egress: 122,406 µ$/GiB (migration 046's derivation from AWS
-- DataTransfer-Out ap-northeast-1 $0.114/GB-decimal; Cloudflare's own egress
-- cost is lower — this is pricing, not cost, and the owner chose one rate).
--
-- 🔴 WHY A NEW KEY AND NOT A RE-PRICE OF infra.egress.bytes. Its events carry
-- RAW BYTES (the puller passes row.Bytes through unconverted, main.go
-- egressMetricAndValue). Setting that row to 122,406 would price per BYTE: a
-- ~10⁹× overcharge on every hour already in usage_events for the open period,
-- and on any hour the old binary records after this migration applies but
-- before the egress-sync alias moves. The honest per-byte figure (0.000114 µ$)
-- floors to 0 in the BIGINT column — rule 5 (migration 020 header): a
-- per-unit COGS under 1 µ$ moves to a coarser unit. So this is the shape
-- migration 046 used for the SSR rows: a new GiB-priced key the producer
-- emits as bytes / 2³⁰ from the deploy onward. infra.egress.bytes stays at 0,
-- stays registered (historical events still roll up, to $0) and stops being
-- emitted.
--
-- CUTOVER. The puller sweeps the last few CLOSED hours each run with a
-- deterministic event_id per (metric, app, module, window). An hour already
-- ingested under infra.egress.bytes is re-ingested under this key on the first
-- sweep after the new binary is live — a different event_id, so it records —
-- and is charged ONCE, at this rate: the old event prices to 0. No hour is
-- charged twice, and no hour recorded before cutover is charged at all
-- unless the sweep's lookback still covers it, which is the intended edge.
--
-- RAW COGS is seeded here, NEVER pre-multiplied; the automatic 1.2× reserved-
-- metric markup is computed at rollup (cycle/money.go isReservedMetric), so the
-- customer rate is 146,887.2 µ$/GiB — the same effective rate as the SSR hop.
--
-- KIND is platform-owned and matches the platformInfraKind() registry case
-- added alongside this migration (internal/account/usage/infra.go):
--   infra.egress.cdn.bytes -> sum
-- display_group 'network' groups it beside infra.egress.api.bytes (021).
--
-- Idempotent seed: ON CONFLICT DO NOTHING — the INITIAL value only; a finance
-- correction lands as a follow-up UPDATE migration, the 017/018/019/020/045/046
-- convention.
INSERT INTO ms_billing.metric_definitions (
    module_id, metric, kind, unit, unit_price_micros, active, display_group
) VALUES
    ('00000000-0000-0000-0000-000000000000', 'infra.egress.cdn.bytes', 'sum', 'GiB', 122406, true, 'network')
ON CONFLICT (module_id, metric) DO NOTHING;
