-- Migration 092 — AI model price rows for the 5.x Claude roster (OD-15).
--
-- WHY: api-platform#890 makes the sidebar agent roster EXACTLY
--   anthropic.claude-haiku-5-5 / sonnet-5-5 / opus-5-5 / fable-5-1
-- (api-platform/internal/agent/models.go; the agent meters under the roster id,
-- ai_meter.go recordAI Model = the roster id — NOT the Bedrock profile name).
-- Until now metric_model_prices (018, 042) only prices older ids. A model with NO
-- row is metered-but-unpriced, i.e. charged 0 — a revenue leak the moment #890 is
-- released. This seeds the 4 token metrics for each of the 4 models.
--
-- µ$/1K-TOKEN BASIS (unchanged from 018/042): unit_price_micros is µ$ PER 1K
-- TOKENS = (USD per 1M tokens) × 1000. Every value below is a whole number of µ$/1k
-- (the finest, Haiku 5.5 cache_read $0.01/1M, is 10), so the integer BIGINT floors
-- nothing to 0 and no finer unit is needed.
--
-- RAW COGS, NOT ×1.2 (same contract as 017/018/042): the AWS Price List global-profile
-- standard-tier list price. The reserved-metric ×12/10 customer markup is applied
-- EXACTLY ONCE at rollup (isReservedMetric on the `infra.` prefix); baking it in
-- here would double-bill.
--   Haiku 5.5  in $0.10  out $0.50  cache_write $0.125 cache_read $0.01  /1M
--   Sonnet 5.5 in $2     out $10    cache_write $2.5   cache_read $0.10  /1M
--   Opus 5.5   in $4     out $20    cache_write $5     cache_read $0.20  /1M
--   Fable 5.1  in $10    out $50    cache_write $12.5  cache_read $0.25  /1M
--
-- Idempotent seed: ON CONFLICT (metric, model) DO NOTHING (NOT DO UPDATE) so a
-- finance edit to a seeded row survives a re-run — identical to 018/042. Older
-- rows (haiku-4-5, sonnet-4-6, sonnet-5) are left as they are.
--
-- Spec: mirrorstack-docs/db/ms_billing/tables.md (metric_model_prices).

INSERT INTO ms_billing.metric_model_prices (
    metric, model, unit_price_micros, active
) VALUES
    -- Claude Haiku 5.5 — in $0.10/1M, out $0.50/1M, cache_write $0.125/1M, cache_read $0.01/1M.
    ('infra.ai.input.tokens',       'anthropic.claude-haiku-5-5',   100, true),
    ('infra.ai.output.tokens',      'anthropic.claude-haiku-5-5',   500, true),
    ('infra.ai.cache_write.tokens', 'anthropic.claude-haiku-5-5',   125, true),
    ('infra.ai.cache_read.tokens',  'anthropic.claude-haiku-5-5',    10, true),
    -- Claude Sonnet 5.5 — in $2/1M, out $10/1M, cache_write $2.5/1M, cache_read $0.10/1M.
    ('infra.ai.input.tokens',       'anthropic.claude-sonnet-5-5', 2000, true),
    ('infra.ai.output.tokens',      'anthropic.claude-sonnet-5-5', 10000, true),
    ('infra.ai.cache_write.tokens', 'anthropic.claude-sonnet-5-5', 2500, true),
    ('infra.ai.cache_read.tokens',  'anthropic.claude-sonnet-5-5',  100, true),
    -- Claude Opus 5.5 — in $4/1M, out $20/1M, cache_write $5/1M, cache_read $0.20/1M.
    ('infra.ai.input.tokens',       'anthropic.claude-opus-5-5',   4000, true),
    ('infra.ai.output.tokens',      'anthropic.claude-opus-5-5',  20000, true),
    ('infra.ai.cache_write.tokens', 'anthropic.claude-opus-5-5',   5000, true),
    ('infra.ai.cache_read.tokens',  'anthropic.claude-opus-5-5',    200, true),
    -- Claude Fable 5.1 — in $10/1M, out $50/1M, cache_write $12.5/1M, cache_read $0.25/1M.
    ('infra.ai.input.tokens',       'anthropic.claude-fable-5-1', 10000, true),
    ('infra.ai.output.tokens',      'anthropic.claude-fable-5-1', 50000, true),
    ('infra.ai.cache_write.tokens', 'anthropic.claude-fable-5-1', 12500, true),
    ('infra.ai.cache_read.tokens',  'anthropic.claude-fable-5-1',   250, true)
ON CONFLICT (metric, model) DO NOTHING;
