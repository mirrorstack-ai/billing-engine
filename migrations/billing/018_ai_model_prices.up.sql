-- Migration 018 — per-MODEL AI-token pricing (the Bedrock-pricing schema).
--
-- Infra-metrics PR #1 (AI metering FOUNDATION; the producer that EMITS these
-- metrics is PR #2 — net-new emit code in api-platform cmd/agent, NOT here).
-- This migration lays the billing-engine schema + catalog so AI tokens can be
-- metered AND priced PER MODEL the moment the producer turns on.
--
-- WHY A SIDE-TABLE (the one real schema decision, design infra-metrics §6):
-- ms_billing.metric_definitions holds ONE unit_price_micros per
-- (module_id, metric) row (migration 006). It therefore CANNOT price the roster
-- models — which differ ~15× under a single `infra.ai.input.tokens` name — from
-- the catalog alone. The decision (design §6 / §8 "Bedrock per-model pricing"):
--   1. add a nullable `model` column to usage_events (the per-event dimension),
--   2. add a (metric, model) → unit_price_micros SIDE-TABLE here.
-- The rollup resolves price by (metric, model) when the event carries a model,
-- else falls back to the metric_definitions catalog row. Rejected alternatives:
-- per-model metric NAMES (explodes the switch + seed rows, breaks on roster
-- churn); price-at-emit on the producer (violates catalog-owns-pricing).
--
-- ===========================================================================
-- SUB-MICRO FLOOR → PRICE PER 1K TOKENS (design infra-metrics §3 rule 5):
-- unit_price_micros is an integer BIGINT and MUST NOT widen to NUMERIC. A
-- per-TOKEN price would floor to 0 for the cheaper models: e.g. a future
-- Gemini Flash-Lite at $0.25 / 1M tokens = 0.00025 µ$/token, which truncates to
-- 0 µ$ at the integer floor and silently charges nothing. The fix (rule 5):
-- bill AI tokens in the COARSEST unit whose per-unit price ≥ 1 µ$ — per 1K
-- tokens. So:
--   * the metered QUANTITY the producer emits is in THOUSANDS of tokens
--     (tokens ÷ 1000; PR #2's emit scales it), and
--   * unit_price_micros here is µ$ PER 1K TOKENS = (USD per 1M tokens) × 1000.
--       price_per_1k_micros = price_usd_per_million × 1e6 (µ$/USD) ÷ 1000 (per-1k)
--                           = price_usd_per_million × 1000
-- Worked: Haiku input $1/1M → 1 × 1000 = 1000 µ$/1k. Sonnet output $15/1M →
-- 15 × 1000 = 15000 µ$/1k. The unit string on every AI token row is
-- '1k tokens' to make the per-1k basis self-documenting on the invoice line.
-- ===========================================================================
--
-- COSTS are the RAW PROVIDER LIST cost (COGS), NOT the agent's 1.2× DISPLAY
-- price (api-platform/internal/agent/models.go priceMarkup = 1.2 is
-- display-only, served by GET /v1/models). The flat reserved-metric ×12/10
-- customer markup is applied EXACTLY ONCE at rollup (cycle/money.go via
-- isReservedMetric — infra.ai.* matches the `infra.` prefix). Embedding 1.2×
-- here would double-bill. Same RAW-COGS contract as migration 017.
--
-- KIND is platform-owned (the platform owns infra-metric semantics):
--   infra.ai.input.tokens        → sum   (additive tokens; per-1k priced)
--   infra.ai.output.tokens       → sum   (additive tokens; ~5× input)
--   infra.ai.cache_write.tokens  → sum   (prompt-cache WRITE; ~1.25× input)
--   infra.ai.cache_read.tokens   → sum   (prompt-cache READ; ~0.1× input)
--   infra.ai.requests            → count (provider-API-call count; price 0)
-- These match the platformInfraKind() registry in internal/account/usage.
--
-- infra.ai.requests is UNPRICED OBSERVABILITY (Bedrock has no per-request fee):
-- its metric_definitions price is 0 and it gets NO metric_model_prices rows —
-- every model bills requests at 0 regardless of model. It exists for
-- request-rate/abuse signal + token-producer reconciliation.
--
-- Idempotent seeds: ON CONFLICT DO NOTHING (NOT DO UPDATE) so a finance edit to
-- a seeded COGS row survives a re-run / re-init — identical to migration 017.
-- The seed is the INITIAL value only; once a row exists the DB row wins.
--
-- Spec: mirrorstack-docs/db/ms_billing/tables.md (metric_model_prices,
--       usage_events.model)

-- 1) The per-(metric, model) price side-table. unit_price_micros is NOT NULL
--    here (a model row exists only to PRICE; an unpriced model has no row and
--    falls back to the catalog). PRIMARY KEY (metric, model) is the conflict
--    target the rollup's LookupModelPrice resolves through.
CREATE TABLE IF NOT EXISTS ms_billing.metric_model_prices (
    metric            TEXT NOT NULL,
    model             TEXT NOT NULL,

    -- RAW provider per-unit COGS in micro-dollars PER 1K TOKENS (see header).
    -- NOT NULL: a row exists only to carry a price.
    unit_price_micros BIGINT NOT NULL CHECK (unit_price_micros >= 0),

    -- A retired (metric, model) price stops resolving but the row stays for
    -- historical reproducibility; the rollup filters active = true.
    active            BOOLEAN NOT NULL DEFAULT true,

    created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT now(),

    PRIMARY KEY (metric, model)
);

-- Auto-maintained updated_at, matching the ms_billing convention
-- (ms_billing.set_updated_at ships in migration 001).
CREATE TRIGGER metric_model_prices_set_updated_at
BEFORE UPDATE ON ms_billing.metric_model_prices
FOR EACH ROW
EXECUTE FUNCTION ms_billing.set_updated_at();

-- 2) The per-event model dimension on usage_events. NULL for every non-AI
--    event (the historical default — no backfill, no column DEFAULT needed; a
--    pure ADD COLUMN ... NULL is a safe online change on Postgres 17). The
--    rollup uses it to dispatch to metric_model_prices (model present) vs the
--    metric_definitions catalog (model NULL).
ALTER TABLE ms_billing.usage_events
    ADD COLUMN model TEXT NULL;

-- 2b) The per-aggregate model dimension on usage_aggregates. The rollup now
--     GROUPs usage_events BY (app, module, metric, model), so two AI aggregate
--     rows for the same metric under the same sentinel module differ ONLY by
--     model (prices differ ~15×). The pre-018 UNIQUE (period, app, module,
--     metric) would collide them — the second model's upsert would clobber the
--     first, under-billing. Add model to the row + the idempotency key.
--     DEFAULT '' (NOT NULL) keys every non-AI row under the empty string,
--     matching the rollup's COALESCE(model, '') so the existing aggregates
--     upsert idempotently with no behavior change.
ALTER TABLE ms_billing.usage_aggregates
    ADD COLUMN model TEXT NOT NULL DEFAULT '';

ALTER TABLE ms_billing.usage_aggregates
    DROP CONSTRAINT usage_aggregates_period_app_module_metric_key;

ALTER TABLE ms_billing.usage_aggregates
    ADD CONSTRAINT usage_aggregates_period_app_module_metric_model_key
        UNIQUE (period_id, app_id, module_id, metric, model);

-- 3) Catalog rows for the AI family under the platform-infra SENTINEL module_id
--    (all-zero UUID, migration 017). These supply the FALLBACK price (used when
--    an event carries no model) AND the platform-owned semantics the ingest
--    kind-lookup resolves through. The token metrics' catalog price is a
--    conservative fallback (Haiku input, the cheapest enabled model) so a
--    model-less AI event never zero-charges; the AUTHORITATIVE per-model price
--    lives in metric_model_prices. infra.ai.requests is priced 0 (unpriced
--    observability). DO NOTHING preserves any finance edit.
INSERT INTO ms_billing.metric_definitions (
    module_id, metric, kind, unit, unit_price_micros, active
) VALUES
    -- Fallback prices = Haiku 4.5 (cheapest enabled) raw COGS, per 1k tokens.
    ('00000000-0000-0000-0000-000000000000', 'infra.ai.input.tokens',       'sum',   '1k tokens', 1000, true),
    ('00000000-0000-0000-0000-000000000000', 'infra.ai.output.tokens',      'sum',   '1k tokens', 5000, true),
    ('00000000-0000-0000-0000-000000000000', 'infra.ai.cache_write.tokens', 'sum',   '1k tokens', 1250, true),
    ('00000000-0000-0000-0000-000000000000', 'infra.ai.cache_read.tokens',  'sum',   '1k tokens',  100, true),
    -- Requests: unpriced observability (Bedrock has no per-request fee).
    ('00000000-0000-0000-0000-000000000000', 'infra.ai.requests',           'count', 'request',      0, true)
ON CONFLICT (module_id, metric) DO NOTHING;

-- 4) Per-model RAW-COGS prices for the ENABLED roster models
--    (api-platform/internal/agent/models.go, Enabled: true):
--      Claude Haiku 4.5  (anthropic.claude-haiku-4-5-20251001-v1:0)  in $1/1M  out $5/1M
--      Claude Sonnet 4.6 (anthropic.claude-sonnet-4-6)               in $3/1M  out $15/1M
--    Prices are µ$ PER 1K TOKENS = (USD per 1M) × 1000 (see header).
--
--    CACHE PRICE PLACEHOLDERS — the roster has NO cache-price fields yet (only
--    PriceIn/PriceOut). PR #2 adds PriceCacheWrite/PriceCacheRead to models.go;
--    until then these are DERIVED from input price per the design's documented
--    ratios: cache_write ≈ 1.25× input, cache_read ≈ 0.1× input.
--    TODO(PR #2): replace the derived cache_write/cache_read values below with
--    the real models.go PriceCacheWrite/PriceCacheRead once that field lands,
--    via a follow-up UPDATE migration for already-seeded environments.
--
--    Disabled roster rows (Gemini, GPT) are NOT seeded: no adapter serves them,
--    so the producer can never emit their model — seeding them would be dead
--    pricing. They join the seed when their adapter + Enabled flag flip.
INSERT INTO ms_billing.metric_model_prices (
    metric, model, unit_price_micros, active
) VALUES
    -- Claude Haiku 4.5 — in $1/1M, out $5/1M.
    ('infra.ai.input.tokens',       'anthropic.claude-haiku-4-5-20251001-v1:0', 1000,  true),
    ('infra.ai.output.tokens',      'anthropic.claude-haiku-4-5-20251001-v1:0', 5000,  true),
    ('infra.ai.cache_write.tokens', 'anthropic.claude-haiku-4-5-20251001-v1:0', 1250,  true), -- TODO(PR#2): 1.25× input placeholder
    ('infra.ai.cache_read.tokens',  'anthropic.claude-haiku-4-5-20251001-v1:0',  100,  true), -- TODO(PR#2): 0.1× input placeholder

    -- Claude Sonnet 4.6 — in $3/1M, out $15/1M.
    ('infra.ai.input.tokens',       'anthropic.claude-sonnet-4-6', 3000,  true),
    ('infra.ai.output.tokens',      'anthropic.claude-sonnet-4-6', 15000, true),
    ('infra.ai.cache_write.tokens', 'anthropic.claude-sonnet-4-6', 3750,  true), -- TODO(PR#2): 1.25× input placeholder
    ('infra.ai.cache_read.tokens',  'anthropic.claude-sonnet-4-6',  300,  true)  -- TODO(PR#2): 0.1× input placeholder
ON CONFLICT (metric, model) DO NOTHING;
