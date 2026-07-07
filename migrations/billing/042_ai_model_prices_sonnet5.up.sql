-- Migration 042 — AI model roster refresh: seed Claude Sonnet 5, retire Sonnet 4.6.
--
-- WHY: the platform's AI model roster (api-platform/internal/agent/models.go)
-- becomes EXACTLY the 4-model Claude line — Haiku 4.5 (default), Sonnet 5,
-- Opus 4.8, Fable 5 — dropping the old Sonnet 4.6 and every Gemini/GPT
-- placeholder. This migration realigns the billing-engine per-model price
-- side-table (metric_model_prices, migration 018) with that new roster:
--   * NEW  → seed Claude Sonnet 5 ('anthropic.claude-sonnet-5') COGS rows,
--   * GONE → deactivate (NOT delete) the removed Sonnet 4.6 rows.
--
-- µ$/1K-TOKEN BASIS (unchanged from 018 — see its header for the full rationale):
-- unit_price_micros is µ$ PER 1K TOKENS = (USD per 1M tokens) × 1000. Billing AI
-- tokens in the coarsest per-1k unit keeps every per-unit price ≥ 1 µ$ so the
-- integer BIGINT never floors a sub-micro per-token price to 0.
--   Sonnet 5 raw COGS (Anthropic standard list price):
--     input  $3/1M  → 3  × 1000 = 3000  µ$/1k
--     output $15/1M → 15 × 1000 = 15000 µ$/1k
--     cache_write $3.75/1M → 3.75 × 1000 = 3750 µ$/1k
--     cache_read  $0.30/1M → 0.3 × 1000 =  300 µ$/1k
--
-- RAW COGS, NOT ×1.2 (same contract as 017/018): these are the RAW PROVIDER LIST
-- cost. The agent's 1.2× DISPLAY markup (models.go priceMarkup, served by
-- GET /v1/models) and the flat reserved-metric ×12/10 customer markup (applied
-- EXACTLY ONCE at rollup via isReservedMetric on the `infra.` prefix) sit ON TOP.
-- Baking 1.2× in here would double-bill.
--
-- WHY OPUS 4.8 / FABLE 5 ARE NOT SEEDED: both ship DISABLED in the roster
-- (Enabled: false). No adapter serves a disabled model, so the producer can
-- never emit it — a metric_model_prices row for it would be dead pricing that
-- resolves to nothing. Their prices live ONLY in the roster for DISPLAY
-- (GET /v1/models returns all 4 with ×1.2 + cache). They join this seed the
-- moment their Enabled flag flips, exactly as Gemini/GPT would have in 018.
--
-- WHY SONNET 4.6 IS DEACTIVATED, NOT DELETED: a retired (metric, model) price
-- must stop resolving (the rollup filters active = true) but the rows stay for
-- historical reproducibility — re-pricing/replaying a past billing period that
-- metered Sonnet 4.6 events must still find the price that was in force. Same
-- active=false "retire, don't delete" convention 018's schema comment documents.
--
-- Idempotent seed: ON CONFLICT (metric, model) DO NOTHING (NOT DO UPDATE) so a
-- finance edit to a seeded Sonnet 5 COGS row survives a re-run / re-init —
-- identical to migration 018. The seed is the INITIAL value only.
--
-- Spec: mirrorstack-docs/db/ms_billing/tables.md (metric_model_prices).

-- 1) NEW — Claude Sonnet 5 ('anthropic.claude-sonnet-5') raw-COGS rows, active.
--    µ$ per 1K tokens = (USD per 1M) × 1000.
INSERT INTO ms_billing.metric_model_prices (
    metric, model, unit_price_micros, active
) VALUES
    -- Claude Sonnet 5 — in $3/1M, out $15/1M, cache_write $3.75/1M, cache_read $0.30/1M.
    ('infra.ai.input.tokens',       'anthropic.claude-sonnet-5', 3000,  true),
    ('infra.ai.output.tokens',      'anthropic.claude-sonnet-5', 15000, true),
    ('infra.ai.cache_write.tokens', 'anthropic.claude-sonnet-5', 3750,  true),
    ('infra.ai.cache_read.tokens',  'anthropic.claude-sonnet-5',  300,  true)
ON CONFLICT (metric, model) DO NOTHING;

-- 2) RETIRE — deactivate the removed Sonnet 4.6 rows. KEEP the rows (historical
--    reproducibility); the rollup's active = true filter stops them resolving.
UPDATE ms_billing.metric_model_prices
SET active = false
WHERE model = 'anthropic.claude-sonnet-4-6';
