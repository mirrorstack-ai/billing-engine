-- Migration 095 — infra.sandbox.* meters, the sandbox_trial source tag, a price
-- stamped on the event, and the org-wide `sandbox` budget category
-- (billing-engine#256; D440, D441): MirrorStack pays a capped authoring-sandbox
-- trial shared by an org. ADDITIVE ONLY; no existing row changes.

-- 1) The two compute meters, seeded like migration 051's task plane: RAW AWS
--    cost (the infra. markup is applied once at rollup), Tokyo ARM Fargate,
--    $0.04045/vCPU-hour = 40,450 and $0.00442/GiB-hour = 4,420 micro-dollars.
--    Producer value = task size x billed hours (ECS task state-change window).
INSERT INTO ms_billing.metric_definitions (
    module_id, metric, kind, unit, unit_price_micros, active, display_group
) VALUES
    ('00000000-0000-0000-0000-000000000000', 'infra.sandbox.vcpu.hours',       'sum', 'vCPU-hour', 40450, true, 'compute'),
    ('00000000-0000-0000-0000-000000000000', 'infra.sandbox.memory.gib_hours', 'sum', 'GiB-hour',   4420,  true, 'compute')
ON CONFLICT (module_id, metric) DO NOTHING;

-- 2) The tag, the run id and the stamped price live on the usage fact. Nullable,
--    no default (metadata-only, no ledger rewrite); NULL = an ordinary event.
ALTER TABLE ms_billing.usage_events
    ADD COLUMN IF NOT EXISTS source               TEXT   NULL,
    ADD COLUMN IF NOT EXISTS run_id               TEXT   NULL,
    ADD COLUMN IF NOT EXISTS stamped_price_micros BIGINT NULL;

COMMENT ON COLUMN ms_billing.usage_events.source IS
    'Migration 095: NULL (ordinary usage) or sandbox_trial (a capped trial run MirrorStack pays for, D441). Recorded and priced, never charged: the CHECK forces dev_served, so every charging filter excludes it, and only the sandbox budget counts it.';
COMMENT ON COLUMN ms_billing.usage_events.run_id IS
    'Migration 095: the author run a sandbox_trial event belongs to, so a spend line traces to one run.';
COMMENT ON COLUMN ms_billing.usage_events.stamped_price_micros IS
    'Migration 095: the raw (cost-basis) unit price in micro-dollars, resolved once at ingest and never recomputed, so a later price edit cannot change what a sandbox_trial event counts against the cap. NULL on ordinary events, which price live at rollup as before.';

-- 3) Enforced by the database, not only by the Go ingest. NOT VALID: checked for
--    every new row without scanning the live ledger (all old rows have source NULL).
ALTER TABLE ms_billing.usage_events
    ADD CONSTRAINT usage_events_source_known
        CHECK (source IS NULL OR source = 'sandbox_trial') NOT VALID,
    ADD CONSTRAINT usage_events_sandbox_trial_shape
        CHECK (source IS DISTINCT FROM 'sandbox_trial'
               OR (dev_served AND run_id IS NOT NULL AND account_id IS NOT NULL
                   AND stamped_price_micros IS NOT NULL AND stamped_price_micros >= 0)) NOT VALID,
    ADD CONSTRAINT usage_events_run_id_needs_source
        CHECK (run_id IS NULL OR source IS NOT NULL) NOT VALID;

CREATE INDEX IF NOT EXISTS usage_events_sandbox_trial_idx
    ON ms_billing.usage_events (account_id, COALESCE(billable_at, recorded_at))
    WHERE source = 'sandbox_trial';

-- 4) The `sandbox` budget category: org or account scope, never per app (the
--    existing template_key CHECK already keeps a template key to ai + app).
ALTER TABLE ms_billing.budgets
    DROP CONSTRAINT IF EXISTS budgets_category_known;
ALTER TABLE ms_billing.budgets
    ADD CONSTRAINT budgets_category_known CHECK (category IN ('all', 'ai', 'exposure', 'sandbox')),
    ADD CONSTRAINT budgets_sandbox_org_or_account_only CHECK (category <> 'sandbox' OR scope IN ('org', 'account'));

COMMENT ON COLUMN ms_billing.budgets.category IS
    'Which spend the cap measures: all, ai (infra.ai.* per model), exposure (system row, PaaS risk pool) or sandbox (every source = sandbox_trial event, compute and tokens, at its stamped price; org or account scope).';
