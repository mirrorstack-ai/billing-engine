-- Migration 006 — platform-owned metric catalog (manifest-fed).
--
-- Milestone D, Axis 1 (declaration-first metering). The per-(module,
-- metric) catalog the platform owns: it defines a metric's accumulation
-- SEMANTICS (kind) and its per-unit CUSTOMER price. The catalog is the
-- AUTHORITATIVE source of a metric's kind + price — it is populated from
-- the module MANIFEST via the SetMetricDefinitions sync (api-platform
-- calls it on install/publish), NOT snapshotted from events. A module
-- DECLARES each metric once (ms.Meter(name, kind, ms.Unit, ms.Price));
-- runtime emits carry only {metric, value} — kind is NEVER on the wire,
-- it is resolved HERE (design §1 / §4 Axis 1 / §3a).
--
-- An undeclared metric (no row here) is REJECTED at ingest with
-- INVALID_INPUT (design §1 "undeclared metric rejected"): the catalog
-- must exist before any event, so RecordUsage refuses anything not in it.
--
-- kind enum models the four accumulation semantics carried end-to-end:
--   count          additive event count (Record)        → period SUM
--   sum            additive quantity     (Record)        → period SUM
--   peak           absolute level, bill the maximum      → period MAX
--   time_weighted  absolute level, integrate over time   → ∫ v dt
-- (count vs sum are distinct so an invoice line can label the unit
--  correctly; both roll up by SUM.) The per-kind rollup SELECTs that
-- consume this enum land in PR #5 (cmd/billing-cycle).
--
-- unit_price_micros is the FINAL per-unit CUSTOMER price in micro-dollars
-- (1e-6 USD); BIGINT, never float (design "money in NUMERIC + micro-dollar
-- BIGINT"). For a third-party CUSTOM metric this is the developer's
-- declared price applied DIRECTLY with NO 1.2× markup — the platform's cut
-- on third-party usage is the dev-side margin-share at settlement, not a
-- customer markup. The flat 1.2× applies ONLY to platform-infra / built-in
-- metrics (cost × 1.2), whose rows arrive in PR #5/#10; it is not in this
-- PR's scope. NULL = metered-but-unpriced (a meter-without-charge metric).
--
-- Spec: mirrorstack-docs/db/ms_billing/tables.md#metric_definitions

CREATE TYPE ms_billing.metric_kind AS ENUM (
    'count',
    'sum',
    'peak',
    'time_weighted'
);

CREATE TABLE IF NOT EXISTS ms_billing.metric_definitions (
    id                UUID PRIMARY KEY DEFAULT gen_random_uuid(),

    -- Soft FK to the platform module id (ms_applications). A metric is
    -- scoped per module so two modules can use the same metric string
    -- without colliding.
    module_id         UUID NOT NULL,
    metric            TEXT NOT NULL,

    -- Accumulation semantics. The ingest path resolves a metric's kind
    -- from HERE (declaration-first) — kind never travels on the wire — and
    -- snapshots it onto each usage_events row so a later catalog edit can't
    -- retro-change how historical events roll up.
    kind              ms_billing.metric_kind NOT NULL,

    -- Display/billing unit ('requests', 'bytes', 'orders', …). Free text;
    -- declared by the module via ms.Unit(...).
    unit              TEXT NOT NULL DEFAULT '',

    -- Final per-unit CUSTOMER price, micro-dollars. For custom metrics this
    -- is the developer's declared price applied directly (NO 1.2× markup);
    -- the flat 1.2× is platform-infra-only and not in this PR's scope.
    -- NULL = metered-but-unpriced.
    unit_price_micros BIGINT NULL CHECK (unit_price_micros >= 0),

    -- A retired metric stops accepting pricing but its historical
    -- aggregates stay reproducible (kind/price were snapshotted).
    active            BOOLEAN NOT NULL DEFAULT true,

    created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT metric_definitions_module_metric_key UNIQUE (module_id, metric)
);

-- Auto-maintained updated_at, matching the ms_billing convention
-- (ms_billing.set_updated_at ships in migration 001).
CREATE TRIGGER metric_definitions_set_updated_at
BEFORE UPDATE ON ms_billing.metric_definitions
FOR EACH ROW
EXECUTE FUNCTION ms_billing.set_updated_at();
