-- Migration 007 — raw immutable usage events (the metering landing zone).
--
-- Milestone D, Axis 2 (ingest) + Axis 3 (storage). Every metered fact
-- that survives the dispatch trust boundary lands here exactly once.
-- Rows are RAW infra facts and IMMUTABLE: the flat 1.2x customer markup
-- and the developer margin-share are applied downstream at rollup, so
-- raw cost/quantity can be corrected and invoices reproduced for
-- disputes (design §4 Axis 3 rationale).
--
-- Trust model (design §3a): app_id / module_id / owner here are the
-- PLATFORM-RE-DERIVED identity asserted by dispatch — the SDK's
-- appIdHint/moduleIdHint are untrusted and never reach this table.
-- The reserved `platform.*` / `infra.*` metric namespaces are rejected
-- at the RecordUsage ingress (§3a build rule 3), so a module cannot
-- self-report a platform-billable metric into this table.
--
-- IDEMPOTENCY: event_id is the PRIMARY KEY; RecordUsage inserts ON
-- CONFLICT(event_id) DO NOTHING so an at-least-once retry of the same
-- SDK call (same minted eventId) cannot double-count.
--
-- LAZY ACCOUNT: account_id is NULLABLE. A metered action can occur
-- before a billing account exists; the event is retained and the
-- rollup parks NULL-account rows for backfill on account conversion
-- (design §8 "Lazy account" risk). ON DELETE CASCADE drops a deleted
-- account's events.
--
-- kind is snapshotted from metric_definitions at ingest so a later
-- catalog edit can't retro-change how a historical event rolls up.
--
-- Money/quantity are NUMERIC / BIGINT, never float (design §8
-- "Float drift").
--
-- Spec: mirrorstack-docs/db/ms_billing/tables.md#usage_events

CREATE TABLE IF NOT EXISTS ms_billing.usage_events (
    -- SDK-minted, stable across the call's HTTP retry. TEXT (not UUID):
    -- the SDK chooses the format; matches webhook_events_processed's
    -- TEXT-PK idempotency precedent (migration 003).
    event_id     TEXT PRIMARY KEY,

    -- Platform-re-derived owner account. NULL = lazy (no account yet).
    account_id   UUID NULL REFERENCES ms_billing.accounts(id) ON DELETE CASCADE,

    -- Platform-re-derived attribution (dispatch-asserted, never SDK hints).
    app_id       UUID NOT NULL,
    module_id    UUID NOT NULL,

    metric       TEXT NOT NULL,

    -- Accumulation semantics snapshotted from metric_definitions at ingest.
    kind         ms_billing.metric_kind NOT NULL,

    -- Reported quantity (counter delta or absolute gauge level). NUMERIC
    -- for exact arithmetic; non-negative (design Axis 1 validation).
    value        NUMERIC NOT NULL CHECK (value >= 0),

    -- Server-asserted event time (dispatch, never the SDK recordedAtHint).
    recorded_at  TIMESTAMPTZ NOT NULL,

    -- Wall-clock arrival, for diagnostics + late-arrival detection.
    ingested_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Per-account rollup scan: the cycle job sweeps an account's metrics
-- over a period window.
CREATE INDEX IF NOT EXISTS usage_events_account_metric_time_idx
    ON ms_billing.usage_events (account_id, metric, recorded_at);

-- Per-(app, module) attribution scan: developer-side usage reporting
-- and the live current-period summary group by app+module+metric.
CREATE INDEX IF NOT EXISTS usage_events_app_module_metric_time_idx
    ON ms_billing.usage_events (app_id, module_id, metric, recorded_at);
