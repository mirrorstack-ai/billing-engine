-- Migration 055 — occurrence-preserving, subject-keyed meter observations.
--
-- Existing v1 rows remain valid and keep their recorded_at aggregation time.
-- A v2 row preserves the authorized end-user subject, bounded diagnostic
-- metadata, and original occurred_at. aggregation_key is catalog-owned and is
-- snapshotted on the event: only a peak metric may select "subject", meaning
-- SUM(MAX(value) per subject) inside one billing period. A NULL key retains the
-- existing peak behavior.
--
-- payload_fingerprint is nullable only for rows written before this migration.
-- Every new application write supplies the SHA-256 canonical fingerprint used
-- to distinguish an identical retry from an event-id collision.

-- Before 055, billing_periods.status was schema-only: rollup and charge wrote
-- aggregates/runs but never advanced the default 'open'. v2 admission treats
-- closing as an immutable intake barrier, so repair every durable historical
-- boundary during the required quiesced cutover. An invoiced run is terminal;
-- every other aggregate/run means the snapshot has begun and is closing (a
-- pending/failed/frozen run can only reclaim its already-frozen boundary).
UPDATE ms_billing.billing_periods period
SET status = CASE
    WHEN EXISTS (
        SELECT 1
        FROM ms_billing.billing_runs run
        WHERE run.account_id = period.account_id
          AND run.period_start = period.period_start
          AND run.period_end = period.period_end
          AND run.status = 'invoiced'
    ) THEN 'invoiced'::ms_billing.billing_period_status
    ELSE 'closing'::ms_billing.billing_period_status
END
WHERE period.status = 'open'
  AND (
      EXISTS (
          SELECT 1
          FROM ms_billing.usage_aggregates aggregate
          WHERE aggregate.period_id = period.id
      )
      OR EXISTS (
          SELECT 1
          FROM ms_billing.billing_runs run
          WHERE run.account_id = period.account_id
            AND run.period_start = period.period_start
            AND run.period_end = period.period_end
      )
  );

ALTER TABLE ms_billing.metric_definitions
    ADD COLUMN aggregation_key TEXT NULL;

ALTER TABLE ms_billing.metric_definitions
    ADD CONSTRAINT metric_definitions_aggregation_key_valid
    CHECK (
        aggregation_key IS NULL
        OR (aggregation_key = 'subject' AND kind = 'peak')
    );

ALTER TABLE ms_billing.usage_events
    ADD COLUMN observation_version SMALLINT NOT NULL DEFAULT 1,
    ADD COLUMN subject             TEXT NULL,
    -- JSON (rather than JSONB) preserves the application's canonical bytes so
    -- the 4 KiB bound is exact; metadata is diagnostic and never indexed.
    ADD COLUMN metadata            JSON NULL,
    ADD COLUMN occurred_at         TIMESTAMPTZ NULL,
    -- New writers persist the authoritative aggregation time explicitly. It
    -- is nullable so old v1 binaries remain compatible during expand; every
    -- read falls back to recorded_at for their rows. Avoiding a STORED
    -- generated column keeps this additive ALTER metadata-only instead of
    -- rewriting the live usage ledger.
    ADD COLUMN billable_at         TIMESTAMPTZ NULL,
    ADD COLUMN aggregation_key     TEXT NULL,
    ADD COLUMN payload_fingerprint BYTEA NULL,
    ADD COLUMN occurrence_policy   TEXT NOT NULL DEFAULT 'v1_ingest_time';

ALTER TABLE ms_billing.usage_events
    ADD CONSTRAINT usage_events_observation_version_valid
        CHECK (observation_version IN (1, 2)),
    ADD CONSTRAINT usage_events_v2_has_occurrence
        CHECK (
            observation_version <> 2
            OR (
                occurred_at IS NOT NULL
                AND billable_at IS NOT NULL
                AND payload_fingerprint IS NOT NULL
            )
        ),
    ADD CONSTRAINT usage_events_subject_bounded
        CHECK (
            subject IS NULL
            OR (
                octet_length(subject) BETWEEN 1 AND 256
                AND subject !~ '[[:cntrl:]]'
            )
        ),
    ADD CONSTRAINT usage_events_metadata_object
        CHECK (
            metadata IS NULL
            OR (
                json_typeof(metadata) = 'object'
                AND octet_length(metadata::text) <= 4096
            )
        ),
    ADD CONSTRAINT usage_events_aggregation_key_valid
        CHECK (
            aggregation_key IS NULL
            OR (aggregation_key = 'subject' AND kind = 'peak' AND subject IS NOT NULL)
        ),
    ADD CONSTRAINT usage_events_payload_fingerprint_sha256
        CHECK (payload_fingerprint IS NULL OR octet_length(payload_fingerprint) = 32),
    ADD CONSTRAINT usage_events_occurrence_policy_valid
        CHECK (occurrence_policy IN ('v1_ingest_time', 'on_time', 'late_open', 'first_funded'));

-- Keep the aggregation contract beside the immutable priced snapshot. The
-- existing uniqueness key still has one row for the keyed metric because the
-- individual subjects are intentionally collapsed by the rollup.
ALTER TABLE ms_billing.usage_aggregates
    ADD COLUMN aggregation_key TEXT NULL;

ALTER TABLE ms_billing.usage_aggregates
    ADD CONSTRAINT usage_aggregates_aggregation_key_valid
    CHECK (
        aggregation_key IS NULL
        OR (aggregation_key = 'subject' AND kind = 'peak')
    );

-- A catalog can switch aggregation mode between module publishes. Keep both
-- immutable bill-line shapes if that occurs inside one period. COALESCE makes
-- the legacy NULL key a single deterministic identity (ordinary UNIQUE treats
-- NULL values as distinct).
CREATE UNIQUE INDEX usage_aggregates_period_line_aggregation_key
    ON ms_billing.usage_aggregates (
        period_id, app_id, module_id, metric, model, module_version,
        COALESCE(aggregation_key, '')
    );

-- Build the replacement arbiter before removing the legacy constraint. psql
-- applies these statements independently, so this ordering leaves no committed
-- interval without a uniqueness guard.
ALTER TABLE ms_billing.usage_aggregates
    DROP CONSTRAINT usage_aggregates_period_app_module_metric_model_version_key;

-- v2 queries use occurred_at; v1 rows fall back to their original recorded_at.
-- A lazy-org repoint uses its already-established recorded_at clamp.
-- Keep migration 007's indexes for old binaries during the expand deployment.
CREATE INDEX usage_events_account_metric_occurrence_idx
    ON ms_billing.usage_events (
        account_id,
        metric,
        COALESCE(billable_at, recorded_at)
    );

CREATE INDEX usage_events_app_module_metric_occurrence_idx
    ON ms_billing.usage_events (
        app_id,
        module_id,
        metric,
        COALESCE(billable_at, recorded_at)
    );

-- The ingest credit delta probes one subject's in-period maximum. Cover every
-- equality dimension before time and include value so a keyed hot subject does
-- not repeatedly scan the whole account/metric ledger.
CREATE INDEX usage_events_keyed_subject_peak_idx
    ON ms_billing.usage_events (
        account_id,
        app_id,
        module_id,
        metric,
        COALESCE(model, ''),
        COALESCE(module_version, ''),
        subject,
        COALESCE(billable_at, recorded_at)
    ) INCLUDE (value)
    WHERE aggregation_key = 'subject';

-- Rejected observations do not belong in the immutable usage ledger, but time
-- policy decisions still need durable operator evidence. Deliberately omit
-- metadata so malformed or sensitive diagnostic payloads are never retained.
-- One canonical event/reason decision is kept once across retry storms.
CREATE TABLE ms_billing.usage_observation_rejections (
    id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    event_id            TEXT NOT NULL,
    account_id          UUID NULL,
    app_id              UUID NOT NULL,
    module_id           UUID NOT NULL,
    owner_user_id       UUID NULL,
    owner_org_id        UUID NULL,
    metric              TEXT NOT NULL,
    subject             TEXT NULL,
    occurred_at         TIMESTAMPTZ NULL,
    rejected_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    reason              TEXT NOT NULL CHECK (
        reason IN ('occurred_at_future', 'occurred_at_too_old', 'period_closed')
    ),
    payload_fingerprint BYTEA NOT NULL CHECK (octet_length(payload_fingerprint) = 32),

    CONSTRAINT usage_observation_rejections_owner_exclusive
        CHECK (owner_user_id IS NULL OR owner_org_id IS NULL),
    CONSTRAINT usage_observation_rejections_subject_bounded
        CHECK (
            subject IS NULL
            OR (
                octet_length(subject) BETWEEN 1 AND 256
                AND subject !~ '[[:cntrl:]]'
            )
        ),
    CONSTRAINT usage_observation_rejections_event_reason_payload_key
        UNIQUE (event_id, reason, payload_fingerprint)
);

CREATE INDEX usage_observation_rejections_time_idx
    ON ms_billing.usage_observation_rejections (rejected_at DESC);

CREATE INDEX usage_observation_rejections_account_time_idx
    ON ms_billing.usage_observation_rejections (account_id, rejected_at DESC)
    WHERE account_id IS NOT NULL;
