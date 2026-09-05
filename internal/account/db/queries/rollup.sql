-- Queries backing internal/account/cycle.pgxStore (the period-rollup +
-- developer-settlement Store interface). All operate on the ms_billing schema.
--
-- Milestone D, PR #5 (aggregation + settlement). This file ships the rollup
-- write path deferred from PR #3 (which would have shipped them as dead code,
-- having no caller):
--   OpenPeriodForAccount     idempotent billing_periods upsert (period anchor)
--   RollupSumKinds           count/sum → SUM(value) per (app, module, metric)
--   RollupPeakKind           peak      → MAX(value)
--   RollupTimeWeightedKind   time_weighted → ∫ v dt (step-function integral)
--   UpsertUsageAggregate     snapshotted billable record (idempotent upsert)
--   ModuleVisibility         the developer margin-share class for a module
--   ModuleIncome             Σ charged_micros per module for a period
--   UpsertDeveloperSettlement  the settlement ledger row (idempotent upsert)
--
-- Pricing plane (design §4 Axis 3): a custom metric is charged at the
-- developer's declared unit_price with NO markup (10/10); the flat 12/10
-- applies only to platform-infra / built-in metrics. The arithmetic is in
-- Go (cycle.Service); these queries only AGGREGATE quantity + snapshot price.

-- OpenPeriodForAccount upserts the billing_periods row keyed
-- (account_id, period_start). The rollup needs a stable period row to attach
-- usage_aggregates to; this returns the existing row's id+status on a re-run
-- (the DO UPDATE is a no-op SET so RETURNING fires on conflict too) and a
-- fresh 'open' row otherwise. period_end is the anchored-period window end (the
-- next card-binding-day boundary — ADR 0005), supplied by the caller.
-- name: OpenPeriodForAccount :one
INSERT INTO ms_billing.billing_periods (account_id, period_start, period_end, status)
VALUES ($1, $2, $3, 'open')
ON CONFLICT (account_id, period_start)
DO UPDATE SET status = ms_billing.billing_periods.status
RETURNING id, status;

-- CloseBillingPeriodForRollup transitions the period to its intake barrier.
-- The caller holds the same account-period advisory transaction lock as v2
-- observation insertion, so no accepted row can race the rollup snapshot.
-- A retry may re-enter an already-closing period; invoiced is also left intact.
-- name: CloseBillingPeriodForRollup :exec
UPDATE ms_billing.billing_periods
SET status = CASE WHEN status = 'open' THEN 'closing' ELSE status END
WHERE account_id = @account_id::uuid
  AND period_start = @period_start::timestamptz
  AND period_end = @period_end::timestamptz;

-- RollupSumKinds aggregates the additive kinds (count, sum) by SUM(value)
-- over [period_start, period_end) per (app, module, metric, model,
-- module_version). count and sum both roll up by SUM; kind is carried
-- through so the aggregate row snapshots the right accumulation semantics.
-- model is grouped so AI events aggregate PER MODEL (the pricing dimension,
-- migration 018) rather than collapsing models that differ ~15× into one
-- row; module_version is grouped so events aggregate PER VERSION (the
-- attribution dimension, migration 023 — never affects price) rather than
-- blending versions into one row. COALESCE(…, '') keys an event that carries
-- neither dimension (NULL model / NULL module_version) under a stable empty
-- string.
--
-- dev_served (migration 073) is grouped too, and it is NOT a variant of the
-- dimensions above: it does not select a price, it selects whether the line is
-- COLLECTABLE AT ALL. A module tunnelled for part of a period has both kinds
-- of usage of the same metric in that one period, and folding them together
-- would either charge the tunnel usage or waive the deployed usage. Two rows,
-- both priced, only one billed downstream.
-- name: RollupSumKinds :many
SELECT
    app_id                         AS app_id,
    module_id                      AS module_id,
    metric                         AS metric,
    kind                           AS kind,
    NULL::text                     AS aggregation_key,
    COALESCE(model, '')            AS model,
    COALESCE(module_version, '')   AS module_version,
    dev_served                     AS dev_served,
    COALESCE(SUM(value), 0)::numeric AS billable_quantity
FROM ms_billing.usage_events
WHERE account_id = $1
  AND COALESCE(billable_at, recorded_at) >= $2
  AND COALESCE(billable_at, recorded_at) <  $3
  -- Card-less calendar rollup freezes legacy usage for existing disclosure
  -- behavior but leaves v2 pending until activation supplies its billable
  -- anchor. include_v2 is true only for an activated, anchor-consistent window.
  AND (sqlc.arg(include_v2)::boolean OR observation_version <> 2)
  AND kind IN ('count', 'sum')
GROUP BY app_id, module_id, metric, kind, COALESCE(model, ''), COALESCE(module_version, ''), dev_served;

-- RollupPeakKind aggregates the peak kind PER VERSION (usage-time-pricing
-- Phase 1, docs-temp/usage-time-pricing/design.md — supersedes the
-- fix/peak-multiversion-overcharge (#58) exploration, which collapsed
-- module_version out of this query entirely). For each (app, module, metric,
-- model, module_version): billable_quantity is MAX(value) over THAT
-- version's OWN events only (never the period-wide max), and active_seconds
-- is the version's ACTIVE WINDOW (window_v) — the summed duration of the
-- time-ordered segments it opened.
--
-- window_v is derived by LEAD-ing across the FULL (app, module, metric,
-- model) stream WITH module_version deliberately OUT of the PARTITION BY —
-- this is #58's one surviving insight: a successor version's first sample
-- terminates its predecessor's segment at the TRUE handoff instant, so no
-- window tail-bleeds past a version boundary. module_version is then back in
-- the OUTER GROUP BY (the disposition that supersedes #58: peak no longer
-- collapses it) so each version's MAX + window are attributed and priced
-- independently.
--
-- UNIFIED LEVEL MODEL: every LEVEL metric (peak, time_weighted) bills
-- charge_v = representative_level_v × (window_v / P) × price_v, P being the
-- whole period length. For peak, representative_level_v is this query's
-- billable_quantity (the version's own MAX) and window_v is active_seconds;
-- cycle/money.go applies the window_v/P proration at pricing time — this
-- query only aggregates quantity + the window snapshot, it never prices.
-- Peak's OLD (pre-this-PR) price convention had ZERO time-weighting (a flat
-- MAX × price regardless of how long the level was held), so this factor is
-- a genuinely NEW proration, not a double-count: the LOAD-BEARING
-- no-regression invariant is that a single-version period must have
-- window_v == P (factor 1, byte-for-byte the pre-this-PR number).
--
-- That invariant does NOT hold from the raw per-row Δt alone: a real
-- gauge samples on some periodic cadence, so the FIRST sample of a period is
-- essentially never at EXACTLY period_start — without correction, window_v
-- telescopes to only (period_end − first_sample), silently under-billing
-- EVERY peak metric on EVERY period by the sampling-cadence gap, even with
-- zero version changes. row_num's CASE WHEN row_num = 1 branch fixes this:
-- the (period_start, first_sample) gap is credited to whichever
-- module_version owns the stream's very first row, restoring Σ window_v ==
-- P exactly (telescoping sum) and window_v == P in the single-version case
-- regardless of sampling cadence. (The residual imprecision this shifts
-- onto a genuinely brand-new mid-period install — crediting it a few extra
-- hours/days it technically didn't run — is the SAME class of approximation
-- Phase 4's "install upgraded_at → billing feed for exact window bounds" is
-- explicitly scoped to tighten later; it is strictly preferable to a
-- universal, permanent under-charge on the common case today.)
--
-- model stays in the GROUP BY (it prices infra.ai.* lines); module_version
-- is ALSO now a pricing key via metric_version_prices (migration 044),
-- version-first-resolved in Go (cycle.MetricPriceMicros).
--
-- dev_served (migration 073) is treated EXACTLY like module_version and for
-- the same structural reason: OUT of the LEAD/ROW_NUMBER PARTITION BY, back in
-- the OUTER GROUP BY. Out of the partition because a tunnel taking over from a
-- deployed module is a HANDOFF, not a second concurrent stream — the tunnel's
-- first sample must terminate the deployed segment at the true instant, or the
-- deployed level bleeds to period_end and is charged for a week it did not
-- serve. In the outer group because the two halves are separately billable:
-- one is collected, the other is only displayed.
-- name: RollupPeakKind :many
WITH raw_events AS (
    SELECT
        app_id, module_id, metric, kind,
        COALESCE(model, '')          AS model,
        COALESCE(module_version, '') AS module_version,
        dev_served,
        value, COALESCE(billable_at, recorded_at) AS observation_at, event_id,
        sqlc.arg(period_start)::timestamptz AS period_start
    FROM ms_billing.usage_events
    WHERE account_id = $1
      AND COALESCE(billable_at, recorded_at) >= sqlc.arg(period_start)::timestamptz
      AND COALESCE(billable_at, recorded_at) <  sqlc.arg(period_end)::timestamptz
      AND (sqlc.arg(include_v2)::boolean OR observation_version <> 2)
      AND kind = 'peak'
      AND aggregation_key IS NULL
),
windowed AS (
    SELECT
        app_id, module_id, metric, kind, model, module_version, dev_served, value, observation_at, period_start,
        LEAD(observation_at, 1, sqlc.arg(period_end)::timestamptz)
            OVER (PARTITION BY app_id, module_id, metric, model ORDER BY observation_at, event_id) AS segment_end,
        -- row_num identifies the EARLIEST-observed row of the WHOLE (app,
        -- module, metric, model) stream this period (rn=1 — the same
        -- ORDER BY as the LEAD above, so ties resolve to the identical row).
        -- Its own window gets extended BACKWARD to period_start below: with
        -- no boundary-snapshot mechanism, [period_start, first_sample) has
        -- no data, but it must NOT go unattributed the way it did before
        -- this fix — a peak metric that has run continuously for months and
        -- simply samples on some periodic cadence (not exactly on the
        -- period boundary) would otherwise lose that gap from EVERY
        -- version's window on EVERY period, silently shrinking window_v/P
        -- below 1 even in the single-version, no-change common case.
        ROW_NUMBER() OVER (PARTITION BY app_id, module_id, metric, model ORDER BY observation_at, event_id) AS row_num
    FROM raw_events
)
SELECT
    app_id,
    module_id,
    metric,
    kind,
    NULL::text AS aggregation_key,
    model,
    module_version,
    dev_served,
    COALESCE(MAX(value), 0)::numeric AS billable_quantity,
    COALESCE(SUM(
        EXTRACT(EPOCH FROM (segment_end - observation_at))
        + CASE WHEN row_num = 1 THEN EXTRACT(EPOCH FROM (observation_at - period_start)) ELSE 0 END
    ), 0)::numeric AS active_seconds
FROM windowed
GROUP BY app_id, module_id, metric, kind, model, module_version, dev_served;

-- RollupKeyedPeakKind implements aggregation_key="subject": inside an
-- account's period, each authoritative subject contributes its own MAX(value),
-- and those subject maxima are summed. Subject identity is scoped to the meter
-- (app, module, metric), not diagnostic metadata, provider, or retry event id.
--
-- Existing authoritative bill-line dimensions remain part of the scope:
-- account/app/module/metric/model/module_version/window. A model or version
-- change is a distinct pricing definition and therefore a distinct keyed line;
-- no arrival-order-dependent "latest wins" reassignment can move usage between
-- prices. Keyed peak is a cardinality-style quantity and receives no
-- level-window proration.
--
-- dev_served (migration 073) joins that scope at BOTH levels: one subject's
-- tunnel-served peak and its deployed peak are different facts, so they are
-- summed into different lines. Collapsing them would let a developer's local
-- test raise the subject maximum that the customer is billed for.
-- name: RollupKeyedPeakKind :many
WITH eligible AS (
    SELECT
        app_id,
        module_id,
        metric,
        subject,
        COALESCE(model, '') AS model,
        COALESCE(module_version, '') AS module_version,
        dev_served,
        value
    FROM ms_billing.usage_events
    WHERE account_id = @account_id::uuid
      AND COALESCE(billable_at, recorded_at) >= @period_start::timestamptz
      AND COALESCE(billable_at, recorded_at) <  @period_end::timestamptz
      AND (sqlc.arg(include_v2)::boolean OR observation_version <> 2)
      AND kind = 'peak'
      AND aggregation_key = 'subject'
),
subject_peaks AS (
    SELECT
        app_id, module_id, metric, subject, model, module_version, dev_served,
        MAX(value)::numeric AS subject_peak
    FROM eligible
    GROUP BY app_id, module_id, metric, subject, model, module_version, dev_served
)
SELECT
    p.app_id,
    p.module_id,
    p.metric,
    'peak'::ms_billing.metric_kind AS kind,
    'subject'::text AS aggregation_key,
    p.model,
    p.module_version,
    p.dev_served,
    COALESCE(SUM(p.subject_peak), 0)::numeric AS billable_quantity
FROM subject_peaks p
GROUP BY p.app_id, p.module_id, p.metric, p.model, p.module_version, p.dev_served;

-- RollupTimeWeightedKind integrates the step function under the ordered
-- samples PER VERSION (usage-time-pricing Phase 1 — supersedes #58's
-- collapse of this query): each sample's value is held until the NEXT
-- sample (or until period_end for the stream's last sample). The segment
-- duration is LEAD(recorded_at, 1, period_end) - recorded_at; the integral
-- is Σ value × duration. EXTRACT(EPOCH ...) yields seconds; /3600 converts
-- to hours, so segment_byte_hours is in byte-hours for a storage gauge (NOT
-- micro-dollars). A period with no samples produces no row (skipped) — its
-- integral is undefined / 0 (design §8). The window ORDER BY is
-- (recorded_at, event_id): event_id is the TEXT PK, so it breaks
-- recorded_at ties deterministically and the LEAD assigns the remaining
-- duration to a stable last row regardless of plan or vacuum.
--
-- module_version is OUT of the LEAD's PARTITION BY (#58's one surviving
-- insight): the window walks the FULL time-ordered (app, module, metric,
-- model) stream, so a successor version's first sample terminates its
-- predecessor's LAST segment at the TRUE handoff instant — no tail bleeds
-- past a version boundary (the double-charge #58 fixed). module_version is
-- back in the OUTER GROUP BY (the disposition that supersedes #58:
-- time_weighted no longer collapses it either), so billable_quantity here is
-- I_v = Σ(value × duration) over THAT version's own segments only, and
-- active_seconds is the same segments' summed duration (window_v) —
-- reproducibility snapshot only, see below.
--
-- UNIT CONVENTION (design doc resolved decision #3 — do not "fix" this by
-- copying peak's proration here): time_weighted's price is ALREADY
-- per-unit-HOUR (e.g. storage.gib_hours — $/GiB-hour), so I_v is already the
-- fully time-weighted billable quantity: charge_v = I_v × price_v, exactly
-- as before this PR. It must NOT ALSO be scaled by (window_v / P) — that
-- would double-normalize time for every per-hour-priced time_weighted
-- metric (storage included), since the integral already bakes in precisely
-- how long each version's level was held. (Contrast RollupPeakKind, whose
-- price is a flat period-wide rate with ZERO built-in time-weighting, so ITS
-- charge genuinely needs the explicit window_v/P factor.) active_seconds is
-- carried through purely for the reproducibility snapshot / a future
-- "used N of P days" display, never as a second charge multiplier. A
-- single-version period trivially reproduces the pre-this-PR number (there
-- is only one version's I_v to sum) — the load-bearing no-regression
-- invariant.
--
-- dev_served (migration 073) sits exactly where module_version sits: OUT of the
-- LEAD's PARTITION BY so a tunnel's first sample terminates the deployed
-- module's last segment at the true handoff (otherwise the deployed level's
-- integral keeps accruing through a week it did not serve, and the customer
-- pays for it), and IN the outer GROUP BY so the tunnel's own integral becomes
-- its own displayed, never-collected line.
-- name: RollupTimeWeightedKind :many
WITH raw_events AS (
    SELECT
        app_id, module_id, metric, kind,
        COALESCE(model, '')          AS model,
        COALESCE(module_version, '') AS module_version,
        dev_served,
        value, COALESCE(billable_at, recorded_at) AS observation_at, event_id
    FROM ms_billing.usage_events
    WHERE account_id = $1
      AND COALESCE(billable_at, recorded_at) >= $2
      AND COALESCE(billable_at, recorded_at) <  $3
      AND (sqlc.arg(include_v2)::boolean OR observation_version <> 2)
      AND kind = 'time_weighted'
),
windowed AS (
    SELECT
        app_id, module_id, metric, kind, model, module_version, dev_served, value, observation_at,
        LEAD(observation_at, 1, $3::timestamptz)
            OVER (PARTITION BY app_id, module_id, metric, model ORDER BY observation_at, event_id) AS segment_end
    FROM raw_events
),
segments AS (
    SELECT
        app_id, module_id, metric, kind, model, module_version, dev_served,
        EXTRACT(EPOCH FROM (segment_end - observation_at)) AS duration_seconds,
        value * EXTRACT(EPOCH FROM (segment_end - observation_at)) / 3600.0 AS segment_byte_hours
    FROM windowed
)
SELECT
    app_id,
    module_id,
    metric,
    kind,
    NULL::text AS aggregation_key,
    model,
    module_version,
    dev_served,
    COALESCE(SUM(segment_byte_hours), 0)::numeric AS billable_quantity,
    COALESCE(SUM(duration_seconds), 0)::numeric   AS active_seconds
FROM segments
GROUP BY app_id, module_id, metric, kind, model, module_version, dev_served;

-- LookupMetricPrice returns the per-unit customer price for a (module, metric)
-- at rollup time, to snapshot onto the aggregate. NULL price → unpriced
-- (decoded to 0 in Go). The rollup prices every aggregated metric through this
-- when the event carries NO model (the catalog row is the fallback price).
-- name: LookupMetricPrice :one
SELECT unit_price_micros
FROM ms_billing.metric_definitions
WHERE module_id = $1 AND metric = $2;

-- LookupMetricVersionPrice returns the per-unit customer price SNAPSHOTTED
-- for a (module, metric, module_version) at version-publish time (migration
-- 044) — the VERSION-FIRST price resolution the rollup tries before falling
-- back to LookupMetricPrice's version-blind catalog row. pgx.ErrNoRows means
-- no snapshot exists for this version (a module_version='' event — pre
-- version-stamping — or any version published with no SetMetricVersionPrices
-- sync for whatever legacy reason); the Go caller (cycle.MetricPriceMicros)
-- falls back to LookupMetricPrice on that error, exactly like a missing
-- per-model row falls back to the catalog for AI metrics. This table is
-- INSERT-ONLY (no UPDATE path — see migration 044), so a row returned here is
-- the price this version was ALWAYS published at: a LATER version's re-price
-- can never change what this query returns for an EARLIER version. This is
-- the fix for the mid-period-reprice bug (design doc "usage-time-pricing")
-- that catalog-only LookupMetricPrice cannot avoid — a catalog row is
-- mutated in place by every SetMetricDefinitions sync, so resolving through
-- it alone would retroactively re-bill already-accrued usage at whatever
-- price the CURRENT publish happens to carry.
-- name: LookupMetricVersionPrice :one
SELECT unit_price_micros
FROM ms_billing.metric_version_prices
WHERE module_id = $1 AND metric = $2 AND module_version = $3;

-- LookupModelPrice returns the RAW provider COGS for a (metric, model) pair from
-- the per-model side-table (migration 018) — the AUTHORITATIVE price when a
-- usage_event carries a model. unit_price_micros is NOT NULL here (a row exists
-- only to price), so it is a plain BIGINT.
--
-- It does NOT filter active in the WHERE: it returns the active flag so the Go
-- caller can DISTINGUISH "no row at all" (pgx.ErrNoRows → fall back to the
-- LookupMetricPrice catalog row, the legitimate unpriced-model path) from "a row
-- exists but was RETIRED to active = false". The latter must NOT silently fall
-- back to the catalog's conservative (Haiku-floor) fallback price — that would
-- under-bill a deliberately-retired model at a cheaper rate, defeating the loud
-- revenue-leak guard the rollup enforces for missing infra prices. The Go caller
-- fails the cycle loud on an inactive AI price instead.
-- name: LookupModelPrice :one
SELECT unit_price_micros, active
FROM ms_billing.metric_model_prices
WHERE metric = $1 AND model = $2;

-- UpsertUsageAggregate writes the snapshotted billable record idempotently:
-- a rollup re-run for the same (period, app, module, metric, model,
-- module_version) upserts the SAME row (identical values) rather than
-- duplicating it. model is '' for non-AI metrics and the roster model id for
-- infra.ai.* (migration 018); module_version is '' for a version-less event
-- and the emitting module's version otherwise (migration 023, attribution
-- only — never priced). Both are part of the idempotency key so two models
-- or two versions on one metric are distinct billable rows. Snapshots
-- billable_quantity + unit_price + the markup multiplier + raw/charged so a
-- closed invoice is reproducible. active_seconds/period_days (migration 044)
-- are the window-proration reproducibility snapshot: NULL for additive kinds
-- (count/sum — proration never applies), populated for peak/time_weighted so
-- a closed invoice can re-derive the exact per-version window fraction
-- without re-reading usage_events.
-- dev_served (migration 073) is part of the idempotency key for the same
-- reason model and module_version are, and with more at stake: it is what
-- separates a collectable line from a displayed-only one. Without it in the
-- conflict target, a module tunnelled mid-period upserts its tunnel row over
-- its deployed row (or the reverse, depending on which kind the rollup emitted
-- last) — one silently overwriting the other, with no error and no duplicate
-- to notice. charged_micros IS still computed and stored for a dev_served row:
-- that is the figure the console shows a developer, and it is excluded from
-- money by the readers, never by writing a zero here.
-- name: UpsertUsageAggregate :exec
INSERT INTO ms_billing.usage_aggregates (
    period_id, account_id, app_id, module_id, metric, model, module_version, kind,
    aggregation_key, dev_served,
    billable_quantity, unit_price_micros,
    customer_markup_num, customer_markup_den,
    raw_cost_micros, charged_micros, active_seconds, period_days, rolled_up_at
) VALUES (
    @period_id::uuid, @account_id::uuid, @app_id::uuid, @module_id::uuid,
    @metric::text, @model::text, @module_version::text,
    @kind::ms_billing.metric_kind, sqlc.narg(aggregation_key)::text,
    @dev_served::boolean,
    @billable_quantity::numeric, @unit_price_micros::bigint,
    @customer_markup_num::integer, @customer_markup_den::integer,
    @raw_cost_micros::bigint, @charged_micros::bigint,
    sqlc.narg(active_seconds)::numeric, sqlc.narg(period_days)::numeric, now()
)
ON CONFLICT (
    period_id, app_id, module_id, metric, model, module_version,
    COALESCE(aggregation_key, ''), dev_served
)
DO UPDATE SET
    billable_quantity   = EXCLUDED.billable_quantity,
    aggregation_key     = EXCLUDED.aggregation_key,
    unit_price_micros   = EXCLUDED.unit_price_micros,
    customer_markup_num = EXCLUDED.customer_markup_num,
    customer_markup_den = EXCLUDED.customer_markup_den,
    raw_cost_micros     = EXCLUDED.raw_cost_micros,
    charged_micros      = EXCLUDED.charged_micros,
    active_seconds      = EXCLUDED.active_seconds,
    period_days         = EXCLUDED.period_days,
    rolled_up_at        = EXCLUDED.rolled_up_at;

-- ModuleIncomeForPeriod sums charged_micros per module across the period's
-- usage_aggregates — the settlement "income" input. Grouped by module so the
-- developer-settlement rollup gets one income figure per module.
--
-- RESERVED METRICS ARE EXCLUDED, and that exclusion is load-bearing. A
-- reserved infra./platform. charge is a PLATFORM↔APP transaction the developer
-- is not party to: the app pays cost × 1.2 and the platform's compensation IS
-- that 0.2 (migration 013's settlement model; markup at cycle/service.go:222-228).
-- Reserved rows are nonetheless stored under a REAL module_id whenever the cost
-- is attributable to an incurring module (cycle/store.go's "(module, metric) →
-- (SENTINEL, metric)" resolution chain), so an unfiltered SUM counted the
-- platform's own infra markup as that module's income and revenue-shared it
-- back — 15%/30% take on 1.2C leaves the platform at −0.82C/−0.64C per infra
-- dollar against a markup meant to earn +0.2C.
--
-- The sentinel module is dropped outright: every one of its metrics is reserved,
-- so it can only ever produce an all-zero settlement row for a module that does
-- not exist. Unfiltered it accrued 70% of ALL residual infra revenue to
-- developer_id = NULL (no module_visibility row → the private default).
--
-- Developer COGS belongs in developer_settlements.infra_micros, NOT here as
-- negative income. Keep the prefixes in sync with usage.reservedMetricPrefixes.
--
-- 🔴 dev_served ROWS ARE EXCLUDED (migration 073). Settlement income is money:
-- platform_take and developer_owed are derived from it, and the developer is
-- accrued a share of it. Nobody paid for tunnel-served usage, so counting it
-- would accrue a payable against revenue that does not exist — and it would
-- pay the developer for calling their own module on their own laptop. The row
-- still carries a real charged_micros for display; this query is where the
-- money reading stops.
-- name: ModuleIncomeForPeriod :many
SELECT
    module_id                              AS module_id,
    COALESCE(SUM(charged_micros), 0)::bigint AS income_micros
FROM ms_billing.usage_aggregates
WHERE period_id = $1
  AND dev_served = false
  AND module_id <> '00000000-0000-0000-0000-000000000000'::uuid
  AND metric NOT LIKE 'infra.%'
  AND metric NOT LIKE 'platform.%'
GROUP BY module_id;

-- ModuleVisibility returns a module's developer margin-share class. No row →
-- the caller defaults to private (30%) so the platform never under-collects
-- on a lagging publish (design §7-B).
-- name: ModuleVisibility :one
SELECT visibility
FROM ms_billing.module_visibility
WHERE module_id = $1;

-- UpsertDeveloperSettlement writes the settlement ledger row idempotently per
-- (period, module): a re-run upserts the same row. developer_id is NULL until
-- a module→developer sync exists; status defaults 'accrued' (payout deferred).
-- name: UpsertDeveloperSettlement :exec
INSERT INTO ms_billing.developer_settlements (
    period_id, account_id, module_id, developer_id,
    income_micros, infra_micros, margin_share_class,
    platform_take_micros, developer_owed_micros, status
) VALUES (
    $1, $2, $3, $4, $5, $6, $7, $8, $9, 'accrued'
)
ON CONFLICT (period_id, module_id)
DO UPDATE SET
    account_id            = EXCLUDED.account_id,
    income_micros         = EXCLUDED.income_micros,
    infra_micros          = EXCLUDED.infra_micros,
    margin_share_class    = EXCLUDED.margin_share_class,
    platform_take_micros  = EXCLUDED.platform_take_micros,
    developer_owed_micros = EXCLUDED.developer_owed_micros;
