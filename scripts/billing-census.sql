-- A census of production's billing history: how much has this system ever done?
--
-- Written because the shadow reconciliation in DESIGN §11 refused to run with
-- "the price catalog is empty", and an empty catalog has two very different
-- causes that look identical from outside:
--
--   (a) the rail has never carried traffic, so there is nothing to reconcile
--       and §11's gate is VACUOUS; or
--   (b) history exists and the diagnostic cannot see it, so §11's gate is
--       genuinely UNMET.
--
-- Those imply opposite decisions about dropping the legacy collectors, and
-- inferring which one holds from "the preconditions all returned zero" is
-- exactly the reasoning that made migration 058 look applied. So: measure.
--
-- 🔴 EVERY ROW IS AN AGGREGATE. No account id, no email, no money figure that
-- belongs to anybody. Same rule as legacy-drop-preconditions.sql, and for the
-- same reason: this runs against production and its output gets pasted into
-- places the invoke permission does not reach.
--
-- Each statement returns exactly one row of (subject, total, detail) so the
-- reader in preconditions.go's shape can carry it without a second parser.

-- 1. The price catalog the shadow run needs. Zero here is the whole question.
SELECT
    'metric_version_prices'                                    AS subject,
    count(*)                                                   AS total,
    'per-version price snapshots (written at module publish)'  AS detail
FROM ms_billing.metric_version_prices;

-- 2. The version-blind catalog. NOT an emptiness signal, and that is the
-- point of asking it beside (1).
--
-- 🔴 Seven migrations SEED this table — 017, 018, 019, 020, 045, 046, 051 —
-- so a freshly migrated, never-used database already reports 22 rows. Reading
-- a non-zero count here as "the system has been used" is wrong, and it is the
-- reading a hurried operator would make. It is asked so that (1) being zero
-- while this is non-zero is legible as the NORMAL pre-publish state rather
-- than as a half-populated catalog.
--
-- At runtime it also grows via SetMetricDefinitions at manifest sync, which is
-- a different trigger from version publish.
SELECT
    'metric_definitions'                                       AS subject,
    count(*)                                                   AS total,
    'version-blind metric catalog rows'                        AS detail
FROM ms_billing.metric_definitions;

-- 3. Has anything ever been metered?
SELECT
    'usage_events'                                             AS subject,
    count(*)                                                   AS total,
    'raw metered events, all time'                             AS detail
FROM ms_billing.usage_events;

SELECT
    'usage_aggregates'                                         AS subject,
    count(*)                                                   AS total,
    'rolled-up usage rows, all time'                           AS detail
FROM ms_billing.usage_aggregates;

-- 4. Has a billing cycle ever closed? This is what shadow reconciles against:
-- with zero closed periods there is nothing for the rater to disagree with.
SELECT
    'billing_periods'                                          AS subject,
    count(*)                                                   AS total,
    'billing periods, all statuses'                            AS detail
FROM ms_billing.billing_periods;

SELECT
    'billing_runs'                                             AS subject,
    count(*)                                                   AS total,
    'cycle runs, all statuses'                                 AS detail
FROM ms_billing.billing_runs;

-- 5. Has money ever been asked for?
SELECT
    'invoices'                                                 AS subject,
    count(*)                                                   AS total,
    'invoices, all statuses'                                   AS detail
FROM ms_billing.invoices;

SELECT
    'credit_ledger'                                            AS subject,
    count(*)                                                   AS total,
    'credit ledger entries, all types and statuses'            AS detail
FROM ms_billing.credit_ledger;

-- 6. The accounts the whole thing exists for.
SELECT
    'accounts'                                                 AS subject,
    count(*)                                                   AS total,
    'billing accounts'                                         AS detail
FROM ms_billing.accounts;

SELECT
    'apps'                                                     AS subject,
    count(*)                                                   AS total,
    'apps known to billing'                                    AS detail
FROM ms_billing.apps;

-- 7. The NEW rail. Non-zero here would mean intents exist in production,
-- which nothing has yet enabled — so this is a control: it should be zero,
-- and a non-zero answer means something armed itself.
SELECT
    'charge_intents'                                           AS subject,
    count(*)                                                   AS total,
    'sealed charge intents (expected 0 until a leg is armed)'  AS detail
FROM ms_billing.charge_intents;

SELECT
    'billing_authorizations'                                   AS subject,
    count(*)                                                   AS total,
    'standing authorizations (expected 0)'                     AS detail
FROM ms_billing.billing_authorizations;

-- 8. WHICH PRICING TIER actually priced production's usage.
--
-- Added 2026-08-31 after the census found 38,326 usage_events and 15 invoices
-- against an EMPTY metric_version_prices. That combination is only possible if
-- the charges resolved through a tier the shadow rater does not read.
--
-- cycle.Store.MetricPriceMicros (store.go:102-120) resolves in THREE tiers:
--   1. version-first — metric_version_prices, when module_version != ''
--   2. per-model     — metric_model_prices, when model != ''
--   3. catalog       — the metric_definitions row
--
-- shadow.Source.PriceBookFor reads tier 1 ONLY. So these four counts decide
-- whether that is a gap the reconciliation can be built past, or a structural
-- one: intent.PriceKey is {Meter, Module, ModuleVersion} and has NO Model
-- dimension, so tier 2 cannot be expressed in the intent price book at all.

SELECT
    'metric_model_prices'                                      AS subject,
    count(*)                                                   AS total,
    'tier-2 per-(metric,model) prices — SEEDED by 018'         AS detail
FROM ms_billing.metric_model_prices;

SELECT
    'aggregates_with_module_version'                           AS subject,
    count(*)                                                   AS total,
    'usage_aggregates that tier 1 could price'                 AS detail
FROM ms_billing.usage_aggregates
WHERE module_version <> '';

SELECT
    'aggregates_with_model'                                    AS subject,
    count(*)                                                   AS total,
    'usage_aggregates needing tier 2 (no PriceKey dimension)'  AS detail
FROM ms_billing.usage_aggregates
WHERE model <> '';

SELECT
    'aggregates_priced_nonzero'                                AS subject,
    count(*)                                                   AS total,
    'usage_aggregates that actually carry a price'             AS detail
FROM ms_billing.usage_aggregates
WHERE unit_price_micros > 0;

-- 9. ROLLUP COVERAGE: was that usage ever actually billable?
--
-- Added 2026-08-31 because the census returned 38,326 usage_events against
-- only 12 usage_aggregates and 1 billing_run. Three readings fit, and they
-- imply different things about whether the legacy collectors are doing the job
-- the legacy-drop decision assumes they are doing:
--
--   (a) most events are structurally unbillable (no account) — normal;
--   (b) most events fall outside any rolled-up period — the rollup is behind;
--   (c) the rollup ran once and stopped — the collectors are already failing.
--
-- usage_events.account_id is NULLABLE by design ("NULL = lazy (no account
-- yet)"), so (a) is a real possibility and must be measured before (b) or (c)
-- is inferred from a raw count.

SELECT
    'events_without_account'                                   AS subject,
    count(*)                                                   AS total,
    'usage_events with NULL account_id — never billable'       AS detail
FROM ms_billing.usage_events
WHERE account_id IS NULL;

SELECT
    'events_inside_a_rolled_period'                            AS subject,
    count(*)                                                   AS total,
    'usage_events covered by a period that has aggregates'     AS detail
FROM ms_billing.usage_events e
WHERE EXISTS (
    SELECT 1
      FROM ms_billing.usage_aggregates a
      JOIN ms_billing.billing_periods p ON p.id = a.period_id
     WHERE p.account_id = e.account_id
       AND e.recorded_at >= p.period_start
       AND e.recorded_at <  p.period_end
);

SELECT
    'periods_with_aggregates'                                  AS subject,
    count(DISTINCT period_id)                                  AS total,
    'billing_periods that were actually rolled up'             AS detail
FROM ms_billing.usage_aggregates;
