-- 091: apply the owner rule to the WP13 audit gaps (core-v2#1758, owner 2026-10-06:
-- every infra price the customer pays = actual infra COST x 1.2; the 1.2 is applied
-- once at rollup, cycle/money.go isReservedMetric, so every raw below is the COST).
-- Source: reports/infra-price-cost-audit.md (T1130), AWS Price List ap-northeast-1
-- fetched 2026-10-06. Left as they are on purpose: egress (owner ruled pricing),
-- transcode, AI, storage.gib_hours 37.
--
-- Rerunnable, and a finance UPDATE is never overwritten: every UPDATE only touches
-- a row still at the AUDITED value (price AND unit for the unit changes).
--
-- 1. infra.db.gib_hours 137 -> 164. 090 took the owner's $0.10/GiB-month as the
--    cost; Aurora PostgreSQL Tokyo storage is $0.12/GB-month = 164.38 µ$/GiB-hour
--    (730 h), so the customer price was storage cost with zero margin.
UPDATE ms_billing.metric_definitions
SET unit_price_micros = 164
WHERE module_id = '00000000-0000-0000-0000-000000000000'
  AND metric = 'infra.db.gib_hours'
  AND unit_price_micros = 137;

-- 2. infra.request.count: APIGW HTTP API Tokyo $1.29/M + Lambda $0.20/M = $1.49/M =
--    1.49 µ$/request, which does not fit an integer per request, so the unit moves
--    to 1k requests (1,490 µ$) like infra.compute.ssr.request.count (045). Unit and
--    price change together, so the guard checks both. No producer emits this metric
--    (audit: none on api-platform / ms-app-modules / cdn-worker main); a future one
--    emits value = requests / 1000.
UPDATE ms_billing.metric_definitions
SET unit = '1k requests', unit_price_micros = 1490
WHERE module_id = '00000000-0000-0000-0000-000000000000'
  AND metric = 'infra.request.count'
  AND unit = 'request'
  AND unit_price_micros = 1;

-- 3. infra.cron.count: EventBridge Scheduler Tokyo $1.25/M = 1.25 µ$/fire -> unit 1k
--    fires at 1,250 µ$ (producer value = fires / 1000; no emitter exists today).
UPDATE ms_billing.metric_definitions
SET unit = '1k fires', unit_price_micros = 1250
WHERE module_id = '00000000-0000-0000-0000-000000000000'
  AND metric = 'infra.cron.count'
  AND unit = 'fire'
  AND unit_price_micros = 1;

-- 4. infra.storage.put.count / list.count: S3 Tokyo PUT/COPY/POST/LIST $0.0047 per
--    1,000 requests = 4,700 µ$ per 1k. The unit stays '1k requests' (the producer
--    contract is unchanged: value = n / 1000). The audit proposed "4,700 per 1M",
--    but $4.70/M = 4.7 µ$/request = 4,700 µ$/1k, which fits an integer per 1k, so a
--    1M unit would only add a producer change. NOTE the seeded 5 was 020's
--    us-east-1 "$0.005 per 1k" mis-keyed as 0.005 µ$/PUT: the price was 940x LOW,
--    not the +6% the audit table shows.
UPDATE ms_billing.metric_definitions
SET unit_price_micros = 4700
WHERE module_id = '00000000-0000-0000-0000-000000000000'
  AND metric IN ('infra.storage.put.count', 'infra.storage.list.count')
  AND unit = '1k requests'
  AND unit_price_micros = 5;

-- 5. infra.compute.walltime.ms retired to 0: the 017 placeholder of 1 µ$/ms is 75x
--    a 1 GB Lambda (0.0133 µ$/ms). Substrate-native metrics (ssr.gb_seconds, task.*)
--    are the real compute basis; the row stays registered and active so the
--    dispatch producer's facts still record, priced 0 (metered-but-unpriced).
UPDATE ms_billing.metric_definitions
SET unit_price_micros = 0
WHERE module_id = '00000000-0000-0000-0000-000000000000'
  AND metric = 'infra.compute.walltime.ms'
  AND unit_price_micros = 1;

-- 6. infra.task.ephemeral.gib_hours: api-platform taskplane/dispatch.go already emits
--    it (task ephemeral storage above Fargate's 20 GiB included), but the catalog had
--    no row (and infra.go no registry case), so the fact was rejected and unbilled.
--    Fargate ARM ephemeral storage Tokyo $0.000133/GB-hour (AmazonECS list 2026-09-11)
--    = 133 µ$ exactly. sum; value = (ephemeral_mib - 20480)/1024 x billed_hours.
INSERT INTO ms_billing.metric_definitions (
    module_id, metric, kind, unit, unit_price_micros, active, display_group
) VALUES
    ('00000000-0000-0000-0000-000000000000', 'infra.task.ephemeral.gib_hours', 'sum', 'GiB-hour', 133, true, 'compute')
ON CONFLICT (module_id, metric) DO NOTHING;
