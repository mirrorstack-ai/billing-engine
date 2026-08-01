-- Migration 051 — heavy-task-tier substrate rates and Tokyo catalog corrections.
--
-- The module SDK's new execution class maps ms.Standard to Lambda, ms.Heavy to
-- AWS Fargate, and ms.GPU to ECS on EC2. This migration is CATALOG-ONLY: like
-- migration 020, it seeds every name before any producer exists. Heavy CPU work
-- is ARM64/Graviton-only. Tokyo Fargate ARM is 20.0% cheaper for vCPU and 20.1%
-- cheaper for memory than x86 ($0.04045 vs $0.05056 and $0.00442 vs $0.00553);
-- all platform Lambdas are already Graviton, and one architecture gives one
-- price without another event dimension. If an arm64 ffmpeg benchmark later
-- shows more than 20% throughput loss, re-seed at x86 rates with a versioned
-- UPDATE; no schema change is needed.
--
-- PRICES BELOW ARE RAW AWS COST (COGS), NEVER CUSTOMER PRICES. The reserved
-- infra./platform. markup x12/10 is applied automatically, exactly once, by
-- internal/account/cycle/types.go:59-60 and cycle/service.go:222-228, with the
-- live estimate mirroring it in internal/account/db/queries/usage.sql:452-458.
-- Pre-multiplying a seed by 1.2 would apply 1.2 again at rollup and bill 1.44x.
--
-- unit_price_micros is BIGINT CHECK (>= 0), and migration 018 deliberately
-- forbids widening it to NUMERIC. PER HOUR is exact here because AWS publishes
-- every Tokyo Fargate rate to five decimals: $0.00442/hour is exactly 4,420
-- whole micro-dollars. Per second would be 4,420/3,600 = 1.2278 µ$/GB-second,
-- truncated to 1 (-18.6%); per millisecond truncates to 0. Producers therefore
-- emit vCPU-hours, GiB-hours, or instance-hours, not seconds or milliseconds.
--
-- A reserved metric with no catalog price aborts that account's ENTIRE rollup
-- (cycle/service.go:236-238), withholding the invoice for every metric in the
-- period. All rows therefore ship atomically here before any producer emits.
-- These are precisely the substrate-native metrics migration 019's
-- infra.compute.walltime.ms fallback charter was waiting for. They must NEVER
-- be co-billed with walltime.ms; suppressing fallback walltime for heavy/GPU
-- work is producer discipline, because no billing-engine code enforces it.
--
-- Spot savings are absorbed by the platform and never passed through. A
-- customer bill must not move with an invisible scheduling choice, and meters
-- count DELIVERED work, never attempts: a Spot-interrupted rerun is platform
-- cost. Seeds use ON CONFLICT DO NOTHING, so each is only the INITIAL value;
-- after insertion the database row wins and a finance edit survives a rerun.
--
-- AWS publishes the two byte-volume costs below per decimal GB while migration
-- 020's producer contracts emit binary GiB. The conversion is therefore
-- load-bearing: 1 GiB = 1.073741824 GB. Omitting it would understate both raw
-- Tokyo costs by 6.87%.
--
-- ===========================================================================
-- PART 1 — heavy-task substrate metric catalog.
-- ===========================================================================
-- Migration 021 requires later seeds to set display_group in the SAME
-- migration or they silently default to 'other' and render outside their billing
-- group. Migrations 045/046 missed that rule; their SSR rows remain 'other'.
-- That separate display bug is flagged but deliberately not repaired here.
INSERT INTO ms_billing.metric_definitions (
    module_id, metric, kind, unit, unit_price_micros, active, display_group
) VALUES
    -- AWS Price List bulk API, AmazonECS ap-northeast-1, publicationDate
    -- 2026-07-07T16:06:51Z:
    -- https://pricing.us-east-1.amazonaws.com/offers/v1.0/aws/AmazonECS/current/region_index.json
    -- APN1-Fargate-ARM-vCPU-Hours $0.04045/vCPU-hour x 1e6 = 40,450
    -- µ$ EXACTLY (five decimals, no rounding). kind=sum because vCPU-hours add
    -- across tasks. PRODUCER value = task_vcpu x billed_hours. The BILLED
    -- WINDOW is EventBridge ECS Task State Change pullStartedAt -> stoppedAt,
    -- floored at 60 seconds, never container-self-reported: the container is
    -- lost on SIGKILL/Spot interruption, and AWS bills from Docker-pull start
    -- with a one-minute Linux minimum, so runner duration would eat image-pull
    -- seconds on every task.
    ('00000000-0000-0000-0000-000000000000', 'infra.task.vcpu.hours',       'sum', 'vCPU-hour',     40450,  true, 'compute'),

    -- Same AmazonECS feed/date: APN1-Fargate-ARM-GB-Hours $0.00442/GB-hour
    -- x 1e6 = 4,420 µ$ EXACTLY. The task memory allocation is expressed in MiB,
    -- so its metered allocation-hour is GiB-hour despite the price-list label.
    -- PRODUCER value = task_memory_mib / 1024 x billed_hours, using the same
    -- pullStartedAt -> stoppedAt, 60-second-minimum billed window.
    ('00000000-0000-0000-0000-000000000000', 'infra.task.memory.gib_hours', 'sum', 'GiB-hour',      4420,   true, 'compute'),

    -- GPU is priced per EC2 INSTANCE TYPE through migration 018's dimension
    -- (Part 2). This catalog row is only the conservative FALLBACK. AWS EC2
    -- on-demand Linux, Tokyo, publication 2026-07-27T20:28:07Z:
    -- https://calculator.aws/pricing/2.0/meteredUnitMaps/ec2/USD/current/ec2-ondemand-without-sec-sel/Asia%20Pacific%20(Tokyo)/Linux/index.json
    -- Cheapest allowlisted box g4dn.xlarge: $0.71000 x 1e6 = 710,000
    -- µ$/instance-hour exactly.
    -- An SDK allowlist addition missing a price therefore under-charges rather
    -- than over-charges, matching migration 018's cheapest-enabled-model
    -- (Haiku) fallback. GPU is instance-billed, not task-billed: the platform
    -- pays for the box even when its containers leave it half idle.
    ('00000000-0000-0000-0000-000000000000', 'infra.task.gpu.hours',        'sum', 'instance-hour', 710000, true, 'compute')
ON CONFLICT (module_id, metric) DO NOTHING;

-- ===========================================================================
-- PART 2 — GPU instance type as migration 018's generic price dimension.
-- ===========================================================================
-- metric_definitions has one price per (module_id, metric), so it cannot price
-- fourteen instance types under one metric. The platform already has exactly
-- this mechanism: migration 018's metric_model_prices(metric, model), the
-- nullable usage_events.model, and cycle.MetricPriceMicros (documented in
-- internal/account/cycle/store.go). A carried dimension wins; the catalog is
-- fallback; an active=false dimension row fails the cycle LOUD instead of
-- silently using the cheap floor. The `model` name reflects its first AI
-- consumer, but MetricPriceMicros treats its contents as an opaque generic
-- pricing dimension, so an EC2 instance type is semantically valid.
--
-- Rejected alternatives: fourteen per-instance metric names would explode the
-- registry/seed and change on every allowlist edit, exactly why 018 rejected
-- per-model metric names; metric_instance_prices would duplicate an existing
-- table and add a second rollup resolution branch. These fourteen values are
-- EXACTLY app-module-sdk internal/core/compute.go's gpuInstances allowlist:
-- NVENC-capable g4dn (T4) and g5 (A10G), excluding bare metal. Keep both lists
-- synchronized; an admitted instance without a row silently uses the
-- g4dn.xlarge floor. Size suffixes do not imply price: Tokyo g5.12xlarge
-- ($8.22609, four A10Gs) exceeds g5.16xlarge ($5.94042, one A10G).
-- Retirement is active=false, never DELETE, preserving historical pricing as
-- migration 042 requires.
--
-- Source: AWS EC2 on-demand Linux, Asia Pacific (Tokyo), publication
-- 2026-07-27T20:28:07Z:
-- https://calculator.aws/pricing/2.0/meteredUnitMaps/ec2/USD/current/ec2-ondemand-without-sec-sel/Asia%20Pacific%20(Tokyo)/Linux/index.json
-- Each row is published USD/instance-hour x 1e6, an exact whole-micro price.
INSERT INTO ms_billing.metric_model_prices (
    metric, model, unit_price_micros, active
) VALUES
    ('infra.task.gpu.hours', 'g4dn.xlarge',     710000, true),
    ('infra.task.gpu.hours', 'g4dn.2xlarge',   1015000, true),
    ('infra.task.gpu.hours', 'g4dn.4xlarge',   1625000, true),
    ('infra.task.gpu.hours', 'g4dn.8xlarge',   2938000, true),
    ('infra.task.gpu.hours', 'g4dn.12xlarge',  5281000, true),
    ('infra.task.gpu.hours', 'g4dn.16xlarge',  5875000, true),
    ('infra.task.gpu.hours', 'g5.xlarge',      1459000, true),
    ('infra.task.gpu.hours', 'g5.2xlarge',     1757760, true),
    ('infra.task.gpu.hours', 'g5.4xlarge',     2355280, true),
    ('infra.task.gpu.hours', 'g5.8xlarge',     3550330, true),
    ('infra.task.gpu.hours', 'g5.12xlarge',    8226090, true),
    ('infra.task.gpu.hours', 'g5.16xlarge',    5940420, true),
    ('infra.task.gpu.hours', 'g5.24xlarge',   11811230, true),
    ('infra.task.gpu.hours', 'g5.48xlarge',   23622460, true)
ON CONFLICT (metric, model) DO NOTHING;

-- ===========================================================================
-- PART 3 — two versioned Tokyo corrections.
-- ===========================================================================
-- Unlike DO-NOTHING initial seeds, each UPDATE deliberately CLOBBERS the old
-- value. This is migration 019's "follow-up UPDATE migration for already-seeded
-- environments" path. A finance edit since 020/046 is overwritten intentionally
-- because these prior values are provably wrong.

-- (a) API egress: migration 020's $0.09/GiB came from us-east-1, although its
-- GiB unit and bytes/1024^3 producer contract were correct. AWSDataTransfer
-- ap-northeast-1, publicationDate 2026-07-20T18:46:45Z:
-- https://pricing.us-east-1.amazonaws.com/offers/v1.0/aws/AWSDataTransfer/current/region_index.json
-- APN1-DataTransfer-Out-Bytes first 10 TB = $0.114/GB. Convert the producer's
-- GiB: $0.114 x 1.073741824 GB/GiB x 1e6 = 122,406.567936 µ$/GiB; floor to
-- 122,406 for BIGINT, matching migration 046's established floor convention.
UPDATE ms_billing.metric_definitions
SET    unit_price_micros = 122406
WHERE  module_id = '00000000-0000-0000-0000-000000000000'
  AND  metric    = 'infra.egress.api.bytes';

-- (b) S3 storage: migration 020 used us-east-1 $0.023/GiB-month. AmazonS3
-- ap-northeast-1, publicationDate 2026-07-27T19:33:43Z:
-- https://pricing.us-east-1.amazonaws.com/offers/v1.0/aws/AmazonS3/current/region_index.json
-- APN1-TimedStorage-ByteHrs first 50 TB = $0.025/GB-month. Convert GB to GiB
-- and use 020's 730-hour average month:
-- $0.025 x 1.073741824 / 730 x 1e6 = 36.7719802739 µ$/GiB-hour → 37.
--
-- ROUND, not floor, and this row specifically. 045/046 established flooring on
-- conversions where the discarded remainder is noise: 046 floored
-- 122,406.567936 → 122,406, an error of 0.0005%. Near 36 µ$ the same operation
-- discards 2.1%, because a per-GiB-HOUR price is only ~1.5 orders of magnitude
-- above the integer floor — the identical quantization problem this migration's
-- header rejects for per-second compute units. Migration 020 seeded THIS row by
-- rounding (its derivation table reads 31.5 µ$/GiB-hr; it seeded 32), so
-- flooring here would also contradict the row's own precedent. Rounding is
-- +0.6%; flooring is -2.1% on a continuously-billed metric the platform already
-- under-recovers on.
--
-- TODO(finance): the durable fix is a coarser unit. GiB-MONTH would be 26,843
-- µ$ (0.004% granularity) instead of 37 µ$ (2.7%). That changes the producer
-- contract (kind=time_weighted, value = GiB-hours), so it is out of scope for a
-- catalog-only migration. The inherited 730-hour average month is unchanged.
UPDATE ms_billing.metric_definitions
SET    unit_price_micros = 37
WHERE  module_id = '00000000-0000-0000-0000-000000000000'
  AND  metric    = 'infra.storage.gib_hours';
