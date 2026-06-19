-- Migration 018 — infra catalog hygiene (P0, no new producer).
--
-- Infra-metrics design (docs-temp/infra-metrics/design.md §1 + §7 P0). Two
-- catalog-only corrections to the platform-infra metrics seeded under the
-- all-zero sentinel module_id by migration 017. NO schema change, NO new
-- metric semantics, NO producer — pure data hygiene so the live billing
-- catalog stops mispricing while the full COGS catalog (P1+) is built.
--
-- This is a VERSIONED FOLLOW-UP UPDATE migration, NOT a clobber of the
-- finance-owned 017 seed: 017 uses DO NOTHING precisely so finance can pin the
-- per-unit COGS rows by hand; this migration re-charters / re-prices those rows
-- via explicit UPDATEs (and one alias INSERT), exactly the "follow-up UPDATE
-- migration for already-seeded environments" path 017's header calls for.
--
-- ───────────────────────────────────────────────────────────────────────────
-- (1) RE-CHARTER infra.compute.ms → infra.compute.walltime.ms.
-- ───────────────────────────────────────────────────────────────────────────
-- design §1: infra.compute.ms is dispatch-OBSERVED wall-time, NOT a
-- substrate-native COGS (Lambda gb_ms / Fargate vcpu_ms). It is re-chartered as
-- the FALLBACK-ONLY compute basis — billed for the dev WSS tunnel and any call
-- lacking a substrate-native metric, NEVER co-billed with one. The rename makes
-- that meaning explicit in the catalog. The per-unit price stays the documented
-- placeholder (1 µ$/ms, design §2.1 "1 (placeholder, re-priced)") — finance
-- pins the real fallback rate later via its own follow-up UPDATE; this PR only
-- renames + restates the fallback charter, it does NOT invent a price.
UPDATE ms_billing.metric_definitions
SET    metric = 'infra.compute.walltime.ms',
       unit   = 'millisecond'
WHERE  module_id = '00000000-0000-0000-0000-000000000000'
  AND  metric    = 'infra.compute.ms';

-- ───────────────────────────────────────────────────────────────────────────
-- (2) DEPRECATED ALIAS row for the OLD name infra.compute.ms.
-- ───────────────────────────────────────────────────────────────────────────
-- The producer rename is a SEPARATE PR (the api-platform dispatch chokepoint —
-- internal/dispatch/handler/{infra_meter.go,dev_tunnel.go} — still emits the
-- literal "infra.compute.ms" via billing.MetricInfraComputeMs; verified live on
-- api-platform main). During the PR#3→PR#4 transition window those events land
-- in usage_events under the OLD name, and the rollup price-lookup
-- (cycle.MetricPriceMicros) resolves price by the LITERAL (module_id, metric).
-- With only the renamed row present, an old-name event would find NO catalog row
-- → priced=false → the reserved-metric LOUD-FAIL in cycle/service.go aborts the
-- whole billing cycle. So we keep a deprecated alias row carrying the SAME
-- sentinel, kind, unit, and placeholder price, so transition-window events still
-- price correctly. UNIQUE(module_id, metric) holds: step (1) vacated the
-- 'infra.compute.ms' slot, so this INSERT fills a now-empty slot alongside the
-- renamed 'infra.compute.walltime.ms' row.
--
-- TODO(PR #4): DROP this alias row (and the platformInfraKind alias case in
-- internal/account/usage/infra.go) once api-platform emits
-- 'infra.compute.walltime.ms' and no in-flight event carries the old name.
INSERT INTO ms_billing.metric_definitions (
    module_id, metric, kind, unit, unit_price_micros, active
) VALUES
    ('00000000-0000-0000-0000-000000000000', 'infra.compute.ms', 'sum', 'millisecond', 1, true)
ON CONFLICT (module_id, metric) DO NOTHING;

-- ───────────────────────────────────────────────────────────────────────────
-- (3) RETIRE the flat infra.egress.bytes — set it UNPRICED (price 0).
-- ───────────────────────────────────────────────────────────────────────────
-- design §1 / §2.5: the flat infra.egress.bytes is retired and kept as an
-- UNPRICED reporting parent (= sum of its future CDN children
-- infra.egress.cdn.{origin,hit}.bytes, which are P2 — NOT in this PR). 017's
-- 1 µ$/byte seed is ~12,000× too high for CDN egress; zeroing it prevents
-- mispricing until the CDN split lands.
--
-- WHY price = 0, NOT NULL: the unpriced reporting parent is still INGESTED —
-- cmd/infra-egress-sync emits "infra.egress.bytes" today — so its events reach
-- the rollup. A NULL price yields priced=false, which for a RESERVED (infra.*)
-- metric trips the loud-fail guard in cycle/service.go and aborts the cycle.
-- An explicit integer 0 yields priced=true with charged = quantity × 0 = 0:
-- the row is genuinely unpriced (charges nothing) AND the cycle succeeds. The
-- row stays active=true so it keeps accepting ingest + reporting.
UPDATE ms_billing.metric_definitions
SET    unit_price_micros = 0
WHERE  module_id = '00000000-0000-0000-0000-000000000000'
  AND  metric    = 'infra.egress.bytes';
