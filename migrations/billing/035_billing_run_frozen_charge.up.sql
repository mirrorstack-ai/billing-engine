-- Migration 035 — freeze the boundary charge amount per billing_run (crash-safe
-- idempotency for the base-fee/overage boundary leg, DESIGN.md "Base fee — v2",
-- scenario 6 / Leg 2).
--
-- The boundary charge (cycle/charge.go RunBillingCycle) computes its total from
-- LIVE state — Σ live apps' base + a live count of ongoing over-modules — and
-- charges it under two DETERMINISTIC Stripe Idempotency-Keys derived from the
-- billing_run id (ii-<run> / inv-<run>). InsertBillingRun RECLAIMS a non-terminal
-- ('pending'/'failed'/'skipped_*') run under the SAME id, so a re-fire keeps the
-- SAME idem keys. But if the live state shifts between a crash (Stripe already
-- charged, MarkBillingRun not yet committed) and the retry — a module uninstalled
-- flipping an over-module to included, an app deleted — the retry RECOMPUTES a
-- DIFFERENT amount and re-sends the SAME idem key with different parameters, which
-- Stripe REJECTS (idempotency-key reuse with a mismatched body), permanently
-- stalling the run ('failed' every cycle) even though the account was already
-- charged.
--
-- This is the exact bug class the superseded account-wide model fixed once (commit
-- ee5043c) via account_overage_snapshots' pending/charged freeze — which migration
-- 033 DROPPED with no replacement for the boundary leg. This column is that
-- replacement, scoped to the boundary run: the amount (and the base/overage
-- description determinant) is FROZEN here BEFORE the first Stripe call, and a
-- reclaimed run REUSES the frozen values instead of the freshly-recomputed ones,
-- so every attempt sends Stripe a byte-identical request under the stable idem key.
--
-- Both columns are NULL until a run reaches its first boundary Stripe charge; a
-- zero-charge / skipped run never freezes. InsertBillingRun's ON CONFLICT DO UPDATE
-- deliberately does NOT touch these columns, so the freeze SURVIVES a reclaim.

ALTER TABLE ms_billing.billing_runs
    ADD COLUMN IF NOT EXISTS frozen_charge_cents    BIGINT  NULL,
    ADD COLUMN IF NOT EXISTS frozen_charge_with_base BOOLEAN NULL;
