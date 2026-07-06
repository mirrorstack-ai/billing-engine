-- 036: charge-attempt markers (review 2026-07-06, H5/H9).
--
-- The grace legs' crash-recovery story used to rest ENTIRELY on deterministic
-- Stripe idempotency keys: a crash between the Stripe charge and the DB mark
-- was healed by the next sweep replaying the same keys. But Stripe prunes
-- idempotency keys once they are at least 24 hours old, and the retry driver
-- is a DAILY cron — the first retry lands exactly on that boundary (a coin
-- flip) and any later retry is guaranteed pruned, at which point the "replay"
-- mints brand-new Stripe objects and double-charges.
--
-- These markers are the durable "a prior attempt reached the Stripe section"
-- bit (the grace-leg analogue of migration 035's billing_runs freeze). A
-- retry that sees the marker set goes RECOVERY-FIRST: it looks the invoice up
-- on Stripe by the ms_charge_ref metadata anchor (stamped on every draft
-- since the C2 pinning fix) and reconciles what it finds — finalized → mirror
-- + mark; draft → finish it; nothing → charge fresh (nothing ever reached
-- Stripe). Recovery runs BEFORE the live FIFO verdict, so a timer whose rank
-- improved over→included between the crash and the retry can never resolve
-- "included" while charged money sits unmirrored (H9).
--
-- Stamped first-write-wins BEFORE the first Stripe call; never cleared.

ALTER TABLE ms_billing.app_module_overage_timers
    ADD COLUMN charge_attempted_at timestamptz;

COMMENT ON COLUMN ms_billing.app_module_overage_timers.charge_attempted_at IS
    'First instant a Leg-1 (or combined-invoice) charge attempt for this timer reached its Stripe section; NULL = never attempted. Recovery marker (036) — a retry with this set reconciles against Stripe (ms_charge_ref) before recomputing any live verdict.';

ALTER TABLE ms_billing.apps
    ADD COLUMN proration_attempted_at timestamptz;

COMMENT ON COLUMN ms_billing.apps.proration_attempted_at IS
    'First instant a creation-proration charge attempt for this app reached its Stripe section; NULL = never attempted. Recovery marker (036) — a retry with this set and an unarmed guard reconciles against Stripe (ms_charge_ref app-proration:<app_id>) before minting new Stripe objects.';
