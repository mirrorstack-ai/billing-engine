-- 077: the app-member count on the roster mirror (core-v2#1412, owner
-- 2026-09-13: "extra app members are charged $2.00 each per period, the same
-- per-unit shape as custom domains").
--
-- The plan includes a number of members (usage.PlanTerms.MembersIncluded —
-- 3 / 10 / 25); every member past it costs usage.ExtraMemberFeeMicros per
-- period, billed IN ARREARS by the boundary leg for the period that just
-- CLOSED, against the plan the app was on during that period (from the
-- plan-change ledger, migration 076 — never the plan a boundary downgrade has
-- just moved it to). This column is the LIVE count api-platform keeps current
-- through RegisterApp (the count at creation) and SyncAppModules (every
-- member add / remove), the same fire-and-forget-with-retry seam module_count
-- rides; the fee is priced from the history table below, not from it. Money
-- never lives here; the price is a reviewed Go constant.
--
-- 🔴 BILLED ON THE HIGH-WATER MARK (owner 2026-09-13): the fee for a period
-- is charged on the MAXIMUM member count seen during it, not the count at
-- the boundary, with no proration. So the roster's live count is not enough
-- on its own: app_member_counts below is the HISTORY — one row per change —
-- and the boundary reads max(count in force when the period opened, max
-- count recorded inside the period). No boundary-time mutation, no reset:
-- a reclaimed run derives the same figure, and the history is auditable.
--
-- Expand-only: every existing row starts at 0, within every plan's allowance,
-- so no bill changes at cut-over.
ALTER TABLE ms_billing.apps
    ADD COLUMN IF NOT EXISTS member_count INT NOT NULL DEFAULT 0
        CONSTRAINT apps_member_count_nonneg CHECK (member_count >= 0);

COMMENT ON COLUMN ms_billing.apps.member_count IS
    'Live app-member count (migration 077), synced by api-platform on every member change. '
    'The boundary leg bills the period''s HIGH-WATER MARK (app_member_counts) minus the plan''s '
    'included count, × $2.00; a deleted app''s count is frozen like its module_count.';

CREATE TABLE IF NOT EXISTS ms_billing.app_member_counts (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    app_id      UUID NOT NULL REFERENCES ms_billing.apps(app_id) ON DELETE CASCADE,
    count       INT NOT NULL CONSTRAINT app_member_counts_nonneg CHECK (count >= 0),
    -- When the count took effect: the app's created_at for the initial row,
    -- the sync instant for every later change.
    recorded_at TIMESTAMPTZ NOT NULL
);

COMMENT ON TABLE ms_billing.app_member_counts IS
    'Member-count history (migration 077): one row per change, written by RegisterApp and '
    'SyncAppModules. The boundary bills the period''s high-water mark from it.';

CREATE INDEX IF NOT EXISTS app_member_counts_app_recorded_idx
    ON ms_billing.app_member_counts (app_id, recorded_at DESC);

-- Every existing app starts its history at its current count, in force
-- since creation, so the first boundary after cut-over reads a mark.
INSERT INTO ms_billing.app_member_counts (app_id, count, recorded_at)
SELECT app_id, member_count, created_at
FROM ms_billing.apps a
WHERE NOT EXISTS (SELECT 1 FROM ms_billing.app_member_counts c WHERE c.app_id = a.app_id);

-- The plan the app was CREATED on (core-v2#1412), frozen at RegisterApp like
-- created_module_count. The creation charge prices its window from THIS plan
-- forward through the app's plan-change ledger (migration 076), never from
-- apps.plan, which a boundary-applied downgrade can move before a late sweep
-- reaches the app — pricing Pro days at Free, or leaving no chain that ends
-- on the row's plan. Every existing row is 'pro', which is what they were
-- created on (075's default).
ALTER TABLE ms_billing.apps
    ADD COLUMN IF NOT EXISTS created_plan TEXT NOT NULL DEFAULT 'pro'
        CONSTRAINT apps_created_plan_known CHECK (created_plan IN ('free', 'pro', 'business'));

COMMENT ON COLUMN ms_billing.apps.created_plan IS
    'The plan the app was registered on (migration 077), immutable. The creation charge prices '
    'its window from this plan through app_plan_changes; apps.plan is the plan in force NOW.';
