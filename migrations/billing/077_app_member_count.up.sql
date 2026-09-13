-- 077: the app-member count on the roster mirror (core-v2#1412, owner
-- 2026-09-13: "extra app members are charged $2.00 each per period, the same
-- per-unit shape as custom domains").
--
-- The plan includes a number of members (usage.PlanTerms.MembersIncluded —
-- 3 / 10 / 25); every member past it costs usage.ExtraMemberFeeMicros per
-- period, billed by the boundary leg for the NEW period from the count in
-- force at the boundary, exactly as the advance base reads the plan in force
-- there. This column is that count: a LIVE snapshot api-platform keeps current
-- through RegisterApp (the count at creation) and SyncAppModules (every
-- member add / remove), the same fire-and-forget-with-retry seam module_count
-- rides. Money never lives here; the price is a reviewed Go constant.
--
-- Unlike a custom domain there is no activation-period proration for a
-- member added mid-period: the member is counted from the next boundary. The
-- owner priced members per period, and a per-period count read at the
-- boundary is that rule with no second mechanism.
--
-- Expand-only: every existing row starts at 0, within every plan's allowance,
-- so no bill changes at cut-over.
ALTER TABLE ms_billing.apps
    ADD COLUMN IF NOT EXISTS member_count INT NOT NULL DEFAULT 0
        CONSTRAINT apps_member_count_nonneg CHECK (member_count >= 0);

COMMENT ON COLUMN ms_billing.apps.member_count IS
    'Live app-member count (migration 077), synced by api-platform on every member change. '
    'The boundary leg bills max(0, member_count − plan members included) × $2.00 for the new '
    'period; a deleted app''s count is frozen like its module_count.';
