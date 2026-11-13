-- 088 down: drop the sweep index and the lazy-user stamp. A row the sweep
-- already repointed keeps its account_id; an unswept stamped row goes back to
-- being the unattributable NULL row it was before 088.
DROP INDEX IF EXISTS ms_billing.usage_events_lazy_owner_user_idx;
ALTER TABLE ms_billing.usage_events DROP COLUMN IF EXISTS owner_user_id;
