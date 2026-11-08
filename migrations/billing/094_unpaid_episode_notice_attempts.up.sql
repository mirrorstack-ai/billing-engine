-- 094: failed-notice counter on an unpaid episode (billing-engine#330,
-- core-v2#1881, owner order D592).
--
-- The 14-day unpaid notice is sent best-effort: a failed send leaves
-- notice_sent_at NULL (the account stays unblocked) and the daily sweep retries.
-- This column counts those failed sends per episode so the third failure can
-- raise an ALERT-level log line instead of failing silently forever.
ALTER TABLE ms_billing.unpaid_episodes
    ADD COLUMN IF NOT EXISTS notice_attempts integer NOT NULL DEFAULT 0;
