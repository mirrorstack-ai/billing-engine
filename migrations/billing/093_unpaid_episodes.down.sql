-- Down 093 — drop the unpaid-episode record (nothing else references it).
DROP INDEX IF EXISTS ms_billing.unpaid_episodes_one_open_per_account;
DROP TABLE IF EXISTS ms_billing.unpaid_episodes;
