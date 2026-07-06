-- Down for 041: drop the designation table and the org-owned roster support.
-- Unbilled (NULL-account) roster rows cannot survive the NOT NULL restore —
-- they exist only for org-owned apps awaiting designation, so dropping them
-- loses no billed state (their usage_events keep account_id NULL regardless).

ALTER TABLE ms_billing.usage_events
    DROP COLUMN IF EXISTS repointed_from;

DELETE FROM ms_billing.apps WHERE account_id IS NULL;

DROP INDEX IF EXISTS ms_billing.apps_owner_org_idx;

ALTER TABLE ms_billing.apps
    DROP CONSTRAINT IF EXISTS apps_unbilled_only_org_check;

ALTER TABLE ms_billing.apps
    DROP COLUMN IF EXISTS owner_org_id;

ALTER TABLE ms_billing.apps
    ALTER COLUMN account_id SET NOT NULL;

DROP INDEX IF EXISTS ms_billing.org_billing_designations_sponsor_account_idx;

DROP TRIGGER IF EXISTS org_billing_designations_set_updated_at
    ON ms_billing.org_billing_designations;

DROP TABLE IF EXISTS ms_billing.org_billing_designations;
