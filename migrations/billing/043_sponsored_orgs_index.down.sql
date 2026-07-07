-- Down for 043: drop the sponsor_user_id lookup index.

DROP INDEX IF EXISTS ms_billing.org_billing_designations_sponsor_user_idx;
