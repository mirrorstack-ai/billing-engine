-- Migration 043 — sponsor_user_id lookup index for ListSponsoredOrgs
-- (org-billing W1, workspace docs-temp/billing-ui-wiring/plan.md Feature B).
--
-- The /me sponsored-orgs read (ListSponsoredOrgs) answers "which orgs does
-- this user sponsor" by scanning org_billing_designations on sponsor_user_id.
-- Migration 041 indexes only sponsor_account_id (the CASCADE's delete-side
-- scan); sponsor_user_id — the user-facing lookup's key — was unindexed. A
-- partial index scoped to funding = 'sponsor' matches the query's own filter
-- (a designation carries a sponsor_user_id only under sponsor funding, so the
-- predicate excludes exactly the NULL-sponsor 'org' rows) and stays small.

CREATE INDEX IF NOT EXISTS org_billing_designations_sponsor_user_idx
    ON ms_billing.org_billing_designations (sponsor_user_id)
    WHERE funding = 'sponsor';
