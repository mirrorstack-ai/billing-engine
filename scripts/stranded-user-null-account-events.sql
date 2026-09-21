-- Read-only: where usage_events sit at account_id NULL, bucketed by what can
-- reach them (core-v2#340). "user, takeable" rows are the user repoint's
-- (RepointUserNullAccountEvents, driven daily by billing-cycle); "user,
-- backlog (D1d)" rows are older than the account's open frontier and stay
-- NULL; "unrostered" rows belong to an app with no ms_billing.apps row, which
-- no sweep can key. SELECT only.
SELECT
    CASE
        WHEN ap.app_id IS NULL THEN 'unrostered'
        WHEN ap.owner_org_id IS NOT NULL THEN 'org (org sweep)'
        WHEN a.activated_at IS NULL THEN 'user, unactivated'
        WHEN COALESCE(e.billable_at, e.recorded_at) >= GREATEST(
                 date_trunc('day', a.activated_at, 'UTC'),
                 COALESCE((SELECT MAX(p.period_end) FROM ms_billing.billing_periods p
                           WHERE p.account_id = a.id AND p.status IN ('closing', 'invoiced')),
                          '-infinity'::timestamptz))
            THEN 'user, takeable'
        ELSE 'user, backlog (D1d)'
    END                      AS bucket,
    COUNT(*)                 AS events,
    COUNT(DISTINCT e.app_id) AS apps,
    MIN(e.recorded_at)       AS oldest,
    MAX(e.recorded_at)       AS newest
FROM ms_billing.usage_events e
LEFT JOIN ms_billing.apps ap ON ap.app_id = e.app_id
LEFT JOIN ms_billing.accounts a ON a.id = ap.account_id
WHERE e.account_id IS NULL
GROUP BY 1
ORDER BY 1;
