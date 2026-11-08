-- Preview before UNPAID_GRACE is turned on (billing-engine#330, core-v2#1881,
-- owner order D592): who is blocked for an UNPAID-BILL reason right now?
--
-- With the flag on, each of these accounts stops being blocked, receives the
-- 14-day notice on its next standing event or the next daily sweep, and is
-- blocked again 14 days after that notice if it has not paid. So this list is
-- exactly "who moves into a 14-day grace on their first notice".
--
-- READ-ONLY. Run it by hand; it is deliberately not embedded in any binary.
-- Output is account ids and counts only, no names, no emails, no amounts.
--
-- It restates the four gates of internal/account/eligibility over the same
-- signals as ServiceBlockSignals + CountUnpaidInvoicesForAccount:
--   gate 1  usable non-fraud, unexpired card >= 1     (NOT part of the grace)
--   gate 2  first non-draft/void invoice is not 'uncollectible'
--   gate 3  failed-invoice streak since the last paid invoice < 2
--   gate 4  open/uncollectible invoices with amount_due > 0 < 2
-- Accounts that also fail gate 1 stay blocked at once and are listed apart.
-- Wallet-gate (out of credits) blocks are not visible from SQL; those accounts
-- stay blocked at once regardless.
-- Org accounts funded by a sponsor are read here on their own invoices, the way
-- GetServiceStatus reads the org's resolved account; their card signal lives on
-- the funding account, so for them gate 1 below is an approximation.

WITH signals AS (
    SELECT
        a.id AS account_id,
        (SELECT COUNT(*) FROM ms_billing.payment_methods_mirror pmm
          WHERE pmm.account_id = a.id AND pmm.deleted_at IS NULL AND NOT pmm.fraud_blocked
            AND (pmm.exp_year, pmm.exp_month) >= (
                EXTRACT(YEAR FROM current_date)::INT, EXTRACT(MONTH FROM current_date)::INT)
        )::int AS usable_cards,
        (SELECT COUNT(*) FROM ms_billing.invoices f
          WHERE f.account_id = a.id
            AND (f.ever_failed OR f.status = 'uncollectible')
            AND f.created_at > COALESCE(
                (SELECT MAX(p.created_at) FROM ms_billing.invoices p
                  WHERE p.account_id = a.id AND p.status = 'paid'), '-infinity'::timestamptz)
        )::int AS failed_streak,
        COALESCE((SELECT inv.status FROM ms_billing.invoices inv
                   WHERE inv.account_id = a.id AND inv.status NOT IN ('draft', 'void')
                   ORDER BY inv.created_at ASC, inv.id ASC LIMIT 1), '') AS first_charge_status,
        (SELECT COUNT(*) FROM ms_billing.invoices u
          WHERE u.account_id = a.id AND u.status IN ('open', 'uncollectible') AND u.amount_due > 0
        )::int AS unpaid_count
    FROM ms_billing.accounts a
),
gates AS (
    SELECT account_id,
           usable_cards >= 1                          AS card_ok,
           (first_charge_status = 'uncollectible'
            OR failed_streak >= 2
            OR unpaid_count >= 2)                     AS unpaid_gate_failing
    FROM signals
)
-- One row per account that is blocked for an unpaid-bill reason today, with the
-- size of its bucket on every row so the count needs no second query:
--   moves_into_14_day_grace                     blocked now, not blocked once the flag is on
--   blocked_at_once_no_usable_card_also_unpaid  also fails the card gate: stays blocked
SELECT CASE WHEN card_ok THEN 'moves_into_14_day_grace'
            ELSE 'blocked_at_once_no_usable_card_also_unpaid' END AS bucket,
       account_id,
       COUNT(*) OVER (PARTITION BY card_ok)                       AS bucket_total
  FROM gates
 WHERE unpaid_gate_failing
 ORDER BY card_ok DESC, account_id;

-- Companion check, run separately: episodes already open (should be 0 before the
-- first run with the flag on).
--   SELECT COUNT(*) FROM ms_billing.unpaid_episodes WHERE closed_at IS NULL;
