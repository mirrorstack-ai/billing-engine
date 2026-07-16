-- ListPMDefaultBackfillCandidates returns every Stripe Customer whose billing
-- account has at least one active, unexpired mirrored payment method. Prefer
-- the advisory default; when none is marked, choose the newest usable mirror
-- row so the one-shot repair can establish a Stripe-side default.
-- name: ListPMDefaultBackfillCandidates :many
SELECT
    COALESCE(a.stripe_customer_id, '')::text AS stripe_customer_id,
    chosen.stripe_payment_method_id AS chosen_stripe_payment_method_id
FROM ms_billing.accounts a
JOIN LATERAL (
    SELECT p.stripe_payment_method_id
    FROM ms_billing.payment_methods_mirror p
    WHERE p.account_id = a.id
      AND p.deleted_at IS NULL
      AND (p.exp_year, p.exp_month) >= (
          EXTRACT(YEAR FROM current_date)::INT,
          EXTRACT(MONTH FROM current_date)::INT
      )
    ORDER BY p.is_default DESC, p.attached_at DESC, p.id DESC
    LIMIT 1
) chosen ON true
WHERE a.stripe_customer_id IS NOT NULL
  AND a.stripe_customer_id <> ''
ORDER BY a.id;
