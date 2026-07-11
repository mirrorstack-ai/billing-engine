-- Account-scoped READS over the ms_billing.invoices Stripe mirror (011 + 026)
-- — the customer-facing invoice history behind the web-account billing page.
-- billing-engine stays the SOLE writer of the mirror (the charge spine's
-- UpsertInvoice + the webhook's ApplyInvoiceStatus); this file is read-only
-- presentation and never touches Stripe.

-- ListInvoicesForAccount is the keyset page read behind the ListInvoices RPC:
-- one page of an account's mirrored Stripe invoices newest-first, EXCLUDING
-- status='draft' rows — a draft is a not-yet-final Stripe artifact the
-- customer was never billed for (it can still mutate or be deleted
-- Stripe-side), so it never renders in the customer's invoice history.
--
-- Pagination is KEYSET on (created_at, id) DESC: id (the UUID PK) breaks
-- created_at ties so the ordering is total, which makes a page boundary
-- unable to skip or duplicate a row — unlike OFFSET, which drifts whenever a
-- new invoice lands between two requests. @has_cursor::boolean short-circuits
-- the OR on the FIRST page, so the cursor params are inert well-formed
-- placeholders there (the same gate pattern as VersionBreakdownForAccount);
-- with a cursor, the row-tuple comparison resumes strictly AFTER the previous
-- page's last row in DESC order. @row_limit arrives service-clamped
-- (default/cap) as page+1 so the service detects a further page without a
-- COUNT. The invoices_account_idx (011) covers the account filter; per-account
-- invoice counts are one-per-period small, so no composite keyset index is
-- warranted yet.
--
-- Money columns are NUMERIC whole cents (Stripe minor units, 011); the Go
-- store converts cents → int64 micro-dollars (×10_000) so micros stay the only
-- money unit above the store boundary. number / hosted_invoice_url /
-- invoice_pdf are NULL until the finalization webhook enriches the row (026).
-- name: ListInvoicesForAccount :many
SELECT id, stripe_invoice_id, number, status,
       amount_due, amount_paid, currency,
       period_start, period_end, created_at,
       hosted_invoice_url, invoice_pdf, is_large_auto_collect
FROM ms_billing.invoices
WHERE account_id = @account_id::uuid
  AND status <> 'draft'
  AND (NOT @has_cursor::boolean
       OR (created_at, id) < (@cursor_created_at::timestamptz, @cursor_id::uuid))
ORDER BY created_at DESC, id DESC
LIMIT @row_limit::int;

-- The UNPAID predicate (funding-gates design, DECIDED 2026-07-11) shared by
-- the two queries below: still-collectible-but-not-collected mirror rows —
-- status 'open' (finalized, Stripe smart-retrying) or 'uncollectible' (Stripe
-- gave up, the account still owes) — with amount_due > 0 (a zero-total
-- invoice was never money owed). 'draft' (never finalized), 'paid' and 'void'
-- (debt forgiven) are clean. Narrower than AccountHasUnpaidInvoice
-- (billing.sql) only by the amount_due > 0 term: standing and the Pay flow
-- must never block on / offer to pay a zero-amount row.

-- CountUnpaidInvoicesForAccount feeds GetServiceStatus's unpaid gate
-- (eligibility gate 4: >= 2 unpaid → blocked).
-- name: CountUnpaidInvoicesForAccount :one
SELECT COUNT(*)::int AS unpaid_count
FROM ms_billing.invoices
WHERE account_id = $1
  AND status IN ('open', 'uncollectible')
  AND amount_due > 0;

-- ListUnpaidInvoicesForAccount is the read behind the ListUnpaidInvoices RPC
-- (the post-card-bind "pay N unpaid invoices?" prompt + the invoices table's
-- Pay affordance). Oldest-first so a sequential pay-all settles the oldest
-- debt first. Unpaid counts are gate-bounded small (the serving gate blocks
-- at 2), so no pagination.
-- name: ListUnpaidInvoicesForAccount :many
SELECT id, COALESCE(number, '')::text AS number, amount_due, created_at
FROM ms_billing.invoices
WHERE account_id = $1
  AND status IN ('open', 'uncollectible')
  AND amount_due > 0
ORDER BY created_at ASC, id ASC;

-- InvoiceForPayment resolves a mirror invoice by (id, account) for the
-- PayInvoice RPC — the account scope IS the ownership check (a foreign or
-- unknown id returns no row → NOT_FOUND, never leaking existence). Status
-- rides along so the service can short-circuit an already-paid row and
-- reject non-payable states before touching Stripe.
-- name: InvoiceForPayment :one
SELECT stripe_invoice_id, status
FROM ms_billing.invoices
WHERE id = $1
  AND account_id = $2;
