-- Queries backing internal/account/webhook.pgxStore (the Stripe webhook
-- side-effect surface). All operate on the ms_billing schema.

-- MarkEventProcessed records the event_id for idempotency. :execrows so
-- the caller can map 1 row → firstTime=true, 0 → duplicate.
-- name: MarkEventProcessed :execrows
INSERT INTO ms_billing.webhook_events_processed (event_id, event_type)
VALUES ($1, $2)
ON CONFLICT (event_id) DO NOTHING;

-- TouchAccountByStripeCustomer bumps updated_at for the account matching
-- a Stripe customer. :execrows → >0 means found.
-- name: TouchAccountByStripeCustomer :execrows
UPDATE ms_billing.accounts SET updated_at = now() WHERE stripe_customer_id = $1;

-- SetDefaultPaymentMethodByCustomer marks one PM as default and unmarks
-- all others for the account in a single UPDATE. Empty defaultStripePMID
-- ($2) clears the flag everywhere (no row equals '').
-- name: SetDefaultPaymentMethodByCustomer :exec
UPDATE ms_billing.payment_methods_mirror
SET is_default = (stripe_payment_method_id = $2)
WHERE account_id = (
    SELECT id FROM ms_billing.accounts WHERE stripe_customer_id = $1
)
AND deleted_at IS NULL;

-- InsertPaymentMethod mirrors a Stripe PM into payment_methods_mirror.
-- First active card on the account becomes the default (NOT EXISTS).
-- Skips when an active row already shares brand/last4/exp (best-effort
-- insert-time dedupe). fingerprint stored via NULLIF($7,''). No RETURNING
-- → :execrows; the Go layer disambiguates 0 rows via AccountExists.
--
-- is_default on INSERT is ADVISORY (the first-card auto-default feature
-- from #14): it gives a brand-new account a usable default without an
-- explicit choice. It is NOT authoritative — customer.updated →
-- SetDefaultPaymentMethodByCustomer is the single source of truth for
-- which PM is default. This INSERT-time value matches #14's raw
-- InsertPaymentMethod exactly (NOT EXISTS over non-soft-deleted rows);
-- do not "promote" it to authoritative here.
-- name: InsertPaymentMethod :execrows
WITH acct AS (
    SELECT id FROM ms_billing.accounts WHERE stripe_customer_id = $1
)
INSERT INTO ms_billing.payment_methods_mirror
    (account_id, stripe_payment_method_id, brand, last4, exp_month, exp_year, is_default, fingerprint)
SELECT acct.id, $2, $3, $4, $5, $6,
    -- ADVISORY first-card default (see header). Authoritative default is
    -- set by customer.updated → SetDefaultPaymentMethodByCustomer.
    NOT EXISTS (
        SELECT 1 FROM ms_billing.payment_methods_mirror p
        WHERE p.account_id = acct.id AND p.deleted_at IS NULL
    ),
    NULLIF($7, '')
FROM acct
WHERE NOT EXISTS (
    SELECT 1 FROM ms_billing.payment_methods_mirror p2
    WHERE p2.account_id = acct.id
      AND p2.deleted_at IS NULL
      AND p2.brand = $3
      AND p2.last4 = $4
      AND p2.exp_month = $5
      AND p2.exp_year = $6
)
ON CONFLICT (stripe_payment_method_id) DO NOTHING;

-- AccountExistsByStripeCustomer disambiguates a 0-row InsertPaymentMethod
-- (drift vs ON CONFLICT/dedupe no-op).
-- name: AccountExistsByStripeCustomer :one
SELECT EXISTS (SELECT 1 FROM ms_billing.accounts WHERE stripe_customer_id = $1) AS account_exists;

-- StampAccountActivated freezes the billing-period ANCHOR (migration 025): the
-- UTC instant the account bound its FIRST credit card. Called from
-- payment_method.attached — the moment Stripe CONFIRMS the card is bound, not the
-- synchronous StartAddPaymentMethod call. The `WHERE activated_at IS NULL` guard
-- makes it FIRST-BIND-WINS and idempotent: a detach + re-add (which mints a fresh
-- pm_* and would move a MIN(attached_at)-derived anchor) never regresses the
-- already-frozen anchor, and a webhook retry is a no-op. Resolved by
-- stripe_customer_id (the same key InsertPaymentMethod uses); a missing account is
-- a no-op (0 rows) that the handler treats as drift, exactly like the mirror
-- insert. :execrows so the Go layer can log a first-time activation.
-- name: StampAccountActivated :execrows
UPDATE ms_billing.accounts
SET activated_at = now()
WHERE stripe_customer_id = $1 AND activated_at IS NULL;

-- SoftDeletePaymentMethod marks a mirror row deleted by Stripe PM id.
-- :execrows → >0 means found.
-- name: SoftDeletePaymentMethod :execrows
UPDATE ms_billing.payment_methods_mirror
SET deleted_at = now()
WHERE stripe_payment_method_id = $1 AND deleted_at IS NULL;

-- SetAddCardRequestStripePM stamps the resolved Stripe PM id onto a
-- still-pending request row matched by setup_intent_id.
-- name: SetAddCardRequestStripePM :exec
UPDATE ms_billing.add_card_requests
SET stripe_pm_id = $2
WHERE setup_intent_id = $1 AND status = 'pending';

-- MirrorRowByStripePM looks up a just-mirrored row by Stripe PM id, for
-- the resolve transaction (step 1). fingerprint is nullable.
-- `AND deleted_at IS NULL` guards the resolve anchor: a soft-deleted
-- mirror row (e.g. detached then a stale attached/setup_intent event
-- replays) must never become the row the request resolves against.
-- name: MirrorRowByStripePM :one
SELECT id, account_id, fingerprint
FROM ms_billing.payment_methods_mirror
WHERE stripe_payment_method_id = $1 AND deleted_at IS NULL;

-- DuplicateFingerprintPM probes for ANOTHER active mirror row on the same
-- account with the same fingerprint (resolve transaction step 2).
-- name: DuplicateFingerprintPM :one
SELECT id
FROM ms_billing.payment_methods_mirror
WHERE account_id = $1
  AND fingerprint = $2
  AND id <> $3
  AND deleted_at IS NULL
LIMIT 1;

-- SoftDeleteMirrorByID soft-deletes the just-mirrored duplicate row by id
-- (resolve transaction step 3).
-- name: SoftDeleteMirrorByID :exec
UPDATE ms_billing.payment_methods_mirror
SET deleted_at = now()
WHERE id = $1 AND deleted_at IS NULL;

-- ResolveAddCardRequest is the terminal resolve (step 4): set status +
-- payment_method_id + resolved_at on the still-pending request matched by
-- stripe_pm_id. $2 is cast to the enum type.
-- name: ResolveAddCardRequest :exec
UPDATE ms_billing.add_card_requests
SET status = $2::ms_billing.add_card_request_status,
    payment_method_id = $3,
    resolved_at = now()
WHERE stripe_pm_id = $1 AND status = 'pending';

-- ApplyInvoiceStatus reconciles a Stripe invoice.* event onto the mirror row
-- keyed by stripe_invoice_id. It updates status + amount_paid + amount_due
-- plus the PRESENTMENT columns below (period_start/end + currency belong to
-- the charge spine's UpsertInvoice and must not be clobbered by a webhook).
-- updated_at is trigger-maintained.
--
-- Presentment enrichment (migration 026): number / hosted_invoice_url /
-- invoice_pdf are Stripe-assigned at FINALIZATION and immutable afterwards, so
-- they ride every finalized-and-later event payload and are absent before
-- that. Each lands upsert-style via COALESCE(NULLIF(new, ''), old): a
-- non-empty value overwrites, an empty one KEEPS the stored value — an event
-- that predates finalization (or a sparse replay) can never clear an
-- already-enriched column back to NULL.
--
-- Out-of-order + at-least-once safety: Stripe delivers webhooks at-least-once
-- and NOT in guaranteed order, so a late invoice.finalized (open) can arrive
-- after invoice.paid (paid). The WHERE guard enforces a MONOTONIC status
-- transition via a rank ladder — draft(0) < open(1) < paid/void/uncollectible(2,
-- terminal) — so the row's status can only move forward. The new status's rank
-- must be strictly greater than the current rank (a forward transition), with
-- one exception: an identical re-apply of the SAME status is allowed through so
-- a replayed paid can refresh amount_paid/amount_due idempotently. A terminal
-- status (rank 2) is never overwritten by a different status because no rank
-- exceeds 2 and the equal-rank branch requires status equality, so paid→void
-- (both rank 2) is a no-op — terminal-once-set holds. The CASE ladder is inline
-- (not a stored ENUM) so a new Stripe status maps to the bottom rung (-1) and
-- can never silently regress a known terminal state.
--
-- :execrows → >0 means the row existed AND the guard let the update through; 0
-- means either no mirror row (drift: the charge spine's UpsertInvoice hasn't
-- run, or the invoice was created out-of-band) OR the guard rejected a stale /
-- regressing event. Both are safe no-ops the Go layer logs as drift_warning,
-- never an error.
-- name: ApplyInvoiceStatus :execrows
UPDATE ms_billing.invoices AS i
SET status             = @status,
    amount_paid        = @amount_paid,
    amount_due         = @amount_due,
    number             = COALESCE(NULLIF(@number::text, ''), i.number),
    hosted_invoice_url = COALESCE(NULLIF(@hosted_invoice_url::text, ''), i.hosted_invoice_url),
    invoice_pdf        = COALESCE(NULLIF(@invoice_pdf::text, ''), i.invoice_pdf)
WHERE i.stripe_invoice_id = @stripe_invoice_id
  AND (
        -- forward transition: incoming rank strictly above the stored rank …
        (CASE @status::text
            WHEN 'draft' THEN 0 WHEN 'open' THEN 1
            WHEN 'paid' THEN 2 WHEN 'void' THEN 2 WHEN 'uncollectible' THEN 2
            ELSE -1 END)
        >
        (CASE i.status
            WHEN 'draft' THEN 0 WHEN 'open' THEN 1
            WHEN 'paid' THEN 2 WHEN 'void' THEN 2 WHEN 'uncollectible' THEN 2
            ELSE -1 END)
        -- … OR an identical re-apply (idempotent amount refresh on replay).
        OR i.status = @status
      );

-- RelaxCollectionOnPaidInvoice is the risk-graded RELAX driver (PR #9, design
-- §7-A "relax back toward arrears only on sustained clean standing"). It is the
-- inverse of the charge cycle's tighten: when an invoice is PAID, an account that
-- was tightened to 'prepaid' is conservatively re-trusted back to 'arrears' — but
-- ONLY when no delinquency remains. The guards make it safe + idempotent:
--   - resolve the account from the paid invoice's mirror row (subquery on
--     stripe_invoice_id);
--   - act only on a 'prepaid' account (a 'arrears' account is already relaxed →
--     no-op);
--   - relax only when the account has NO other 'open'/'uncollectible' invoice
--     (the same delinquency predicate AccountHasUnpaidInvoice uses) — a single
--     paid invoice while another is still unpaid must NOT re-trust.
-- It NEVER charges (the charge cycle is tighten-only); relax + charge are
-- decoupled so an account is never relaxed and charged in the same beat. The
-- caller invokes this AFTER ApplyInvoiceStatus has landed the 'paid' status, so
-- the just-paid invoice is no longer counted as unpaid by the NOT EXISTS guard.
-- :execrows → 1 = relaxed, 0 = no-op (not prepaid, or still delinquent, or no
-- mirror row); the Go layer logs the outcome but treats 0 as success.
-- name: RelaxCollectionOnPaidInvoice :execrows
UPDATE ms_billing.accounts AS a
SET usage_billing_mode = 'arrears'
WHERE a.id = (
        SELECT inv.account_id FROM ms_billing.invoices AS inv
        WHERE inv.stripe_invoice_id = $1
      )
  AND a.usage_billing_mode = 'prepaid'
  AND NOT EXISTS (
        SELECT 1 FROM ms_billing.invoices i
        WHERE i.account_id = a.id
          AND i.status IN ('open', 'uncollectible')
      );
