-- Queries backing internal/account/einvoice.pgxStore (migration 093,
-- billing-engine#285). One row per billing account; GetBuyerInvoiceProfile /
-- PutBuyerInvoiceProfile are the only two verbs this issue wires — online
-- vendor validation is the port's job (#286), not this table's.

-- name: GetBuyerInvoiceProfile :one
SELECT *
FROM ms_billing.buyer_invoice_profiles
WHERE account_id = $1;

-- PutBuyerInvoiceProfile upserts the whole row keyed on account_id. Every
-- column is overwritten — there is no partial-field update — so a caller
-- reading-then-writing a subset must round-trip the untouched fields itself.
-- name: PutBuyerInvoiceProfile :one
INSERT INTO ms_billing.buyer_invoice_profiles (
    account_id, kind, tax_id, company_name, address, email, carrier_type, carrier_num, donation_code
) VALUES (
    $1, $2, $3, $4, $5, $6, $7, $8, $9
)
ON CONFLICT (account_id) DO UPDATE SET
    kind          = EXCLUDED.kind,
    tax_id        = EXCLUDED.tax_id,
    company_name  = EXCLUDED.company_name,
    address       = EXCLUDED.address,
    email         = EXCLUDED.email,
    carrier_type  = EXCLUDED.carrier_type,
    carrier_num   = EXCLUDED.carrier_num,
    donation_code = EXCLUDED.donation_code
RETURNING *;
