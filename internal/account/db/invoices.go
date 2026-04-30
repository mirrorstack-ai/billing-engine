package db

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

const upsertInvoiceSQL = `
INSERT INTO ms_billing_account.billing_invoices (
    billing_account_id, stripe_invoice_id, status,
    total, currency, hosted_invoice_url,
    period_start, period_end
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
ON CONFLICT (stripe_invoice_id) DO UPDATE
SET status             = EXCLUDED.status,
    total              = EXCLUDED.total,
    currency           = EXCLUDED.currency,
    hosted_invoice_url = EXCLUDED.hosted_invoice_url,
    period_start       = EXCLUDED.period_start,
    period_end         = EXCLUDED.period_end
`

// InvoiceUpsert is the payload for UpsertInvoice.
type InvoiceUpsert struct {
	BillingAccountID uuid.UUID
	StripeInvoiceID  string
	Status           string
	Total            int64
	Currency         string
	HostedInvoiceURL string
	PeriodStart      time.Time
	PeriodEnd        time.Time
}

// UpsertInvoice inserts-or-updates a billing_invoices row.
func UpsertInvoice(ctx context.Context, tx pgx.Tx, in InvoiceUpsert) error {
	if _, err := tx.Exec(ctx, upsertInvoiceSQL,
		in.BillingAccountID,
		in.StripeInvoiceID,
		in.Status,
		in.Total,
		in.Currency,
		in.HostedInvoiceURL,
		in.PeriodStart,
		in.PeriodEnd,
	); err != nil {
		return fmt.Errorf("upsert invoice: %w", err)
	}
	return nil
}
