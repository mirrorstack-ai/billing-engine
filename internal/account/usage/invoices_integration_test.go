//go:build integration

package usage_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/mirrorstack-ai/billing-engine/internal/account/usage"
	"github.com/mirrorstack-ai/billing-engine/internal/shared/testutil"
)

// These exercise the ListInvoicesForAccount sqlc query against a real
// Postgres (gated by the `integration` build tag). They verify what the
// fake-store unit tests can't: the SQL-side draft exclusion, the keyset
// (created_at, id) DESC tuple comparison — including a created_at tie broken
// by the uuid — and the NUMERIC-cents → int64-micros decode at the store
// boundary. Reuses appSeedAccount from app_usage_integration_test.go (same
// package + tag).

// seedInvoiceMirror inserts one ms_billing.invoices row with an explicit id +
// created_at (the keyset the query pages on). number/hostedURL/pdf empty →
// stored NULL (the pre-enrichment state migration 026 documents).
func seedInvoiceMirror(t *testing.T, pool *pgxpool.Pool, acct, id uuid.UUID, stripeID, status string, dueCents, paidCents int64, createdAt time.Time, number, hostedURL, pdf string) {
	t.Helper()
	var numberArg, hostedArg, pdfArg any
	if number != "" {
		numberArg = number
	}
	if hostedURL != "" {
		hostedArg = hostedURL
	}
	if pdf != "" {
		pdfArg = pdf
	}
	_, err := pool.Exec(context.Background(),
		`INSERT INTO ms_billing.invoices
		   (id, account_id, stripe_invoice_id, status, amount_due, amount_paid, currency,
		    created_at, number, hosted_invoice_url, invoice_pdf)
		 VALUES ($1,$2,$3,$4,$5,$6,'usd',$7,$8,$9,$10)`,
		id.String(), acct.String(), stripeID, status, dueCents, paidCents, createdAt,
		numberArg, hostedArg, pdfArg)
	require.NoError(t, err)
}

// TestListInvoices_Integration_KeysetPaginationAndDraftExclusion walks an
// account's history in pages of 2 through the real query: the draft row never
// surfaces, every non-draft row appears exactly once, and the created_at TIE
// is broken by the uuid so the page boundary can't skip or duplicate.
func TestListInvoices_Integration_KeysetPaginationAndDraftExclusion(t *testing.T) {
	pool := testutil.NewTestDB(t)
	store := usage.NewStore(pool)
	ctx := context.Background()

	acct := appSeedAccount(t, pool)
	base := time.Date(2026, 6, 19, 0, 0, 0, 0, time.UTC)

	// 5 non-draft rows: 3 distinct instants + a tie pair sharing one instant.
	ids := make([]uuid.UUID, 5)
	for i := range ids {
		ids[i] = uuid.New()
	}
	seedInvoiceMirror(t, pool, acct, ids[0], "in_a", "paid", 2000, 2000, base, "", "", "")
	seedInvoiceMirror(t, pool, acct, ids[1], "in_b", "open", 2000, 0, base.AddDate(0, -1, 0), "", "", "")
	seedInvoiceMirror(t, pool, acct, ids[2], "in_c1", "paid", 2000, 2000, base.AddDate(0, -2, 0), "", "", "")
	seedInvoiceMirror(t, pool, acct, ids[3], "in_c2", "void", 2000, 0, base.AddDate(0, -2, 0), "", "", "") // created_at tie with in_c1
	seedInvoiceMirror(t, pool, acct, ids[4], "in_d", "uncollectible", 2000, 0, base.AddDate(0, -3, 0), "", "", "")
	// The draft must never surface, even though it is the newest row.
	seedInvoiceMirror(t, pool, acct, uuid.New(), "in_draft", "draft", 2000, 0, base.AddDate(0, 0, 1), "", "", "")

	seen := map[uuid.UUID]bool{}
	var cursor *usage.InvoiceCursor
	total := 0
	for page := 0; page < 4; page++ {
		rows, err := store.ListInvoices(ctx, acct, 2, cursor)
		require.NoError(t, err)
		if len(rows) == 0 {
			break
		}
		for i, r := range rows {
			require.NotEqual(t, "draft", r.Status, "draft rows are excluded in SQL")
			require.False(t, seen[r.ID], "row %s duplicated across pages", r.ID)
			seen[r.ID] = true
			if i > 0 {
				prev := rows[i-1]
				require.False(t, r.CreatedAt.After(prev.CreatedAt), "rows must be created_at DESC")
			}
			total++
		}
		last := rows[len(rows)-1]
		cursor = &usage.InvoiceCursor{CreatedAt: last.CreatedAt, ID: last.ID}
	}
	require.Equal(t, 5, total, "every non-draft row exactly once (tie pair included)")
	for _, id := range ids {
		require.True(t, seen[id], "row %s missing from the paged walk", id)
	}
}

// TestListInvoices_Integration_CentsToMicrosAndNullDecodes: the mirror's
// NUMERIC whole cents come back ×10_000 as int64 micros, NULL presentment
// columns decode to "" and NULL period bounds to nil, while an enriched row
// carries its values verbatim.
func TestListInvoices_Integration_CentsToMicrosAndNullDecodes(t *testing.T) {
	pool := testutil.NewTestDB(t)
	store := usage.NewStore(pool)
	ctx := context.Background()

	acct := appSeedAccount(t, pool)
	bare := uuid.New()
	seedInvoiceMirror(t, pool, acct, bare, "in_bare", "open", 1234, 0,
		time.Date(2026, 5, 19, 0, 0, 0, 0, time.UTC), "", "", "")

	enriched := uuid.New()
	seedInvoiceMirror(t, pool, acct, enriched, "in_rich", "paid", 1234, 1234,
		time.Date(2026, 6, 19, 0, 0, 0, 0, time.UTC),
		"813C8918-0001", "https://invoice.stripe.com/i/in_rich", "https://pay.stripe.com/invoice/in_rich/pdf")

	rows, err := store.ListInvoices(ctx, acct, 10, nil)
	require.NoError(t, err)
	require.Len(t, rows, 2)

	// Newest-first: the enriched June row leads.
	rich, plain := rows[0], rows[1]
	require.Equal(t, enriched, rich.ID)
	require.Equal(t, bare, plain.ID)

	// $12.34 = 1234 cents → 12_340_000 micros.
	require.EqualValues(t, 12_340_000, rich.AmountDueMicros)
	require.EqualValues(t, 12_340_000, rich.AmountPaidMicros)
	require.EqualValues(t, 12_340_000, plain.AmountDueMicros)
	require.EqualValues(t, 0, plain.AmountPaidMicros)

	require.Equal(t, "813C8918-0001", rich.Number)
	require.Equal(t, "https://invoice.stripe.com/i/in_rich", rich.HostedInvoiceURL)
	require.Equal(t, "https://pay.stripe.com/invoice/in_rich/pdf", rich.InvoicePDF)

	require.Empty(t, plain.Number, "NULL number decodes to the empty pre-enrichment state")
	require.Empty(t, plain.HostedInvoiceURL)
	require.Empty(t, plain.InvoicePDF)
	require.Nil(t, plain.PeriodStart, "NULL period bound decodes to nil")
	require.Nil(t, plain.PeriodEnd)
}

// TestListInvoices_Integration_AccountScoped: another account's invoices
// never leak into the page.
func TestListInvoices_Integration_AccountScoped(t *testing.T) {
	pool := testutil.NewTestDB(t)
	store := usage.NewStore(pool)
	ctx := context.Background()

	mine := appSeedAccount(t, pool)
	other := appSeedAccount(t, pool)
	seedInvoiceMirror(t, pool, mine, uuid.New(), "in_mine", "paid", 100, 100,
		time.Date(2026, 6, 19, 0, 0, 0, 0, time.UTC), "", "", "")
	seedInvoiceMirror(t, pool, other, uuid.New(), "in_other", "paid", 100, 100,
		time.Date(2026, 6, 20, 0, 0, 0, 0, time.UTC), "", "", "")

	rows, err := store.ListInvoices(ctx, mine, 10, nil)
	require.NoError(t, err)
	require.Len(t, rows, 1)
	require.Equal(t, "in_mine", rows[0].StripeInvoiceID)
}
