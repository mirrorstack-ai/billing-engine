package usage_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/mirrorstack-ai/billing-engine/internal/account/billing"
	"github.com/mirrorstack-ai/billing-engine/internal/account/usage"
)

// invAt builds a non-draft invoice mirror row created at the given RFC3339
// instant. Money is int64 micros (the store already converted cents ×10_000;
// the service passes it through untouched).
func invAt(t *testing.T, createdAt string, dueMicros int64) usage.InvoiceMirrorRaw {
	t.Helper()
	ts, err := time.Parse(time.RFC3339, createdAt)
	require.NoError(t, err)
	return usage.InvoiceMirrorRaw{
		ID:               uuid.New(),
		StripeInvoiceID:  "in_" + uuid.NewString()[:8],
		Status:           "paid",
		AmountDueMicros:  dueMicros,
		AmountPaidMicros: dueMicros,
		Currency:         "usd",
		CreatedAt:        ts,
	}
}

// seedOwnerAccount wires owner → account into the fake and returns the owner.
func seedOwnerAccount(store *fakeStore) uuid.UUID {
	owner := uuid.New()
	store.accounts[owner] = uuid.New()
	return owner
}

func TestListInvoices_RequiresOwner(t *testing.T) {
	_, err := newService(newFakeStore()).ListInvoices(context.Background(), usage.ListInvoicesRequest{})
	requireCode(t, err, billing.CodeInvalidInput)
}

func TestListInvoices_RejectsBothOwners(t *testing.T) {
	_, err := newService(newFakeStore()).ListInvoices(context.Background(), usage.ListInvoicesRequest{
		OwnerUserID: uuid.New(), OwnerOrgID: uuid.New(),
	})
	requireCode(t, err, billing.CodeInvalidInput)
}

func TestListInvoices_NegativeLimitRejected(t *testing.T) {
	_, err := newService(newFakeStore()).ListInvoices(context.Background(), usage.ListInvoicesRequest{
		OwnerUserID: uuid.New(), Limit: -1,
	})
	requireCode(t, err, billing.CodeInvalidInput)
}

// TestListInvoices_NoAccountReturnsEmpty: an owner with no billing account yet
// gets the empty page (non-nil slice, "" cursor), NOT an error — the shared
// lazy-account outcome of every read RPC in this package.
func TestListInvoices_NoAccountReturnsEmpty(t *testing.T) {
	resp, err := newService(newFakeStore()).ListInvoices(context.Background(), usage.ListInvoicesRequest{
		OwnerUserID: uuid.New(),
	})
	require.NoError(t, err)
	require.NotNil(t, resp.Invoices, "wire must carry [] not null")
	require.Empty(t, resp.Invoices)
	require.Empty(t, resp.NextCursor)
}

// TestListInvoices_DefaultLimit: an omitted limit resolves to the default page
// size, and the store is asked for page+1 rows (the further-page probe).
func TestListInvoices_DefaultLimit(t *testing.T) {
	store := newFakeStore()
	owner := seedOwnerAccount(store)
	for i := 0; i < usage.DefaultInvoicePageSize+5; i++ {
		store.invoiceRows = append(store.invoiceRows,
			invAt(t, time.Date(2026, 1, 1+i, 0, 0, 0, 0, time.UTC).Format(time.RFC3339), 1_000_000))
	}

	resp, err := newService(store).ListInvoices(context.Background(), usage.ListInvoicesRequest{OwnerUserID: owner})
	require.NoError(t, err)
	require.Len(t, resp.Invoices, usage.DefaultInvoicePageSize)
	require.EqualValues(t, usage.DefaultInvoicePageSize+1, store.gotInvoiceLimit, "store is asked for page+1 rows")
	require.NotEmpty(t, resp.NextCursor, "more rows exist → cursor minted")
}

// TestListInvoices_CapsLimit: an oversized ask is clamped to the cap, not
// rejected.
func TestListInvoices_CapsLimit(t *testing.T) {
	store := newFakeStore()
	owner := seedOwnerAccount(store)
	for i := 0; i < usage.MaxInvoicePageSize+3; i++ {
		store.invoiceRows = append(store.invoiceRows,
			invAt(t, time.Date(2026, 1, 1, 0, 0, i, 0, time.UTC).Format(time.RFC3339), 1_000_000))
	}

	resp, err := newService(store).ListInvoices(context.Background(), usage.ListInvoicesRequest{
		OwnerUserID: owner, Limit: 500,
	})
	require.NoError(t, err)
	require.Len(t, resp.Invoices, usage.MaxInvoicePageSize)
	require.EqualValues(t, usage.MaxInvoicePageSize+1, store.gotInvoiceLimit)
}

// TestListInvoices_LastPageEmptyCursor: a page that exhausts the history
// carries next_cursor "" (the client's stop signal).
func TestListInvoices_LastPageEmptyCursor(t *testing.T) {
	store := newFakeStore()
	owner := seedOwnerAccount(store)
	store.invoiceRows = []usage.InvoiceMirrorRaw{
		invAt(t, "2026-06-19T00:00:00Z", 1_000_000),
		invAt(t, "2026-05-19T00:00:00Z", 2_000_000),
	}

	resp, err := newService(store).ListInvoices(context.Background(), usage.ListInvoicesRequest{
		OwnerUserID: owner, Limit: 5,
	})
	require.NoError(t, err)
	require.Len(t, resp.Invoices, 2)
	require.Empty(t, resp.NextCursor, "exactly-exhausted history mints no cursor")
}

// TestListInvoices_InvalidCursorRejected: a malformed cursor is INVALID_INPUT
// — never a silent first-page restart (that would duplicate rows).
func TestListInvoices_InvalidCursorRejected(t *testing.T) {
	store := newFakeStore()
	owner := seedOwnerAccount(store)
	svc := newService(store)

	for _, cursor := range []string{
		"!!!not-base64!!!",    // not base64 at all
		"Z2FyYmFnZQ",          // base64("garbage") — no separator
		"bm90LWEtdGltZXwxMjM", // base64("not-a-time|123") — bad timestamp + uuid
	} {
		_, err := svc.ListInvoices(context.Background(), usage.ListInvoicesRequest{
			OwnerUserID: owner, Cursor: cursor,
		})
		requireCode(t, err, billing.CodeInvalidInput)
	}
}

// TestListInvoices_CursorRoundTrip_NoOverlapNoSkip walks the full history in
// pages of 2 — including a created_at TIE broken by id — and requires the
// concatenation of pages to equal one newest-first pass with no duplicate and
// no gap, with the final page carrying an empty cursor.
func TestListInvoices_CursorRoundTrip_NoOverlapNoSkip(t *testing.T) {
	store := newFakeStore()
	owner := seedOwnerAccount(store)
	rows := []usage.InvoiceMirrorRaw{
		invAt(t, "2026-06-19T00:00:00Z", 1),
		invAt(t, "2026-05-19T00:00:00Z", 2),
		invAt(t, "2026-04-19T00:00:00Z", 3),
		invAt(t, "2026-04-19T00:00:00Z", 4), // created_at tie with the row above
		invAt(t, "2026-03-19T00:00:00Z", 5),
	}
	store.invoiceRows = rows
	svc := newService(store)

	var collected []string
	cursor := ""
	pages := 0
	for {
		resp, err := svc.ListInvoices(context.Background(), usage.ListInvoicesRequest{
			OwnerUserID: owner, Limit: 2, Cursor: cursor,
		})
		require.NoError(t, err)
		for _, inv := range resp.Invoices {
			collected = append(collected, inv.ID)
		}
		pages++
		require.LessOrEqual(t, pages, 4, "pagination must terminate")
		if resp.NextCursor == "" {
			break
		}
		cursor = resp.NextCursor
	}

	require.Equal(t, 3, pages, "5 rows in pages of 2 → 3 pages")
	require.Len(t, collected, len(rows), "every row exactly once (no overlap, no skip)")
	seen := map[string]bool{}
	for _, id := range collected {
		require.False(t, seen[id], "row %s returned twice across pages", id)
		seen[id] = true
	}
}

// TestListInvoices_ExcludesDraft: draft rows never surface. The fake mirrors
// the SQL contract (status <> 'draft' lives in ListInvoicesForAccount); the
// authoritative filter is exercised against real Postgres by the integration
// test.
func TestListInvoices_ExcludesDraft(t *testing.T) {
	store := newFakeStore()
	owner := seedOwnerAccount(store)
	draft := invAt(t, "2026-06-20T00:00:00Z", 9)
	draft.Status = "draft"
	open := invAt(t, "2026-06-19T00:00:00Z", 1)
	open.Status = "open"
	store.invoiceRows = []usage.InvoiceMirrorRaw{draft, open}

	resp, err := newService(store).ListInvoices(context.Background(), usage.ListInvoicesRequest{OwnerUserID: owner})
	require.NoError(t, err)
	require.Len(t, resp.Invoices, 1)
	require.Equal(t, "open", resp.Invoices[0].Status)
}

// TestListInvoices_RowPassthrough: the wire row carries the store row's
// fields verbatim — micros untouched (the ×10_000 conversion already happened
// at the store boundary), Stripe status verbatim, presentment fields and the
// period window passed through.
func TestListInvoices_RowPassthrough(t *testing.T) {
	store := newFakeStore()
	owner := seedOwnerAccount(store)
	ps := time.Date(2026, 5, 19, 0, 0, 0, 0, time.UTC)
	pe := time.Date(2026, 6, 19, 0, 0, 0, 0, time.UTC)
	row := invAt(t, "2026-06-19T00:00:00Z", 12_340_000)
	row.Number = "813C8918-0001"
	row.HostedInvoiceURL = "https://invoice.stripe.com/i/x"
	row.InvoicePDF = "https://pay.stripe.com/invoice/x/pdf"
	row.PeriodStart, row.PeriodEnd = &ps, &pe
	store.invoiceRows = []usage.InvoiceMirrorRaw{row}

	resp, err := newService(store).ListInvoices(context.Background(), usage.ListInvoicesRequest{OwnerUserID: owner})
	require.NoError(t, err)
	require.Len(t, resp.Invoices, 1)
	got := resp.Invoices[0]
	require.Equal(t, row.ID.String(), got.ID)
	require.Equal(t, row.StripeInvoiceID, got.StripeInvoiceID)
	require.Equal(t, "813C8918-0001", got.Number)
	require.Equal(t, "paid", got.Status)
	require.EqualValues(t, 12_340_000, got.AmountDueMicros)
	require.EqualValues(t, 12_340_000, got.AmountPaidMicros)
	require.Equal(t, "usd", got.Currency)
	require.Equal(t, &ps, got.PeriodStart)
	require.Equal(t, &pe, got.PeriodEnd)
	require.Equal(t, row.HostedInvoiceURL, got.HostedInvoiceURL)
	require.Equal(t, row.InvoicePDF, got.InvoicePDF)
}

func TestListInvoices_InternalOnStoreError(t *testing.T) {
	store := newFakeStore()
	owner := seedOwnerAccount(store)
	store.errListInvoices = errors.New("db down")

	_, err := newService(store).ListInvoices(context.Background(), usage.ListInvoicesRequest{OwnerUserID: owner})
	requireCode(t, err, billing.CodeInternal)
}
