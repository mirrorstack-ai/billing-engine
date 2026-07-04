package usage

import (
	"context"
	"encoding/base64"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/mirrorstack-ai/billing-engine/internal/account/billing"
)

// ============================================================================
// ListInvoices — the account's Stripe invoice HISTORY read.
//
// Serves the web-account billing page's invoices list from the local
// ms_billing.invoices mirror (011 + 026) — never a Stripe round-trip.
// Read-only presentation over rows the charge spine + webhook already own;
// draft rows are excluded (the customer was never billed for a draft, and it
// can still mutate Stripe-side). billing_runs stays internal — it is an ops
// surface, not a customer one.
//
// Pagination is KEYSET on (created_at, id) DESC behind an OPAQUE cursor:
// clients treat the token as a black box (its format may change), pass "" /
// omit it for the first page, and stop when next_cursor comes back "".
// ============================================================================

const (
	// DefaultInvoicePageSize is the page size when the request omits limit
	// (0). 12 ≈ a year of monthly invoices — the web list's natural first
	// screen.
	DefaultInvoicePageSize = 12

	// MaxInvoicePageSize caps a requested limit. An oversized ask is CLAMPED
	// (not rejected): the caller still gets a valid page + cursor, and the
	// clamp keeps a single request from dragging an unbounded row count
	// through the cents→micros decode path.
	MaxInvoicePageSize = 50
)

// ListInvoicesRequest is the payload of ListInvoices: the owner principal
// whose invoice history is listed (the payer — exactly one of OwnerUserID /
// OwnerOrgID), plus optional paging controls.
type ListInvoicesRequest struct {
	OwnerUserID uuid.UUID `json:"owner_user_id,omitempty"`
	OwnerOrgID  uuid.UUID `json:"owner_org_id,omitempty"`
	// Limit is the requested page size. 0 / omitted → DefaultInvoicePageSize;
	// values above MaxInvoicePageSize are clamped; negative → INVALID_INPUT.
	Limit int `json:"limit,omitempty"`
	// Cursor is the OPAQUE next_cursor token from the previous page's
	// response. "" / omitted starts at the newest invoice. Clients must not
	// parse or construct it; a malformed token → INVALID_INPUT.
	Cursor string `json:"cursor,omitempty"`
}

// InvoiceRow is one invoice on the wire. Money is integer micro-USD
// (converted from the mirror's whole Stripe cents, ×10_000); Status is the
// Stripe invoice status vocabulary VERBATIM (open/paid/uncollectible/void —
// draft rows are excluded), so the web's badge map keys directly on it.
type InvoiceRow struct {
	// ID is the mirror row's UUID (a stable local identity for React keys /
	// deduping); StripeInvoiceID is the Stripe-side anchor (in_…).
	ID              string `json:"id"`
	StripeInvoiceID string `json:"stripe_invoice_id"`
	// Number is Stripe's customer-facing invoice number, assigned at
	// finalization (migration 026). Omitted while empty — historic rows
	// mirrored before 026 stay unenriched until a later webhook delivery.
	Number string `json:"number,omitempty"`
	Status string `json:"status"`

	AmountDueMicros  int64  `json:"amount_due_micros"`
	AmountPaidMicros int64  `json:"amount_paid_micros"`
	Currency         string `json:"currency"`

	// PeriodStart / PeriodEnd echo the billed [start, end) window; omitted
	// for a non-period (manual) invoice whose mirror row carries NULLs.
	PeriodStart *time.Time `json:"period_start,omitempty"`
	PeriodEnd   *time.Time `json:"period_end,omitempty"`

	CreatedAt time.Time `json:"created_at"`

	// HostedInvoiceURL / InvoicePDF are the Stripe-hosted View / Download
	// links (migration 026). Omitted while empty — the UI hides both buttons
	// for an unenriched row rather than rendering dead links.
	HostedInvoiceURL string `json:"hosted_invoice_url,omitempty"`
	InvoicePDF       string `json:"invoice_pdf,omitempty"`

	// IsLargeAutoCollect is the server-computed post-hoc disclosure flag
	// (migration 031): true when this invoice's off-session charge exceeded the
	// account's auto-collect threshold that applied when it fired. The billing
	// page surfaces flagged charges in its large-auto-collected disclosure
	// section. Always present (defaults false); api-client-shared's Invoice type
	// should add a matching `is_large_auto_collect` field to consume it.
	IsLargeAutoCollect bool `json:"is_large_auto_collect"`
}

// ListInvoicesResponse is one page of the invoice history, newest-first.
// NextCursor is the opaque token for the next page, "" when this page
// exhausted the history (the client's stop signal).
type ListInvoicesResponse struct {
	Invoices   []InvoiceRow `json:"invoices"`
	NextCursor string       `json:"next_cursor"`
}

// ListInvoices returns one keyset page of the owner's mirrored Stripe
// invoices, newest-first — the read behind the web-account billing page's
// invoice history. It:
//  1. resolves the payer's billing account from the owner principal — no
//     account yet is the normal lazy-state outcome (no invoice could ever
//     exist), answered with an EMPTY page, not an error,
//  2. clamps the page size (default 12, cap 50) and decodes the opaque
//     cursor — a malformed cursor is INVALID_INPUT, never a silent
//     first-page restart (that would duplicate rows for the caller),
//  3. reads limit+1 rows from the mirror (draft-excluded, keyset-ordered in
//     SQL): the extra row only PROVES a further page exists and is dropped,
//  4. mints NextCursor from the last RETURNED row when more pages exist, ""
//     when exhausted.
//
// Money is int64 micros end-to-end above the store (the store converts the
// mirror's whole Stripe cents ×10_000); Status is Stripe vocabulary verbatim.
func (s *Service) ListInvoices(ctx context.Context, req ListInvoicesRequest) (*ListInvoicesResponse, error) {
	if req.OwnerUserID == uuid.Nil && req.OwnerOrgID == uuid.Nil {
		return nil, billing.InvalidInput("owner_user_id or owner_org_id required")
	}
	if req.OwnerUserID != uuid.Nil && req.OwnerOrgID != uuid.Nil {
		return nil, billing.InvalidInput("owner_user_id and owner_org_id are mutually exclusive")
	}
	if req.Limit < 0 {
		return nil, billing.InvalidInput("limit must be non-negative")
	}
	limit := req.Limit
	if limit == 0 {
		limit = DefaultInvoicePageSize
	}
	if limit > MaxInvoicePageSize {
		limit = MaxInvoicePageSize
	}

	var cursor *InvoiceCursor
	if req.Cursor != "" {
		c, err := decodeInvoiceCursor(req.Cursor)
		if err != nil {
			return nil, billing.InvalidInput("invalid cursor")
		}
		cursor = &c
	}

	owner := Owner{UserID: req.OwnerUserID, OrgID: req.OwnerOrgID}
	accountID, found, err := s.store.AccountByOwner(ctx, owner)
	if err != nil {
		return nil, billing.Internal("account lookup failed", err)
	}
	if !found {
		// No billing account yet → no invoice was ever mirrored. The same
		// lazy-account empty answer every read RPC here shares.
		return &ListInvoicesResponse{Invoices: []InvoiceRow{}}, nil
	}

	// Ask for ONE row past the page: its presence proves a further page
	// exists without a COUNT; it is dropped, never returned.
	rows, err := s.store.ListInvoices(ctx, accountID, int32(limit)+1, cursor)
	if err != nil {
		return nil, billing.Internal("list invoices failed", err)
	}
	hasMore := len(rows) > limit
	if hasMore {
		rows = rows[:limit]
	}

	invoices := make([]InvoiceRow, 0, len(rows))
	for _, r := range rows {
		invoices = append(invoices, InvoiceRow{
			ID:                 r.ID.String(),
			StripeInvoiceID:    r.StripeInvoiceID,
			Number:             r.Number,
			Status:             r.Status,
			AmountDueMicros:    r.AmountDueMicros,
			AmountPaidMicros:   r.AmountPaidMicros,
			Currency:           r.Currency,
			PeriodStart:        r.PeriodStart,
			PeriodEnd:          r.PeriodEnd,
			CreatedAt:          r.CreatedAt,
			HostedInvoiceURL:   r.HostedInvoiceURL,
			InvoicePDF:         r.InvoicePDF,
			IsLargeAutoCollect: r.IsLargeAutoCollect,
		})
	}

	nextCursor := ""
	if hasMore {
		last := rows[len(rows)-1]
		nextCursor = encodeInvoiceCursor(last.CreatedAt, last.ID)
	}
	return &ListInvoicesResponse{Invoices: invoices, NextCursor: nextCursor}, nil
}

// --- cursor codec -----------------------------------------------------------

// invoiceCursorSeparator joins the two keyset components inside the decoded
// token. '|' can appear in neither an RFC3339Nano timestamp nor a UUID, so a
// single Cut is unambiguous.
const invoiceCursorSeparator = "|"

// encodeInvoiceCursor mints the opaque page token: the base64url form of
// "<created_at RFC3339Nano>|<id>" — the (created_at, id) keyset position of
// the LAST row on the page just returned. RFC3339Nano keeps the full
// timestamptz precision (a truncated timestamp would make the strict `<`
// resume comparison skip same-microsecond neighbors); base64url keeps the
// token opaque on the wire AND query-string-safe for the account-service
// proxy's ?cursor= passthrough.
func encodeInvoiceCursor(createdAt time.Time, id uuid.UUID) string {
	raw := createdAt.UTC().Format(time.RFC3339Nano) + invoiceCursorSeparator + id.String()
	return base64.RawURLEncoding.EncodeToString([]byte(raw))
}

// decodeInvoiceCursor parses a wire token back into its keyset tuple. Every
// malformed shape (bad base64, missing separator, unparseable timestamp or
// UUID) is an error the service maps to INVALID_INPUT — never a panic, and
// never a silent first-page reset.
func decodeInvoiceCursor(token string) (InvoiceCursor, error) {
	raw, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil {
		return InvoiceCursor{}, fmt.Errorf("decode cursor base64: %w", err)
	}
	tsPart, idPart, ok := strings.Cut(string(raw), invoiceCursorSeparator)
	if !ok {
		return InvoiceCursor{}, fmt.Errorf("cursor missing separator")
	}
	ts, err := time.Parse(time.RFC3339Nano, tsPart)
	if err != nil {
		return InvoiceCursor{}, fmt.Errorf("parse cursor timestamp: %w", err)
	}
	id, err := uuid.Parse(idPart)
	if err != nil {
		return InvoiceCursor{}, fmt.Errorf("parse cursor id: %w", err)
	}
	return InvoiceCursor{CreatedAt: ts, ID: id}, nil
}
