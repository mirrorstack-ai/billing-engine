package billing

import (
	"context"
	"time"

	"github.com/google/uuid"
)

// ============================================================================
// ListUnpaidInvoices + PayInvoice — the unpaid-invoice surface of the
// funding-gates wave (design: docs-temp/billing-funding-gates/design.md,
// DECIDED 2026-07-11).
//
// ListUnpaidInvoices feeds the web-account post-card-bind "pay N unpaid
// invoices ($X)?" prompt and the invoices table's Pay affordance: count +
// total + the rows, over the SAME unpaid predicate GetServiceStatus's gate 4
// blocks on (open/uncollectible mirror rows with amount_due > 0) — paying
// them down through PayInvoice is the unblock-recovery flow.
//
// PayInvoice pays ONE open Stripe invoice with the owner's default card. The
// mirror row settles via the existing invoice.* webhook reconciliation — this
// RPC NEVER hand-updates mirror status (single-writer rule: the webhook owns
// status transitions).
// ============================================================================

// ListUnpaidInvoicesRequest is the payload of ListUnpaidInvoices: the owner
// principal (exactly one of OwnerUserID / OwnerOrgID — the same owner shape
// as usage.ListInvoices). An org resolves through its funding designation.
type ListUnpaidInvoicesRequest struct {
	OwnerUserID uuid.UUID `json:"owner_user_id,omitempty"`
	OwnerOrgID  uuid.UUID `json:"owner_org_id,omitempty"`
}

// UnpaidInvoice is one unpaid invoice on the wire. InvoiceID is the mirror
// row's UUID (the id PayInvoice takes); Number is Stripe's customer-facing
// invoice number ("" until the finalization webhook enriches the row — kept
// present-but-empty so the client's formatting is unconditional). Money is
// integer micro-USD.
type UnpaidInvoice struct {
	InvoiceID       string    `json:"invoice_id"`
	Number          string    `json:"number"`
	AmountDueMicros int64     `json:"amount_due_micros"`
	CreatedAt       time.Time `json:"created_at"`
}

// ListUnpaidInvoicesResponse is the body of the success envelope: the unpaid
// rows oldest-first plus the precomputed count + total the post-bind prompt
// renders ("pay N unpaid invoices ($X)?").
type ListUnpaidInvoicesResponse struct {
	Invoices    []UnpaidInvoice `json:"invoices"`
	TotalMicros int64           `json:"total_micros"`
	Count       int             `json:"count"`
}

// ListUnpaidInvoices returns the owner's unpaid (open/uncollectible,
// amount_due > 0) mirrored Stripe invoices, oldest-first, with count + total.
// No billing account yet (or an unfunded org) is the normal lazy outcome —
// no invoice could exist — answered with an empty page, not an error.
func (s *Service) ListUnpaidInvoices(ctx context.Context, req ListUnpaidInvoicesRequest) (*ListUnpaidInvoicesResponse, error) {
	if err := validateOwner(req.OwnerUserID, req.OwnerOrgID); err != nil {
		return nil, err
	}

	accountID, found, err := s.invoiceOwnerAccount(ctx, req.OwnerUserID, req.OwnerOrgID)
	if err != nil {
		return nil, Internal("account lookup failed", err)
	}
	if !found {
		return &ListUnpaidInvoicesResponse{Invoices: []UnpaidInvoice{}}, nil
	}

	rows, err := s.store.ListUnpaidInvoices(ctx, accountID)
	if err != nil {
		return nil, Internal("list unpaid invoices failed", err)
	}
	resp := &ListUnpaidInvoicesResponse{
		Invoices: make([]UnpaidInvoice, 0, len(rows)),
		Count:    len(rows),
	}
	for _, r := range rows {
		resp.Invoices = append(resp.Invoices, UnpaidInvoice{
			InvoiceID:       r.ID.String(),
			Number:          r.Number,
			AmountDueMicros: r.AmountDueMicros,
			CreatedAt:       r.CreatedAt,
		})
		// Guard the sum: each amount is non-negative int64 micros, so a wrap
		// shows as the total going DOWN (the same cheap check the rollup's
		// period total uses).
		if resp.TotalMicros+r.AmountDueMicros < resp.TotalMicros {
			return nil, Internal("unpaid total overflows int64 micros", nil)
		}
		resp.TotalMicros += r.AmountDueMicros
	}
	return resp, nil
}

// PayInvoiceRequest is the payload of PayInvoice: the owner principal
// (exactly one of OwnerUserID / OwnerOrgID) plus the MIRROR invoice id
// (ms_billing.invoices.id — the invoice_id ListUnpaidInvoices returned,
// never a Stripe in_… id).
type PayInvoiceRequest struct {
	OwnerUserID uuid.UUID `json:"owner_user_id,omitempty"`
	OwnerOrgID  uuid.UUID `json:"owner_org_id,omitempty"`
	InvoiceID   uuid.UUID `json:"invoice_id"`
}

// PayInvoiceResponse reports Stripe's post-pay invoice state: "paid" when the
// charge settled synchronously, "pending" when Stripe accepted the pay but
// the payment is still processing (e.g. asynchronous payment methods). Either
// way the mirror row settles via the invoice webhook — the client refetches
// rather than trusting this echo as the mirror state.
type PayInvoiceResponse struct {
	Status string `json:"status"`
}

// PayInvoice pays one unpaid Stripe invoice with the owner's default card:
//
//  1. resolve the owner's account and the mirror row scoped to it — a foreign
//     or unknown invoice_id is NOT_FOUND (never leaking existence, matching
//     the payment-method ownership gates);
//  2. an already-'paid' mirror row short-circuits to {"status":"paid"} — the
//     retry-after-success path stays idempotent without a Stripe call;
//     any other non-payable state (draft/void) is INVALID_INPUT;
//  3. require a usable default card on the FUNDING account (the sponsor hop
//     for sponsor-funded orgs — Stripe collects from the invoice customer's
//     default PM, which lives there) — else PAYMENT_REQUIRED;
//  4. pay via Stripe under the deterministic idempotency key
//     "payinv-<mirror uuid>": a client retry replays the original attempt
//     instead of double-charging.
//
// The mirror row is NOT touched here — the invoice.paid webhook settles it
// (the webhook is the mirror's single status writer).
func (s *Service) PayInvoice(ctx context.Context, req PayInvoiceRequest) (*PayInvoiceResponse, error) {
	if err := validateOwner(req.OwnerUserID, req.OwnerOrgID); err != nil {
		return nil, err
	}
	if req.InvoiceID == uuid.Nil {
		return nil, InvalidInput("invoice_id required")
	}

	accountID, found, err := s.invoiceOwnerAccount(ctx, req.OwnerUserID, req.OwnerOrgID)
	if err != nil {
		return nil, Internal("account lookup failed", err)
	}
	if !found {
		// No account ⇒ the caller owns no invoices; indistinguishable from a
		// foreign id on purpose.
		return nil, NotFound("invoice not found")
	}

	target, found, err := s.store.InvoiceForPayment(ctx, req.InvoiceID, accountID)
	if err != nil {
		return nil, Internal("invoice lookup failed", err)
	}
	if !found {
		return nil, NotFound("invoice not found")
	}
	if target.Status == "paid" {
		return &PayInvoiceResponse{Status: "paid"}, nil
	}
	if target.Status != "open" && target.Status != "uncollectible" {
		return nil, InvalidInput("invoice is not payable")
	}

	fundingID, err := s.store.ChargeFundingAccount(ctx, accountID)
	if err != nil {
		return nil, Internal("funding account lookup failed", err)
	}
	hasPM, err := s.store.HasUsableDefaultPM(ctx, fundingID)
	if err != nil {
		return nil, Internal("usable PM check failed", err)
	}
	if !hasPM {
		return nil, PaymentRequired("no usable payment card on file; add a card before paying")
	}

	inv, err := s.stripe.PayInvoice(ctx, target.StripeInvoiceID, "payinv-"+req.InvoiceID.String())
	if err != nil {
		return nil, StripeError("pay invoice failed", err)
	}
	status := "pending"
	if inv.Status == "paid" {
		status = "paid"
	}
	return &PayInvoiceResponse{Status: status}, nil
}

// invoiceOwnerAccount resolves the account whose invoices the (user XOR org)
// principal owns: users by their own account row; orgs through the funding
// designation (the same resolution usage's invoice reads use — invoices are
// attributed to the org account, which only exists as a billable target once
// designated + activated).
func (s *Service) invoiceOwnerAccount(ctx context.Context, userID, orgID uuid.UUID) (uuid.UUID, bool, error) {
	if orgID != uuid.Nil {
		return s.store.ResolveOrgFundedAccount(ctx, orgID)
	}
	return s.store.AccountByUser(ctx, userID)
}
