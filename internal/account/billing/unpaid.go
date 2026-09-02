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
// blocks on (open/uncollectible mirror rows with amount_due > 0).
//
// PayInvoice used to collect: it paid ONE open Stripe invoice with the owner's
// default card, through an UNKEYED Invoices.Pay. That collector is DELETED.
// PayInvoice now proposes a receivable against the intent the rail raised the
// invoice from and moves no money at all, so nothing in this file reaches the
// provider. The webhook remains the policy writer for relax/notify and
// ever_failed, and still settles the mirror however an invoice gets paid.
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

// PayInvoiceResponse reports what happened to the unpaid row: "proposed" when
// a receivable was sealed for the remainder (the only outcome that advances
// anything — the obligation is recorded and NO money moved), and "paid" for
// the idempotent echo of a mirror row the webhook already settled.
//
// "pending" is gone with the collector that produced it: nothing here asks a
// provider to charge a card any more, so there is no in-flight payment to
// report.
type PayInvoiceResponse struct {
	Status string `json:"status"`
	// IntentDigest is the receivable this retry was sealed as. Omitted from
	// the wire on the "paid" echo, where nothing was sealed, so an existing
	// client sees exactly the response it saw before.
	IntentDigest string `json:"intent_digest,omitempty"`
}

// PayInvoice records what the owner still owes on one unpaid invoice as a
// sealed receivable:
//
//  1. resolve the owner's account and the mirror row scoped to it — a foreign
//     or unknown invoice_id is NOT_FOUND (never leaking existence, matching
//     the payment-method ownership gates);
//  2. an already-'paid' mirror row short-circuits to {"status":"paid"} — the
//     retry-after-success path stays idempotent, and needs no proposer since
//     there is nothing left to collect; any other non-payable state
//     (draft/void) is INVALID_INPUT;
//  3. propose a receivable for the remainder, linked to the intent the rail
//     raised this invoice from.
//
// The funding-account hop, the usable-card gate, the frozen-customer
// coherence read and the Invoices.Pay call that used to follow are DELETED.
// They are not missing gates: every one of them existed to choose a Stripe
// customer and a card for a collection this leg no longer performs, which is
// why the proposal already sat in front of them before the cutover. A
// proposal moves no money, and the payer is resolved by the proposer from the
// account rather than chosen here.
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

	// 🔴 THIS LEG IS CUT OVER. There is no second branch.
	//
	// The proposer is no longer optional, and this is NOT the old
	// fall-through nil check: there is nothing left to fall through to. An
	// unarmed service cannot answer this RPC at all, and has to say so rather
	// than nil-panic at the seal. It is checked HERE rather than at the top
	// of the function so the two answers that need no collector — the
	// ownership NOT_FOUND and the already-'paid' echo — keep working
	// unchanged on a service that was never armed.
	if s.proposer == nil {
		return nil, Internal("PayInvoice requires an intent proposer: the legacy collect path is deleted", nil)
	}
	sourceDigest, found, err := s.proposer.SourceIntentFor(ctx, target.StripeInvoiceID)
	if err != nil {
		return nil, Internal("source intent lookup failed", err)
	}
	if !found {
		// 🔴 §6's collect_receivable is CollectRemainderOf(source): it links
		// to a SOURCE INTENT and collects what is left of it, so an invoice
		// the rail never raised has nothing to collect the remainder OF.
		//
		// This deliberately does NOT keep a provider path, and it is not the
		// in-flight case the other legs keep one for. Such an invoice is at
		// REST, not mid-flight: finalized, open or uncollectible, amount_due
		// > 0, and mirrored. Nothing is ambiguous about whether money moved —
		// it demonstrably did not — so refusing strands no charge that nobody
		// can prove. The debt also stays payable without us: the finalization
		// webhook stores hosted_invoice_url (migration 026) and invoice.paid
		// settles the mirror however the customer pays.
		//
		// Both alternatives are worse than refusing. Proposing with an empty
		// source would fabricate the link §6 requires. Keeping Invoices.Pay
		// for these rows would carry the one UNKEYED collector in the tree
		// past the cutover — the exact property the intent path's keyed,
		// instrument-named collect exists to remove — on a set that is only
		// finite because no leg raises invoices outside the rail any more.
		//
		// The set therefore DRAINS: once every leg raises through the rail, a
		// settlement records which provider object it moved through
		// (migration 069) and SourceIntentFor finds it. What is left is the
		// pre-rail backlog, and its size is a production QUESTION, not a code
		// one — scripts/legacy-drop-preconditions.sql check 7 counts it and
		// deliberately answers REVIEW rather than READY, because each of those
		// rows needs a replacement intent before this refusal is harmless.
		return nil, InvalidInput("invoice predates the receivable rail and cannot be retried here — pay it from the invoice link, or contact support")
	}
	digest, err := s.proposer.ProposeReceivable(
		ctx, sourceDigest, accountID.String(), target.AmountDueMicros)
	if err != nil {
		return nil, Internal("receivable proposal failed", err)
	}
	// Not "paid" and not "pending". The obligation is recorded and
	// nothing was collected; reporting either would claim a payment
	// attempt that did not happen.
	return &PayInvoiceResponse{Status: "proposed", IntentDigest: digest}, nil
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
