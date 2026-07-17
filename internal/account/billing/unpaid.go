package billing

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/google/uuid"
	stripego "github.com/stripe/stripe-go/v85"

	billingstripe "github.com/mirrorstack-ai/billing-engine/internal/shared/stripe"
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
// PayInvoice pays ONE open Stripe invoice with the owner's default card and
// applies Stripe's returned post-pay snapshot through the same monotonic guard
// the webhook uses. The webhook remains the policy writer for relax/notify and
// ever_failed; its later status re-apply is idempotent.
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
// way the returned snapshot is synchronously applied to the mirror; on the
// paid path, the client's first refetch can observe the settled state.
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
//  4. verify the Stripe invoice's customer IS the pay-time funding account's
//     customer — the invoice's payer was frozen at creation, so an org
//     funding-designation switch since then would otherwise charge the
//     PREVIOUS funding account's card behind a gate that checked the new
//     one; a mismatch is INVALID_INPUT, never a silent stale charge;
//  5. pay via Stripe with NO idempotency key: Stripe replays a keyed
//     response — a decline included — for ~24h, which would dead-end the
//     fix-card-then-retry recovery this RPC exists for. Double-pay safety is
//     resource-level instead: the mirror 'paid' short-circuit (step 2)
//     absorbs the client double-tap, and a concurrent double-submit loses to
//     Stripe's invoice_already_paid, absorbed as {"status":"paid"}. Card
//     declines map to PAYMENT_REQUIRED (a 402 the UI renders as a payment
//     problem), never STRIPE_ERROR (a 502 that reads as a Stripe outage).
//
// After Stripe succeeds, its returned snapshot is applied synchronously through
// the webhook's monotonic status guard. A later invoice.paid webhook re-apply is
// idempotent and still owns policy side effects (relax/notify/ever_failed).
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

	// Gate/charge coherence: Stripe collects from the INVOICE's customer,
	// frozen at invoice creation — while the gates above checked the owner's
	// CURRENT funding account. After an org funding-designation switch the
	// two diverge, and paying would charge the previous funding account's
	// card. The mirror doesn't carry the invoice's customer, so read it from
	// Stripe and compare before any money moves.
	fundingCustomer, err := s.store.AccountStripeCustomer(ctx, fundingID)
	if err != nil {
		return nil, Internal("funding customer lookup failed", err)
	}
	stripeInv, err := s.stripe.GetInvoice(ctx, target.StripeInvoiceID)
	if err != nil {
		return nil, StripeError("invoice lookup failed", err)
	}
	if fundingCustomer == "" || stripeInv.CustomerID != fundingCustomer {
		return nil, InvalidInput("invoice belongs to a previous funding account — contact support")
	}

	// The mirror usable-PM gate can pass while Stripe's Customer has no actual
	// invoice-settings default; Invoices.Pay would then fail as a non-card
	// invalid request and surface as a misleading 502. Refuse deterministically
	// as PAYMENT_REQUIRED before any payment attempt.
	cust, err := s.stripe.GetCustomer(ctx, fundingCustomer)
	if err != nil {
		return nil, StripeError("customer lookup failed", err)
	}
	if cust.InvoiceSettings == nil || cust.InvoiceSettings.DefaultPaymentMethod == nil ||
		cust.InvoiceSettings.DefaultPaymentMethod.ID == "" {
		return nil, PaymentRequired("no default payment method on file; re-add or set a default card")
	}

	inv, err := s.stripe.PayInvoice(ctx, target.StripeInvoiceID)
	if err != nil {
		return s.payFailure(ctx, target.StripeInvoiceID, err)
	}
	// Settle the mirror NOW from Stripe's returned snapshot so the first
	// post-pay refetch reads 'paid' (core#162 — the webhook's later
	// invoice.paid re-apply is an idempotent no-op via the monotonic
	// guard's identical-re-apply branch; relax/notify still run there).
	s.syncPaidMirror(ctx, inv)
	status := "pending"
	if inv.Status == "paid" {
		status = "paid"
	}
	return &PayInvoiceResponse{Status: status}, nil
}

// syncPaidMirror applies a post-pay Stripe invoice snapshot onto the mirror
// through the webhook's monotonic status guard, so the first post-pay refetch
// reads the settled status instead of the still-open row's Failed badge
// (core#162 — the webhook's later invoice.paid re-apply is idempotent; it still
// owns policy side effects like relax/notify/ever_failed). Best-effort by
// contract: the money already moved, so a mirror miss must NEVER fail the RPC —
// the webhook settles the row seconds later regardless. Both a store error and a
// guard no-op (applied=false: no mirror row, or a transition the guard rejected)
// are logged, since on the pay path the row provably exists (InvoiceForPayment
// just read it) and 'paid' outranks open/uncollectible, so a no-op is unexpected.
func (s *Service) syncPaidMirror(ctx context.Context, inv billingstripe.Invoice) {
	switch applied, err := s.store.SyncInvoiceMirror(ctx, inv); {
	case err != nil:
		slog.WarnContext(ctx, "PayInvoice mirror sync failed; webhook will settle",
			"stripe_invoice_id", inv.ID, "error", err)
	case !applied:
		slog.WarnContext(ctx, "PayInvoice mirror sync not applied; webhook will settle",
			"stripe_invoice_id", inv.ID)
	}
}

// payFailure maps a failed Stripe Invoices.Pay to the RPC surface. Declines
// and off-session 3DS challenges are the USER's card problem — mapped to
// PAYMENT_REQUIRED (402, which web-account's pay path renders as a payment
// problem), never STRIPE_ERROR (502, indistinguishable from a Stripe outage).
// The invoice_already_paid loser — a concurrent double-submit, OR an invoice
// settled out-of-band during the webhook-lag window (the hosted invoice page one
// click from the same row, a second org payer, the Stripe dashboard) — is
// absorbed as the same {"status":"paid"} echo the winner got, not an error.
func (s *Service) payFailure(ctx context.Context, stripeInvoiceID string, err error) (*PayInvoiceResponse, error) {
	var se *stripego.Error
	if !errors.As(err, &se) {
		return nil, StripeError("pay invoice failed", err)
	}
	// Not in v85's generated ErrorCode enum; match Stripe's wire string.
	if se.Code == "invoice_already_paid" {
		// The money is in, but THIS request never ran the success-path sync, so
		// the mirror can still be open+ever_failed → the first post-pay refetch
		// renders Failed under the success snackbar (core#162). Re-read the
		// now-settled snapshot and sync it before echoing paid; best-effort like
		// the success path (the pure double-submit case is already settled by the
		// winner, so this re-apply is an idempotent no-op there).
		if inv, gerr := s.stripe.GetInvoice(ctx, stripeInvoiceID); gerr != nil {
			slog.WarnContext(ctx, "PayInvoice already-paid re-read failed; webhook will settle",
				"stripe_invoice_id", stripeInvoiceID, "error", gerr)
		} else {
			s.syncPaidMirror(ctx, inv)
		}
		return &PayInvoiceResponse{Status: "paid"}, nil
	}
	if se.Type == stripego.ErrorTypeCard || se.DeclineCode != "" {
		// Prefer the issuer's decline reason; fall back to the error code
		// (e.g. expired_card arrives without a decline_code).
		reason := string(se.DeclineCode)
		if reason == "" {
			reason = string(se.Code)
		}
		if reason == "" {
			reason = "card_declined"
		}
		return nil, PaymentRequired("card declined: " + reason)
	}
	if se.Code == stripego.ErrorCodeInvoicePaymentIntentRequiresAction {
		// Off-session 3DS challenge — the card needs the user, not Stripe.
		return nil, PaymentRequired("card declined: authentication_required")
	}
	return nil, StripeError("pay invoice failed", err)
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
