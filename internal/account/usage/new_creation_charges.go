package usage

import (
	"bytes"
	"context"
	"slices"
	"time"

	"github.com/google/uuid"

	"github.com/mirrorstack-ai/billing-engine/internal/account/billing"
)

// ============================================================================
// ListNewCreationCharges — the 本期新建立 ("new this period") read behind the
// web-account /me/billing (and /orgs/{slug}/billing) BillSummaryCard.
//
// It maps 1:1 to the CREATION-PRORATION leg (proration.go): a newly created app
// is NOT charged at RegisterApp; after surviving GraceDays the sweep mints ONE
// invoice for [creation-day, coverage-end), arms apps.proration_invoice_id
// (migration 027), and mirrors it in ms_billing.invoices. This read surfaces,
// for the resolved period, the apps CREATED in it whose base is charged via that
// leg:
//
//   - SETTLED: the proration already fired — the row carries the invoice's
//     ACTUAL settled total (which may include co-created over-module line items
//     on the SAME combined invoice, proration.go scenario 3), the invoice
//     number/id, and the invoice created_at as the "recorded at".
//   - PENDING: the app is still in its creation grace (uncharged, live,
//     un-skipped) — shown with a charge ETA (created_at + GraceDays) and the
//     PROJECTED accruing base fee (the flat plan base; the exact prorated
//     amount is the sweep's to mint, so this is an estimate like every other
//     un-invoiced line). Pending rows exist ONLY for the CURRENT live window —
//     a past period has no still-in-grace apps.
//   - PENDING ADD-ON: over-modules installed AFTER an app's creation carry
//     their own in-grace overage timers (migration 033, Leg 1) — surfaced as a
//     per-app pending row with base 0, the projected flat surcharge per timer,
//     and the earliest timer expiry as the ETA. Current live window only, like
//     the creation pendings.
//
// Read-only over the apps mirror + the invoices mirror this service already
// owns; NO Stripe round-trip, NO schema change.
// ============================================================================

// NewCreationChargeStatus classifies one NewCreationCharge row on the wire.
const (
	// NewCreationChargeStatusSettled: the creation-proration invoice has fired; the
	// row carries the invoice's real settled amount + number/id + recorded_at.
	NewCreationChargeStatusSettled = "settled"
	// NewCreationChargeStatusPending: the app is still in creation grace (uncharged);
	// the row carries a charge_eta and its PROJECTED (accruing) base fee.
	NewCreationChargeStatusPending = "pending"
)

// ListNewCreationChargesRequest is the payload of ListNewCreationCharges: the account
// OWNER principal (the payer — exactly one of OwnerUserID / OwnerOrgID, mirroring
// GetAccountBillRequest) plus an OPTIONAL period reference. "" / omitted PeriodID
// selects the account's CURRENT anchored window (the only window that can carry
// pending rows); a non-empty value must be a billing_periods id belonging to
// this account (resolved the SAME way GetAccountBill resolves it).
type ListNewCreationChargesRequest struct {
	OwnerUserID uuid.UUID `json:"owner_user_id,omitempty"`
	OwnerOrgID  uuid.UUID `json:"owner_org_id,omitempty"`
	// PeriodID is the account-scoped selector value, forwarded verbatim by the
	// account proxy — a STRING because the current entry's id IS "" on the wire
	// (BillingPeriodRef), exactly like GetAccountBillRequest.PeriodID.
	PeriodID string `json:"period_id,omitempty"`
}

// NewCreationCharge is one 本期新建立 row: an app created in the resolved period whose
// base is (or will be) charged via the creation-proration leg. For a SETTLED row
// AmountMicros is the invoice's actual settled total, RecordedAt is the invoice
// created_at, and InvoiceID is the invoice Number (else the mirror UUID); for a
// PENDING row AmountMicros/BaseFeeMicros is the PROJECTED base fee it will
// accrue (the whole cycle bills at period close), ChargeETA is created_at +
// GraceDays, and RecordedAt/InvoiceID are absent. Money is integer micro-USD.
//
// The per-component BREAKDOWN lets the UI render "App · <Name> · 基礎費用" and
// "App · <Name> · <AddonModuleCount> 加購模組": Name is the app's display name
// ("" when unknown); AddonModuleCount is max(0, created_module_count −
// IncludedModules), the count of add-on modules beyond the bundled allowance;
// BaseFeeMicros + AddonMicros partition AmountMicros for a settled row
// (BaseFeeMicros is the settled creation base, AddonMicros the co-created
// over-module component on the same invoice). A pending CREATION row carries
// the projected flat base in AmountMicros/BaseFeeMicros and AddonMicros 0
// (the co-created overage is not projected — only its COUNT surfaces); a
// pending ADD-ON row (post-creation installs) is the inverse: BaseFeeMicros 0,
// AmountMicros/AddonMicros the projected flat surcharge × AddonModuleCount.
// The UI derives the descriptor from the partition: base only, base + N
// add-ons, or N add-ons only.
type NewCreationCharge struct {
	AppID            uuid.UUID  `json:"app_id"`
	Status           string     `json:"status"`
	AmountMicros     int64      `json:"amount_micros"`
	RecordedAt       *time.Time `json:"recorded_at,omitempty"`
	InvoiceID        string     `json:"invoice_id,omitempty"`
	ChargeETA        *time.Time `json:"charge_eta,omitempty"`
	Name             string     `json:"name"`
	BaseFeeMicros    int64      `json:"base_fee_micros"`
	AddonModuleCount int        `json:"addon_module_count"`
	AddonMicros      int64      `json:"addon_micros"`
}

// addonModuleCount is the count of CHARGED add-on modules for an app: those
// installed beyond the account's bundled IncludedModules allowance. Reuses the
// single IncludedModules const (bill.go) rather than hardcoding the threshold.
func addonModuleCount(createdModuleCount int) int {
	if n := createdModuleCount - IncludedModules; n > 0 {
		return n
	}
	return 0
}

// ListNewCreationChargesResponse is the ordered 本期新建立 list: settled rows first
// (newest-first by recorded_at), then pending rows (soonest-first by charge_eta).
// Charges is an empty slice (never nil) when the account has no new apps in the
// window.
type ListNewCreationChargesResponse struct {
	Charges []NewCreationCharge `json:"charges"`
}

// ListNewCreationCharges returns the account-owner's 本期新建立 list for ONE period. It:
//  1. validates the owner principal (exactly one of user/org) and resolves the
//     payer's billing account,
//  2. resolves the billed window via resolveBillPeriod — the SAME resolution
//     GetAccountBill uses ("" → the current anchored window; a real
//     billing_periods id → that frozen window, account-scoped else NOT_FOUND),
//  3. lazy account (no billing account row): an EMPTY list — no app could have
//     been charged yet, the same posture ListInvoices takes,
//  4. reads the SETTLED rows (apps created in the window with an armed
//     proration guard, joined to the invoice mirror) — newest-first from SQL,
//  5. for the CURRENT live window ONLY (resolved period id == ""), reads the
//     PENDING rows (still-in-grace apps) and appends them with a charge ETA
//     (created_at + GraceDays) and amount 0; a past period skips this entirely.
func (s *Service) ListNewCreationCharges(ctx context.Context, req ListNewCreationChargesRequest) (*ListNewCreationChargesResponse, error) {
	if req.OwnerUserID == uuid.Nil && req.OwnerOrgID == uuid.Nil {
		return nil, billing.InvalidInput("owner_user_id or owner_org_id required")
	}
	if req.OwnerUserID != uuid.Nil && req.OwnerOrgID != uuid.Nil {
		return nil, billing.InvalidInput("owner_user_id and owner_org_id are mutually exclusive")
	}
	// "" (or omitted) means the current window; a non-empty value must be a
	// billing_periods id — malformed is INVALID_INPUT here, unknown/foreign is
	// NOT_FOUND in resolveBillPeriod, exactly like GetAccountBill.
	periodRef := uuid.Nil
	if req.PeriodID != "" {
		id, err := uuid.Parse(req.PeriodID)
		if err != nil {
			return nil, billing.InvalidInput("period_id must be a billing period id (empty for the current period)")
		}
		periodRef = id
	}

	owner := Owner{UserID: req.OwnerUserID, OrgID: req.OwnerOrgID}
	accountID, found, err := s.store.AccountByOwner(ctx, owner)
	if err != nil {
		return nil, billing.Internal("account lookup failed", err)
	}

	periodID, periodStart, periodEnd, err := s.resolveBillPeriod(ctx, accountID, found, periodRef)
	if err != nil {
		return nil, err
	}
	if !found {
		// Lazy / never-activated account: no app could have been charged yet.
		return &ListNewCreationChargesResponse{Charges: []NewCreationCharge{}}, nil
	}

	settled, err := s.store.SettledNewCreationCharges(ctx, accountID, periodStart, periodEnd)
	if err != nil {
		return nil, billing.Internal("settled new-app charges query failed", err)
	}

	charges := make([]NewCreationCharge, 0, len(settled))
	for _, r := range settled {
		// invoice_id = the customer-facing Number when enriched, else the mirror
		// UUID (a stable identity for a row not yet number-enriched by a webhook).
		invoiceID := r.Number
		if invoiceID == "" {
			invoiceID = r.InvoiceID.String()
		}
		recordedAt := r.RecordedAt
		// base + add-on partition the invoice total: BaseFeeMicros is the settled
		// creation base (the 'proration' snapshot; 0 when absent), AddonMicros the
		// co-created over-module component billed on the SAME invoice. By
		// construction base + addon == AmountMicros (the contract invariant).
		charges = append(charges, NewCreationCharge{
			AppID:            r.AppID,
			Status:           NewCreationChargeStatusSettled,
			AmountMicros:     r.AmountDueMicros,
			RecordedAt:       &recordedAt,
			InvoiceID:        invoiceID,
			Name:             r.Name,
			BaseFeeMicros:    r.BaseMicros,
			AddonModuleCount: addonModuleCount(r.CreatedModuleCount),
			AddonMicros:      r.AmountDueMicros - r.BaseMicros,
		})
	}

	// PENDING rows exist only in the CURRENT live window (resolved id == ""): a
	// past period has no still-in-grace apps or in-grace install timers.
	// graceCutoff = now − GraceDays, mirroring SweepCreationProrations'
	// createdBefore from the other side, so an app is "in grace" here iff it
	// has NOT yet elapsed grace there.
	if periodID == "" {
		now := s.nowFn().UTC()
		graceCutoff := now.AddDate(0, 0, -GraceDays)
		pending, err := s.store.PendingNewCreationCharges(ctx, accountID, periodStart, periodEnd, graceCutoff)
		if err != nil {
			return nil, billing.Internal("pending new-app charges query failed", err)
		}
		pendingStart := len(charges)
		for _, r := range pending {
			eta := r.CreatedAt.AddDate(0, 0, GraceDays)
			charges = append(charges, NewCreationCharge{
				AppID:  r.AppID,
				Status: NewCreationChargeStatusPending,
				// PROJECTED base fee: the whole cycle bills at period close, so a
				// still-in-grace app shows its accruing base fee (BaseFeeMicros),
				// not 0 — it is an unpaid estimate, like every other line. Base
				// only; the add-on overage is not projected here (its COUNT is
				// still surfaced from the frozen registration count).
				AmountMicros:     BaseFeeMicros,
				ChargeETA:        &eta,
				Name:             r.Name,
				BaseFeeMicros:    BaseFeeMicros,
				AddonModuleCount: addonModuleCount(r.CreatedModuleCount),
				AddonMicros:      0,
			})
		}
		// Pending ADD-ON rows: over-modules installed AFTER creation (never
		// co-created — those ride the app's own pending row above) whose own
		// grace timers (migration 033) have not yet elapsed. One row per app,
		// PROJECTED at the steady flat surcharge per timer ($3 each — Leg 1's
		// exact proration is the sweep's to mint), base 0, ETA = the earliest
		// timer expiry. An app past ITS creation grace can still surface here —
		// installing a 6th+ module later mints a new upcoming charge.
		addons, err := s.store.PendingAddonModuleCharges(ctx, accountID, IncludedModules, now)
		if err != nil {
			return nil, billing.Internal("pending add-on module charges query failed", err)
		}
		for _, r := range addons {
			eta := r.ChargeETA
			amount := ModuleOverageFeeMicros * int64(r.AddonCount)
			charges = append(charges, NewCreationCharge{
				AppID:            r.AppID,
				Status:           NewCreationChargeStatusPending,
				AmountMicros:     amount,
				ChargeETA:        &eta,
				Name:             r.Name,
				BaseFeeMicros:    0,
				AddonModuleCount: r.AddonCount,
				AddonMicros:      amount,
			})
		}
		// Keep the pending tail soonest-first ACROSS both sources (creation +
		// add-on), app_id tie-break — the contract the response documents.
		slices.SortStableFunc(charges[pendingStart:], func(a, b NewCreationCharge) int {
			if c := a.ChargeETA.Compare(*b.ChargeETA); c != 0 {
				return c
			}
			return bytes.Compare(a.AppID[:], b.AppID[:])
		})
	}

	return &ListNewCreationChargesResponse{Charges: charges}, nil
}
