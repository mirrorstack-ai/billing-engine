package billing

import (
	"context"
	"encoding/base64"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/mirrorstack-ai/billing-engine/internal/account/eligibility"
	billingstripe "github.com/mirrorstack-ai/billing-engine/internal/shared/stripe"
)

const (
	// Credit purchases and auto-top-ups use the same finance-owned bounds as
	// migration 048: $5.00 through $5,000.00, expressed in micro-dollars.
	MinCreditPurchaseMicros int64 = 5_000_000
	MaxCreditPurchaseMicros int64 = 5_000_000_000

	// DefaultCreditsLimitMicros is applied whenever credits mode is enabled and
	// the caller omits an explicit credit limit. $5.00 is intentionally small.
	DefaultCreditsLimitMicros int64 = 5_000_000

	DefaultCreditLedgerPageSize = 50
	MaxCreditLedgerPageSize     = 100

	microsPerCent int64 = 10_000
)

// GetCreditStanding returns the authoritative posted wallet snapshot and folds
// in the existing service eligibility verdict. The original card/charge gates
// keep priority; OUT_OF_CREDITS becomes the primary block only when those gates
// were otherwise eligible.
func (s *Service) GetCreditStanding(ctx context.Context, req GetCreditStandingRequest) (*GetCreditStandingResponse, error) {
	if err := validateCreditOwner(req.OwnerUserID, req.OwnerOrgID); err != nil {
		return nil, err
	}
	if !s.walletEnabled {
		return nil, Unavailable("credit wallet is not enabled")
	}

	status, err := s.GetServiceStatus(ctx, GetServiceStatusRequest{
		UserID: req.OwnerUserID,
		OrgID:  req.OwnerOrgID,
	})
	if err != nil {
		return nil, err
	}

	accountID, found, err := s.ownerAccount(ctx, req.OwnerUserID, req.OwnerOrgID)
	if err != nil {
		return nil, Internal("account lookup failed", err)
	}
	if !found {
		return &GetCreditStandingResponse{
			BillingMode: BillingModeStandard,
			Blocked:     status.Blocked,
			BlockReason: status.BlockReason,
		}, nil
	}

	standing, err := s.store.CreditStanding(ctx, accountID)
	if err != nil {
		return nil, Internal("credit standing lookup failed", err)
	}
	blocked := status.Blocked
	blockReason := status.BlockReason
	if creditLimitExhausted(standing) && !blocked {
		blocked = true
		blockReason = blockReasonForEligibility(eligibility.ReasonOutOfCredits)
	}

	return &GetCreditStandingResponse{
		BillingMode:       standing.BillingMode,
		BalanceMicros:     standing.BalanceMicros,
		CreditLimitMicros: standing.CreditLimitMicros,
		AutoTopUp:         standing.AutoTopUp,
		Blocked:           blocked,
		BlockReason:       blockReason,
	}, nil
}

// ListCreditLedger returns a stable newest-first keyset page. Missing accounts
// are a normal lazy-wallet state and return an empty (non-nil) page.
func (s *Service) ListCreditLedger(ctx context.Context, req ListCreditLedgerRequest) (*ListCreditLedgerResponse, error) {
	if err := validateCreditOwner(req.OwnerUserID, req.OwnerOrgID); err != nil {
		return nil, err
	}
	if req.Limit < 0 {
		return nil, InvalidInput("limit must be non-negative")
	}
	limit := req.Limit
	if limit == 0 {
		limit = DefaultCreditLedgerPageSize
	}
	if limit > MaxCreditLedgerPageSize {
		limit = MaxCreditLedgerPageSize
	}

	var cursor *CreditLedgerCursor
	if req.Cursor != "" {
		decoded, err := decodeCreditLedgerCursor(req.Cursor)
		if err != nil {
			return nil, InvalidInput("invalid cursor")
		}
		cursor = &decoded
	}
	if !s.walletEnabled {
		return nil, Unavailable("credit wallet is not enabled")
	}

	accountID, found, err := s.ownerAccount(ctx, req.OwnerUserID, req.OwnerOrgID)
	if err != nil {
		return nil, Internal("account lookup failed", err)
	}
	if !found {
		return &ListCreditLedgerResponse{Entries: []CreditLedgerEntry{}}, nil
	}

	entries, err := s.store.ListCreditLedger(ctx, accountID, int32(limit+1), cursor)
	if err != nil {
		return nil, Internal("list credit ledger failed", err)
	}
	hasMore := len(entries) > limit
	if hasMore {
		entries = entries[:limit]
	}
	if entries == nil {
		entries = []CreditLedgerEntry{}
	}

	nextCursor := ""
	if hasMore {
		last := entries[len(entries)-1]
		lastID, err := uuid.Parse(last.ID)
		if err != nil {
			return nil, Internal("credit ledger returned an invalid id", err)
		}
		nextCursor = encodeCreditLedgerCursor(last.CreatedAt, lastID)
	}
	return &ListCreditLedgerResponse{Entries: entries, NextCursor: nextCursor}, nil
}

// StartCreditPurchase creates (or recovers) one pending purchase ledger row,
// then drives Stripe's draft→pinned item→finalize sequence under deterministic
// idempotency keys. The invoice id is attached before finalization so a retry
// can always recover the money-moving resource.
func (s *Service) StartCreditPurchase(ctx context.Context, req StartCreditPurchaseRequest) (*StartCreditPurchaseResponse, error) {
	if err := validateCreditOwner(req.OwnerUserID, req.OwnerOrgID); err != nil {
		return nil, err
	}
	if req.AmountMicros < MinCreditPurchaseMicros || req.AmountMicros > MaxCreditPurchaseMicros {
		return nil, InvalidInput("amount_micros must be between 5000000 and 5000000000")
	}
	if strings.TrimSpace(req.IdempotencyKey) == "" {
		return nil, InvalidInput("idempotency_key required")
	}
	if !s.walletEnabled {
		return nil, Unavailable("credit wallet is not enabled")
	}

	accountID, found, err := s.ownerAccount(ctx, req.OwnerUserID, req.OwnerOrgID)
	if err != nil {
		return nil, Internal("account lookup failed", err)
	}
	if !found {
		return nil, PaymentRequired("billing account required before purchasing credits")
	}

	if existing, found, err := s.store.CreditLedgerByIdempotencyKey(ctx, req.IdempotencyKey); err != nil {
		return nil, Internal("credit purchase idempotency lookup failed", err)
	} else if found {
		if err := validateExistingPurchase(existing, accountID, req.AmountMicros); err != nil {
			return nil, err
		}
		return s.resumeCreditPurchase(ctx, creditPurchaseFromRecord(existing))
	}

	purchase, err := s.store.CreatePendingCreditPurchase(ctx, accountID, req.AmountMicros, req.IdempotencyKey)
	if err != nil {
		// A concurrent retry may have won the global unique key after our read.
		// Re-read and accept only the exact same semantic operation.
		existing, found, lookupErr := s.store.CreditLedgerByIdempotencyKey(ctx, req.IdempotencyKey)
		if lookupErr == nil && found {
			if validationErr := validateExistingPurchase(existing, accountID, req.AmountMicros); validationErr != nil {
				return nil, validationErr
			}
			return s.resumeCreditPurchase(ctx, creditPurchaseFromRecord(existing))
		}
		return nil, Internal("create pending credit purchase failed", err)
	}
	return s.resumeCreditPurchase(ctx, purchase)
}

// resumeCreditPurchase is the crash/retry spine shared by fresh starts and
// idempotent replays. Stripe resource-level state decides which steps remain.
func (s *Service) resumeCreditPurchase(ctx context.Context, purchase CreditPurchaseRow) (*StartCreditPurchaseResponse, error) {
	if purchase.Status == "failed" && purchase.StripeInvoiceID == "" {
		return creditPurchaseStartResponse(purchase, billingstripe.Invoice{}), nil
	}

	ref := "credit-purchase:" + purchase.ID.String()
	invoice := billingstripe.Invoice{}
	var err error
	if purchase.StripeInvoiceID != "" {
		invoice, err = s.stripe.GetInvoice(ctx, purchase.StripeInvoiceID)
		if err != nil {
			return nil, StripeError("retrieve credit purchase invoice failed", err)
		}
	} else {
		fundingAccountID, err := s.store.ChargeFundingAccount(ctx, purchase.AccountID)
		if err != nil {
			return nil, Internal("funding account lookup failed", err)
		}
		stripeCustomerID, err := s.store.AccountStripeCustomer(ctx, fundingAccountID)
		if err != nil {
			return nil, Internal("stripe customer lookup failed", err)
		}
		if stripeCustomerID == "" {
			return nil, PaymentRequired("Stripe customer required before purchasing credits")
		}

		var found bool
		invoice, found, err = s.stripe.FindInvoiceByRef(ctx, stripeCustomerID, ref)
		if err != nil {
			return nil, StripeError("recover credit purchase invoice failed", err)
		}
		if !found {
			invoice, err = s.stripe.CreateDraftInvoice(ctx, stripeCustomerID, ref, "credit-inv:"+purchase.ID.String())
			if err != nil {
				return nil, StripeError("create credit purchase invoice failed", err)
			}
		}
		if err := s.store.AttachCreditPurchaseInvoice(ctx, purchase.ID, purchase.AccountID, invoice.ID, invoice.HostedInvoiceURL); err != nil {
			return nil, Internal("attach credit purchase invoice failed", err)
		}
		purchase.StripeInvoiceID = invoice.ID
	}

	if invoice.Status == "draft" || invoice.Status == "" {
		amountCents := microsToCentsRoundHalfUp(purchase.AmountMicros)
		if _, err := s.stripe.CreateInvoiceItem(
			ctx,
			invoice.CustomerID,
			invoice.ID,
			amountCents,
			"usd",
			"MirrorStack credit purchase",
			billingstripe.LinePeriod{},
			"credit-item:"+purchase.ID.String(),
		); err != nil {
			return nil, StripeError("create credit purchase invoice item failed", err)
		}
		invoice, err = s.stripe.FinalizeInvoice(ctx, invoice.ID, "credit-fin:"+purchase.ID.String())
		if err != nil {
			return nil, StripeError("finalize credit purchase invoice failed", err)
		}
	}

	if invoice.ID != "" && purchase.Status == "pending" {
		if err := s.store.AttachCreditPurchaseInvoice(ctx, purchase.ID, purchase.AccountID, invoice.ID, invoice.HostedInvoiceURL); err != nil {
			return nil, Internal("enrich credit purchase invoice failed", err)
		}
	}
	if target := creditPurchaseStatusFromInvoice(invoice.Status); purchase.Status == "pending" && target != "pending" {
		if target == "settled" && invoice.AmountPaid != microsToCentsRoundHalfUp(purchase.AmountMicros) {
			return nil, Internal("credit purchase invoice paid amount does not match the ledger amount", nil)
		}
		purchase, err = s.store.FinalizeCreditPurchase(ctx, purchase.ID, purchase.AccountID, target, invoice.HostedInvoiceURL)
		if err != nil {
			return nil, Internal("finalize credit purchase ledger failed", err)
		}
	}
	return creditPurchaseStartResponse(purchase, invoice), nil
}

// FinishCreditPurchase polls Stripe's invoice and applies the one-way ledger
// transition. Ownership is enforced by the (purchase id, account id) lookup.
func (s *Service) FinishCreditPurchase(ctx context.Context, req FinishCreditPurchaseRequest) (*FinishCreditPurchaseResponse, error) {
	if err := validateCreditOwner(req.OwnerUserID, req.OwnerOrgID); err != nil {
		return nil, err
	}
	purchaseID, err := uuid.Parse(req.PurchaseID)
	if err != nil {
		return nil, InvalidInput("purchase_id must be a UUID")
	}
	if !s.walletEnabled {
		return nil, Unavailable("credit wallet is not enabled")
	}
	accountID, found, err := s.ownerAccount(ctx, req.OwnerUserID, req.OwnerOrgID)
	if err != nil {
		return nil, Internal("account lookup failed", err)
	}
	if !found {
		return nil, NotFound("credit purchase not found")
	}
	purchase, found, err := s.store.CreditPurchase(ctx, purchaseID, accountID)
	if err != nil {
		return nil, Internal("credit purchase lookup failed", err)
	}
	if !found {
		return nil, NotFound("credit purchase not found")
	}

	receiptURL := purchase.ReceiptURL
	if purchase.Status == "pending" && purchase.StripeInvoiceID != "" {
		invoice, err := s.stripe.GetInvoice(ctx, purchase.StripeInvoiceID)
		if err != nil {
			return nil, StripeError("retrieve credit purchase invoice failed", err)
		}
		if invoice.HostedInvoiceURL != "" {
			receiptURL = invoice.HostedInvoiceURL
		}
		target := creditPurchaseStatusFromInvoice(invoice.Status)
		if target == "settled" && invoice.AmountPaid != microsToCentsRoundHalfUp(purchase.AmountMicros) {
			return nil, Internal("credit purchase invoice paid amount does not match the ledger amount", nil)
		}
		if target != "pending" {
			purchase, err = s.store.FinalizeCreditPurchase(ctx, purchase.ID, purchase.AccountID, target, receiptURL)
			if err != nil {
				return nil, Internal("finalize credit purchase ledger failed", err)
			}
		} else if err := s.store.AttachCreditPurchaseInvoice(ctx, purchase.ID, purchase.AccountID, invoice.ID, receiptURL); err != nil {
			return nil, Internal("enrich credit purchase invoice failed", err)
		}
	}

	standing, err := s.store.CreditStanding(ctx, accountID)
	if err != nil {
		return nil, Internal("credit balance lookup failed", err)
	}
	return &FinishCreditPurchaseResponse{
		Status:        purchase.Status,
		BalanceMicros: standing.BalanceMicros,
		ReceiptURL:    receiptURL,
	}, nil
}

// SetAutoTopUp validates bounds and payment-method ownership before persisting
// the configuration. Disabling with amount=0 retains the schema's $5 default.
func (s *Service) SetAutoTopUp(ctx context.Context, req SetAutoTopUpRequest) (*SetAutoTopUpResponse, error) {
	if err := validateCreditOwner(req.OwnerUserID, req.OwnerOrgID); err != nil {
		return nil, err
	}
	if req.ThresholdMicros < 0 {
		return nil, InvalidInput("threshold_micros must be non-negative")
	}
	amount := req.AmountMicros
	if amount == 0 && !req.Enabled {
		amount = MinCreditPurchaseMicros
	}
	if amount < MinCreditPurchaseMicros || amount > MaxCreditPurchaseMicros {
		return nil, InvalidInput("amount_micros must be between 5000000 and 5000000000")
	}
	if req.Enabled && strings.TrimSpace(req.PaymentMethodID) == "" {
		return nil, InvalidInput("payment_method_id required when auto top-up is enabled")
	}
	var paymentMethodID uuid.UUID
	if req.PaymentMethodID != "" {
		var err error
		paymentMethodID, err = uuid.Parse(req.PaymentMethodID)
		if err != nil {
			return nil, InvalidInput("payment_method_id must be a UUID")
		}
	}
	if !s.walletEnabled {
		return nil, Unavailable("credit wallet is not enabled")
	}

	accountID, found, err := s.ownerAccount(ctx, req.OwnerUserID, req.OwnerOrgID)
	if err != nil {
		return nil, Internal("account lookup failed", err)
	}
	if !found {
		return nil, NotFound("billing account not found")
	}
	if req.PaymentMethodID != "" {
		_, _, _, owned, err := s.paymentMethodTarget(ctx, req.OwnerUserID, req.OwnerOrgID, paymentMethodID)
		if err != nil {
			return nil, Internal("payment method lookup failed", err)
		}
		if !owned {
			return nil, NotFound("payment method not found")
		}
	}

	config, err := s.store.UpsertCreditAutoTopUp(ctx, accountID, AutoTopUpConfig{
		Enabled:         req.Enabled,
		ThresholdMicros: req.ThresholdMicros,
		AmountMicros:    amount,
		PaymentMethodID: req.PaymentMethodID,
	})
	if err != nil {
		return nil, Internal("set credit auto top-up failed", err)
	}
	return &SetAutoTopUpResponse{AutoTopUp: config}, nil
}

// SetCustomerBillingMode supports a self-scoped personal path and a
// distributor-scoped org path. Distributor writes are rejected unless the
// existing org_billing_designations relationship resolves to that customer.
func (s *Service) SetCustomerBillingMode(ctx context.Context, req SetCustomerBillingModeRequest) (*SetCustomerBillingModeResponse, error) {
	if err := validateCreditOwner(req.OwnerUserID, req.OwnerOrgID); err != nil {
		return nil, err
	}
	if req.BillingMode != BillingModeStandard && req.BillingMode != BillingModeCredits {
		return nil, InvalidInput("billing_mode must be standard or credits")
	}
	if req.CreditLimitMicros != nil && *req.CreditLimitMicros < 0 {
		return nil, InvalidInput("credit_limit_micros must be non-negative")
	}
	if req.DistributorOrgID != uuid.Nil {
		if req.OwnerOrgID == uuid.Nil || req.OwnerUserID != uuid.Nil {
			return nil, InvalidInput("distributor calls require owner_org_id")
		}
	} else if req.OwnerUserID == uuid.Nil {
		return nil, InvalidInput("self-scoped billing mode changes require owner_user_id")
	}
	if !s.walletEnabled {
		return nil, Unavailable("credit wallet is not enabled")
	}

	var accountID uuid.UUID
	if req.DistributorOrgID != uuid.Nil {
		var found bool
		var err error
		accountID, found, err = s.store.DistributorCustomerAccount(ctx, req.DistributorOrgID, req.OwnerOrgID)
		if err != nil {
			return nil, Internal("distributor relationship lookup failed", err)
		}
		if !found {
			return nil, InvalidInput("distributor does not manage customer org")
		}
	} else {
		var err error
		accountID, _, err = s.ensureOwnerAccount(ctx, req.OwnerUserID, uuid.Nil)
		if err != nil {
			return nil, Internal("ensure billing account failed", err)
		}
	}

	standing, err := s.store.CreditStanding(ctx, accountID)
	if err != nil {
		return nil, Internal("credit standing lookup failed", err)
	}
	creditLimit := standing.CreditLimitMicros
	if req.CreditLimitMicros != nil {
		creditLimit = *req.CreditLimitMicros
	} else if req.BillingMode == BillingModeCredits {
		creditLimit = DefaultCreditsLimitMicros
	}
	if err := s.store.SetCreditBillingMode(ctx, accountID, req.BillingMode, creditLimit); err != nil {
		return nil, Internal("set customer billing mode failed", err)
	}
	return &SetCustomerBillingModeResponse{BillingMode: req.BillingMode}, nil
}

// ListDistributorCustomers returns one wallet snapshot per related customer.
func (s *Service) ListDistributorCustomers(ctx context.Context, req ListDistributorCustomersRequest) (*ListDistributorCustomersResponse, error) {
	if req.DistributorOrgID == uuid.Nil {
		return nil, InvalidInput("distributor_org_id required")
	}
	if !s.walletEnabled {
		return nil, Unavailable("credit wallet is not enabled")
	}
	states, err := s.store.ListDistributorCustomerStates(ctx, req.DistributorOrgID)
	if err != nil {
		return nil, Internal("list distributor customers failed", err)
	}
	customers := make([]DistributorCustomerRow, 0, len(states))
	for _, state := range states {
		customers = append(customers, DistributorCustomerRow{
			CustomerOrgID:     state.CustomerOrgID,
			BillingMode:       state.BillingMode,
			CreditLimitMicros: state.CreditLimitMicros,
			Standing:          distributorCreditStanding(state),
			AutoTopUpEnabled:  state.AutoTopUpEnabled,
			BalanceMicros:     state.BalanceMicros,
		})
	}
	return &ListDistributorCustomersResponse{Customers: customers}, nil
}

// GrantCredits appends an idempotent settled grant. Distributor actors must
// prove the designation relationship; system actors are deliberately scoped
// only to the requested customer account.
func (s *Service) GrantCredits(ctx context.Context, req GrantCreditsRequest) (*GrantCreditsResponse, error) {
	if req.CustomerOrgID == uuid.Nil {
		return nil, InvalidInput("customer_org_id required")
	}
	if req.AmountMicros <= 0 {
		return nil, InvalidInput("amount_micros must be positive")
	}
	if req.Actor != "distributor" && req.Actor != "system" {
		return nil, InvalidInput("actor must be distributor or system")
	}
	if strings.TrimSpace(req.IdempotencyKey) == "" {
		return nil, InvalidInput("idempotency_key required")
	}
	if req.ExpiresAt != nil && !req.ExpiresAt.After(time.Now()) {
		return nil, InvalidInput("expires_at must be in the future")
	}
	if req.Actor == "distributor" && req.DistributorOrgID == uuid.Nil {
		return nil, InvalidInput("distributor_org_id required for distributor grants")
	}
	if !s.walletEnabled {
		return nil, Unavailable("credit wallet is not enabled")
	}

	var accountID uuid.UUID
	if req.Actor == "distributor" {
		var found bool
		var err error
		accountID, found, err = s.store.DistributorCustomerAccount(ctx, req.DistributorOrgID, req.CustomerOrgID)
		if err != nil {
			return nil, Internal("distributor relationship lookup failed", err)
		}
		if !found {
			return nil, InvalidInput("distributor does not manage customer org")
		}
	} else {
		var found bool
		var err error
		accountID, found, err = s.store.AccountByOrg(ctx, req.CustomerOrgID)
		if err != nil {
			return nil, Internal("customer account lookup failed", err)
		}
		if !found {
			return nil, NotFound("customer billing account not found")
		}
	}

	if existing, found, err := s.store.CreditLedgerByIdempotencyKey(ctx, req.IdempotencyKey); err != nil {
		return nil, Internal("credit grant idempotency lookup failed", err)
	} else if found {
		if err := validateExistingGrant(existing, accountID, req); err != nil {
			return nil, err
		}
		return &GrantCreditsResponse{LedgerID: existing.ID.String(), BalanceMicros: existing.BalanceAfterMicros}, nil
	}

	grant, err := s.store.InsertCreditGrant(ctx, accountID, req.AmountMicros, req.Actor, req.IdempotencyKey, req.ExpiresAt)
	if err != nil {
		// Resolve a concurrent idempotent insert exactly as the purchase path does.
		existing, found, lookupErr := s.store.CreditLedgerByIdempotencyKey(ctx, req.IdempotencyKey)
		if lookupErr == nil && found {
			if validationErr := validateExistingGrant(existing, accountID, req); validationErr != nil {
				return nil, validationErr
			}
			return &GrantCreditsResponse{LedgerID: existing.ID.String(), BalanceMicros: existing.BalanceAfterMicros}, nil
		}
		return nil, Internal("insert credit grant failed", err)
	}
	return &GrantCreditsResponse{LedgerID: grant.ID.String(), BalanceMicros: grant.BalanceAfterMicros}, nil
}

func validateCreditOwner(userID, orgID uuid.UUID) error {
	if userID == uuid.Nil && orgID == uuid.Nil {
		return InvalidInput("owner_user_id or owner_org_id required")
	}
	if userID != uuid.Nil && orgID != uuid.Nil {
		return InvalidInput("owner_user_id and owner_org_id are mutually exclusive")
	}
	return nil
}

func creditLimitExhausted(standing CreditStandingRow) bool {
	return standing.BillingMode == BillingModeCredits && standing.BalanceMicros <= -standing.CreditLimitMicros
}

func blockReasonForEligibility(reason eligibility.Reason) string {
	return blockReason(reason)
}

func creditPurchaseStatusFromInvoice(status string) string {
	switch status {
	case "paid":
		return "settled"
	case "void", "uncollectible":
		return "failed"
	default:
		return "pending"
	}
}

func creditPurchaseStartResponse(purchase CreditPurchaseRow, invoice billingstripe.Invoice) *StartCreditPurchaseResponse {
	hostedURL := invoice.HostedInvoiceURL
	if hostedURL == "" {
		hostedURL = purchase.ReceiptURL
	}
	return &StartCreditPurchaseResponse{
		PurchaseID: purchase.ID.String(),
		Rail:       "stripe",
		Stripe: &StripePurchaseInit{
			ClientSecret:     invoice.ClientSecret,
			HostedInvoiceURL: hostedURL,
		},
	}
}

func validateExistingPurchase(existing CreditLedgerRecord, accountID uuid.UUID, amountMicros int64) error {
	if existing.Type != "purchase" || existing.AccountID != accountID || existing.AmountMicros != amountMicros || existing.Actor != "self" {
		return InvalidInput("idempotency_key is already used by a different credit operation")
	}
	return nil
}

func creditPurchaseFromRecord(record CreditLedgerRecord) CreditPurchaseRow {
	return CreditPurchaseRow{
		ID:                 record.ID,
		AccountID:          record.AccountID,
		AmountMicros:       record.AmountMicros,
		Type:               record.Type,
		Status:             record.Status,
		BalanceAfterMicros: record.BalanceAfterMicros,
		Actor:              record.Actor,
		IdempotencyKey:     record.IdempotencyKey,
		StripeInvoiceID:    record.StripeInvoiceID,
		ReceiptURL:         record.ReceiptURL,
		CreatedAt:          record.CreatedAt,
	}
}

func validateExistingGrant(existing CreditLedgerRecord, accountID uuid.UUID, req GrantCreditsRequest) error {
	if existing.Type != "grant" || existing.AccountID != accountID || existing.AmountMicros != req.AmountMicros || existing.Actor != req.Actor || !sameOptionalTime(existing.ExpiresAt, req.ExpiresAt) {
		return InvalidInput("idempotency_key is already used by a different credit operation")
	}
	return nil
}

func sameOptionalTime(a, b *time.Time) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	// PostgreSQL timestamptz is microsecond-precision. Normalize both sides so
	// an idempotent replay of a JSON timestamp carrying nanoseconds is not
	// mistaken for a semantically different grant after the DB round-trip.
	return a.UTC().Truncate(time.Microsecond).Equal(b.UTC().Truncate(time.Microsecond))
}

func distributorCreditStanding(state DistributorCustomerState) string {
	if state.BillingMode != BillingModeCredits {
		return "ok"
	}
	if state.BalanceMicros <= -state.CreditLimitMicros {
		return "blocked"
	}
	if state.BalanceMicros <= 0 || (state.AutoTopUpEnabled && state.BalanceMicros <= state.AutoTopUpThresholdMicros) {
		return "low"
	}
	return "ok"
}

func microsToCentsRoundHalfUp(micros int64) int64 {
	return (micros + microsPerCent/2) / microsPerCent
}

const creditLedgerCursorSeparator = "|"

func encodeCreditLedgerCursor(createdAt time.Time, id uuid.UUID) string {
	raw := createdAt.UTC().Format(time.RFC3339Nano) + creditLedgerCursorSeparator + id.String()
	return base64.RawURLEncoding.EncodeToString([]byte(raw))
}

func decodeCreditLedgerCursor(encoded string) (CreditLedgerCursor, error) {
	raw, err := base64.RawURLEncoding.DecodeString(encoded)
	if err != nil {
		return CreditLedgerCursor{}, fmt.Errorf("decode cursor: %w", err)
	}
	createdRaw, idRaw, ok := strings.Cut(string(raw), creditLedgerCursorSeparator)
	if !ok || createdRaw == "" || idRaw == "" || strings.Contains(idRaw, creditLedgerCursorSeparator) {
		return CreditLedgerCursor{}, fmt.Errorf("invalid cursor shape")
	}
	createdAt, err := time.Parse(time.RFC3339Nano, createdRaw)
	if err != nil {
		return CreditLedgerCursor{}, fmt.Errorf("parse cursor timestamp: %w", err)
	}
	id, err := uuid.Parse(idRaw)
	if err != nil {
		return CreditLedgerCursor{}, fmt.Errorf("parse cursor id: %w", err)
	}
	return CreditLedgerCursor{CreatedAt: createdAt, ID: id}, nil
}
