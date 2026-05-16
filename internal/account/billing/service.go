package billing

import (
	"context"

	"github.com/google/uuid"

	billingstripe "github.com/mirrorstack-ai/billing-engine/internal/shared/stripe"
)

// Service implements the v1 RPC catalog (Ensure, PrepareAddPaymentMethod,
// GetPaymentMethods). It composes a Store (Postgres) and a Stripe Client
// (real Stripe API); both are injected for testability.
type Service struct {
	store  Store
	stripe billingstripe.Client
}

// NewService wires a Service. Both dependencies are required;
// passing nil panics at the first call site.
func NewService(store Store, stripe billingstripe.Client) *Service {
	return &Service{store: store, stripe: stripe}
}

// Ensure is the read-only gate. Pure DB read; no Stripe API call;
// no DB write. Returns response.Missing populated when the user
// is not in a payable state.
//
// Note: a missing user row in ms_account.users is NOT detected here —
// billing-engine doesn't have authoritative read access to that table.
// The user_id is treated as opaque; if the caller passes a non-existent
// UUID, Ensure simply returns Missing: ["billing_account"]. api-platform
// is responsible for resolving the user before invoking Ensure.
func (s *Service) Ensure(ctx context.Context, req EnsureRequest) (*EnsureResponse, error) {
	if req.UserID == uuid.Nil {
		return nil, InvalidInput("user_id required")
	}

	resp := &EnsureResponse{Missing: []string{}}

	accountID, found, err := s.store.AccountByUser(ctx, req.UserID)
	if err != nil {
		return nil, Internal("account lookup failed", err)
	}
	if !found {
		resp.Missing = append(resp.Missing, MissingBillingAccount)
		return resp, nil
	}

	hasPM, err := s.store.HasUsablePaymentMethod(ctx, accountID)
	if err != nil {
		return nil, Internal("payment-method lookup failed", err)
	}
	if !hasPM {
		resp.Missing = append(resp.Missing, MissingPaymentMethod)
	}
	return resp, nil
}

// PrepareAddPaymentMethod implements the one-shot setup flow:
//  1. INSERT or SELECT the accounts row (idempotent on user_id).
//  2. Create the Stripe Customer if not yet present; record its ID.
//  3. Create a fresh SetupIntent and return its client_secret.
//
// Failure mode: if step 2 succeeds Stripe-side but step 2b
// (SetStripeCustomer) fails, an orphan Stripe Customer is created.
// Operational recovery via metadata lookup; not addressed in v1
// handler.
func (s *Service) PrepareAddPaymentMethod(ctx context.Context, req PrepareAddPaymentMethodRequest) (*PrepareAddPaymentMethodResponse, error) {
	if req.UserID == uuid.Nil {
		return nil, InvalidInput("user_id required")
	}

	accountID, stripeCustomerID, err := s.store.EnsureAccount(ctx, req.UserID)
	if err != nil {
		return nil, Internal("ensure account failed", err)
	}

	if stripeCustomerID == "" {
		// First paid action for this user — create the Stripe Customer.
		cust, err := s.stripe.CreateCustomer(ctx, accountID.String())
		if err != nil {
			return nil, StripeError("create customer failed", err)
		}
		stripeCustomerID = cust.ID
		if err := s.store.SetStripeCustomer(ctx, accountID, stripeCustomerID); err != nil {
			// Orphan Stripe Customer; see failure-mode note above. We
			// still return STRIPE_ERROR rather than INTERNAL because the
			// caller's retry path is clear (try again; idempotency on
			// (owner_kind, owner_user_id) covers the row, and metadata
			// reconciliation covers the Customer).
			return nil, StripeError("set stripe_customer_id failed", err)
		}
	}

	si, err := s.stripe.CreateSetupIntent(ctx, stripeCustomerID)
	if err != nil {
		return nil, StripeError("create setup intent failed", err)
	}
	return &PrepareAddPaymentMethodResponse{
		BillingAccountID:        accountID,
		SetupIntentClientSecret: si.ClientSecret,
	}, nil
}

// GetPaymentMethods returns the user's active payment methods.
// Returns an empty slice (not nil, not an error) when the user has
// no accounts row or no methods attached.
func (s *Service) GetPaymentMethods(ctx context.Context, req GetPaymentMethodsRequest) (*GetPaymentMethodsResponse, error) {
	if req.UserID == uuid.Nil {
		return nil, InvalidInput("user_id required")
	}

	accountID, found, err := s.store.AccountByUser(ctx, req.UserID)
	if err != nil {
		return nil, Internal("account lookup failed", err)
	}
	if !found {
		return &GetPaymentMethodsResponse{PaymentMethods: []PaymentMethod{}}, nil
	}

	methods, err := s.store.ListPaymentMethods(ctx, accountID)
	if err != nil {
		return nil, Internal("list payment methods failed", err)
	}
	return &GetPaymentMethodsResponse{PaymentMethods: methods}, nil
}
