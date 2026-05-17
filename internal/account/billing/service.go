package billing

import (
	"context"
	"slices"

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
// no DB write. Returns response.Missing populated for each requested
// capability the user does not have.
//
// Caller specifies what to verify via req.Require. Default (empty
// Require) checks payment_method only — PaaS/BaaS first. Subscription
// is currently a v1 stub: req.Require containing "subscription"
// always returns "subscription" in Missing because the subscriptions
// table doesn't yet ship; this is honest behavior, not a bug. Real
// subscription checks land when ms_billing.subscriptions ships in v2.
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

	// Default + validate the Require list. Capability is a typed string,
	// so unknown values from in-process callers can't compile; the
	// runtime validation here catches JSON-deserialized requests that
	// might carry arbitrary strings from across the lambda.Invoke boundary.
	require := req.Require
	if len(require) == 0 {
		require = []Capability{RequirePaymentMethod}
	}
	for _, r := range require {
		if r != RequirePaymentMethod && r != RequireSubscription {
			return nil, InvalidInput("unknown require: " + string(r))
		}
	}

	resp := &EnsureResponse{Missing: []string{}}

	// Missing billing account short-circuits: per-capability checks need account_id.
	accountID, found, err := s.store.AccountByUser(ctx, req.UserID)
	if err != nil {
		return nil, Internal("account lookup failed", err)
	}
	if !found {
		resp.Missing = append(resp.Missing, MissingBillingAccount)
		return resp, nil
	}

	// Per-capability checks. Order is fixed (PM before subscription) so
	// the Missing slice is deterministic regardless of Require ordering.
	if slices.Contains(require, RequirePaymentMethod) {
		hasPM, err := s.store.HasUsablePaymentMethod(ctx, accountID)
		if err != nil {
			return nil, Internal("payment-method lookup failed", err)
		}
		if !hasPM {
			resp.Missing = append(resp.Missing, MissingPaymentMethod)
		}
	}
	if slices.Contains(require, RequireSubscription) {
		// v1: subscriptions table not yet shipped; always missing.
		resp.Missing = append(resp.Missing, MissingSubscription)
	}
	return resp, nil
}

// PrepareAddPaymentMethod implements the one-shot setup flow:
//  1. INSERT or SELECT the accounts row (idempotent on user_id).
//  2. If no Stripe Customer yet: create one and persist its ID.
//  3. Create a fresh SetupIntent and return its client_secret.
//
// Failure mode: step 2's "create then persist" pair is non-atomic. If
// CreateCustomer succeeds but SetStripeCustomer fails, an orphan Stripe
// Customer exists with no DB row pointing at it. Operational recovery
// via Stripe metadata reconciliation (metadata.billing_account_id is
// stable); not addressed in the v1 handler.
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
			// DB write failure, not a Stripe API failure — INTERNAL is
			// the honest code. The orphan-Customer recovery path is
			// covered by the operational reconciliation job; the caller's
			// retry is also safe (row exists; second attempt reuses it).
			return nil, Internal("persist stripe_customer_id failed", err)
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
