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
	store     Store
	stripe    billingstripe.Client
	returnURL string
}

// NewService wires a Service. store and stripe are required; passing
// nil panics at the first call site. returnURL is the post-confirmation
// redirect target for the setup-mode Checkout Session (the frontend
// billing page); elements mode requires it.
func NewService(store Store, stripe billingstripe.Client, returnURL string) *Service {
	return &Service{store: store, stripe: stripe, returnURL: returnURL}
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
//  3. Create a fresh setup-mode Checkout Session and return its
//     client_secret.
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
		// First paid action for this user — create the Stripe Customer
		// carrying the account email (Stripe needs it to confirm the
		// setup-mode Checkout Session).
		cust, err := s.stripe.CreateCustomer(ctx, accountID.String(), req.Email)
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
	} else if req.Email != "" {
		// Existing Customer (possibly created before email capture):
		// backfill the email so the Checkout Session can be confirmed.
		// Idempotent when the email is already set.
		if err := s.stripe.UpdateCustomerEmail(ctx, stripeCustomerID, req.Email); err != nil {
			return nil, StripeError("update customer email failed", err)
		}
	}

	session, err := s.stripe.CreateCheckoutSession(ctx, stripeCustomerID, s.returnURL)
	if err != nil {
		return nil, StripeError("create checkout session failed", err)
	}
	return &PrepareAddPaymentMethodResponse{
		BillingAccountID: accountID,
		ClientSecret:     session.ClientSecret,
	}, nil
}

// StartAddPaymentMethod opens an add-card attempt and returns the
// durable request_id the frontend polls against. Logical sequence:
//
//  1. Ensure the accounts row (idempotent on user_id).
//  2. Create the Stripe Customer if absent (one-shot, persisted).
//  3. INSERT a pending row in ms_billing.add_card_requests.
//  4. CreateCheckoutSession (setup mode); the response expands
//     setup_intent so we read its id immediately.
//  5. Stamp setup_intent_id onto the request row so the webhook can
//     correlate setup_intent.succeeded back here.
//
// Failure modes are bounded:
//   - Steps 1–2 share the orphan-Customer recovery path as
//     PrepareAddPaymentMethod (metadata.billing_account_id anchor).
//   - Step 5 failing after 4 succeeds leaves a pending row with no
//     setup_intent_id; the row is harmless (frontend's poll returns
//     pending until the 24h TTL purge picks it up) but a retry would
//     create a fresh request, which is fine.
func (s *Service) StartAddPaymentMethod(ctx context.Context, req StartAddPaymentMethodRequest) (*StartAddPaymentMethodResponse, error) {
	if req.UserID == uuid.Nil {
		return nil, InvalidInput("user_id required")
	}

	accountID, stripeCustomerID, err := s.store.EnsureAccount(ctx, req.UserID)
	if err != nil {
		return nil, Internal("ensure account failed", err)
	}

	if stripeCustomerID == "" {
		// First paid action for this user — create the Stripe Customer
		// carrying the account email (Stripe needs it to confirm the
		// setup-mode Checkout Session).
		cust, err := s.stripe.CreateCustomer(ctx, accountID.String(), req.Email)
		if err != nil {
			return nil, StripeError("create customer failed", err)
		}
		stripeCustomerID = cust.ID
		if err := s.store.SetStripeCustomer(ctx, accountID, stripeCustomerID); err != nil {
			return nil, Internal("persist stripe_customer_id failed", err)
		}
	} else if req.Email != "" {
		// Existing Customer (possibly created before email capture):
		// backfill the email so the Checkout Session can be confirmed.
		// Idempotent when the email is already set.
		if err := s.stripe.UpdateCustomerEmail(ctx, stripeCustomerID, req.Email); err != nil {
			return nil, StripeError("update customer email failed", err)
		}
	}

	requestID, err := s.store.InsertAddCardRequest(ctx, accountID)
	if err != nil {
		return nil, Internal("insert add-card request failed", err)
	}

	session, err := s.stripe.CreateCheckoutSession(ctx, stripeCustomerID, s.returnURL)
	if err != nil {
		return nil, StripeError("create checkout session failed", err)
	}

	// The session expands setup_intent; the inner *SetupIntent is
	// the one Stripe will emit setup_intent.succeeded for. Defensive
	// nil-check: stripe-go has marked SetupIntent optional historically.
	if session.SetupIntent != nil && session.SetupIntent.ID != "" {
		if err := s.store.SetAddCardRequestSetupIntent(ctx, requestID, session.SetupIntent.ID); err != nil {
			return nil, Internal("persist setup_intent_id failed", err)
		}
	}

	return &StartAddPaymentMethodResponse{
		RequestID:        requestID,
		BillingAccountID: accountID,
		ClientSecret:     session.ClientSecret,
	}, nil
}

// FinishAddPaymentMethod returns the current resolution status of an
// add-card request. The frontend polls this endpoint; the webhook
// (setup_intent.succeeded + payment_method.attached) is what flips
// the row from pending → completed / duplicate / failed.
//
// Ownership is enforced by joining account_id to the user's account:
// a user can only read requests they themselves started. A request_id
// that doesn't belong to the caller (or doesn't exist) returns 404
// rather than leaking existence to a different user.
func (s *Service) FinishAddPaymentMethod(ctx context.Context, req FinishAddPaymentMethodRequest) (*FinishAddPaymentMethodResponse, error) {
	if req.UserID == uuid.Nil {
		return nil, InvalidInput("user_id required")
	}
	if req.RequestID == uuid.Nil {
		return nil, InvalidInput("request_id required")
	}

	accountID, found, err := s.store.AccountByUser(ctx, req.UserID)
	if err != nil {
		return nil, Internal("account lookup failed", err)
	}
	if !found {
		return nil, NotFound("add-card request not found")
	}

	row, err := s.store.GetAddCardRequest(ctx, req.RequestID, accountID)
	if err != nil {
		return nil, Internal("get add-card request failed", err)
	}
	if row == nil {
		return nil, NotFound("add-card request not found")
	}

	return &FinishAddPaymentMethodResponse{
		Status:        row.Status,
		PaymentMethod: row.PaymentMethod,
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

// DetachPaymentMethod detaches a saved card from the user's Stripe
// Customer. Ownership is enforced via PaymentMethodTarget — the PM must
// belong to the caller's account. The mirror row is soft-deleted
// asynchronously by the payment_method.detached webhook.
func (s *Service) DetachPaymentMethod(ctx context.Context, req DetachPaymentMethodRequest) (*DetachPaymentMethodResponse, error) {
	if req.UserID == uuid.Nil || req.PaymentMethodID == uuid.Nil {
		return nil, InvalidInput("user_id and payment_method_id required")
	}
	stripePMID, _, found, err := s.store.PaymentMethodTarget(ctx, req.UserID, req.PaymentMethodID)
	if err != nil {
		return nil, Internal("payment method lookup failed", err)
	}
	if !found {
		return nil, NotFound("payment method not found")
	}
	if err := s.stripe.DetachPaymentMethod(ctx, stripePMID); err != nil {
		return nil, StripeError("detach payment method failed", err)
	}
	return &DetachPaymentMethodResponse{}, nil
}

// SetDefaultPaymentMethod points the user's Stripe Customer
// invoice-settings default at the given card. Ownership-checked as above;
// is_default is synced asynchronously by the customer.updated webhook.
func (s *Service) SetDefaultPaymentMethod(ctx context.Context, req SetDefaultPaymentMethodRequest) (*SetDefaultPaymentMethodResponse, error) {
	if req.UserID == uuid.Nil || req.PaymentMethodID == uuid.Nil {
		return nil, InvalidInput("user_id and payment_method_id required")
	}
	stripePMID, stripeCustomerID, found, err := s.store.PaymentMethodTarget(ctx, req.UserID, req.PaymentMethodID)
	if err != nil {
		return nil, Internal("payment method lookup failed", err)
	}
	if !found {
		return nil, NotFound("payment method not found")
	}
	if err := s.stripe.SetDefaultPaymentMethod(ctx, stripeCustomerID, stripePMID); err != nil {
		return nil, StripeError("set default payment method failed", err)
	}
	return &SetDefaultPaymentMethodResponse{}, nil
}
