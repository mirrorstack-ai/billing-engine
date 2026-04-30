// Package service hosts billing-engine business logic for the account
// surface. Handlers in internal/account/handler delegate here so the
// logic is testable without an HTTP harness.
package service

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"

	"github.com/google/uuid"
	stripego "github.com/stripe/stripe-go/v85"

	"github.com/mirrorstack-ai/billing-engine/internal/account/db"
	mstripe "github.com/mirrorstack-ai/billing-engine/internal/shared/stripe"
)

// Service-level validation errors. Handlers translate these into 4xx.
var (
	ErrInvalidOwnerType = errors.New("subscriptions: invalid owner_type")
	ErrInvalidCurrency  = errors.New("subscriptions: invalid currency")
	ErrMissingField     = errors.New("subscriptions: missing required field")
)

// allowed enums kept in lock-step with the CHECK constraints in
// migrations/account/001_billing_accounts.up.sql.
var (
	allowedOwnerTypes = map[string]struct{}{"user": {}, "org": {}}
	allowedCurrencies = map[string]struct{}{"USD": {}, "TWD": {}, "EUR": {}}
)

// BillingAccountDB is the subset of the db package the service needs.
// Narrowing to an interface lets tests mock the DB without Postgres.
type BillingAccountDB interface {
	GetByOwner(ctx context.Context, ownerType string, ownerID uuid.UUID) (*db.BillingAccount, error)
	Insert(ctx context.Context, ownerType string, ownerID uuid.UUID, stripeCustomerID, currency string) (uuid.UUID, error)
}

// StripeAPI is the slice of stripe-go we exercise. Tests substitute a
// fake; production wires the real client.
type StripeAPI interface {
	CreateCustomer(ctx context.Context, params *stripego.CustomerCreateParams) (*stripego.Customer, error)
	CreateCheckoutSession(ctx context.Context, params *stripego.CheckoutSessionCreateParams) (*stripego.CheckoutSession, error)
}

// NewStripeAdapter wraps a shared Stripe client so it satisfies StripeAPI.
// Lives here (not in main.go) so other services that need the same slice
// can reuse one adapter.
func NewStripeAdapter(c *mstripe.Client) StripeAPI {
	return stripeAdapter{c: c}
}

type stripeAdapter struct{ c *mstripe.Client }

func (a stripeAdapter) CreateCustomer(ctx context.Context, params *stripego.CustomerCreateParams) (*stripego.Customer, error) {
	return a.c.API.V1Customers.Create(ctx, params)
}

func (a stripeAdapter) CreateCheckoutSession(ctx context.Context, params *stripego.CheckoutSessionCreateParams) (*stripego.CheckoutSession, error) {
	return a.c.API.V1CheckoutSessions.Create(ctx, params)
}

// Subscriptions is the business-logic struct for subscription operations.
type Subscriptions struct {
	accounts BillingAccountDB
	stripe   StripeAPI
}

// NewSubscriptions wires a Subscriptions service.
func NewSubscriptions(accounts BillingAccountDB, stripe StripeAPI) *Subscriptions {
	return &Subscriptions{accounts: accounts, stripe: stripe}
}

// CreateInput is the request shape for Create. All fields are required.
type CreateInput struct {
	OwnerType  string
	OwnerID    uuid.UUID
	PlanID     string
	Currency   string
	SuccessURL string
	CancelURL  string
}

// CreateOutput is the response shape for Create.
type CreateOutput struct {
	CheckoutURL string
	SessionID   string
}

// Create looks up (or creates) a billing_accounts row, then opens a
// Stripe Checkout Session in subscription mode. The returned URL is
// what the user is redirected to; the session id is returned for
// observability.
func (s *Subscriptions) Create(ctx context.Context, in CreateInput) (*CreateOutput, error) {
	if err := validateCreate(in); err != nil {
		return nil, err
	}

	account, err := s.lookupOrCreateAccount(ctx, in)
	if err != nil {
		return nil, err
	}

	// Idempotency: Stripe dedupes Checkout Session creates by header
	// for ~24h, so a deterministic hash of (owner_type, owner_id,
	// plan_id) gives us callable-twice safety inside that window
	// without requiring a local idempotency table. The 5-min product
	// requirement is a strict subset of Stripe's dedup window.
	idemKey := deterministicIdempotencyKey("subscriptions.create", in.OwnerType, in.OwnerID.String(), in.PlanID)

	params := &stripego.CheckoutSessionCreateParams{
		Mode:       stripego.String(string(stripego.CheckoutSessionModeSubscription)),
		Customer:   stripego.String(account.StripeCustomerID),
		SuccessURL: stripego.String(in.SuccessURL),
		CancelURL:  stripego.String(in.CancelURL),
		LineItems: []*stripego.CheckoutSessionCreateLineItemParams{
			{Price: stripego.String(in.PlanID), Quantity: stripego.Int64(1)},
		},
	}
	params.SetIdempotencyKey(idemKey)

	var session *stripego.CheckoutSession
	err = mstripe.Do(ctx, func() error {
		var callErr error
		session, callErr = s.stripe.CreateCheckoutSession(ctx, params)
		return callErr
	})
	if err != nil {
		return nil, mstripe.MapError(err)
	}

	return &CreateOutput{CheckoutURL: session.URL, SessionID: session.ID}, nil
}

func (s *Subscriptions) lookupOrCreateAccount(ctx context.Context, in CreateInput) (*db.BillingAccount, error) {
	existing, err := s.accounts.GetByOwner(ctx, in.OwnerType, in.OwnerID)
	if err == nil {
		return existing, nil
	}
	if !errors.Is(err, db.ErrBillingAccountNotFound) {
		return nil, fmt.Errorf("lookup billing_account: %w", err)
	}

	custParams := &stripego.CustomerCreateParams{
		Metadata: map[string]string{
			"owner_type": in.OwnerType,
			"owner_id":   in.OwnerID.String(),
		},
	}
	var customer *stripego.Customer
	err = mstripe.Do(ctx, func() error {
		var callErr error
		customer, callErr = s.stripe.CreateCustomer(ctx, custParams)
		return callErr
	})
	if err != nil {
		return nil, mstripe.MapError(err)
	}

	id, err := s.accounts.Insert(ctx, in.OwnerType, in.OwnerID, customer.ID, in.Currency)
	if err != nil {
		return nil, fmt.Errorf("insert billing_account: %w", err)
	}
	return &db.BillingAccount{
		ID:               id,
		OwnerType:        in.OwnerType,
		OwnerID:          in.OwnerID,
		StripeCustomerID: customer.ID,
		Currency:         in.Currency,
	}, nil
}

func validateCreate(in CreateInput) error {
	if _, ok := allowedOwnerTypes[in.OwnerType]; !ok {
		return ErrInvalidOwnerType
	}
	if in.OwnerID == uuid.Nil {
		return fmt.Errorf("%w: owner_id", ErrMissingField)
	}
	if _, ok := allowedCurrencies[in.Currency]; !ok {
		return ErrInvalidCurrency
	}
	if in.PlanID == "" {
		return fmt.Errorf("%w: plan_id", ErrMissingField)
	}
	if in.SuccessURL == "" {
		return fmt.Errorf("%w: success_url", ErrMissingField)
	}
	if in.CancelURL == "" {
		return fmt.Errorf("%w: cancel_url", ErrMissingField)
	}
	return nil
}

// deterministicIdempotencyKey hashes the variant tuple so two calls
// with the same (owner_type, owner_id, plan_id) hit Stripe with the
// same Idempotency-Key — Stripe then dedupes server-side and returns
// the original session instead of double-creating.
func deterministicIdempotencyKey(operation string, parts ...string) string {
	h := sha256.New()
	for _, p := range parts {
		h.Write([]byte(p))
		h.Write([]byte{0}) // null separator avoids collision on concat
	}
	return "bill." + operation + "." + hex.EncodeToString(h.Sum(nil))
}
