// Package service hosts the business logic for the account-side
// billing-engine surface. Handlers stay thin and delegate here.
package service

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	stripego "github.com/stripe/stripe-go/v85"

	"github.com/mirrorstack-ai/billing-engine/internal/account/db"
	mstripe "github.com/mirrorstack-ai/billing-engine/internal/shared/stripe"
)

// ErrBillingAccountNotFound is the service-level not-found sentinel.
// It also covers the defensive case where a row exists but its
// stripe_customer_id is empty — both surface as 404 at the HTTP layer
// since they are indistinguishable from the caller's perspective.
var ErrBillingAccountNotFound = errors.New("service: billing account not found")

// PortalSessionCreator is the slice of stripe-go we depend on.
// Matched against *v1BillingPortalSessionService in production and
// against a fake in tests.
type PortalSessionCreator interface {
	Create(ctx context.Context, params *stripego.BillingPortalSessionCreateParams) (*stripego.BillingPortalSession, error)
}

// BillingPortalService wires DB lookup + Stripe portal session creation.
type BillingPortalService struct {
	reader   db.BillingAccountReader
	sessions PortalSessionCreator
}

// NewBillingPortalService constructs the service from its collaborators.
func NewBillingPortalService(reader db.BillingAccountReader, sessions PortalSessionCreator) *BillingPortalService {
	return &BillingPortalService{reader: reader, sessions: sessions}
}

// CreatePortalSession looks up the billing account, asks Stripe to mint
// a customer-portal session, and returns the hosted URL.
func (s *BillingPortalService) CreatePortalSession(ctx context.Context, billingAccountID uuid.UUID, returnURL string) (string, error) {
	ba, err := s.reader.GetBillingAccountByID(ctx, billingAccountID)
	if err != nil {
		if errors.Is(err, db.ErrBillingAccountNotFound) {
			return "", ErrBillingAccountNotFound
		}
		return "", fmt.Errorf("lookup billing account: %w", err)
	}
	// #5's flow guarantees stripe_customer_id is set on insert; this is a
	// belt-and-braces check so we never hand Stripe an empty Customer.
	if ba.StripeCustomerID == "" {
		return "", ErrBillingAccountNotFound
	}

	params := &stripego.BillingPortalSessionCreateParams{
		Customer:  stripego.String(ba.StripeCustomerID),
		ReturnURL: stripego.String(returnURL),
	}

	var session *stripego.BillingPortalSession
	err = mstripe.Do(ctx, func() error {
		var callErr error
		session, callErr = s.sessions.Create(ctx, params)
		return callErr
	})
	if err != nil {
		return "", mstripe.MapResourceError(err, mstripe.ResourceCustomer)
	}
	return session.URL, nil
}
