package stripe

import (
	"errors"

	stripego "github.com/stripe/stripe-go/v85"
)

// Sentinel errors callers can match with errors.Is. MapError joins the
// original *stripe.Error with one of these so both the high-level category
// and the raw Stripe payload remain reachable.
var (
	ErrCustomerNotFound      = errors.New("stripe: customer not found")
	ErrSubscriptionNotFound  = errors.New("stripe: subscription not found")
	ErrInvoiceNotFound       = errors.New("stripe: invoice not found")
	ErrPaymentMethodNotFound = errors.New("stripe: payment method not found")
	ErrCardDeclined          = errors.New("stripe: card declined")
	ErrIdempotencyConflict   = errors.New("stripe: idempotency key conflict")
	ErrStripeAPI             = errors.New("stripe: api error")
)

// MapError translates a stripe-go error into one of our sentinel errors,
// joined with the original so callers can read the raw Stripe fields when
// needed. Non-Stripe errors (network, context, etc.) pass through unchanged.
func MapError(err error) error {
	if err == nil {
		return nil
	}
	var se *stripego.Error
	if !errors.As(err, &se) {
		return err
	}

	// resource_missing is shared across all object types; disambiguate by
	// the surrounding context (we surface ErrStripeAPI for unknown resources
	// rather than guessing the caller's intent).
	switch se.Code {
	case stripego.ErrorCodeCardDeclined:
		return errors.Join(ErrCardDeclined, se)
	}

	if se.Type == stripego.ErrorTypeIdempotency {
		return errors.Join(ErrIdempotencyConflict, se)
	}

	return errors.Join(ErrStripeAPI, se)
}

// MapResourceError is like MapError but uses the caller's known resource
// kind to map resource_missing to a more specific sentinel. Use this from
// service code that already knows what it was looking up.
func MapResourceError(err error, kind ResourceKind) error {
	if err == nil {
		return nil
	}
	var se *stripego.Error
	if !errors.As(err, &se) {
		return err
	}
	if se.Code == stripego.ErrorCodeResourceMissing {
		switch kind {
		case ResourceCustomer:
			return errors.Join(ErrCustomerNotFound, se)
		case ResourceSubscription:
			return errors.Join(ErrSubscriptionNotFound, se)
		case ResourceInvoice:
			return errors.Join(ErrInvoiceNotFound, se)
		case ResourcePaymentMethod:
			return errors.Join(ErrPaymentMethodNotFound, se)
		}
	}
	return MapError(err)
}

// ResourceKind tags Stripe object types so MapResourceError can pick the
// right not-found sentinel.
type ResourceKind int

const (
	ResourceCustomer ResourceKind = iota + 1
	ResourceSubscription
	ResourceInvoice
	ResourcePaymentMethod
)
