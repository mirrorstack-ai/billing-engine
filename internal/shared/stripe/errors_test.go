package stripe

import (
	"errors"
	"fmt"
	"testing"

	stripego "github.com/stripe/stripe-go/v85"
)

func TestMapError_NilPassThrough(t *testing.T) {
	if err := MapError(nil); err != nil {
		t.Fatalf("MapError(nil) = %v, want nil", err)
	}
}

func TestMapError_NonStripePassThrough(t *testing.T) {
	in := fmt.Errorf("network unreachable")
	if got := MapError(in); got != in {
		t.Fatalf("MapError(non-stripe) = %v, want %v", got, in)
	}
}

func TestMapError_CardDeclined(t *testing.T) {
	se := &stripego.Error{
		Code:           stripego.ErrorCodeCardDeclined,
		Type:           stripego.ErrorTypeCard,
		HTTPStatusCode: 402,
	}
	got := MapError(se)
	if !errors.Is(got, ErrCardDeclined) {
		t.Fatalf("expected ErrCardDeclined, got %v", got)
	}
	var unwrapped *stripego.Error
	if !errors.As(got, &unwrapped) {
		t.Fatal("expected joined error to expose *stripe.Error")
	}
}

func TestMapError_IdempotencyConflict(t *testing.T) {
	se := &stripego.Error{
		Type:           stripego.ErrorTypeIdempotency,
		HTTPStatusCode: 400,
	}
	got := MapError(se)
	if !errors.Is(got, ErrIdempotencyConflict) {
		t.Fatalf("expected ErrIdempotencyConflict, got %v", got)
	}
}

func TestMapError_GenericAPI(t *testing.T) {
	se := &stripego.Error{
		Type:           stripego.ErrorTypeAPI,
		HTTPStatusCode: 500,
	}
	got := MapError(se)
	if !errors.Is(got, ErrStripeAPI) {
		t.Fatalf("expected ErrStripeAPI, got %v", got)
	}
}

func TestMapResourceError_NotFound(t *testing.T) {
	cases := []struct {
		name string
		kind ResourceKind
		want error
	}{
		{"customer", ResourceCustomer, ErrCustomerNotFound},
		{"subscription", ResourceSubscription, ErrSubscriptionNotFound},
		{"invoice", ResourceInvoice, ErrInvoiceNotFound},
		{"payment_method", ResourcePaymentMethod, ErrPaymentMethodNotFound},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			se := &stripego.Error{
				Code:           stripego.ErrorCodeResourceMissing,
				Type:           stripego.ErrorTypeInvalidRequest,
				HTTPStatusCode: 404,
			}
			got := MapResourceError(se, tc.kind)
			if !errors.Is(got, tc.want) {
				t.Fatalf("expected %v, got %v", tc.want, got)
			}
		})
	}
}

func TestMapResourceError_FallsBackToMapError(t *testing.T) {
	se := &stripego.Error{
		Code:           stripego.ErrorCodeCardDeclined,
		Type:           stripego.ErrorTypeCard,
		HTTPStatusCode: 402,
	}
	got := MapResourceError(se, ResourceCustomer)
	if !errors.Is(got, ErrCardDeclined) {
		t.Fatalf("expected ErrCardDeclined fallthrough, got %v", got)
	}
}

func TestMapResourceError_NilAndNonStripe(t *testing.T) {
	if err := MapResourceError(nil, ResourceCustomer); err != nil {
		t.Fatalf("nil case: got %v", err)
	}
	plain := errors.New("boom")
	if got := MapResourceError(plain, ResourceCustomer); got != plain {
		t.Fatalf("non-stripe pass-through: got %v", got)
	}
}
