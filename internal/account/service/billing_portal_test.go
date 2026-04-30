package service

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"
	stripego "github.com/stripe/stripe-go/v85"

	"github.com/mirrorstack-ai/billing-engine/internal/account/db"
	mstripe "github.com/mirrorstack-ai/billing-engine/internal/shared/stripe"
)

type fakeReader struct {
	ba  db.BillingAccount
	err error
}

func (f *fakeReader) GetBillingAccountByID(_ context.Context, _ uuid.UUID) (db.BillingAccount, error) {
	return f.ba, f.err
}

type fakeSessions struct {
	url    string
	err    error
	called int
	last   *stripego.BillingPortalSessionCreateParams
}

func (f *fakeSessions) Create(_ context.Context, p *stripego.BillingPortalSessionCreateParams) (*stripego.BillingPortalSession, error) {
	f.called++
	f.last = p
	if f.err != nil {
		return nil, f.err
	}
	return &stripego.BillingPortalSession{URL: f.url}, nil
}

func TestCreatePortalSession_Success(t *testing.T) {
	id := uuid.New()
	r := &fakeReader{ba: db.BillingAccount{ID: id, StripeCustomerID: "cus_abc"}}
	s := &fakeSessions{url: "https://billing.stripe.com/p/session/abc"}

	got, err := NewBillingPortalService(r, s).CreatePortalSession(context.Background(), id, "https://app.example/return")
	if err != nil {
		t.Fatalf("err = %v, want nil", err)
	}
	if got != "https://billing.stripe.com/p/session/abc" {
		t.Fatalf("url = %q", got)
	}
	if s.called != 1 {
		t.Fatalf("Stripe Create calls = %d, want 1", s.called)
	}
	if s.last.Customer == nil || *s.last.Customer != "cus_abc" {
		t.Fatalf("Customer param = %v, want cus_abc", s.last.Customer)
	}
	if s.last.ReturnURL == nil || *s.last.ReturnURL != "https://app.example/return" {
		t.Fatalf("ReturnURL param = %v", s.last.ReturnURL)
	}
}

func TestCreatePortalSession_NotFound(t *testing.T) {
	r := &fakeReader{err: db.ErrBillingAccountNotFound}
	s := &fakeSessions{}

	_, err := NewBillingPortalService(r, s).CreatePortalSession(context.Background(), uuid.New(), "https://app.example/return")
	if !errors.Is(err, ErrBillingAccountNotFound) {
		t.Fatalf("err = %v, want ErrBillingAccountNotFound", err)
	}
	if s.called != 0 {
		t.Fatalf("Stripe should not be called when account is missing; calls = %d", s.called)
	}
}

func TestCreatePortalSession_EmptyStripeCustomerID_TreatedAsNotFound(t *testing.T) {
	id := uuid.New()
	r := &fakeReader{ba: db.BillingAccount{ID: id, StripeCustomerID: ""}}
	s := &fakeSessions{}

	_, err := NewBillingPortalService(r, s).CreatePortalSession(context.Background(), id, "https://app.example/return")
	if !errors.Is(err, ErrBillingAccountNotFound) {
		t.Fatalf("err = %v, want ErrBillingAccountNotFound", err)
	}
	if s.called != 0 {
		t.Fatalf("Stripe should not be called when stripe_customer_id is empty; calls = %d", s.called)
	}
}

func TestCreatePortalSession_StripeCustomerMissing_MapsToCustomerNotFound(t *testing.T) {
	id := uuid.New()
	r := &fakeReader{ba: db.BillingAccount{ID: id, StripeCustomerID: "cus_dead"}}
	// resource_missing on a 404 — Stripe says the customer no longer exists.
	s := &fakeSessions{err: &stripego.Error{
		HTTPStatusCode: 404,
		Type:           stripego.ErrorTypeInvalidRequest,
		Code:           stripego.ErrorCodeResourceMissing,
	}}

	_, err := NewBillingPortalService(r, s).CreatePortalSession(context.Background(), id, "https://app.example/return")
	if !errors.Is(err, mstripe.ErrCustomerNotFound) {
		t.Fatalf("err = %v, want ErrCustomerNotFound", err)
	}
}

func TestCreatePortalSession_Stripe5xx_ReturnsStripeAPIError(t *testing.T) {
	// Speed up Do() retries so this test stays fast.
	id := uuid.New()
	r := &fakeReader{ba: db.BillingAccount{ID: id, StripeCustomerID: "cus_abc"}}
	s := &fakeSessions{err: &stripego.Error{HTTPStatusCode: 500, Type: stripego.ErrorTypeAPI}}

	_, err := NewBillingPortalService(r, s).CreatePortalSession(context.Background(), id, "https://app.example/return")
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !errors.Is(err, mstripe.ErrStripeAPI) {
		t.Fatalf("err = %v, want ErrStripeAPI", err)
	}
}
