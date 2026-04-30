package service

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/google/uuid"
	stripego "github.com/stripe/stripe-go/v85"

	"github.com/mirrorstack-ai/billing-engine/internal/account/db"
)

type fakeAccounts struct {
	getByOwner func(ctx context.Context, ownerType string, ownerID uuid.UUID) (*db.BillingAccount, error)
	insert     func(ctx context.Context, ownerType string, ownerID uuid.UUID, stripeCustomerID, currency string) (uuid.UUID, error)

	insertCalls atomic.Int32
}

func (f *fakeAccounts) GetByOwner(ctx context.Context, ownerType string, ownerID uuid.UUID) (*db.BillingAccount, error) {
	return f.getByOwner(ctx, ownerType, ownerID)
}

func (f *fakeAccounts) Insert(ctx context.Context, ownerType string, ownerID uuid.UUID, stripeCustomerID, currency string) (uuid.UUID, error) {
	f.insertCalls.Add(1)
	return f.insert(ctx, ownerType, ownerID, stripeCustomerID, currency)
}

type fakeStripe struct {
	createCustomer        func(ctx context.Context, params *stripego.CustomerCreateParams) (*stripego.Customer, error)
	createCheckoutSession func(ctx context.Context, params *stripego.CheckoutSessionCreateParams) (*stripego.CheckoutSession, error)

	custCalls    atomic.Int32
	sessionCalls atomic.Int32

	lastCustomerParams *stripego.CustomerCreateParams
	lastSessionParams  *stripego.CheckoutSessionCreateParams
}

func (f *fakeStripe) CreateCustomer(ctx context.Context, params *stripego.CustomerCreateParams) (*stripego.Customer, error) {
	f.custCalls.Add(1)
	f.lastCustomerParams = params
	return f.createCustomer(ctx, params)
}

func (f *fakeStripe) CreateCheckoutSession(ctx context.Context, params *stripego.CheckoutSessionCreateParams) (*stripego.CheckoutSession, error) {
	f.sessionCalls.Add(1)
	f.lastSessionParams = params
	return f.createCheckoutSession(ctx, params)
}

func validInput() CreateInput {
	return CreateInput{
		OwnerType:  "user",
		OwnerID:    uuid.New(),
		PlanID:     "price_123",
		Currency:   "USD",
		SuccessURL: "https://example.com/success",
		CancelURL:  "https://example.com/cancel",
	}
}

func TestCreate_Validation(t *testing.T) {
	tests := []struct {
		name string
		mut  func(in *CreateInput)
		want error
	}{
		{"bad owner_type", func(in *CreateInput) { in.OwnerType = "bot" }, ErrInvalidOwnerType},
		{"empty owner_type", func(in *CreateInput) { in.OwnerType = "" }, ErrInvalidOwnerType},
		{"zero owner_id", func(in *CreateInput) { in.OwnerID = uuid.Nil }, ErrMissingField},
		{"bad currency", func(in *CreateInput) { in.Currency = "JPY" }, ErrInvalidCurrency},
		{"empty currency", func(in *CreateInput) { in.Currency = "" }, ErrInvalidCurrency},
		{"empty plan_id", func(in *CreateInput) { in.PlanID = "" }, ErrMissingField},
		{"empty success_url", func(in *CreateInput) { in.SuccessURL = "" }, ErrMissingField},
		{"empty cancel_url", func(in *CreateInput) { in.CancelURL = "" }, ErrMissingField},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			in := validInput()
			tc.mut(&in)

			s := NewSubscriptions(&fakeAccounts{}, &fakeStripe{})
			_, err := s.Create(context.Background(), in)
			if !errors.Is(err, tc.want) {
				t.Fatalf("expected %v; got %v", tc.want, err)
			}
		})
	}
}

func TestCreate_NewAccount_CreatesStripeCustomerAndInsertsRow(t *testing.T) {
	in := validInput()
	wantAccountID := uuid.New()

	accts := &fakeAccounts{
		getByOwner: func(_ context.Context, _ string, _ uuid.UUID) (*db.BillingAccount, error) {
			return nil, db.ErrBillingAccountNotFound
		},
		insert: func(_ context.Context, ownerType string, ownerID uuid.UUID, stripeCustomerID, currency string) (uuid.UUID, error) {
			if ownerType != in.OwnerType || ownerID != in.OwnerID || stripeCustomerID != "cus_new" || currency != in.Currency {
				t.Fatalf("unexpected insert args: %s %s %s %s", ownerType, ownerID, stripeCustomerID, currency)
			}
			return wantAccountID, nil
		},
	}
	stripe := &fakeStripe{
		createCustomer: func(_ context.Context, params *stripego.CustomerCreateParams) (*stripego.Customer, error) {
			if params.Metadata["owner_type"] != in.OwnerType || params.Metadata["owner_id"] != in.OwnerID.String() {
				t.Fatalf("metadata not propagated: %+v", params.Metadata)
			}
			return &stripego.Customer{ID: "cus_new"}, nil
		},
		createCheckoutSession: func(_ context.Context, params *stripego.CheckoutSessionCreateParams) (*stripego.CheckoutSession, error) {
			if params.Customer == nil || *params.Customer != "cus_new" {
				t.Fatalf("customer not threaded through to checkout session")
			}
			if params.Mode == nil || *params.Mode != "subscription" {
				t.Fatalf("expected subscription mode")
			}
			if params.SubscriptionData != nil && params.SubscriptionData.BillingCycleAnchor != nil {
				t.Fatalf("billing_cycle_anchor must NOT be set (signup-date anchor)")
			}
			if len(params.LineItems) != 1 || params.LineItems[0].Price == nil || *params.LineItems[0].Price != in.PlanID {
				t.Fatalf("line item not built correctly: %+v", params.LineItems)
			}
			if params.IdempotencyKey == nil || *params.IdempotencyKey == "" {
				t.Fatalf("idempotency key must be set")
			}
			return &stripego.CheckoutSession{ID: "cs_1", URL: "https://checkout.stripe.com/cs_1"}, nil
		},
	}

	s := NewSubscriptions(accts, stripe)
	out, err := s.Create(context.Background(), in)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if out.SessionID != "cs_1" || out.CheckoutURL != "https://checkout.stripe.com/cs_1" {
		t.Fatalf("unexpected output: %+v", out)
	}
	if stripe.custCalls.Load() != 1 {
		t.Fatalf("expected 1 customer create; got %d", stripe.custCalls.Load())
	}
	if accts.insertCalls.Load() != 1 {
		t.Fatalf("expected 1 insert; got %d", accts.insertCalls.Load())
	}
}

func TestCreate_ExistingAccount_SkipsCustomerCreate(t *testing.T) {
	in := validInput()
	existing := &db.BillingAccount{
		ID:               uuid.New(),
		OwnerType:        in.OwnerType,
		OwnerID:          in.OwnerID,
		StripeCustomerID: "cus_existing",
		Currency:         in.Currency,
	}

	accts := &fakeAccounts{
		getByOwner: func(_ context.Context, _ string, _ uuid.UUID) (*db.BillingAccount, error) {
			return existing, nil
		},
		insert: func(_ context.Context, _ string, _ uuid.UUID, _, _ string) (uuid.UUID, error) {
			t.Fatal("insert should not be called for existing account")
			return uuid.Nil, nil
		},
	}
	stripe := &fakeStripe{
		createCustomer: func(_ context.Context, _ *stripego.CustomerCreateParams) (*stripego.Customer, error) {
			t.Fatal("CreateCustomer should not be called for existing account")
			return nil, nil
		},
		createCheckoutSession: func(_ context.Context, params *stripego.CheckoutSessionCreateParams) (*stripego.CheckoutSession, error) {
			if *params.Customer != "cus_existing" {
				t.Fatalf("expected reuse of cus_existing; got %s", *params.Customer)
			}
			return &stripego.CheckoutSession{ID: "cs_2", URL: "https://x"}, nil
		},
	}

	s := NewSubscriptions(accts, stripe)
	out, err := s.Create(context.Background(), in)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if out.SessionID != "cs_2" {
		t.Fatalf("unexpected output: %+v", out)
	}
}

func TestCreate_Idempotency_SamePayloadProducesSameKey(t *testing.T) {
	in := validInput()

	accts := &fakeAccounts{
		getByOwner: func(_ context.Context, _ string, _ uuid.UUID) (*db.BillingAccount, error) {
			return &db.BillingAccount{StripeCustomerID: "cus_x"}, nil
		},
	}

	var keys []string
	stripe := &fakeStripe{
		createCheckoutSession: func(_ context.Context, params *stripego.CheckoutSessionCreateParams) (*stripego.CheckoutSession, error) {
			keys = append(keys, *params.IdempotencyKey)
			return &stripego.CheckoutSession{ID: "cs_dup", URL: "https://x"}, nil
		},
	}
	s := NewSubscriptions(accts, stripe)

	for i := 0; i < 2; i++ {
		if _, err := s.Create(context.Background(), in); err != nil {
			t.Fatalf("call %d err: %v", i, err)
		}
	}

	if len(keys) != 2 || keys[0] != keys[1] {
		t.Fatalf("expected both idempotency keys to match; got %v", keys)
	}
	if !strings.HasPrefix(keys[0], "bill.subscriptions.create.") {
		t.Fatalf("idempotency key missing namespace prefix: %s", keys[0])
	}
}

func TestCreate_Idempotency_DifferentPlanIDProducesDifferentKey(t *testing.T) {
	in1 := validInput()
	in2 := in1
	in2.PlanID = "price_other"

	accts := &fakeAccounts{
		getByOwner: func(_ context.Context, _ string, _ uuid.UUID) (*db.BillingAccount, error) {
			return &db.BillingAccount{StripeCustomerID: "cus_x"}, nil
		},
	}

	var keys []string
	stripe := &fakeStripe{
		createCheckoutSession: func(_ context.Context, params *stripego.CheckoutSessionCreateParams) (*stripego.CheckoutSession, error) {
			keys = append(keys, *params.IdempotencyKey)
			return &stripego.CheckoutSession{ID: "cs_x", URL: "https://x"}, nil
		},
	}
	s := NewSubscriptions(accts, stripe)

	if _, err := s.Create(context.Background(), in1); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Create(context.Background(), in2); err != nil {
		t.Fatal(err)
	}

	if len(keys) != 2 || keys[0] == keys[1] {
		t.Fatalf("expected different idempotency keys; got %v", keys)
	}
}

func TestCreate_LookupErrorPropagates(t *testing.T) {
	in := validInput()
	boom := errors.New("db connection refused")
	accts := &fakeAccounts{
		getByOwner: func(_ context.Context, _ string, _ uuid.UUID) (*db.BillingAccount, error) {
			return nil, boom
		},
	}
	s := NewSubscriptions(accts, &fakeStripe{})
	_, err := s.Create(context.Background(), in)
	if !errors.Is(err, boom) {
		t.Fatalf("expected wrap of underlying err; got %v", err)
	}
}
