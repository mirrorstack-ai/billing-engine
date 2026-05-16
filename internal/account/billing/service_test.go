package billing_test

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	stripego "github.com/stripe/stripe-go/v85"

	"github.com/mirrorstack-ai/billing-engine/internal/account/billing"
)

// --- in-memory Store fake -------------------------------------------------

type fakeStore struct {
	accountsByUser   map[uuid.UUID]fakeAccount
	hasUsablePM      map[uuid.UUID]bool
	paymentMethodsBy map[uuid.UUID][]billing.PaymentMethod

	// Injected failures (set per-test as needed).
	errEnsureAccount      error
	errSetStripeCustomer  error
	errAccountByUser      error
	errHasUsablePaymentMx error
	errListPaymentMethods error
}

type fakeAccount struct {
	id               uuid.UUID
	stripeCustomerID string
}

func newFakeStore() *fakeStore {
	return &fakeStore{
		accountsByUser:   map[uuid.UUID]fakeAccount{},
		hasUsablePM:      map[uuid.UUID]bool{},
		paymentMethodsBy: map[uuid.UUID][]billing.PaymentMethod{},
	}
}

func (s *fakeStore) EnsureAccount(_ context.Context, userID uuid.UUID) (uuid.UUID, string, error) {
	if s.errEnsureAccount != nil {
		return uuid.Nil, "", s.errEnsureAccount
	}
	if a, ok := s.accountsByUser[userID]; ok {
		return a.id, a.stripeCustomerID, nil
	}
	a := fakeAccount{id: uuid.New()}
	s.accountsByUser[userID] = a
	return a.id, "", nil
}

func (s *fakeStore) SetStripeCustomer(_ context.Context, accountID uuid.UUID, sid string) error {
	if s.errSetStripeCustomer != nil {
		return s.errSetStripeCustomer
	}
	for u, a := range s.accountsByUser {
		if a.id == accountID {
			a.stripeCustomerID = sid
			s.accountsByUser[u] = a
			return nil
		}
	}
	return errors.New("account not found")
}

func (s *fakeStore) AccountByUser(_ context.Context, userID uuid.UUID) (uuid.UUID, bool, error) {
	if s.errAccountByUser != nil {
		return uuid.Nil, false, s.errAccountByUser
	}
	a, ok := s.accountsByUser[userID]
	if !ok {
		return uuid.Nil, false, nil
	}
	return a.id, true, nil
}

func (s *fakeStore) HasUsablePaymentMethod(_ context.Context, accountID uuid.UUID) (bool, error) {
	if s.errHasUsablePaymentMx != nil {
		return false, s.errHasUsablePaymentMx
	}
	return s.hasUsablePM[accountID], nil
}

func (s *fakeStore) ListPaymentMethods(_ context.Context, accountID uuid.UUID) ([]billing.PaymentMethod, error) {
	if s.errListPaymentMethods != nil {
		return nil, s.errListPaymentMethods
	}
	return s.paymentMethodsBy[accountID], nil
}

// --- in-memory Stripe Client fake ----------------------------------------

type fakeStripe struct {
	createdCustomers     []string
	customerIDToReturn   string
	setupIntentToReturn  string
	errCreateCustomer    error
	errCreateSetupIntent error
}

func (f *fakeStripe) CreateCustomer(_ context.Context, billingAccountID string) (*stripego.Customer, error) {
	if f.errCreateCustomer != nil {
		return nil, f.errCreateCustomer
	}
	f.createdCustomers = append(f.createdCustomers, billingAccountID)
	id := f.customerIDToReturn
	if id == "" {
		id = "cus_test_" + billingAccountID[:8]
	}
	return &stripego.Customer{ID: id}, nil
}

func (f *fakeStripe) CreateSetupIntent(_ context.Context, _ string) (*stripego.SetupIntent, error) {
	if f.errCreateSetupIntent != nil {
		return nil, f.errCreateSetupIntent
	}
	cs := f.setupIntentToReturn
	if cs == "" {
		cs = "seti_test_secret_xyz"
	}
	return &stripego.SetupIntent{ClientSecret: cs}, nil
}

// --- tests ----------------------------------------------------------------

func TestEnsure_NoAccount_ReturnsMissingBillingAccount(t *testing.T) {
	svc := billing.NewService(newFakeStore(), &fakeStripe{})

	resp, err := svc.Ensure(context.Background(), billing.EnsureRequest{UserID: uuid.New()})

	require.NoError(t, err)
	require.False(t, resp.Ready())
	require.Equal(t, []string{billing.MissingBillingAccount}, resp.Missing)
}

func TestEnsure_AccountButNoPM_ReturnsMissingPaymentMethod(t *testing.T) {
	store := newFakeStore()
	userID := uuid.New()
	accountID := uuid.New()
	store.accountsByUser[userID] = fakeAccount{id: accountID, stripeCustomerID: "cus_existing"}
	svc := billing.NewService(store, &fakeStripe{})

	resp, err := svc.Ensure(context.Background(), billing.EnsureRequest{UserID: userID})

	require.NoError(t, err)
	require.False(t, resp.Ready())
	require.Equal(t, []string{billing.MissingPaymentMethod}, resp.Missing)
}

func TestEnsure_AccountAndPM_Ready(t *testing.T) {
	store := newFakeStore()
	userID := uuid.New()
	accountID := uuid.New()
	store.accountsByUser[userID] = fakeAccount{id: accountID, stripeCustomerID: "cus_existing"}
	store.hasUsablePM[accountID] = true
	svc := billing.NewService(store, &fakeStripe{})

	resp, err := svc.Ensure(context.Background(), billing.EnsureRequest{UserID: userID})

	require.NoError(t, err)
	require.True(t, resp.Ready())
	require.Empty(t, resp.Missing)
}

func TestEnsure_NilUserID_ReturnsInvalidInput(t *testing.T) {
	svc := billing.NewService(newFakeStore(), &fakeStripe{})

	_, err := svc.Ensure(context.Background(), billing.EnsureRequest{UserID: uuid.Nil})

	require.Error(t, err)
	var be *billing.Error
	require.ErrorAs(t, err, &be)
	require.Equal(t, billing.CodeInvalidInput, be.Code)
}

func TestEnsure_StoreError_BecomesInternal(t *testing.T) {
	store := newFakeStore()
	store.errAccountByUser = errors.New("conn dropped")
	svc := billing.NewService(store, &fakeStripe{})

	_, err := svc.Ensure(context.Background(), billing.EnsureRequest{UserID: uuid.New()})

	require.Error(t, err)
	var be *billing.Error
	require.ErrorAs(t, err, &be)
	require.Equal(t, billing.CodeInternal, be.Code)
}

func TestPrepareAddPaymentMethod_FirstTime_CreatesCustomerAndSetupIntent(t *testing.T) {
	store := newFakeStore()
	stripeFake := &fakeStripe{}
	svc := billing.NewService(store, stripeFake)
	userID := uuid.New()

	resp, err := svc.PrepareAddPaymentMethod(context.Background(), billing.PrepareAddPaymentMethodRequest{UserID: userID})

	require.NoError(t, err)
	require.NotEqual(t, uuid.Nil, resp.BillingAccountID)
	require.NotEmpty(t, resp.SetupIntentClientSecret)
	require.Len(t, stripeFake.createdCustomers, 1, "should create Stripe Customer on first call")
	require.NotEmpty(t, store.accountsByUser[userID].stripeCustomerID)
}

func TestPrepareAddPaymentMethod_SecondTime_ReusesCustomerAndMintsNewIntent(t *testing.T) {
	store := newFakeStore()
	stripeFake := &fakeStripe{}
	svc := billing.NewService(store, stripeFake)
	userID := uuid.New()

	_, err := svc.PrepareAddPaymentMethod(context.Background(), billing.PrepareAddPaymentMethodRequest{UserID: userID})
	require.NoError(t, err)
	resp2, err := svc.PrepareAddPaymentMethod(context.Background(), billing.PrepareAddPaymentMethodRequest{UserID: userID})
	require.NoError(t, err)

	require.Len(t, stripeFake.createdCustomers, 1, "should NOT create a second Stripe Customer")
	require.NotEmpty(t, resp2.SetupIntentClientSecret, "still mints a fresh SetupIntent")
}

func TestPrepareAddPaymentMethod_StripeCustomerFails_ReturnsStripeError(t *testing.T) {
	store := newFakeStore()
	stripeFake := &fakeStripe{errCreateCustomer: errors.New("stripe down")}
	svc := billing.NewService(store, stripeFake)

	_, err := svc.PrepareAddPaymentMethod(context.Background(), billing.PrepareAddPaymentMethodRequest{UserID: uuid.New()})

	require.Error(t, err)
	var be *billing.Error
	require.ErrorAs(t, err, &be)
	require.Equal(t, billing.CodeStripeError, be.Code)
}

func TestPrepareAddPaymentMethod_NilUserID_InvalidInput(t *testing.T) {
	svc := billing.NewService(newFakeStore(), &fakeStripe{})

	_, err := svc.PrepareAddPaymentMethod(context.Background(), billing.PrepareAddPaymentMethodRequest{UserID: uuid.Nil})

	require.Error(t, err)
	var be *billing.Error
	require.ErrorAs(t, err, &be)
	require.Equal(t, billing.CodeInvalidInput, be.Code)
}

func TestGetPaymentMethods_NoAccount_EmptySlice(t *testing.T) {
	svc := billing.NewService(newFakeStore(), &fakeStripe{})

	resp, err := svc.GetPaymentMethods(context.Background(), billing.GetPaymentMethodsRequest{UserID: uuid.New()})

	require.NoError(t, err)
	require.NotNil(t, resp.PaymentMethods)
	require.Empty(t, resp.PaymentMethods)
}

func TestGetPaymentMethods_HasMethods_ReturnsAll(t *testing.T) {
	store := newFakeStore()
	userID := uuid.New()
	accountID := uuid.New()
	store.accountsByUser[userID] = fakeAccount{id: accountID, stripeCustomerID: "cus_x"}
	store.paymentMethodsBy[accountID] = []billing.PaymentMethod{
		{ID: uuid.New(), StripePaymentMethodID: "pm_1", Brand: "visa", Last4: "4242", ExpMonth: 12, ExpYear: 2029, IsDefault: true},
		{ID: uuid.New(), StripePaymentMethodID: "pm_2", Brand: "mastercard", Last4: "5454", ExpMonth: 6, ExpYear: 2028, IsDefault: false},
	}
	svc := billing.NewService(store, &fakeStripe{})

	resp, err := svc.GetPaymentMethods(context.Background(), billing.GetPaymentMethodsRequest{UserID: userID})

	require.NoError(t, err)
	require.Len(t, resp.PaymentMethods, 2)
	require.True(t, resp.PaymentMethods[0].IsDefault)
}
