package billing_test

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	stripego "github.com/stripe/stripe-go/v85"

	"github.com/mirrorstack-ai/billing-engine/internal/account/billing"
	billingstripe "github.com/mirrorstack-ai/billing-engine/internal/shared/stripe"
)

// --- in-memory Store fake -------------------------------------------------

type fakeStore struct {
	accountsByUser   map[uuid.UUID]fakeAccount
	hasUsablePM      map[uuid.UUID]bool
	paymentMethodsBy map[uuid.UUID][]billing.PaymentMethod
	addCardRequests  map[uuid.UUID]*fakeAddCardRequest

	// PM-target lookups for detach / set-default, keyed by payment method id.
	pmTargets map[uuid.UUID]pmTarget

	// Injected failures (set per-test as needed).
	errEnsureAccount        error
	errSetStripeCustomer    error
	errAccountByUser        error
	errHasUsablePaymentMx   error
	errListPaymentMethods   error
	errPaymentMethodTarget  error
	errInsertAddCardRequest error
	errSetSetupIntent       error
	errGetAddCardRequest    error
}

type fakeAddCardRequest struct {
	accountID     uuid.UUID
	setupIntentID string
	status        billing.AddCardStatus
	paymentMethod *billing.PaymentMethod
}

type fakeAccount struct {
	id               uuid.UUID
	stripeCustomerID string
}

type pmTarget struct {
	stripePMID       string
	stripeCustomerID string
	isDefault        bool
}

func newFakeStore() *fakeStore {
	return &fakeStore{
		accountsByUser:   map[uuid.UUID]fakeAccount{},
		hasUsablePM:      map[uuid.UUID]bool{},
		paymentMethodsBy: map[uuid.UUID][]billing.PaymentMethod{},
		pmTargets:        map[uuid.UUID]pmTarget{},
		addCardRequests:  map[uuid.UUID]*fakeAddCardRequest{},
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

func (s *fakeStore) PaymentMethodTarget(_ context.Context, _ uuid.UUID, paymentMethodID uuid.UUID) (string, string, bool, bool, error) {
	if s.errPaymentMethodTarget != nil {
		return "", "", false, false, s.errPaymentMethodTarget
	}
	t, ok := s.pmTargets[paymentMethodID]
	if !ok {
		return "", "", false, false, nil
	}
	return t.stripePMID, t.stripeCustomerID, t.isDefault, true, nil
}

func (s *fakeStore) InsertAddCardRequest(_ context.Context, accountID uuid.UUID) (uuid.UUID, error) {
	if s.errInsertAddCardRequest != nil {
		return uuid.Nil, s.errInsertAddCardRequest
	}
	id := uuid.New()
	s.addCardRequests[id] = &fakeAddCardRequest{
		accountID: accountID,
		status:    billing.AddCardStatusPending,
	}
	return id, nil
}

func (s *fakeStore) SetAddCardRequestSetupIntent(_ context.Context, requestID uuid.UUID, setupIntentID string) error {
	if s.errSetSetupIntent != nil {
		return s.errSetSetupIntent
	}
	if r, ok := s.addCardRequests[requestID]; ok && r.status == billing.AddCardStatusPending {
		r.setupIntentID = setupIntentID
	}
	return nil
}

func (s *fakeStore) GetAddCardRequest(_ context.Context, requestID, accountID uuid.UUID) (*billing.AddCardRequestStatus, error) {
	if s.errGetAddCardRequest != nil {
		return nil, s.errGetAddCardRequest
	}
	r, ok := s.addCardRequests[requestID]
	if !ok || r.accountID != accountID {
		return nil, nil
	}
	return &billing.AddCardRequestStatus{Status: r.status, PaymentMethod: r.paymentMethod}, nil
}

// --- in-memory Stripe Client fake ----------------------------------------

type fakeStripe struct {
	createdCustomers         []string
	createdCustomerEmails    []string
	updatedEmails            []string // "customerID=email" per UpdateCustomerEmail call
	detached                 []string // pm ids passed to DetachPaymentMethod
	defaultsSet              []string // "customerID=pmID" per SetDefaultPaymentMethod
	customerIDToReturn       string
	checkoutSecretToReturn   string
	setupIntentIDToReturn    string
	errCreateCustomer        error
	errCreateCheckoutSession error
	errUpdateCustomerEmail   error
	errDetach                error
	errSetDefault            error
}

func (f *fakeStripe) CreateCustomer(_ context.Context, billingAccountID, email string) (*stripego.Customer, error) {
	if f.errCreateCustomer != nil {
		return nil, f.errCreateCustomer
	}
	f.createdCustomers = append(f.createdCustomers, billingAccountID)
	f.createdCustomerEmails = append(f.createdCustomerEmails, email)
	id := f.customerIDToReturn
	if id == "" {
		id = "cus_test_" + billingAccountID[:8]
	}
	return &stripego.Customer{ID: id}, nil
}

func (f *fakeStripe) UpdateCustomerEmail(_ context.Context, stripeCustomerID, email string) error {
	if f.errUpdateCustomerEmail != nil {
		return f.errUpdateCustomerEmail
	}
	f.updatedEmails = append(f.updatedEmails, stripeCustomerID+"="+email)
	return nil
}

func (f *fakeStripe) CreateCheckoutSession(_ context.Context, _, _ string) (*stripego.CheckoutSession, error) {
	if f.errCreateCheckoutSession != nil {
		return nil, f.errCreateCheckoutSession
	}
	cs := f.checkoutSecretToReturn
	if cs == "" {
		cs = "cs_test_secret_xyz"
	}
	siID := f.setupIntentIDToReturn
	if siID == "" {
		siID = "seti_test_xyz"
	}
	return &stripego.CheckoutSession{
		ClientSecret: cs,
		SetupIntent:  &stripego.SetupIntent{ID: siID},
	}, nil
}

func (f *fakeStripe) DetachPaymentMethod(_ context.Context, stripePaymentMethodID string) error {
	if f.errDetach != nil {
		return f.errDetach
	}
	f.detached = append(f.detached, stripePaymentMethodID)
	return nil
}

func (f *fakeStripe) SetDefaultPaymentMethod(_ context.Context, stripeCustomerID, stripePaymentMethodID string) error {
	if f.errSetDefault != nil {
		return f.errSetDefault
	}
	f.defaultsSet = append(f.defaultsSet, stripeCustomerID+"="+stripePaymentMethodID)
	return nil
}

// CreateInvoiceItem / CreateInvoice are the PR #6 charge methods. The billing
// package never calls them (the charge cycle lives in internal/account/cycle),
// so these are panic stubs present only to keep this fake satisfying the
// widened billingstripe.Client interface.
func (f *fakeStripe) CreateInvoiceItem(context.Context, string, int64, string, string, string) (billingstripe.InvoiceItem, error) {
	panic("CreateInvoiceItem must not be called by the billing package")
}

func (f *fakeStripe) CreateInvoice(context.Context, string, bool, string) (billingstripe.Invoice, error) {
	panic("CreateInvoice must not be called by the billing package")
}

// --- tests ----------------------------------------------------------------

func TestEnsure_NoAccount_ReturnsMissingBillingAccount(t *testing.T) {
	svc := billing.NewService(newFakeStore(), &fakeStripe{}, "")

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
	svc := billing.NewService(store, &fakeStripe{}, "")

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
	svc := billing.NewService(store, &fakeStripe{}, "")

	resp, err := svc.Ensure(context.Background(), billing.EnsureRequest{UserID: userID})

	require.NoError(t, err)
	require.True(t, resp.Ready())
	require.Empty(t, resp.Missing)
}

func TestEnsure_NilUserID_ReturnsInvalidInput(t *testing.T) {
	svc := billing.NewService(newFakeStore(), &fakeStripe{}, "")

	_, err := svc.Ensure(context.Background(), billing.EnsureRequest{UserID: uuid.Nil})

	require.Error(t, err)
	var be *billing.Error
	require.ErrorAs(t, err, &be)
	require.Equal(t, billing.CodeInvalidInput, be.Code)
}

func TestEnsure_StoreError_BecomesInternal(t *testing.T) {
	store := newFakeStore()
	store.errAccountByUser = errors.New("conn dropped")
	svc := billing.NewService(store, &fakeStripe{}, "")

	_, err := svc.Ensure(context.Background(), billing.EnsureRequest{UserID: uuid.New()})

	require.Error(t, err)
	var be *billing.Error
	require.ErrorAs(t, err, &be)
	require.Equal(t, billing.CodeInternal, be.Code)
}

func TestEnsure_RequireSubscription_AlwaysMissing_v1Stub(t *testing.T) {
	// v1 has no subscriptions table; the handler stubs subscription as
	// always-missing regardless of state.
	store := newFakeStore()
	userID := uuid.New()
	accountID := uuid.New()
	store.accountsByUser[userID] = fakeAccount{id: accountID, stripeCustomerID: "cus_x"}
	store.hasUsablePM[accountID] = true // even with a PM, subscription is missing
	svc := billing.NewService(store, &fakeStripe{}, "")

	resp, err := svc.Ensure(context.Background(), billing.EnsureRequest{
		UserID:  userID,
		Require: []billing.Capability{billing.RequireSubscription},
	})

	require.NoError(t, err)
	require.False(t, resp.Ready())
	require.Equal(t, []string{billing.MissingSubscription}, resp.Missing)
}

func TestEnsure_RequirePaymentMethodAndSubscription_BothRequired(t *testing.T) {
	// PM met, subscription stubbed-missing → Missing should be just
	// ["subscription"]. Order is deterministic (PM before subscription).
	store := newFakeStore()
	userID := uuid.New()
	accountID := uuid.New()
	store.accountsByUser[userID] = fakeAccount{id: accountID, stripeCustomerID: "cus_x"}
	store.hasUsablePM[accountID] = true
	svc := billing.NewService(store, &fakeStripe{}, "")

	resp, err := svc.Ensure(context.Background(), billing.EnsureRequest{
		UserID:  userID,
		Require: []billing.Capability{billing.RequirePaymentMethod, billing.RequireSubscription},
	})

	require.NoError(t, err)
	require.False(t, resp.Ready())
	require.Equal(t, []string{billing.MissingSubscription}, resp.Missing)
}

func TestEnsure_RequireBoth_NoPM_BothMissing(t *testing.T) {
	store := newFakeStore()
	userID := uuid.New()
	accountID := uuid.New()
	store.accountsByUser[userID] = fakeAccount{id: accountID, stripeCustomerID: "cus_x"}
	// hasUsablePM[accountID] is false (default zero value)
	svc := billing.NewService(store, &fakeStripe{}, "")

	resp, err := svc.Ensure(context.Background(), billing.EnsureRequest{
		UserID:  userID,
		Require: []billing.Capability{billing.RequirePaymentMethod, billing.RequireSubscription},
	})

	require.NoError(t, err)
	require.False(t, resp.Ready())
	// Deterministic order: payment_method first, then subscription.
	require.Equal(t, []string{billing.MissingPaymentMethod, billing.MissingSubscription}, resp.Missing)
}

func TestEnsure_NoAccountRow_BillingAccountAlone(t *testing.T) {
	// Even when Require includes subscription, a missing accounts row
	// short-circuits to just ["billing_account"] — there's nothing else
	// the handler can usefully check.
	svc := billing.NewService(newFakeStore(), &fakeStripe{}, "")

	resp, err := svc.Ensure(context.Background(), billing.EnsureRequest{
		UserID:  uuid.New(),
		Require: []billing.Capability{billing.RequirePaymentMethod, billing.RequireSubscription},
	})

	require.NoError(t, err)
	require.Equal(t, []string{billing.MissingBillingAccount}, resp.Missing)
}

func TestEnsure_UnknownRequire_InvalidInput(t *testing.T) {
	svc := billing.NewService(newFakeStore(), &fakeStripe{}, "")

	_, err := svc.Ensure(context.Background(), billing.EnsureRequest{
		UserID:  uuid.New(),
		Require: []billing.Capability{"some_future_thing"},
	})

	require.Error(t, err)
	var be *billing.Error
	require.ErrorAs(t, err, &be)
	require.Equal(t, billing.CodeInvalidInput, be.Code)
}

func TestPrepareAddPaymentMethod_FirstTime_CreatesCustomerAndSetupIntent(t *testing.T) {
	store := newFakeStore()
	stripeFake := &fakeStripe{}
	svc := billing.NewService(store, stripeFake, "")
	userID := uuid.New()

	resp, err := svc.PrepareAddPaymentMethod(context.Background(), billing.PrepareAddPaymentMethodRequest{UserID: userID, Email: "user@example.com"})

	require.NoError(t, err)
	require.NotEqual(t, uuid.Nil, resp.BillingAccountID)
	require.NotEmpty(t, resp.ClientSecret)
	require.Len(t, stripeFake.createdCustomers, 1, "should create Stripe Customer on first call")
	require.Equal(t, []string{"user@example.com"}, stripeFake.createdCustomerEmails, "account email should be set on the new Customer")
	require.NotEmpty(t, store.accountsByUser[userID].stripeCustomerID)
}

func TestPrepareAddPaymentMethod_SecondTime_ReusesCustomerAndMintsNewIntent(t *testing.T) {
	store := newFakeStore()
	stripeFake := &fakeStripe{}
	svc := billing.NewService(store, stripeFake, "")
	userID := uuid.New()

	_, err := svc.PrepareAddPaymentMethod(context.Background(), billing.PrepareAddPaymentMethodRequest{UserID: userID})
	require.NoError(t, err)
	resp2, err := svc.PrepareAddPaymentMethod(context.Background(), billing.PrepareAddPaymentMethodRequest{UserID: userID})
	require.NoError(t, err)

	require.Len(t, stripeFake.createdCustomers, 1, "should NOT create a second Stripe Customer")
	require.NotEmpty(t, resp2.ClientSecret, "still mints a fresh Checkout Session")
}

func TestPrepareAddPaymentMethod_ExistingCustomer_BackfillsEmail(t *testing.T) {
	// A Customer created before email capture must have its email
	// backfilled so the setup-mode Checkout Session can be confirmed.
	store := newFakeStore()
	userID := uuid.New()
	store.accountsByUser[userID] = fakeAccount{id: uuid.New(), stripeCustomerID: "cus_existing"}
	stripeFake := &fakeStripe{}
	svc := billing.NewService(store, stripeFake, "")

	_, err := svc.PrepareAddPaymentMethod(context.Background(), billing.PrepareAddPaymentMethodRequest{UserID: userID, Email: "user@example.com"})

	require.NoError(t, err)
	require.Empty(t, stripeFake.createdCustomers, "should reuse the existing Customer")
	require.Equal(t, []string{"cus_existing=user@example.com"}, stripeFake.updatedEmails, "existing Customer email should be backfilled")
}

func TestPrepareAddPaymentMethod_BackfillEmailFails_ReturnsStripeError(t *testing.T) {
	store := newFakeStore()
	userID := uuid.New()
	store.accountsByUser[userID] = fakeAccount{id: uuid.New(), stripeCustomerID: "cus_existing"}
	stripeFake := &fakeStripe{errUpdateCustomerEmail: errors.New("stripe down")}
	svc := billing.NewService(store, stripeFake, "")

	_, err := svc.PrepareAddPaymentMethod(context.Background(), billing.PrepareAddPaymentMethodRequest{UserID: userID, Email: "user@example.com"})

	require.Error(t, err)
	var be *billing.Error
	require.ErrorAs(t, err, &be)
	require.Equal(t, billing.CodeStripeError, be.Code)
}

func TestPrepareAddPaymentMethod_StripeCustomerFails_ReturnsStripeError(t *testing.T) {
	store := newFakeStore()
	stripeFake := &fakeStripe{errCreateCustomer: errors.New("stripe down")}
	svc := billing.NewService(store, stripeFake, "")

	_, err := svc.PrepareAddPaymentMethod(context.Background(), billing.PrepareAddPaymentMethodRequest{UserID: uuid.New()})

	require.Error(t, err)
	var be *billing.Error
	require.ErrorAs(t, err, &be)
	require.Equal(t, billing.CodeStripeError, be.Code)
}

func TestPrepareAddPaymentMethod_SetStripeCustomerFails_ReturnsInternal(t *testing.T) {
	// Stripe Customer creates OK but persisting the ID fails. This is a
	// DB error, not a Stripe error — INTERNAL is the honest code.
	store := newFakeStore()
	store.errSetStripeCustomer = errors.New("pool exhausted")
	stripeFake := &fakeStripe{}
	svc := billing.NewService(store, stripeFake, "")

	_, err := svc.PrepareAddPaymentMethod(context.Background(), billing.PrepareAddPaymentMethodRequest{UserID: uuid.New()})

	require.Error(t, err)
	var be *billing.Error
	require.ErrorAs(t, err, &be)
	require.Equal(t, billing.CodeInternal, be.Code)
}

func TestPrepareAddPaymentMethod_NilUserID_InvalidInput(t *testing.T) {
	svc := billing.NewService(newFakeStore(), &fakeStripe{}, "")

	_, err := svc.PrepareAddPaymentMethod(context.Background(), billing.PrepareAddPaymentMethodRequest{UserID: uuid.Nil})

	require.Error(t, err)
	var be *billing.Error
	require.ErrorAs(t, err, &be)
	require.Equal(t, billing.CodeInvalidInput, be.Code)
}

func TestGetPaymentMethods_NoAccount_EmptySlice(t *testing.T) {
	svc := billing.NewService(newFakeStore(), &fakeStripe{}, "")

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
	svc := billing.NewService(store, &fakeStripe{}, "")

	resp, err := svc.GetPaymentMethods(context.Background(), billing.GetPaymentMethodsRequest{UserID: userID})

	require.NoError(t, err)
	require.Len(t, resp.PaymentMethods, 2)
	require.True(t, resp.PaymentMethods[0].IsDefault)
}

func TestDetachPaymentMethod_OwnedPM_DetachesFromStripe(t *testing.T) {
	store := newFakeStore()
	pmID := uuid.New()
	store.pmTargets[pmID] = pmTarget{stripePMID: "pm_x", stripeCustomerID: "cus_x"}
	stripeFake := &fakeStripe{}
	svc := billing.NewService(store, stripeFake, "")

	_, err := svc.DetachPaymentMethod(context.Background(),
		billing.DetachPaymentMethodRequest{UserID: uuid.New(), PaymentMethodID: pmID})

	require.NoError(t, err)
	require.Equal(t, []string{"pm_x"}, stripeFake.detached)
}

func TestDetachPaymentMethod_NotOwned_ReturnsNotFound(t *testing.T) {
	svc := billing.NewService(newFakeStore(), &fakeStripe{}, "")

	_, err := svc.DetachPaymentMethod(context.Background(),
		billing.DetachPaymentMethodRequest{UserID: uuid.New(), PaymentMethodID: uuid.New()})

	var be *billing.Error
	require.ErrorAs(t, err, &be)
	require.Equal(t, billing.CodeNotFound, be.Code)
}

func TestDetachPaymentMethod_NilIDs_InvalidInput(t *testing.T) {
	svc := billing.NewService(newFakeStore(), &fakeStripe{}, "")

	_, err := svc.DetachPaymentMethod(context.Background(), billing.DetachPaymentMethodRequest{})

	var be *billing.Error
	require.ErrorAs(t, err, &be)
	require.Equal(t, billing.CodeInvalidInput, be.Code)
}

func TestDetachPaymentMethod_DefaultPM_Rejected(t *testing.T) {
	// Removing the default card on Stripe clears
	// invoice_settings.default_payment_method (account ends up with no
	// default → next invoice fails with no_payment_method). The service
	// must refuse with INVALID_INPUT so a direct API caller can't slip
	// past the UI guard. The Stripe API is not called.
	store := newFakeStore()
	pmID := uuid.New()
	store.pmTargets[pmID] = pmTarget{stripePMID: "pm_def", stripeCustomerID: "cus_z", isDefault: true}
	stripeFake := &fakeStripe{}
	svc := billing.NewService(store, stripeFake, "")

	_, err := svc.DetachPaymentMethod(context.Background(),
		billing.DetachPaymentMethodRequest{UserID: uuid.New(), PaymentMethodID: pmID})

	var be *billing.Error
	require.ErrorAs(t, err, &be)
	require.Equal(t, billing.CodeInvalidInput, be.Code)
	require.Contains(t, be.Message, "default")
	require.Empty(t, stripeFake.detached, "Stripe must not be called when the request is rejected up-front")
}

func TestSetDefaultPaymentMethod_OwnedPM_SetsCustomerDefault(t *testing.T) {
	store := newFakeStore()
	pmID := uuid.New()
	store.pmTargets[pmID] = pmTarget{stripePMID: "pm_y", stripeCustomerID: "cus_y"}
	stripeFake := &fakeStripe{}
	svc := billing.NewService(store, stripeFake, "")

	_, err := svc.SetDefaultPaymentMethod(context.Background(),
		billing.SetDefaultPaymentMethodRequest{UserID: uuid.New(), PaymentMethodID: pmID})

	require.NoError(t, err)
	require.Equal(t, []string{"cus_y=pm_y"}, stripeFake.defaultsSet)
}

func TestSetDefaultPaymentMethod_NotOwned_ReturnsNotFound(t *testing.T) {
	svc := billing.NewService(newFakeStore(), &fakeStripe{}, "")

	_, err := svc.SetDefaultPaymentMethod(context.Background(),
		billing.SetDefaultPaymentMethodRequest{UserID: uuid.New(), PaymentMethodID: uuid.New()})

	var be *billing.Error
	require.ErrorAs(t, err, &be)
	require.Equal(t, billing.CodeNotFound, be.Code)
}
