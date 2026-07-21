package billing_test

import (
	"context"
	"errors"
	"testing"
	"time"

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
	hasUnpaidInvoice map[uuid.UUID]bool
	paymentMethodsBy map[uuid.UUID][]billing.PaymentMethod
	serviceSignals   map[uuid.UUID]billing.ServiceSignals
	addCardRequests  map[uuid.UUID]*fakeAddCardRequest

	// PM-target lookups for detach / set-default, keyed by payment method id.
	pmTargets map[uuid.UUID]pmTarget

	// org-billing state (migration 041). accountsByOrg models the org account
	// rows (existence — AccountByOrg / EnsureOrgAccount); fundedOrgs the
	// designation+activation gate ResolveOrgFundedAccount reads; fundingOf the
	// sponsor funding hop (absent → identity); orgPMTargets the org twin of
	// pmTargets (PaymentMethodTargetForOrg).
	accountsByOrg map[uuid.UUID]fakeAccount
	fundedOrgs    map[uuid.UUID]bool
	fundingOf     map[uuid.UUID]uuid.UUID
	orgPMTargets  map[uuid.UUID]pmTarget

	// unpaid-invoice surface (funding-gates wave). unpaidCount feeds
	// GetServiceStatus's gate 4; unpaidInvoices the ListUnpaidInvoices read;
	// payTargets the (invoice, account)-scoped PayInvoice ownership lookup;
	// hasUsableDefPM the PayInvoice card gate; stripeCustomerOf the
	// AccountStripeCustomer read behind the gate/charge coherence check.
	unpaidCount      map[uuid.UUID]int
	unpaidInvoices   map[uuid.UUID][]billing.UnpaidInvoiceRow
	payTargets       map[uuid.UUID]fakePayTarget
	hasUsableDefPM   map[uuid.UUID]bool
	stripeCustomerOf map[uuid.UUID]string
	syncedMirrors    []billingstripe.Invoice

	// credit-wallet state. These maps model the service-facing projections,
	// not the SQL implementation details: an idempotency key resolves one
	// immutable ledger record and purchases are separately owner-scoped.
	creditStanding           map[uuid.UUID]billing.CreditStandingRow
	creditLedgerEntries      map[uuid.UUID][]billing.CreditLedgerEntry
	creditLedgerByKey        map[string]billing.CreditLedgerRecord
	creditPurchases          map[uuid.UUID]billing.CreditPurchaseRow
	creditAutoTopUps         map[uuid.UUID]billing.AutoTopUpConfig
	distributorCustomers     map[distributorCustomerKey]uuid.UUID
	distributorStates        map[uuid.UUID][]billing.DistributorCustomerState
	creditPurchaseCreates    int
	creditIdempotencyReads   int
	distributorRelationReads int
	creditGrantInserts       int

	// Injected failures (set per-test as needed).
	errEnsureAccount        error
	errSetStripeCustomer    error
	errAccountByUser        error
	errHasUsablePaymentMx   error
	errHasUnpaidInvoice     error
	errListPaymentMethods   error
	errServiceSignals       error
	errPaymentMethodTarget  error
	errInsertAddCardRequest error
	errSetSetupIntent       error
	errGetAddCardRequest    error
	errUnpaidCount          error
	errListUnpaid           error
	errInvoiceForPayment    error
	errHasUsableDefPM       error
	errAccountStripeCust    error
	errSyncInvoiceMirror    error
}

// fakePayTarget models one invoices-mirror row for InvoiceForPayment: the
// owning account (the ownership scope) plus the pay projection.
type fakePayTarget struct {
	accountID uuid.UUID
	target    billing.InvoicePayTarget
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
		accountsByUser:       map[uuid.UUID]fakeAccount{},
		hasUsablePM:          map[uuid.UUID]bool{},
		hasUnpaidInvoice:     map[uuid.UUID]bool{},
		paymentMethodsBy:     map[uuid.UUID][]billing.PaymentMethod{},
		serviceSignals:       map[uuid.UUID]billing.ServiceSignals{},
		pmTargets:            map[uuid.UUID]pmTarget{},
		addCardRequests:      map[uuid.UUID]*fakeAddCardRequest{},
		accountsByOrg:        map[uuid.UUID]fakeAccount{},
		fundedOrgs:           map[uuid.UUID]bool{},
		fundingOf:            map[uuid.UUID]uuid.UUID{},
		orgPMTargets:         map[uuid.UUID]pmTarget{},
		unpaidCount:          map[uuid.UUID]int{},
		unpaidInvoices:       map[uuid.UUID][]billing.UnpaidInvoiceRow{},
		payTargets:           map[uuid.UUID]fakePayTarget{},
		hasUsableDefPM:       map[uuid.UUID]bool{},
		stripeCustomerOf:     map[uuid.UUID]string{},
		creditStanding:       map[uuid.UUID]billing.CreditStandingRow{},
		creditLedgerEntries:  map[uuid.UUID][]billing.CreditLedgerEntry{},
		creditLedgerByKey:    map[string]billing.CreditLedgerRecord{},
		creditPurchases:      map[uuid.UUID]billing.CreditPurchaseRow{},
		creditAutoTopUps:     map[uuid.UUID]billing.AutoTopUpConfig{},
		distributorCustomers: map[distributorCustomerKey]uuid.UUID{},
		distributorStates:    map[uuid.UUID][]billing.DistributorCustomerState{},
	}
}

type distributorCustomerKey struct {
	distributorOrgID uuid.UUID
	customerOrgID    uuid.UUID
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

func (s *fakeStore) HasUnpaidInvoice(_ context.Context, accountID uuid.UUID) (bool, error) {
	if s.errHasUnpaidInvoice != nil {
		return false, s.errHasUnpaidInvoice
	}
	return s.hasUnpaidInvoice[accountID], nil
}

func (s *fakeStore) ListPaymentMethods(_ context.Context, accountID uuid.UUID) ([]billing.PaymentMethod, error) {
	if s.errListPaymentMethods != nil {
		return nil, s.errListPaymentMethods
	}
	if pms, ok := s.paymentMethodsBy[accountID]; ok {
		return pms, nil
	}
	return []billing.PaymentMethod{}, nil // Store contract: empty slice, never nil
}

func (s *fakeStore) ServiceBlockSignals(_ context.Context, accountID uuid.UUID) (billing.ServiceSignals, error) {
	if s.errServiceSignals != nil {
		return billing.ServiceSignals{}, s.errServiceSignals
	}
	return s.serviceSignals[accountID], nil
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

func (s *fakeStore) EnsureOrgAccount(_ context.Context, orgID uuid.UUID) (uuid.UUID, string, error) {
	if a, ok := s.accountsByOrg[orgID]; ok {
		return a.id, a.stripeCustomerID, nil
	}
	a := fakeAccount{id: uuid.New()}
	s.accountsByOrg[orgID] = a
	return a.id, "", nil
}

func (s *fakeStore) AccountByOrg(_ context.Context, orgID uuid.UUID) (uuid.UUID, bool, error) {
	a, ok := s.accountsByOrg[orgID]
	if !ok {
		return uuid.Nil, false, nil
	}
	return a.id, true, nil
}

func (s *fakeStore) ResolveOrgFundedAccount(_ context.Context, orgID uuid.UUID) (uuid.UUID, bool, error) {
	// Row EXISTENCE is not enough — the funded gate (designation + activation)
	// is modeled as an explicit flag.
	a, ok := s.accountsByOrg[orgID]
	if !ok || !s.fundedOrgs[orgID] {
		return uuid.Nil, false, nil
	}
	return a.id, true, nil
}

func (s *fakeStore) ChargeFundingAccount(_ context.Context, accountID uuid.UUID) (uuid.UUID, error) {
	// Identity unless a sponsor funding mapping is configured (org D1 hop).
	if funding, ok := s.fundingOf[accountID]; ok {
		return funding, nil
	}
	return accountID, nil
}

func (s *fakeStore) PaymentMethodTargetForOrg(_ context.Context, _, paymentMethodID uuid.UUID) (string, string, bool, bool, error) {
	if s.errPaymentMethodTarget != nil {
		return "", "", false, false, s.errPaymentMethodTarget
	}
	t, ok := s.orgPMTargets[paymentMethodID]
	if !ok {
		return "", "", false, false, nil
	}
	return t.stripePMID, t.stripeCustomerID, t.isDefault, true, nil
}

func (s *fakeStore) UnpaidInvoiceCount(_ context.Context, accountID uuid.UUID) (int, error) {
	if s.errUnpaidCount != nil {
		return 0, s.errUnpaidCount
	}
	return s.unpaidCount[accountID], nil
}

func (s *fakeStore) ListUnpaidInvoices(_ context.Context, accountID uuid.UUID) ([]billing.UnpaidInvoiceRow, error) {
	if s.errListUnpaid != nil {
		return nil, s.errListUnpaid
	}
	if rows, ok := s.unpaidInvoices[accountID]; ok {
		return rows, nil
	}
	return []billing.UnpaidInvoiceRow{}, nil
}

func (s *fakeStore) InvoiceForPayment(_ context.Context, invoiceID, accountID uuid.UUID) (billing.InvoicePayTarget, bool, error) {
	if s.errInvoiceForPayment != nil {
		return billing.InvoicePayTarget{}, false, s.errInvoiceForPayment
	}
	t, ok := s.payTargets[invoiceID]
	if !ok || t.accountID != accountID {
		return billing.InvoicePayTarget{}, false, nil
	}
	return t.target, true, nil
}

func (s *fakeStore) HasUsableDefaultPM(_ context.Context, accountID uuid.UUID) (bool, error) {
	if s.errHasUsableDefPM != nil {
		return false, s.errHasUsableDefPM
	}
	return s.hasUsableDefPM[accountID], nil
}

func (s *fakeStore) AccountStripeCustomer(_ context.Context, accountID uuid.UUID) (string, error) {
	if s.errAccountStripeCust != nil {
		return "", s.errAccountStripeCust
	}
	return s.stripeCustomerOf[accountID], nil
}

func (s *fakeStore) SyncInvoiceMirror(_ context.Context, inv billingstripe.Invoice) (bool, error) {
	s.syncedMirrors = append(s.syncedMirrors, inv)
	if s.errSyncInvoiceMirror != nil {
		return false, s.errSyncInvoiceMirror
	}
	return true, nil
}

func (s *fakeStore) CreditStanding(_ context.Context, accountID uuid.UUID) (billing.CreditStandingRow, error) {
	return s.creditStanding[accountID], nil
}

func (s *fakeStore) ListCreditLedger(_ context.Context, accountID uuid.UUID, limit int32, _ *billing.CreditLedgerCursor) ([]billing.CreditLedgerEntry, error) {
	entries := s.creditLedgerEntries[accountID]
	if int32(len(entries)) > limit {
		entries = entries[:limit]
	}
	return entries, nil
}

func (s *fakeStore) CreditLedgerByIdempotencyKey(_ context.Context, key string) (billing.CreditLedgerRecord, bool, error) {
	s.creditIdempotencyReads++
	record, ok := s.creditLedgerByKey[key]
	return record, ok, nil
}

func (s *fakeStore) CreatePendingCreditPurchase(_ context.Context, accountID uuid.UUID, amountMicros int64, idempotencyKey string) (billing.CreditPurchaseRow, error) {
	s.creditPurchaseCreates++
	standing := s.creditStanding[accountID]
	purchase := billing.CreditPurchaseRow{
		ID:                 uuid.New(),
		AccountID:          accountID,
		AmountMicros:       amountMicros,
		Type:               "purchase",
		Status:             "pending",
		BalanceAfterMicros: standing.BalanceMicros + amountMicros,
		Actor:              "self",
		IdempotencyKey:     idempotencyKey,
		CreatedAt:          time.Now().UTC(),
	}
	s.creditPurchases[purchase.ID] = purchase
	s.putCreditPurchaseRecord(purchase)
	return purchase, nil
}

func (s *fakeStore) CreditPurchase(_ context.Context, purchaseID, accountID uuid.UUID) (billing.CreditPurchaseRow, bool, error) {
	purchase, ok := s.creditPurchases[purchaseID]
	if !ok || purchase.AccountID != accountID {
		return billing.CreditPurchaseRow{}, false, nil
	}
	return purchase, true, nil
}

func (s *fakeStore) AttachCreditPurchaseInvoice(_ context.Context, purchaseID, accountID uuid.UUID, stripeInvoiceID, receiptURL string) error {
	purchase, ok := s.creditPurchases[purchaseID]
	if !ok || purchase.AccountID != accountID {
		return errors.New("credit purchase not found")
	}
	if purchase.StripeInvoiceID != "" && purchase.StripeInvoiceID != stripeInvoiceID {
		return errors.New("credit purchase already attached to another invoice")
	}
	purchase.StripeInvoiceID = stripeInvoiceID
	if receiptURL != "" {
		purchase.ReceiptURL = receiptURL
	}
	s.creditPurchases[purchaseID] = purchase
	s.putCreditPurchaseRecord(purchase)
	return nil
}

func (s *fakeStore) FinalizeCreditPurchase(_ context.Context, purchaseID, accountID uuid.UUID, status, receiptURL string) (billing.CreditPurchaseRow, error) {
	purchase, ok := s.creditPurchases[purchaseID]
	if !ok || purchase.AccountID != accountID {
		return billing.CreditPurchaseRow{}, errors.New("credit purchase not found")
	}
	if purchase.Status == "pending" {
		purchase.Status = status
	}
	if receiptURL != "" {
		purchase.ReceiptURL = receiptURL
	}
	s.creditPurchases[purchaseID] = purchase
	s.putCreditPurchaseRecord(purchase)
	return purchase, nil
}

func (s *fakeStore) UpsertCreditAutoTopUp(_ context.Context, accountID uuid.UUID, cfg billing.AutoTopUpConfig) (billing.AutoTopUpConfig, error) {
	s.creditAutoTopUps[accountID] = cfg
	standing := s.creditStanding[accountID]
	standing.AutoTopUp = &cfg
	s.creditStanding[accountID] = standing
	return cfg, nil
}

func (s *fakeStore) SetCreditBillingMode(_ context.Context, accountID uuid.UUID, mode billing.BillingMode, creditLimitMicros int64) error {
	standing := s.creditStanding[accountID]
	standing.BillingMode = mode
	standing.CreditLimitMicros = creditLimitMicros
	s.creditStanding[accountID] = standing
	return nil
}

func (s *fakeStore) DistributorCustomerAccount(_ context.Context, distributorOrgID, customerOrgID uuid.UUID) (uuid.UUID, bool, error) {
	s.distributorRelationReads++
	accountID, ok := s.distributorCustomers[distributorCustomerKey{
		distributorOrgID: distributorOrgID,
		customerOrgID:    customerOrgID,
	}]
	return accountID, ok, nil
}

func (s *fakeStore) ListDistributorCustomerStates(_ context.Context, distributorOrgID uuid.UUID) ([]billing.DistributorCustomerState, error) {
	return s.distributorStates[distributorOrgID], nil
}

func (s *fakeStore) InsertCreditGrant(_ context.Context, accountID uuid.UUID, amountMicros int64, actor, idempotencyKey string, expiresAt *time.Time) (billing.CreditLedgerRecord, error) {
	s.creditGrantInserts++
	standing := s.creditStanding[accountID]
	record := billing.CreditLedgerRecord{
		ID:                 uuid.New(),
		AccountID:          accountID,
		AmountMicros:       amountMicros,
		Type:               "grant",
		Status:             "settled",
		BalanceAfterMicros: standing.BalanceMicros + amountMicros,
		Actor:              actor,
		IdempotencyKey:     idempotencyKey,
		ExpiresAt:          expiresAt,
		CreatedAt:          time.Now().UTC(),
	}
	s.creditLedgerByKey[idempotencyKey] = record
	standing.BalanceMicros = record.BalanceAfterMicros
	s.creditStanding[accountID] = standing
	return record, nil
}

func (s *fakeStore) putCreditPurchaseRecord(purchase billing.CreditPurchaseRow) {
	s.creditLedgerByKey[purchase.IdempotencyKey] = billing.CreditLedgerRecord{
		ID:                 purchase.ID,
		AccountID:          purchase.AccountID,
		AmountMicros:       purchase.AmountMicros,
		Type:               purchase.Type,
		Status:             purchase.Status,
		BalanceAfterMicros: purchase.BalanceAfterMicros,
		Actor:              purchase.Actor,
		IdempotencyKey:     purchase.IdempotencyKey,
		StripeInvoiceID:    purchase.StripeInvoiceID,
		ReceiptURL:         purchase.ReceiptURL,
		CreatedAt:          purchase.CreatedAt,
	}
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
	paidInvoices             []string              // stripeInvoiceID per PayInvoice call
	payInvoiceToReturn       billingstripe.Invoice // zero ID/Status default to stripeInvoiceID/"paid"
	getInvoiceCustomer       string                // CustomerID GetInvoice reports (the invoice's frozen payer)
	getInvoiceStatus         string                // Status GetInvoice reports; "" → "open"
	creditInvoices           map[string]billingstripe.Invoice
	creditInvoiceRefs        map[string]string
	creditDraftCalls         []creditDraftInvoiceCall
	creditItemCalls          []creditInvoiceItemCall
	creditFinalizeCalls      []string
	creditFindCalls          []string
	creditGetCalls           []string
	creditFinalizeStatus     string
	customerNoDefaultPM      bool
	errCreateCustomer        error
	errCreateCheckoutSession error
	errUpdateCustomerEmail   error
	errDetach                error
	errSetDefault            error
	errPayInvoice            error
	errGetInvoice            error
	errGetCustomer           error
	errCreateDraftInvoice    error
	errCreateInvoiceItem     error
	errFinalizeInvoice       error
	errFindInvoice           error
}

type creditDraftInvoiceCall struct {
	customerID     string
	ref            string
	idempotencyKey string
}

type creditInvoiceItemCall struct {
	customerID     string
	invoiceID      string
	amountCents    int64
	currency       string
	idempotencyKey string
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

func (f *fakeStripe) RetrieveCharge(_ context.Context, _ string) (billingstripe.ChargeCardRef, error) {
	return billingstripe.ChargeCardRef{}, nil // unused by the billing service
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

func (f *fakeStripe) GetCustomer(_ context.Context, _ string) (*stripego.Customer, error) {
	if f.errGetCustomer != nil {
		return nil, f.errGetCustomer
	}
	if f.customerNoDefaultPM {
		return &stripego.Customer{}, nil
	}
	return &stripego.Customer{
		InvoiceSettings: &stripego.CustomerInvoiceSettings{
			DefaultPaymentMethod: &stripego.PaymentMethod{ID: "pm_default"},
		},
	}, nil
}

// Credit purchases reuse the same safe draft→pinned-item→finalize Stripe
// sequence as cycle charges. The fake retains resources by invoice id/ref so
// a service retry exercises resource recovery instead of creating duplicates.
func (f *fakeStripe) CreateDraftInvoice(_ context.Context, customerID, ref, idempotencyKey string) (billingstripe.Invoice, error) {
	if f.errCreateDraftInvoice != nil {
		return billingstripe.Invoice{}, f.errCreateDraftInvoice
	}
	f.ensureCreditInvoiceMaps()
	f.creditDraftCalls = append(f.creditDraftCalls, creditDraftInvoiceCall{
		customerID:     customerID,
		ref:            ref,
		idempotencyKey: idempotencyKey,
	})
	invoice := billingstripe.Invoice{
		ID:         "in_credit_" + uuid.New().String(),
		CustomerID: customerID,
		Status:     "draft",
		Currency:   "usd",
	}
	f.creditInvoices[invoice.ID] = invoice
	f.creditInvoiceRefs[ref] = invoice.ID
	return invoice, nil
}

func (f *fakeStripe) CreateInvoiceItem(_ context.Context, customerID, invoiceID string, amountCents int64, currency, _ string, _ billingstripe.LinePeriod, idempotencyKey string) (billingstripe.InvoiceItem, error) {
	if f.errCreateInvoiceItem != nil {
		return billingstripe.InvoiceItem{}, f.errCreateInvoiceItem
	}
	f.creditItemCalls = append(f.creditItemCalls, creditInvoiceItemCall{
		customerID:     customerID,
		invoiceID:      invoiceID,
		amountCents:    amountCents,
		currency:       currency,
		idempotencyKey: idempotencyKey,
	})
	return billingstripe.InvoiceItem{ID: "ii_credit_" + uuid.New().String()}, nil
}

func (f *fakeStripe) FinalizeInvoice(_ context.Context, invoiceID, idempotencyKey string) (billingstripe.Invoice, error) {
	if f.errFinalizeInvoice != nil {
		return billingstripe.Invoice{}, f.errFinalizeInvoice
	}
	f.ensureCreditInvoiceMaps()
	f.creditFinalizeCalls = append(f.creditFinalizeCalls, invoiceID+"="+idempotencyKey)
	invoice := f.creditInvoices[invoiceID]
	if invoice.ID == "" {
		invoice.ID = invoiceID
	}
	status := f.creditFinalizeStatus
	if status == "" {
		status = "open"
	}
	invoice.Status = status
	invoice.ClientSecret = "pi_credit_secret"
	invoice.HostedInvoiceURL = "https://invoice.test/" + invoiceID
	if status == "paid" {
		for _, call := range f.creditItemCalls {
			if call.invoiceID == invoiceID {
				invoice.AmountPaid = call.amountCents
			}
		}
	}
	f.creditInvoices[invoiceID] = invoice
	return invoice, nil
}

func (f *fakeStripe) FindInvoiceByRef(_ context.Context, customerID, ref string) (billingstripe.Invoice, bool, error) {
	if f.errFindInvoice != nil {
		return billingstripe.Invoice{}, false, f.errFindInvoice
	}
	f.ensureCreditInvoiceMaps()
	f.creditFindCalls = append(f.creditFindCalls, customerID+"="+ref)
	invoiceID, ok := f.creditInvoiceRefs[ref]
	if !ok {
		return billingstripe.Invoice{}, false, nil
	}
	invoice := f.creditInvoices[invoiceID]
	if invoice.CustomerID != customerID {
		return billingstripe.Invoice{}, false, nil
	}
	return invoice, true, nil
}

func (f *fakeStripe) GetInvoice(_ context.Context, stripeInvoiceID string) (billingstripe.Invoice, error) {
	if f.errGetInvoice != nil {
		return billingstripe.Invoice{}, f.errGetInvoice
	}
	f.creditGetCalls = append(f.creditGetCalls, stripeInvoiceID)
	if invoice, ok := f.creditInvoices[stripeInvoiceID]; ok {
		return invoice, nil
	}
	status := f.getInvoiceStatus
	if status == "" {
		status = "open"
	}
	return billingstripe.Invoice{ID: stripeInvoiceID, Status: status, CustomerID: f.getInvoiceCustomer}, nil
}

func (f *fakeStripe) ensureCreditInvoiceMaps() {
	if f.creditInvoices == nil {
		f.creditInvoices = map[string]billingstripe.Invoice{}
	}
	if f.creditInvoiceRefs == nil {
		f.creditInvoiceRefs = map[string]string{}
	}
}

func (f *fakeStripe) PayInvoice(_ context.Context, stripeInvoiceID string) (billingstripe.Invoice, error) {
	if f.errPayInvoice != nil {
		return billingstripe.Invoice{}, f.errPayInvoice
	}
	f.paidInvoices = append(f.paidInvoices, stripeInvoiceID)
	inv := f.payInvoiceToReturn
	if inv.ID == "" {
		inv.ID = stripeInvoiceID
	}
	if inv.Status == "" {
		inv.Status = "paid"
	}
	return inv, nil
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

func TestEnsure_RequireNotDelinquent_UnpaidInvoice_ReportsDelinquent(t *testing.T) {
	store := newFakeStore()
	userID := uuid.New()
	accountID := uuid.New()
	store.accountsByUser[userID] = fakeAccount{id: accountID, stripeCustomerID: "cus_x"}
	store.hasUnpaidInvoice[accountID] = true
	svc := billing.NewService(store, &fakeStripe{}, "")

	resp, err := svc.Ensure(context.Background(), billing.EnsureRequest{
		UserID:  userID,
		Require: []billing.Capability{billing.RequireNotDelinquent},
	})

	require.NoError(t, err)
	require.False(t, resp.Ready())
	require.Equal(t, []string{billing.MissingDelinquent}, resp.Missing)
}

func TestEnsure_RequireNotDelinquent_NoUnpaidInvoice_Ready(t *testing.T) {
	store := newFakeStore()
	userID := uuid.New()
	accountID := uuid.New()
	store.accountsByUser[userID] = fakeAccount{id: accountID, stripeCustomerID: "cus_x"}
	// hasUnpaidInvoice[accountID] is false (default zero value)
	svc := billing.NewService(store, &fakeStripe{}, "")

	resp, err := svc.Ensure(context.Background(), billing.EnsureRequest{
		UserID:  userID,
		Require: []billing.Capability{billing.RequireNotDelinquent},
	})

	require.NoError(t, err)
	require.True(t, resp.Ready())
	require.Empty(t, resp.Missing)
}

func TestEnsure_RequirePMAndNotDelinquent_DeterministicOrder(t *testing.T) {
	// PM missing AND delinquent → Missing is [payment_method, delinquent]:
	// fixed order (PM before delinquency before subscription) regardless of
	// Require ordering.
	store := newFakeStore()
	userID := uuid.New()
	accountID := uuid.New()
	store.accountsByUser[userID] = fakeAccount{id: accountID, stripeCustomerID: "cus_x"}
	store.hasUnpaidInvoice[accountID] = true
	svc := billing.NewService(store, &fakeStripe{}, "")

	resp, err := svc.Ensure(context.Background(), billing.EnsureRequest{
		UserID:  userID,
		Require: []billing.Capability{billing.RequireNotDelinquent, billing.RequirePaymentMethod},
	})

	require.NoError(t, err)
	require.Equal(t, []string{billing.MissingPaymentMethod, billing.MissingDelinquent}, resp.Missing)
}

func TestEnsure_DelinquencyLookupError_BecomesInternal(t *testing.T) {
	store := newFakeStore()
	userID := uuid.New()
	accountID := uuid.New()
	store.accountsByUser[userID] = fakeAccount{id: accountID, stripeCustomerID: "cus_x"}
	store.errHasUnpaidInvoice = errors.New("conn dropped")
	svc := billing.NewService(store, &fakeStripe{}, "")

	_, err := svc.Ensure(context.Background(), billing.EnsureRequest{
		UserID:  userID,
		Require: []billing.Capability{billing.RequireNotDelinquent},
	})

	require.Error(t, err)
	var be *billing.Error
	require.ErrorAs(t, err, &be)
	require.Equal(t, billing.CodeInternal, be.Code)
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

func TestEnsure_UnfundedOrg_MissingBillingAccount(t *testing.T) {
	// The org leg resolves through the FUNDING gate (designation + activation),
	// not row existence: an org whose account row exists but never funded still
	// has no billing capability.
	store := newFakeStore()
	org := uuid.New()
	store.accountsByOrg[org] = fakeAccount{id: uuid.New()} // row exists, NOT funded
	svc := billing.NewService(store, &fakeStripe{}, "")

	resp, err := svc.Ensure(context.Background(), billing.EnsureRequest{OrgID: org})

	require.NoError(t, err)
	require.Equal(t, []string{billing.MissingBillingAccount}, resp.Missing)
}

func TestEnsure_SponsorFundedOrg_PMSatisfiedThroughFundingHop(t *testing.T) {
	// A sponsor-funded org account owns NO PM rows itself — the payment_method
	// capability checks the FUNDING account (org-billing D1 hop).
	store := newFakeStore()
	org := uuid.New()
	orgAcct, sponsorAcct := uuid.New(), uuid.New()
	store.accountsByOrg[org] = fakeAccount{id: orgAcct}
	store.fundedOrgs[org] = true
	store.fundingOf[orgAcct] = sponsorAcct
	store.hasUsablePM[sponsorAcct] = true // hasUsablePM[orgAcct] stays false
	svc := billing.NewService(store, &fakeStripe{}, "")

	resp, err := svc.Ensure(context.Background(), billing.EnsureRequest{OrgID: org})

	require.NoError(t, err)
	require.True(t, resp.Ready())
	require.Empty(t, resp.Missing)
}

func TestEnsure_UserAndOrgBothSet_InvalidInput(t *testing.T) {
	svc := billing.NewService(newFakeStore(), &fakeStripe{}, "")

	_, err := svc.Ensure(context.Background(), billing.EnsureRequest{UserID: uuid.New(), OrgID: uuid.New()})

	var be *billing.Error
	require.ErrorAs(t, err, &be)
	require.Equal(t, billing.CodeInvalidInput, be.Code)
}

func TestStartAddPaymentMethod_OwnerValidation(t *testing.T) {
	// The exactly-one owner-principal contract (user XOR org) gates every
	// payment-method RPC, not just Ensure.
	store := newFakeStore()
	stripeFake := &fakeStripe{}
	svc := billing.NewService(store, stripeFake, "")
	for _, tc := range []struct {
		name string
		req  billing.StartAddPaymentMethodRequest
	}{
		{"neither owner", billing.StartAddPaymentMethodRequest{}},
		{"both owners", billing.StartAddPaymentMethodRequest{UserID: uuid.New(), OrgID: uuid.New()}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := svc.StartAddPaymentMethod(context.Background(), tc.req)
			var be *billing.Error
			require.ErrorAs(t, err, &be)
			require.Equal(t, billing.CodeInvalidInput, be.Code)
			require.Empty(t, stripeFake.createdCustomers, "rejected up-front, before any Stripe call")
			require.Empty(t, store.addCardRequests)
		})
	}
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

func TestGetPaymentMethods_OrgAccountNoCards_EmptySlice(t *testing.T) {
	// The org read resolves by row EXISTENCE (cards are manageable while a
	// funding='org' designation awaits its first bind); a card-less org
	// account lists empty, exactly like the user leg.
	store := newFakeStore()
	org := uuid.New()
	store.accountsByOrg[org] = fakeAccount{id: uuid.New(), stripeCustomerID: "cus_org"}
	svc := billing.NewService(store, &fakeStripe{}, "")

	resp, err := svc.GetPaymentMethods(context.Background(), billing.GetPaymentMethodsRequest{OrgID: org})

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

// --- GetServiceStatus (service-block gate) --------------------------------

func TestGetServiceStatus_MissingUserID_InvalidInput(t *testing.T) {
	svc := billing.NewService(newFakeStore(), &fakeStripe{}, "")

	_, err := svc.GetServiceStatus(context.Background(), billing.GetServiceStatusRequest{})

	var be *billing.Error
	require.ErrorAs(t, err, &be)
	require.Equal(t, billing.CodeInvalidInput, be.Code)
}

func TestGetServiceStatus_NoAccount_BlockedNoCard(t *testing.T) {
	// A user with no billing account has no card on file → blocked on the card
	// gate, not a 404.
	svc := billing.NewService(newFakeStore(), &fakeStripe{}, "")

	resp, err := svc.GetServiceStatus(context.Background(), billing.GetServiceStatusRequest{UserID: uuid.New()})

	require.NoError(t, err)
	require.True(t, resp.Blocked)
	require.Equal(t, "NO_USABLE_CARD", resp.Reason)
	require.Equal(t, []string{"NO_USABLE_CARD"}, resp.Reasons)
}

func TestGetServiceStatus_Eligible(t *testing.T) {
	store := newFakeStore()
	userID, accountID := uuid.New(), uuid.New()
	store.accountsByUser[userID] = fakeAccount{id: accountID, stripeCustomerID: "cus_x"}
	store.serviceSignals[accountID] = billing.ServiceSignals{
		UsableCardCount: 1, FailedChargeStreak: 1, FirstChargeStatus: "paid",
	}
	svc := billing.NewService(store, &fakeStripe{}, "")

	resp, err := svc.GetServiceStatus(context.Background(), billing.GetServiceStatusRequest{UserID: userID})

	require.NoError(t, err)
	require.False(t, resp.Blocked)
	require.Equal(t, "ELIGIBLE", resp.Reason)
	require.Empty(t, resp.Reasons)
}

func TestGetServiceStatus_NewAccountWithCardIsGraced(t *testing.T) {
	// No charge yet ("" first-charge status) + a card → eligible (grace).
	store := newFakeStore()
	userID, accountID := uuid.New(), uuid.New()
	store.accountsByUser[userID] = fakeAccount{id: accountID}
	store.serviceSignals[accountID] = billing.ServiceSignals{UsableCardCount: 1, FirstChargeStatus: ""}
	svc := billing.NewService(store, &fakeStripe{}, "")

	resp, err := svc.GetServiceStatus(context.Background(), billing.GetServiceStatusRequest{UserID: userID})

	require.NoError(t, err)
	require.False(t, resp.Blocked)
}

func TestGetServiceStatus_MapsSignalsToBlockingReasons(t *testing.T) {
	cases := []struct {
		name       string
		signals    billing.ServiceSignals
		wantReason string
	}{
		{"no card", billing.ServiceSignals{UsableCardCount: 0, FirstChargeStatus: "paid"}, "NO_USABLE_CARD"},
		{"first charge uncollectible", billing.ServiceSignals{UsableCardCount: 1, FirstChargeStatus: "uncollectible"}, "FIRST_CHARGE_FAILED"},
		{"streak of two", billing.ServiceSignals{UsableCardCount: 1, FirstChargeStatus: "paid", FailedChargeStreak: 2}, "TOO_MANY_FAILURES"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			store := newFakeStore()
			userID, accountID := uuid.New(), uuid.New()
			store.accountsByUser[userID] = fakeAccount{id: accountID}
			store.serviceSignals[accountID] = tc.signals
			svc := billing.NewService(store, &fakeStripe{}, "")

			resp, err := svc.GetServiceStatus(context.Background(), billing.GetServiceStatusRequest{UserID: userID})

			require.NoError(t, err)
			require.True(t, resp.Blocked)
			require.Equal(t, tc.wantReason, resp.Reason)
		})
	}
}

func TestGetServiceStatus_SignalsError_Internal(t *testing.T) {
	store := newFakeStore()
	userID, accountID := uuid.New(), uuid.New()
	store.accountsByUser[userID] = fakeAccount{id: accountID}
	store.errServiceSignals = errors.New("conn dropped")
	svc := billing.NewService(store, &fakeStripe{}, "")

	_, err := svc.GetServiceStatus(context.Background(), billing.GetServiceStatusRequest{UserID: userID})

	var be *billing.Error
	require.ErrorAs(t, err, &be)
	require.Equal(t, billing.CodeInternal, be.Code)
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

func TestDetachPaymentMethod_OrgOwnedPM_ResolvesThroughOrgTarget(t *testing.T) {
	// An org-scoped detach dispatches to PaymentMethodTargetForOrg: the PM must
	// belong to the ORG account — a user-owned PM is invisible through the org
	// gate (404, not a cross-owner detach).
	store := newFakeStore()
	orgPM, userPM := uuid.New(), uuid.New()
	store.orgPMTargets[orgPM] = pmTarget{stripePMID: "pm_org", stripeCustomerID: "cus_org"}
	store.pmTargets[userPM] = pmTarget{stripePMID: "pm_user", stripeCustomerID: "cus_u"}
	stripeFake := &fakeStripe{}
	svc := billing.NewService(store, stripeFake, "")

	_, err := svc.DetachPaymentMethod(context.Background(),
		billing.DetachPaymentMethodRequest{OrgID: uuid.New(), PaymentMethodID: orgPM})
	require.NoError(t, err)
	require.Equal(t, []string{"pm_org"}, stripeFake.detached)

	_, err = svc.DetachPaymentMethod(context.Background(),
		billing.DetachPaymentMethodRequest{OrgID: uuid.New(), PaymentMethodID: userPM})
	var be *billing.Error
	require.ErrorAs(t, err, &be)
	require.Equal(t, billing.CodeNotFound, be.Code)
}

func TestSetDefaultPaymentMethod_OrgOwnedPM_SetsOrgCustomerDefault(t *testing.T) {
	store := newFakeStore()
	pmID := uuid.New()
	store.orgPMTargets[pmID] = pmTarget{stripePMID: "pm_org", stripeCustomerID: "cus_org"}
	stripeFake := &fakeStripe{}
	svc := billing.NewService(store, stripeFake, "")

	_, err := svc.SetDefaultPaymentMethod(context.Background(),
		billing.SetDefaultPaymentMethodRequest{OrgID: uuid.New(), PaymentMethodID: pmID})

	require.NoError(t, err)
	require.Equal(t, []string{"cus_org=pm_org"}, stripeFake.defaultsSet)
}
