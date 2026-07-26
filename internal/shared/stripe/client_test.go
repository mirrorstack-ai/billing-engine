package stripe

import (
	"bytes"
	"context"
	"net/http"
	"net/url"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	stripego "github.com/stripe/stripe-go/v85"
	stripeclient "github.com/stripe/stripe-go/v85/client"
)

// TestNewClient is a smoke test: NewClient returns a non-nil Client
// satisfying the interface. End-to-end Customer / SetupIntent creation
// is exercised in integration tests against Stripe test mode.
func TestNewClient(t *testing.T) {
	c := NewClient("sk_test_dummy")
	require.NotNil(t, c)

	// Interface satisfaction is checked at compile time via the
	// assignment. This var declaration would fail to build if
	// NewClient's return type didn't satisfy Client.
	var _ Client = c
}

func TestNewAutoTopUpClient(t *testing.T) {
	c := NewAutoTopUpClient("sk_test_dummy")
	require.NotNil(t, c)
	var _ AutoTopUpClient = c
}

func TestNewCreditPurchaseClient(t *testing.T) {
	c := NewCreditPurchaseClient("sk_test_dummy")
	require.NotNil(t, c)
	var _ CreditPurchaseClient = c
}

func TestNewVerifier(t *testing.T) {
	v := NewVerifier("whsec_dummy")
	require.NotNil(t, v)
	var _ Verifier = v
}

// TestVerifier_BadSignature pins the negative-path contract of the
// real verifier without needing a Stripe API call: ConstructEvent
// rejects malformed signatures locally. A genuine end-to-end Verify
// test (with a valid HMAC signature constructed against the secret)
// lives in the webhook integration suite, alongside payload fixtures.
func TestVerifier_BadSignature(t *testing.T) {
	v := NewVerifier("whsec_dev_secret_used_only_in_tests")

	_, err := v.Verify([]byte(`{"id":"evt_test","type":"payment_method.attached"}`), "t=12345,v1=garbage")

	require.Error(t, err)
}

func TestVerifier_EmptySignature(t *testing.T) {
	v := NewVerifier("whsec_dev_secret_used_only_in_tests")

	_, err := v.Verify([]byte(`{"id":"evt_test"}`), "")

	require.Error(t, err)
}

func TestItemPeriodParams(t *testing.T) {
	t.Run("populated", func(t *testing.T) {
		start := time.Date(2026, time.July, 17, 8, 30, 0, 0, time.FixedZone("UTC+8", 8*60*60))
		end := time.Date(2026, time.August, 10, 8, 30, 0, 0, time.FixedZone("UTC+8", 8*60*60))

		got := itemPeriodParams(LinePeriod{Start: start, End: end})

		require.NotNil(t, got)
		require.Equal(t, start.UTC().Unix(), *got.Start)
		require.Equal(t, end.UTC().Unix(), *got.End)
	})

	for _, tt := range []struct {
		name   string
		period LinePeriod
	}{
		{name: "zero value"},
		{name: "missing start", period: LinePeriod{End: time.Unix(200, 0)}},
		{name: "missing end", period: LinePeriod{Start: time.Unix(100, 0)}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			require.Nil(t, itemPeriodParams(tt.period))
		})
	}
}

func TestProjectInvoice_ConfirmationSecret(t *testing.T) {
	got := projectInvoice(&stripego.Invoice{
		ID:                   "in_credit_purchase",
		Total:                501,
		AmountRemaining:      17,
		CollectionMethod:     stripego.InvoiceCollectionMethodChargeAutomatically,
		AutoAdvance:          false,
		DefaultPaymentMethod: &stripego.PaymentMethod{ID: "pm_frozen"},
		Metadata: map[string]string{
			"ms_charge_ref":        "credit-auto-topup:ledger-id",
			"ms_credit_operation":  "auto_topup",
			"ms_credit_account_id": "account-id",
			"ms_credit_ledger_id":  "ledger-id",
		},
		ConfirmationSecret: &stripego.InvoiceConfirmationSecret{
			ClientSecret: "pi_secret_for_client",
		},
	})

	require.Equal(t, "pi_secret_for_client", got.ClientSecret)
	require.Equal(t, int64(501), got.Total)
	require.Equal(t, int64(17), got.AmountRemaining)
	require.Equal(t, "charge_automatically", got.CollectionMethod)
	require.False(t, got.AutoAdvance)
	require.Equal(t, "pm_frozen", got.DefaultPaymentMethodID)
	require.Equal(t, "credit-auto-topup:ledger-id", got.ChargeRef)
	require.Equal(t, "auto_topup", got.CreditOperation)
	require.Equal(t, "account-id", got.CreditAccountID)
	require.Equal(t, "ledger-id", got.CreditLedgerID)
}

func TestFinalizeInvoice_ExpandsAndProjectsConfirmationSecret(t *testing.T) {
	backend := &invoiceTestBackend{
		t:          t,
		wantMethod: http.MethodPost,
		wantPath:   "/v1/invoices/in_credit_purchase/finalize",
		response: stripego.Invoice{
			ID: "in_credit_purchase",
			ConfirmationSecret: &stripego.InvoiceConfirmationSecret{
				ClientSecret: "pi_secret_from_finalize",
			},
		},
		checkParams: func(params stripego.ParamsContainer) {
			got, ok := params.(*stripego.InvoiceFinalizeInvoiceParams)
			require.True(t, ok)
			require.Len(t, got.Expand, 1)
			require.Equal(t, "confirmation_secret", *got.Expand[0])
			require.Equal(t, "credit-purchase-finalize", *got.IdempotencyKey)
		},
	}
	client := testRealClient(backend)
	got, err := client.FinalizeInvoice(context.Background(), "in_credit_purchase", "credit-purchase-finalize")

	require.NoError(t, err)
	require.Equal(t, "pi_secret_from_finalize", got.ClientSecret)
}

func TestGetInvoice_ExpandsAndProjectsConfirmationSecret(t *testing.T) {
	backend := &invoiceTestBackend{
		t:          t,
		wantMethod: http.MethodGet,
		wantPath:   "/v1/invoices/in_credit_purchase",
		response: stripego.Invoice{
			ID: "in_credit_purchase",
			ConfirmationSecret: &stripego.InvoiceConfirmationSecret{
				ClientSecret: "pi_secret_from_get",
			},
		},
		checkParams: func(params stripego.ParamsContainer) {
			got, ok := params.(*stripego.InvoiceParams)
			require.True(t, ok)
			require.Len(t, got.Expand, 1)
			require.Equal(t, "confirmation_secret", *got.Expand[0])
		},
	}
	client := testRealClient(backend)
	got, err := client.GetInvoice(context.Background(), "in_credit_purchase")

	require.NoError(t, err)
	require.Equal(t, "pi_secret_from_get", got.ClientSecret)
}

func TestCreateAutoTopUpInvoice_PinsSelectedMethodAndRemainsInert(t *testing.T) {
	backend := &invoiceTestBackend{
		t:          t,
		wantMethod: http.MethodPost,
		wantPath:   "/v1/invoices",
		response: stripego.Invoice{
			ID: "in_topup",
		},
		checkParams: func(params stripego.ParamsContainer) {
			got, ok := params.(*stripego.InvoiceParams)
			require.True(t, ok)
			require.Equal(t, "cus_frozen", *got.Customer)
			require.Equal(t, "pm_frozen", *got.DefaultPaymentMethod)
			require.Equal(t, string(stripego.InvoiceCollectionMethodChargeAutomatically), *got.CollectionMethod)
			require.NotNil(t, got.AutoAdvance)
			require.False(t, *got.AutoAdvance)
			require.Equal(t, "exclude", *got.PendingInvoiceItemsBehavior)
			require.Equal(t, map[string]string{
				"ms_charge_ref":        "credit-auto-topup:attempt-1",
				"ms_credit_operation":  "auto_topup",
				"ms_credit_account_id": "account-1",
				"ms_credit_ledger_id":  "attempt-1",
			}, got.Metadata)
			require.Equal(t, "credit-auto-topup-invoice:attempt-1", *got.IdempotencyKey)
		},
	}
	client := testRealClient(backend)

	got, err := client.CreateAutoTopUpInvoice(
		context.Background(),
		"cus_frozen",
		"pm_frozen",
		"account-1",
		"attempt-1",
		"credit-auto-topup-invoice:attempt-1",
	)

	require.NoError(t, err)
	require.Equal(t, "in_topup", got.ID)
}

func TestCreateCreditPurchaseInvoice_StampsExactRoutingAnchors(t *testing.T) {
	backend := &invoiceTestBackend{
		t:          t,
		wantMethod: http.MethodPost,
		wantPath:   "/v1/invoices",
		response:   stripego.Invoice{ID: "in_purchase"},
		checkParams: func(params stripego.ParamsContainer) {
			got, ok := params.(*stripego.InvoiceParams)
			require.True(t, ok)
			require.Equal(t, "cus_purchase", *got.Customer)
			require.Equal(t, map[string]string{
				"ms_charge_ref":        "credit-purchase:ledger-1",
				"ms_credit_operation":  "purchase",
				"ms_credit_account_id": "account-1",
				"ms_credit_ledger_id":  "ledger-1",
			}, got.Metadata)
			require.Equal(t, "credit-inv:ledger-1", *got.IdempotencyKey)
		},
	}
	client := testRealClient(backend)

	got, err := client.CreateCreditPurchaseInvoice(
		context.Background(),
		"cus_purchase",
		"account-1",
		"ledger-1",
		"credit-inv:ledger-1",
	)

	require.NoError(t, err)
	require.Equal(t, "in_purchase", got.ID)
}

func TestCreateCombinedProrationInvoiceItem_StampsExactIdentity(t *testing.T) {
	period := LinePeriod{
		Start: time.Date(2026, 7, 11, 0, 0, 0, 0, time.UTC),
		End:   time.Date(2026, 8, 11, 0, 0, 0, 0, time.UTC),
	}
	tests := []struct {
		name         string
		identity     CombinedProrationItemIdentity
		wantMetadata map[string]string
	}{
		{
			name:     "app base",
			identity: CombinedProrationItemIdentity{AppID: "app-1"},
			wantMetadata: map[string]string{
				combinedProrationComponentMetadata: CombinedProrationComponentAppBase,
				combinedProrationAppMetadata:       "app-1",
			},
		},
		{
			name:     "module timer",
			identity: CombinedProrationItemIdentity{AppID: "app-1", TimerID: "timer-1"},
			wantMetadata: map[string]string{
				combinedProrationComponentMetadata: CombinedProrationComponentModuleOverage,
				combinedProrationAppMetadata:       "app-1",
				combinedProrationTimerMetadata:     "timer-1",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			response := &stripego.InvoiceItem{
				ID:          "ii_combined",
				Amount:      150,
				Currency:    stripego.CurrencyUSD,
				Description: "frozen description",
				Period:      &stripego.Period{Start: period.Start.Unix(), End: period.End.Unix()},
				Metadata:    tt.wantMetadata,
			}
			backend := &invoiceTestBackend{
				t:            t,
				wantMethod:   http.MethodPost,
				wantPath:     "/v1/invoiceitems",
				itemResponse: response,
				checkParams: func(params stripego.ParamsContainer) {
					got, ok := params.(*stripego.InvoiceItemParams)
					require.True(t, ok)
					require.Equal(t, "cus_combined", *got.Customer)
					require.Equal(t, "in_combined", *got.Invoice)
					require.EqualValues(t, 150, *got.Amount)
					require.Equal(t, "usd", *got.Currency)
					require.Equal(t, "frozen description", *got.Description)
					require.Equal(t, period.Start.Unix(), *got.Period.Start)
					require.Equal(t, period.End.Unix(), *got.Period.End)
					require.Equal(t, tt.wantMetadata, got.Metadata)
					require.Equal(t, "combined-item-key", *got.IdempotencyKey)
				},
			}
			client := testRealClient(backend)

			got, err := client.CreateCombinedProrationInvoiceItem(
				context.Background(),
				"cus_combined",
				"in_combined",
				150,
				"usd",
				"frozen description",
				period,
				"combined-item-key",
				tt.identity,
			)

			require.NoError(t, err)
			require.Equal(t, InvoiceItem{
				ID:                         "ii_combined",
				AmountCents:                150,
				Currency:                   "usd",
				Description:                "frozen description",
				Period:                     period,
				CombinedProrationComponent: tt.wantMetadata[combinedProrationComponentMetadata],
				CombinedProrationAppID:     "app-1",
				CombinedProrationTimerID:   tt.wantMetadata[combinedProrationTimerMetadata],
			}, got)
		})
	}
}

func TestCreateCombinedProrationInvoiceItem_RequiresAppIdentity(t *testing.T) {
	client, ok := NewClient("sk_test_dummy").(CombinedProrationClient)
	require.True(t, ok)
	_, err := client.CreateCombinedProrationInvoiceItem(
		context.Background(),
		"cus_combined",
		"in_combined",
		150,
		"usd",
		"frozen description",
		LinePeriod{},
		"combined-item-key",
		CombinedProrationItemIdentity{},
	)
	require.ErrorContains(t, err, "app id required")
}

func TestListInvoiceItems_FiltersOneInvoiceAndProjectsResourceTruth(t *testing.T) {
	start := time.Date(2026, 7, 11, 0, 0, 0, 0, time.UTC)
	end := time.Date(2026, 8, 11, 0, 0, 0, 0, time.UTC)
	backend := &invoiceTestBackend{
		t:          t,
		wantMethod: http.MethodGet,
		wantPath:   "/v1/invoiceitems",
		rawListResult: &stripego.InvoiceItemList{
			ListMeta: stripego.ListMeta{HasMore: false},
			Data: []*stripego.InvoiceItem{
				{
					ID:          "ii_one",
					Amount:      500,
					Currency:    stripego.CurrencyUSD,
					Description: "frozen base",
					Period:      &stripego.Period{Start: start.Unix(), End: end.Unix()},
					Metadata: map[string]string{
						combinedProrationComponentMetadata: CombinedProrationComponentAppBase,
						combinedProrationAppMetadata:       "app-1",
					},
				},
				{ID: "ii_two", Amount: 250, Currency: stripego.CurrencyEUR},
			},
		},
		checkRawParams: func(values url.Values) {
			require.Equal(t, "in_topup", values.Get("invoice"))
			require.Equal(t, "100", values.Get("limit"))
		},
	}
	client := testRealClient(backend)

	got, err := client.ListInvoiceItems(context.Background(), "in_topup")

	require.NoError(t, err)
	require.Equal(t, []InvoiceItem{
		{
			ID:                         "ii_one",
			AmountCents:                500,
			Currency:                   "usd",
			Description:                "frozen base",
			Period:                     LinePeriod{Start: start, End: end},
			CombinedProrationComponent: CombinedProrationComponentAppBase,
			CombinedProrationAppID:     "app-1",
		},
		{ID: "ii_two", AmountCents: 250, Currency: "eur"},
	}, got)
}

func TestListInvoicePayments_FiltersPaidAndProjectsFrozenCardProof(t *testing.T) {
	backend := &invoiceTestBackend{
		t:          t,
		wantMethod: http.MethodGet,
		wantPath:   "/v1/invoice_payments",
		rawPaymentList: &stripego.InvoicePaymentList{
			ListMeta: stripego.ListMeta{HasMore: false},
			Data: []*stripego.InvoicePayment{{
				ID:              "inpay_exact",
				Invoice:         &stripego.Invoice{ID: "in_topup"},
				Status:          "paid",
				IsDefault:       true,
				AmountPaid:      500,
				AmountRequested: 500,
				Currency:        stripego.CurrencyUSD,
				Payment: &stripego.InvoicePaymentPayment{
					Type: stripego.InvoicePaymentPaymentTypePaymentIntent,
					PaymentIntent: &stripego.PaymentIntent{
						ID:             "pi_exact",
						Status:         stripego.PaymentIntentStatusSucceeded,
						Customer:       &stripego.Customer{ID: "cus_frozen"},
						PaymentMethod:  &stripego.PaymentMethod{ID: "pm_frozen"},
						Amount:         500,
						AmountReceived: 500,
						Currency:       stripego.CurrencyUSD,
					},
				},
			}},
		},
		checkRawParams: func(values url.Values) {
			require.Equal(t, "in_topup", values.Get("invoice"))
			require.Equal(t, "paid", values.Get("status"))
			require.Equal(t, "100", values.Get("limit"))
			require.Equal(t,
				"data.payment.payment_intent.customer",
				values.Get("expand[0]"),
			)
			require.Equal(t,
				"data.payment.payment_intent.payment_method",
				values.Get("expand[1]"),
			)
		},
	}
	client := testRealClient(backend)

	got, err := client.ListInvoicePayments(context.Background(), "in_topup")

	require.NoError(t, err)
	require.Equal(t, []InvoicePaymentProof{{
		ID:                    "inpay_exact",
		InvoiceID:             "in_topup",
		Status:                "paid",
		IsDefault:             true,
		AmountPaid:            500,
		AmountRequested:       500,
		Currency:              "usd",
		PaymentType:           "payment_intent",
		PaymentIntentID:       "pi_exact",
		PaymentIntentStatus:   "succeeded",
		PaymentIntentCustomer: "cus_frozen",
		PaymentMethodID:       "pm_frozen",
		PaymentIntentAmount:   500,
		AmountReceived:        500,
		PaymentIntentCurrency: "usd",
	}}, got)
}

func TestFinalizeInvoiceWithoutAutoAdvance_ExplicitlyStaysInert(t *testing.T) {
	backend := &invoiceTestBackend{
		t:          t,
		wantMethod: http.MethodPost,
		wantPath:   "/v1/invoices/in_topup/finalize",
		response:   stripego.Invoice{ID: "in_topup", Status: stripego.InvoiceStatusOpen},
		checkParams: func(params stripego.ParamsContainer) {
			got, ok := params.(*stripego.InvoiceFinalizeInvoiceParams)
			require.True(t, ok)
			require.NotNil(t, got.AutoAdvance)
			require.False(t, *got.AutoAdvance)
			require.Equal(t, "credit-auto-topup-finalize:attempt-1", *got.IdempotencyKey)
			require.Len(t, got.Expand, 1)
			require.Equal(t, "confirmation_secret", *got.Expand[0])
		},
	}
	client := testRealClient(backend)

	got, err := client.FinalizeInvoiceWithoutAutoAdvance(
		context.Background(),
		"in_topup",
		"credit-auto-topup-finalize:attempt-1",
	)

	require.NoError(t, err)
	require.Equal(t, "open", got.Status)
}

func TestPayInvoiceWithMethod_PinsFrozenMethodOffSessionAndIdempotency(t *testing.T) {
	backend := &invoiceTestBackend{
		t:          t,
		wantMethod: http.MethodPost,
		wantPath:   "/v1/invoices/in_topup/pay",
		response:   stripego.Invoice{ID: "in_topup", Status: stripego.InvoiceStatusPaid},
		checkParams: func(params stripego.ParamsContainer) {
			got, ok := params.(*stripego.InvoicePayParams)
			require.True(t, ok)
			require.Equal(t, "pm_frozen", *got.PaymentMethod)
			require.NotNil(t, got.OffSession)
			require.True(t, *got.OffSession)
			require.Equal(t, "credit-auto-topup-pay:attempt-1", *got.IdempotencyKey)
		},
	}
	client := testRealClient(backend)

	got, err := client.PayInvoiceWithMethod(
		context.Background(),
		"in_topup",
		"pm_frozen",
		"credit-auto-topup-pay:attempt-1",
	)

	require.NoError(t, err)
	require.Equal(t, "paid", got.Status)
}

func TestVoidInvoice_UsesDeterministicIdempotency(t *testing.T) {
	backend := &invoiceTestBackend{
		t:          t,
		wantMethod: http.MethodPost,
		wantPath:   "/v1/invoices/in_topup/void",
		response:   stripego.Invoice{ID: "in_topup", Status: stripego.InvoiceStatusVoid},
		checkParams: func(params stripego.ParamsContainer) {
			got, ok := params.(*stripego.InvoiceVoidInvoiceParams)
			require.True(t, ok)
			require.Equal(t, "credit-auto-topup-void:attempt-1", *got.IdempotencyKey)
		},
	}
	client := testRealClient(backend)

	got, err := client.VoidInvoice(
		context.Background(),
		"in_topup",
		"credit-auto-topup-void:attempt-1",
	)

	require.NoError(t, err)
	require.Equal(t, "void", got.Status)
}

func TestDeleteDraftInvoice_UsesExactResourceAndProjectsDeletion(t *testing.T) {
	backend := &invoiceTestBackend{
		t:          t,
		wantMethod: http.MethodDelete,
		wantPath:   "/v1/invoices/in_topup",
		response:   stripego.Invoice{ID: "in_topup", Deleted: true},
		checkParams: func(params stripego.ParamsContainer) {
			_, ok := params.(*stripego.InvoiceParams)
			require.True(t, ok)
		},
	}
	client := testRealClient(backend)

	got, err := client.DeleteDraftInvoice(context.Background(), "in_topup")

	require.NoError(t, err)
	require.Equal(t, "in_topup", got.ID)
	require.True(t, got.Deleted)
}

type invoiceTestBackend struct {
	t              *testing.T
	wantMethod     string
	wantPath       string
	checkParams    func(stripego.ParamsContainer)
	checkRawParams func(url.Values)
	response       stripego.Invoice
	itemResponse   *stripego.InvoiceItem
	rawListResult  *stripego.InvoiceItemList
	rawPaymentList *stripego.InvoicePaymentList
}

func (b *invoiceTestBackend) Call(method, path, _ string, params stripego.ParamsContainer, v stripego.LastResponseSetter) error {
	b.t.Helper()
	require.Equal(b.t, b.wantMethod, method)
	require.Equal(b.t, b.wantPath, path)
	b.checkParams(params)
	switch got := v.(type) {
	case *stripego.Invoice:
		*got = b.response
	case *stripego.InvoiceItem:
		require.NotNil(b.t, b.itemResponse, "unexpected invoice-item create call")
		*got = *b.itemResponse
	default:
		b.t.Fatalf("unexpected Stripe response type %T", v)
	}
	return nil
}

func (*invoiceTestBackend) CallStreaming(string, string, string, stripego.ParamsContainer, stripego.StreamingLastResponseSetter) error {
	panic("unexpected streaming Stripe call")
}

func (b *invoiceTestBackend) CallRaw(method, path, _ string, body []byte, _ *stripego.Params, v stripego.LastResponseSetter) error {
	b.t.Helper()
	require.Equal(b.t, b.wantMethod, method)
	require.Equal(b.t, b.wantPath, path)
	values, err := url.ParseQuery(string(body))
	require.NoError(b.t, err)
	if b.checkRawParams != nil {
		b.checkRawParams(values)
	}
	switch got := v.(type) {
	case *stripego.InvoiceItemList:
		require.NotNil(b.t, b.rawListResult, "unexpected invoice-item list call")
		*got = *b.rawListResult
	case *stripego.InvoicePaymentList:
		require.NotNil(b.t, b.rawPaymentList, "unexpected invoice-payment list call")
		*got = *b.rawPaymentList
	default:
		b.t.Fatalf("unexpected raw Stripe response type %T", v)
	}
	return nil
}

func (*invoiceTestBackend) CallMultipart(string, string, string, string, *bytes.Buffer, *stripego.Params, stripego.LastResponseSetter) error {
	panic("unexpected multipart Stripe call")
}

func (*invoiceTestBackend) SetMaxNetworkRetries(int64) {}

func testRealClient(backend stripego.Backend) *realClient {
	backends := &stripego.Backends{
		API:         backend,
		Connect:     backend,
		Uploads:     backend,
		MeterEvents: backend,
	}
	sc := &stripeclient.API{}
	sc.Init("sk_test_dummy", backends)
	return &realClient{sc: sc}
}
