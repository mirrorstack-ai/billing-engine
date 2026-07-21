package stripe

import (
	"bytes"
	"context"
	"net/http"
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
		ID: "in_credit_purchase",
		ConfirmationSecret: &stripego.InvoiceConfirmationSecret{
			ClientSecret: "pi_secret_for_client",
		},
	})

	require.Equal(t, "pi_secret_for_client", got.ClientSecret)
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

type invoiceTestBackend struct {
	t           *testing.T
	wantMethod  string
	wantPath    string
	checkParams func(stripego.ParamsContainer)
	response    stripego.Invoice
}

func (b *invoiceTestBackend) Call(method, path, _ string, params stripego.ParamsContainer, v stripego.LastResponseSetter) error {
	b.t.Helper()
	require.Equal(b.t, b.wantMethod, method)
	require.Equal(b.t, b.wantPath, path)
	b.checkParams(params)
	got, ok := v.(*stripego.Invoice)
	require.True(b.t, ok)
	*got = b.response
	return nil
}

func (*invoiceTestBackend) CallStreaming(string, string, string, stripego.ParamsContainer, stripego.StreamingLastResponseSetter) error {
	panic("unexpected streaming Stripe call")
}

func (*invoiceTestBackend) CallRaw(string, string, string, []byte, *stripego.Params, stripego.LastResponseSetter) error {
	panic("unexpected raw Stripe call")
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
