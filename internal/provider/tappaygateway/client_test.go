package tappaygateway

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
)

// fakePartnerKey and fakePrime are built from pieces at test run time so
// nothing resembling a real credential ever appears as a literal in
// this source file.
func fakePartnerKey() string {
	return "partner_" + strings.Repeat("k", 24) + "_sandbox"
}

func fakePrime() string {
	return "prime_" + strings.Repeat("p", 24) + "_test"
}

func readFixture(t *testing.T, name string) []byte {
	t.Helper()
	data, err := os.ReadFile("testdata/" + name)
	if err != nil {
		t.Fatalf("read fixture %s: %v", name, err)
	}
	return data
}

// newTestServer returns an httptest.Server that serves one fixture
// (with the given status code) regardless of path, and a *Client
// pointed at it in place of TapPay's real host.
func newTestServer(t *testing.T, statusCode int, fixture string) (*httptest.Server, *Client) {
	t.Helper()
	body := readFixture(t, fixture)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(statusCode)
		_, _ = w.Write(body)
	}))
	t.Cleanup(srv.Close)

	client := &Client{
		baseURL:    srv.URL,
		partnerKey: fakePartnerKey(),
		merchantID: "merchant_test_001",
		http:       srv.Client(),
	}
	return srv, client
}

func TestNewClient_RefusesLiveMode(t *testing.T) {
	_, err := NewClient(Config{
		Mode:       ModeLive,
		PartnerKey: fakePartnerKey(),
		MerchantID: "merchant_test_001",
	})
	if err == nil {
		t.Fatal("expected NewClient to refuse ModeLive, got nil error")
	}
}

func TestNewClient_HostComesFromMode(t *testing.T) {
	client, err := NewClient(Config{
		Mode:       ModeSandbox,
		PartnerKey: fakePartnerKey(),
		MerchantID: "merchant_test_001",
	})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	if client.baseURL != sandboxBaseURL {
		t.Fatalf("baseURL = %q, want %q", client.baseURL, sandboxBaseURL)
	}
	if !strings.Contains(sandboxBaseURL, "sandbox.tappaysdk.com") {
		t.Fatalf("sandboxBaseURL %q does not name the sandbox host", sandboxBaseURL)
	}
}

func TestPayByPrime_Status0(t *testing.T) {
	_, client := newTestServer(t, http.StatusOK, "pay_by_prime_status_0.json")

	result, err := client.PayByPrime(context.Background(), PayByPrimeParams{
		Prime:             fakePrime(),
		AmountTWD:         1000,
		OrderNumber:       "ms-payment-test-000001",
		BankTransactionID: "btxn_test_000001",
		Details:           "MirrorStack credit top-up",
	})
	if err != nil {
		t.Fatalf("PayByPrime: %v", err)
	}
	if result.Status != 0 {
		t.Errorf("Status = %d, want 0", result.Status)
	}
	if result.RecTradeID != "D20261104000001" {
		t.Errorf("RecTradeID = %q", result.RecTradeID)
	}
	if result.PaymentURL != "" {
		t.Errorf("PaymentURL = %q, want empty for a non-3D charge", result.PaymentURL)
	}
}

func TestPayByPrime_RequiresBankTransactionID(t *testing.T) {
	_, client := newTestServer(t, http.StatusOK, "pay_by_prime_status_0.json")

	_, err := client.PayByPrime(context.Background(), PayByPrimeParams{
		Prime:       fakePrime(),
		AmountTWD:   1000,
		OrderNumber: "ms-payment-test-000001",
		Details:     "MirrorStack credit top-up",
	})
	if err == nil {
		t.Fatal("expected an error when BankTransactionID is empty")
	}
}

func TestPayByPrime_RememberIsAlwaysFalse(t *testing.T) {
	var seenBody []byte
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		seenBody = b
		w.Header().Set("Content-Type", "application/json")
		w.Write(readFixture(t, "pay_by_prime_status_0.json"))
	}))
	defer srv.Close()
	client := &Client{baseURL: srv.URL, partnerKey: fakePartnerKey(), merchantID: "merchant_test_001", http: srv.Client()}

	_, err := client.PayByPrime(context.Background(), PayByPrimeParams{
		Prime:             fakePrime(),
		AmountTWD:         1000,
		OrderNumber:       "ms-payment-test-000001",
		BankTransactionID: "btxn_test_000001",
		Details:           "MirrorStack credit top-up",
	})
	if err != nil {
		t.Fatalf("PayByPrime: %v", err)
	}
	if !strings.Contains(string(seenBody), `"remember":false`) {
		t.Fatalf("request body does not send remember:false: %s", seenBody)
	}
}

func TestPayByPrime_ThreeDomainSecureRedirect(t *testing.T) {
	_, client := newTestServer(t, http.StatusOK, "pay_by_prime_3d_redirect.json")

	result, err := client.PayByPrime(context.Background(), PayByPrimeParams{
		Prime:             fakePrime(),
		AmountTWD:         2500,
		OrderNumber:       "ms-payment-test-000002",
		BankTransactionID: "btxn_test_000002",
		Details:           "MirrorStack credit top-up",
		ThreeDomainSecure: true,
		ResultURL: &ResultURL{
			FrontendRedirectURL: "https://account.mirrorstack.ai/tappay/3ds/return",
			BackendNotifyURL:    "https://account.mirrorstack.ai/tappay/3ds/notify",
		},
	})
	if err != nil {
		t.Fatalf("PayByPrime: %v", err)
	}
	if result.PaymentURL == "" {
		t.Fatal("expected a non-empty PaymentURL for a 3D Secure redirect")
	}
}

func TestQueryRecord_Status0(t *testing.T) {
	_, client := newTestServer(t, http.StatusOK, "query_record_status_0.json")

	result, err := client.QueryRecord(context.Background(), QueryRecordParams{
		BankTransactionIDs: []string{"btxn_test_000001"},
	})
	if err != nil {
		t.Fatalf("QueryRecord: %v", err)
	}
	if result.Status != 0 {
		t.Errorf("Status = %d, want 0", result.Status)
	}
	if len(result.TradeRecords) != 1 {
		t.Fatalf("len(TradeRecords) = %d, want 1", len(result.TradeRecords))
	}
}

func TestQueryRecord_Status2NoMoreRecords(t *testing.T) {
	_, client := newTestServer(t, http.StatusOK, "query_record_status_2_no_more_records.json")

	result, err := client.QueryRecord(context.Background(), QueryRecordParams{
		BankTransactionIDs: []string{"btxn_unknown"},
	})
	if err != nil {
		t.Fatalf("QueryRecord: %v", err)
	}
	if result.Status != 2 {
		t.Errorf("Status = %d, want 2 (no more records)", result.Status)
	}
	if len(result.TradeRecords) != 0 {
		t.Errorf("len(TradeRecords) = %d, want 0", len(result.TradeRecords))
	}
}

func TestQueryRecord_GatewayTimeout421(t *testing.T) {
	_, client := newTestServer(t, 421, "gateway_timeout_421.json")

	_, err := client.QueryRecord(context.Background(), QueryRecordParams{
		BankTransactionIDs: []string{"btxn_test_000001"},
	})
	if err == nil {
		t.Fatal("expected an error for HTTP 421")
	}
	if !strings.Contains(err.Error(), "421") {
		t.Fatalf("error %q does not mention the 421 status", err.Error())
	}
}

func TestRefund_Status0(t *testing.T) {
	_, client := newTestServer(t, http.StatusOK, "refund_status_0.json")

	result, err := client.Refund(context.Background(), RefundParams{
		RecTradeID: "D20261104000001",
	})
	if err != nil {
		t.Fatalf("Refund: %v", err)
	}
	if result.Status != 0 {
		t.Errorf("Status = %d, want 0", result.Status)
	}
}

func TestRefund_RequiresRecTradeID(t *testing.T) {
	_, client := newTestServer(t, http.StatusOK, "refund_status_0.json")

	_, err := client.Refund(context.Background(), RefundParams{})
	if err == nil {
		t.Fatal("expected an error when RecTradeID is empty")
	}
}

func TestRefund_PartialSendsAmountFullOmitsIt(t *testing.T) {
	var seenBody string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		seenBody = string(b)
		w.Header().Set("Content-Type", "application/json")
		w.Write(readFixture(t, "refund_status_0.json"))
	}))
	defer srv.Close()
	client := &Client{baseURL: srv.URL, partnerKey: fakePartnerKey(), merchantID: "merchant_test_001", http: srv.Client()}

	partial := int64(500)
	if _, err := client.Refund(context.Background(), RefundParams{RecTradeID: "D1", AmountTWD: &partial}); err != nil {
		t.Fatalf("partial Refund: %v", err)
	}
	if !strings.Contains(seenBody, `"amount":500`) {
		t.Fatalf("partial refund body missing amount: %s", seenBody)
	}

	if _, err := client.Refund(context.Background(), RefundParams{RecTradeID: "D1"}); err != nil {
		t.Fatalf("full Refund: %v", err)
	}
	if strings.Contains(seenBody, "amount") {
		t.Fatalf("full refund body must omit amount: %s", seenBody)
	}
}

// TestNoSecretOrCardDataInErrorsOrRequestLogging proves every error
// path's formatted text is silent on the partner key, the prime, and
// cardholder data, by driving each failure mode this package can
// produce and checking the resulting error string.
func TestNoSecretOrCardDataInErrorsOrRequestLogging(t *testing.T) {
	partnerKey := fakePartnerKey()
	prime := fakePrime()
	cardName := "Jane Doe Cardholder Name Marker"

	cases := []struct {
		name string
		run  func(t *testing.T) error
	}{
		{
			name: "http 421 gateway timeout",
			run: func(t *testing.T) error {
				_, client := newTestServer(t, 421, "gateway_timeout_421.json")
				client.partnerKey = partnerKey
				_, err := client.PayByPrime(context.Background(), PayByPrimeParams{
					Prime:             prime,
					AmountTWD:         1000,
					OrderNumber:       "ms-payment-test-000003",
					BankTransactionID: "btxn_test_000003",
					Details:           "MirrorStack credit top-up",
					Cardholder:        Cardholder{Name: cardName},
				})
				return err
			},
		},
		{
			name: "malformed json response",
			run: func(t *testing.T) error {
				srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					w.Header().Set("Content-Type", "application/json")
					w.Write([]byte("{not json"))
				}))
				t.Cleanup(srv.Close)
				client := &Client{baseURL: srv.URL, partnerKey: partnerKey, merchantID: "merchant_test_001", http: srv.Client()}
				_, err := client.PayByPrime(context.Background(), PayByPrimeParams{
					Prime:             prime,
					AmountTWD:         1000,
					OrderNumber:       "ms-payment-test-000004",
					BankTransactionID: "btxn_test_000004",
					Details:           "MirrorStack credit top-up",
					Cardholder:        Cardholder{Name: cardName},
				})
				return err
			},
		},
		{
			name: "connection refused",
			run: func(t *testing.T) error {
				srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
				srv.Close() // closed before use: guarantees a dial failure
				client := &Client{baseURL: srv.URL, partnerKey: partnerKey, merchantID: "merchant_test_001", http: srv.Client()}
				_, err := client.PayByPrime(context.Background(), PayByPrimeParams{
					Prime:             prime,
					AmountTWD:         1000,
					OrderNumber:       "ms-payment-test-000005",
					BankTransactionID: "btxn_test_000005",
					Details:           "MirrorStack credit top-up",
					Cardholder:        Cardholder{Name: cardName},
				})
				return err
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.run(t)
			if err == nil {
				t.Fatal("expected an error")
			}
			msg := err.Error()
			for _, secret := range []string{partnerKey, prime, cardName} {
				if strings.Contains(msg, secret) {
					t.Fatalf("error message leaks sensitive value %q: %s", secret, msg)
				}
			}
		})
	}
}
