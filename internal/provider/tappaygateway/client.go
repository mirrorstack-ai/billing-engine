package tappaygateway

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

// requestTimeout is TapPay's own advised ceiling for a call to any of
// its three verbs.
const requestTimeout = 30 * time.Second

// HTTPDoer is the slice of *http.Client this package needs. Tests
// substitute a server under httptest rather than this interface, so
// requests go through real JSON encoding/decoding; it exists so a
// caller can install its own transport (e.g. to pin a timeout or add
// tracing) without this package reaching for http.DefaultClient.
type HTTPDoer interface {
	Do(req *http.Request) (*http.Response, error)
}

// Config configures a Client.
//
// PartnerKey and MerchantID are TapPay credentials. This package does
// not read them from anywhere itself — see doc.go's Assumption
// (D526) — so whatever constructs a Config is the thing responsible
// for keeping them out of logs upstream of this package too.
type Config struct {
	Mode       Mode
	PartnerKey string
	MerchantID string
	// HTTPClient overrides the default *http.Client (with
	// requestTimeout applied). Tests use this to point at an
	// httptest.Server.
	HTTPClient HTTPDoer
}

// Client talks to TapPay's Pay by Prime, Record and Refund endpoints.
type Client struct {
	baseURL    string
	partnerKey string
	merchantID string
	http       HTTPDoer
}

// NewClient returns a Client for the given Config.
//
// It refuses any Mode but ModeSandbox: billing-engine#310 item 10
// keeps TapPay sandbox-only until billing-engine#318 builds the rest
// of mode handling, and the production host has no deploy path to
// reach today regardless of what a caller passes here.
func NewClient(cfg Config) (*Client, error) {
	if cfg.Mode != ModeSandbox {
		return nil, fmt.Errorf("tappaygateway: mode %s is not enabled; TapPay is sandbox-only until billing-engine#318", cfg.Mode)
	}
	baseURL, ok := cfg.Mode.baseURL()
	if !ok {
		return nil, fmt.Errorf("tappaygateway: unknown mode %d", int(cfg.Mode))
	}
	if cfg.PartnerKey == "" {
		return nil, fmt.Errorf("tappaygateway: partner key required")
	}
	if cfg.MerchantID == "" {
		return nil, fmt.Errorf("tappaygateway: merchant id required")
	}
	httpClient := cfg.HTTPClient
	if httpClient == nil {
		httpClient = &http.Client{Timeout: requestTimeout}
	}
	return &Client{
		baseURL:    baseURL,
		partnerKey: cfg.PartnerKey,
		merchantID: cfg.MerchantID,
		http:       httpClient,
	}, nil
}

// wire request/response shapes. Unexported: callers use the typed
// requests and results above, never these directly, so a field rename
// on TapPay's side is a one-file fix.

type wireCardholder struct {
	PhoneNumber string `json:"phone_number,omitempty"`
	Name        string `json:"name,omitempty"`
	Email       string `json:"email,omitempty"`
	ZipCode     string `json:"zip_code,omitempty"`
	Address     string `json:"address,omitempty"`
	NationalID  string `json:"national_id,omitempty"`
}

type wireResultURL struct {
	FrontendRedirectURL string `json:"frontend_redirect_url,omitempty"`
	BackendNotifyURL    string `json:"backend_notify_url,omitempty"`
	GoBackURL           string `json:"go_back_url,omitempty"`
}

type wirePayByPrimeParams struct {
	Prime             string         `json:"prime"`
	PartnerKey        string         `json:"partner_key"`
	MerchantID        string         `json:"merchant_id"`
	Amount            int64          `json:"amount"`
	Cardholder        wireCardholder `json:"cardholder"`
	OrderNumber       string         `json:"order_number,omitempty"`
	BankTransactionID string         `json:"bank_transaction_id"`
	Details           string         `json:"details"`
	ResultURL         *wireResultURL `json:"result_url,omitempty"`
	ThreeDomainSecure bool           `json:"three_domain_secure"`
	Remember          bool           `json:"remember"`
}

type wirePayByPrimeResult struct {
	Status            int    `json:"status"`
	Msg               string `json:"msg"`
	RecTradeID        string `json:"rec_trade_id"`
	BankTransactionID string `json:"bank_transaction_id"`
	OrderNumber       string `json:"order_number"`
	Amount            int64  `json:"amount"`
	PaymentURL        string `json:"payment_url"`
}

type wireQueryRecordFilters struct {
	RecTradeID        []string `json:"rec_trade_id,omitempty"`
	BankTransactionID []string `json:"bank_transaction_id,omitempty"`
	OrderNumber       string   `json:"order_number,omitempty"`
}

type wireQueryRecordParams struct {
	PartnerKey string                 `json:"partner_key"`
	Filters    wireQueryRecordFilters `json:"filters"`
	Records    int                    `json:"records,omitempty"`
}

type wireTradeRecord struct {
	RecTradeID        string `json:"rec_trade_id"`
	BankTransactionID string `json:"bank_transaction_id"`
	OrderNumber       string `json:"order_number"`
	Amount            int64  `json:"amount"`
	RecordStatus      int    `json:"record_status"`
}

type wireQueryRecordResult struct {
	Status       int               `json:"status"`
	Msg          string            `json:"msg"`
	TradeRecords []wireTradeRecord `json:"trade_records"`
}

type wireRefundParams struct {
	PartnerKey string `json:"partner_key"`
	RecTradeID string `json:"rec_trade_id"`
	Amount     *int64 `json:"amount,omitempty"`
}

type wireRefundResult struct {
	Status       int    `json:"status"`
	Msg          string `json:"msg"`
	RecTradeID   string `json:"rec_trade_id"`
	RefundAmount int64  `json:"refund_amount"`
}

// PayByPrime charges a single-use, 90-second prime token.
//
// Remember is never forwarded: this adapter always sends false,
// regardless of anything a caller might set, because nothing in this
// issue's scope stores a reusable card token.
func (c *Client) PayByPrime(ctx context.Context, req PayByPrimeParams) (PayByPrimeResult, error) {
	if req.BankTransactionID == "" {
		return PayByPrimeResult{}, fmt.Errorf("tappaygateway: PayByPrime requires BankTransactionID so a 421 can still be looked up")
	}
	if req.Details == "" {
		return PayByPrimeResult{}, fmt.Errorf("tappaygateway: PayByPrime requires Details (TapPay's PCI item-name requirement)")
	}

	wireReq := wirePayByPrimeParams{
		Prime:             req.Prime,
		PartnerKey:        c.partnerKey,
		MerchantID:        c.merchantID,
		Amount:            req.AmountTWD,
		Cardholder:        wireCardholder(req.Cardholder),
		OrderNumber:       req.OrderNumber,
		BankTransactionID: req.BankTransactionID,
		Details:           req.Details,
		ThreeDomainSecure: req.ThreeDomainSecure,
		Remember:          false,
	}
	if req.ResultURL != nil {
		wireReq.ResultURL = &wireResultURL{
			FrontendRedirectURL: req.ResultURL.FrontendRedirectURL,
			BackendNotifyURL:    req.ResultURL.BackendNotifyURL,
			GoBackURL:           req.ResultURL.GoBackURL,
		}
	}

	var resp wirePayByPrimeResult
	if err := c.do(ctx, payByPrimePath, wireReq, &resp); err != nil {
		return PayByPrimeResult{}, err
	}
	return PayByPrimeResult{
		Status:            resp.Status,
		Msg:               resp.Msg,
		RecTradeID:        resp.RecTradeID,
		BankTransactionID: resp.BankTransactionID,
		OrderNumber:       resp.OrderNumber,
		AmountTWD:         resp.Amount,
		PaymentURL:        resp.PaymentURL,
	}, nil
}

// QueryRecord reads back the current state of one or more trades. The
// engine's settle path keys this to the one BankTransactionID or
// RecTradeID it is confirming — see billing-engine#310 item 6.
func (c *Client) QueryRecord(ctx context.Context, req QueryRecordParams) (QueryRecordResult, error) {
	wireReq := wireQueryRecordParams{
		PartnerKey: c.partnerKey,
		Filters: wireQueryRecordFilters{
			RecTradeID:        req.RecTradeIDs,
			BankTransactionID: req.BankTransactionIDs,
			OrderNumber:       req.OrderNumber,
		},
		Records: req.Records,
	}

	var resp wireQueryRecordResult
	if err := c.do(ctx, queryRecordPath, wireReq, &resp); err != nil {
		return QueryRecordResult{}, err
	}
	records := make([]TradeRecord, 0, len(resp.TradeRecords))
	for _, r := range resp.TradeRecords {
		records = append(records, TradeRecord{
			RecTradeID:        r.RecTradeID,
			BankTransactionID: r.BankTransactionID,
			OrderNumber:       r.OrderNumber,
			AmountTWD:         r.Amount,
			RecordStatus:      r.RecordStatus,
		})
	}
	return QueryRecordResult{
		Status:       resp.Status,
		Msg:          resp.Msg,
		TradeRecords: records,
	}, nil
}

// Refund refunds a trade, partially when Amount is set and fully when
// it is nil.
func (c *Client) Refund(ctx context.Context, req RefundParams) (RefundResult, error) {
	if req.RecTradeID == "" {
		return RefundResult{}, fmt.Errorf("tappaygateway: Refund requires RecTradeID")
	}
	wireReq := wireRefundParams{
		PartnerKey: c.partnerKey,
		RecTradeID: req.RecTradeID,
		Amount:     req.AmountTWD,
	}

	var resp wireRefundResult
	if err := c.do(ctx, refundPath, wireReq, &resp); err != nil {
		return RefundResult{}, err
	}
	return RefundResult{
		Status:       resp.Status,
		Msg:          resp.Msg,
		RecTradeID:   resp.RecTradeID,
		RefundAmount: resp.RefundAmount,
	}, nil
}

// do posts wireReq as JSON to path and decodes the response into out.
//
// Every error returned here goes through newGatewayError, which never
// takes the request or response body as an argument — see redact.go —
// so there is no path by which a caller formatting or logging the
// returned error can observe the partner key, a prime, or cardholder
// data: that content was never reachable from the error in the first
// place.
func (c *Client) do(ctx context.Context, path string, wireReq, out any) error {
	body, err := json.Marshal(wireReq)
	if err != nil {
		return newGatewayError(path, "encode request", err)
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+path, bytes.NewReader(body))
	if err != nil {
		return newGatewayError(path, "build request", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("x-api-key", c.partnerKey)

	resp, err := c.http.Do(httpReq)
	if err != nil {
		return newGatewayError(path, "do request", err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return newGatewayError(path, "read response", err)
	}
	if resp.StatusCode != http.StatusOK {
		return newGatewayError(path, fmt.Sprintf("http status %d", resp.StatusCode), nil)
	}
	if err := json.Unmarshal(respBody, out); err != nil {
		return newGatewayError(path, "decode response", err)
	}
	return nil
}
