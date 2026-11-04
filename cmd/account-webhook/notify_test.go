package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"testing"

	"github.com/aws/aws-lambda-go/events"
)

func goodNoticeBody() string {
	v := url.Values{}
	v.Set("Status", "SUCCESS")
	v.Set("MerchantID", "MS12345678")
	v.Set("TradeInfo", "planted-tradeinfo-blob")
	v.Set("TradeSha", "planted-tradesha-mac")
	v.Set("Version", "2.0")
	v.Set("EncryptType", "1")
	return v.Encode()
}

// --- local HTTP handler: the route table ---

func TestHTTPHandler_SandboxNotify_GoodBody_Answers200AndQueuesOne(t *testing.T) {
	sink := &InMemorySink{}
	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, sandboxNotifyPath, strings.NewReader(goodNoticeBody()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	httpHandler(sink).ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status: got %d, want %d, body=%s", rr.Code, http.StatusOK, rr.Body.String())
	}
	if got := sink.Count(); got != 1 {
		t.Fatalf("sink notices: got %d, want 1", got)
	}
	if sink.Notices[0].Mode != "sandbox" {
		t.Errorf("notice mode: got %q, want %q", sink.Notices[0].Mode, "sandbox")
	}
	if sink.Notices[0].Provider != "newebpay" {
		t.Errorf("notice provider: got %q, want %q", sink.Notices[0].Provider, "newebpay")
	}
}

func TestHTTPHandler_SandboxNotify_SinkFails_Answers503AndNoNoticeRecorded(t *testing.T) {
	sink := &InMemorySink{Err: errors.New("sqs unavailable")}
	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, sandboxNotifyPath, strings.NewReader(goodNoticeBody()))

	httpHandler(sink).ServeHTTP(rr, req)

	if rr.Code != http.StatusServiceUnavailable {
		t.Fatalf("status: got %d, want %d", rr.Code, http.StatusServiceUnavailable)
	}
	if rr.Code == http.StatusOK {
		t.Fatal("a failing sink must never answer 200")
	}
	if got := sink.Count(); got != 0 {
		t.Fatalf("sink notices: got %d, want 0", got)
	}
}

func TestHTTPHandler_SandboxNotify_OversizeBody_Answers413(t *testing.T) {
	sink := &InMemorySink{}
	rr := httptest.NewRecorder()
	oversize := strings.Repeat("a", maxNoticeBodyBytes+1)
	req := httptest.NewRequest(http.MethodPost, sandboxNotifyPath, strings.NewReader("MerchantID="+oversize))

	httpHandler(sink).ServeHTTP(rr, req)

	if rr.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("status: got %d, want %d", rr.Code, http.StatusRequestEntityTooLarge)
	}
	if got := sink.Count(); got != 0 {
		t.Fatalf("sink notices: got %d, want 0 — an oversize body must never reach the sink", got)
	}
}

func TestHTTPHandler_SandboxNotify_MissingRequiredField_Answers400(t *testing.T) {
	for _, missing := range []string{"MerchantID", "TradeInfo", "TradeSha"} {
		t.Run(missing, func(t *testing.T) {
			v := url.Values{}
			v.Set("Status", "SUCCESS")
			v.Set("MerchantID", "MS12345678")
			v.Set("TradeInfo", "blob")
			v.Set("TradeSha", "mac")
			v.Del(missing)

			sink := &InMemorySink{}
			rr := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodPost, sandboxNotifyPath, strings.NewReader(v.Encode()))

			httpHandler(sink).ServeHTTP(rr, req)

			if rr.Code != http.StatusBadRequest {
				t.Fatalf("status: got %d, want %d", rr.Code, http.StatusBadRequest)
			}
			if got := sink.Count(); got != 0 {
				t.Fatalf("sink notices: got %d, want 0", got)
			}
		})
	}
}

func TestHTTPHandler_SandboxNotify_WrongMethod_AnswersTodaysAnswer(t *testing.T) {
	sink := &InMemorySink{}
	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, sandboxNotifyPath, nil)

	httpHandler(sink).ServeHTTP(rr, req)

	if rr.Code != http.StatusNotImplemented {
		t.Fatalf("status: got %d, want %d", rr.Code, http.StatusNotImplemented)
	}
	var body map[string]string
	if err := json.Unmarshal(rr.Body.Bytes(), &body); err != nil {
		t.Fatalf("body is not JSON: %v", err)
	}
	if body["error"] != noProviderMessage {
		t.Errorf("error: got %q, want %q", body["error"], noProviderMessage)
	}
}

func TestHTTPHandler_LiveNotifyPath_StaysUnrouted(t *testing.T) {
	sink := &InMemorySink{}
	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/newebpay/notify", strings.NewReader(goodNoticeBody()))

	httpHandler(sink).ServeHTTP(rr, req)

	if rr.Code != http.StatusNotImplemented {
		t.Fatalf("status: got %d, want %d — the live path must stay unrouted until core-v2#1849", rr.Code, http.StatusNotImplemented)
	}
	if got := sink.Count(); got != 0 {
		t.Fatalf("sink notices: got %d, want 0 — the live path must never reach the sink", got)
	}
}

func TestHTTPHandler_WebhookPath_StillAnswers501(t *testing.T) {
	sink := &InMemorySink{}
	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, webhookPath, strings.NewReader(goodNoticeBody()))

	httpHandler(sink).ServeHTTP(rr, req)

	if rr.Code != http.StatusNotImplemented {
		t.Fatalf("status: got %d, want %d", rr.Code, http.StatusNotImplemented)
	}
}

// --- route table: method+path yields provider and mode, not one hard-coded path ---

func TestMatchNoticeRoute(t *testing.T) {
	route, ok := matchNoticeRoute(http.MethodPost, sandboxNotifyPath)
	if !ok {
		t.Fatalf("expected a route match for %s %s", http.MethodPost, sandboxNotifyPath)
	}
	if route.Provider != "newebpay" || route.Mode != "sandbox" {
		t.Errorf("route: got %+v, want {Provider:newebpay Mode:sandbox}", route)
	}

	if _, ok := matchNoticeRoute(http.MethodPost, "/newebpay/notify"); ok {
		t.Error("the live path must not be in the route table yet (core-v2#1849)")
	}
	if _, ok := matchNoticeRoute(http.MethodGet, sandboxNotifyPath); ok {
		t.Error("GET must not match the sandbox notify route")
	}
}

// --- unconfigured sink: the notice queue isn't deployed yet ---

func TestHTTPHandler_SandboxNotify_UnconfiguredSink_Answers501WithTodaysBody(t *testing.T) {
	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, sandboxNotifyPath, strings.NewReader(goodNoticeBody()))

	httpHandler(unconfiguredSink{}).ServeHTTP(rr, req)

	if rr.Code != http.StatusNotImplemented {
		t.Fatalf("status: got %d, want %d", rr.Code, http.StatusNotImplemented)
	}
	var body map[string]string
	if err := json.Unmarshal(rr.Body.Bytes(), &body); err != nil {
		t.Fatalf("body is not JSON: %v", err)
	}
	if body["error"] != noProviderMessage {
		t.Errorf("error: got %q, want %q (today's body, like every other unwired path)", body["error"], noProviderMessage)
	}
}

func TestProxyHandler_SandboxNotify_UnconfiguredSink_Answers501(t *testing.T) {
	resp, err := proxyHandler(unconfiguredSink{})(context.Background(), events.APIGatewayProxyRequest{
		Path:       sandboxNotifyPath,
		HTTPMethod: http.MethodPost,
		Body:       goodNoticeBody(),
	})
	if err != nil {
		t.Fatalf("proxyHandler returned an error: %v", err)
	}
	if resp.StatusCode != http.StatusNotImplemented {
		t.Fatalf("status: got %d, want %d — a missing queue URL must never exit or 503, only answer like an unwired path", resp.StatusCode, http.StatusNotImplemented)
	}
	if !strings.Contains(resp.Body, noProviderMessage) {
		t.Errorf("body %q does not carry today's not-wired message", resp.Body)
	}
}

// --- Lambda handler: same route table, same shared function ---

func TestProxyHandler_SandboxNotify_GoodBody_Answers200AndQueuesOne(t *testing.T) {
	sink := &InMemorySink{}
	resp, err := proxyHandler(sink)(context.Background(), events.APIGatewayProxyRequest{
		Path:       sandboxNotifyPath,
		HTTPMethod: http.MethodPost,
		Body:       goodNoticeBody(),
	})
	if err != nil {
		t.Fatalf("proxyHandler returned an error: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want %d, body=%s", resp.StatusCode, http.StatusOK, resp.Body)
	}
	if got := sink.Count(); got != 1 {
		t.Fatalf("sink notices: got %d, want 1", got)
	}
}

func TestProxyHandler_SandboxNotify_SinkFails_Answers503(t *testing.T) {
	sink := &InMemorySink{Err: errors.New("sqs unavailable")}
	resp, err := proxyHandler(sink)(context.Background(), events.APIGatewayProxyRequest{
		Path:       sandboxNotifyPath,
		HTTPMethod: http.MethodPost,
		Body:       goodNoticeBody(),
	})
	if err != nil {
		t.Fatalf("proxyHandler returned an error: %v", err)
	}
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("status: got %d, want %d", resp.StatusCode, http.StatusServiceUnavailable)
	}
}

func TestProxyHandler_SandboxNotify_OversizeBody_Answers413(t *testing.T) {
	sink := &InMemorySink{}
	oversize := strings.Repeat("a", maxNoticeBodyBytes+1)
	resp, err := proxyHandler(sink)(context.Background(), events.APIGatewayProxyRequest{
		Path:       sandboxNotifyPath,
		HTTPMethod: http.MethodPost,
		Body:       "MerchantID=" + oversize,
	})
	if err != nil {
		t.Fatalf("proxyHandler returned an error: %v", err)
	}
	if resp.StatusCode != http.StatusRequestEntityTooLarge {
		t.Fatalf("status: got %d, want %d", resp.StatusCode, http.StatusRequestEntityTooLarge)
	}
}

func TestProxyHandler_SandboxNotify_MissingField_Answers400(t *testing.T) {
	resp, err := proxyHandler(&InMemorySink{})(context.Background(), events.APIGatewayProxyRequest{
		Path:       sandboxNotifyPath,
		HTTPMethod: http.MethodPost,
		Body:       "MerchantID=MS1&TradeInfo=blob",
	})
	if err != nil {
		t.Fatalf("proxyHandler returned an error: %v", err)
	}
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status: got %d, want %d", resp.StatusCode, http.StatusBadRequest)
	}
}

func TestProxyHandler_SandboxNotify_WrongMethod_AnswersTodaysAnswer(t *testing.T) {
	resp, err := proxyHandler(&InMemorySink{})(context.Background(), events.APIGatewayProxyRequest{
		Path:       sandboxNotifyPath,
		HTTPMethod: http.MethodGet,
	})
	if err != nil {
		t.Fatalf("proxyHandler returned an error: %v", err)
	}
	if resp.StatusCode != http.StatusNotImplemented {
		t.Fatalf("status: got %d, want %d", resp.StatusCode, http.StatusNotImplemented)
	}
}

func TestProxyHandler_LiveNotifyPath_StaysUnrouted(t *testing.T) {
	resp, err := proxyHandler(&InMemorySink{})(context.Background(), events.APIGatewayProxyRequest{
		Path:       "/newebpay/notify",
		HTTPMethod: http.MethodPost,
		Body:       goodNoticeBody(),
	})
	if err != nil {
		t.Fatalf("proxyHandler returned an error: %v", err)
	}
	if resp.StatusCode != http.StatusNotImplemented {
		t.Fatalf("status: got %d, want %d — the live path must stay unrouted until core-v2#1849", resp.StatusCode, http.StatusNotImplemented)
	}
}

// --- log scan: the secrets must never appear in a log line ---

func TestSandboxNotify_NeverLogsTradeInfoOrTradeSha(t *testing.T) {
	const plantedTradeInfo = "plant-marker-tradeinfo-DEADBEEF"
	const plantedTradeSha = "plant-marker-tradesha-CAFEF00D"

	var buf bytes.Buffer
	prevLogger := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&buf, nil)))
	defer slog.SetDefault(prevLogger)

	v := url.Values{}
	v.Set("MerchantID", "MS12345678")
	v.Set("TradeInfo", plantedTradeInfo)
	v.Set("TradeSha", plantedTradeSha)

	sink := &InMemorySink{}
	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, sandboxNotifyPath, strings.NewReader(v.Encode()))
	httpHandler(sink).ServeHTTP(rr, req)

	// Also exercise the failure path and the oversize path, since those are
	// the other two places a log line gets written for this route.
	failSink := &InMemorySink{Err: errors.New("boom")}
	rr2 := httptest.NewRecorder()
	req2 := httptest.NewRequest(http.MethodPost, sandboxNotifyPath, strings.NewReader(v.Encode()))
	httpHandler(failSink).ServeHTTP(rr2, req2)

	logs := buf.String()
	for _, line := range strings.Split(strings.TrimSpace(logs), "\n") {
		if strings.Contains(line, plantedTradeInfo) {
			t.Fatalf("log line contains TradeInfo: %s", line)
		}
		if strings.Contains(line, plantedTradeSha) {
			t.Fatalf("log line contains TradeSha: %s", line)
		}
	}
	if !strings.Contains(logs, strconv.Itoa(http.StatusOK)) {
		t.Fatalf("expected a logged outcome for the successful request; logs=%s", logs)
	}
}
