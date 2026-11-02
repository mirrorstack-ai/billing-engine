package main

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/url"
	"time"
)

// sandboxNotifyPath is the only NewebPay route this binary answers today —
// owner decision D515 (docs/newebpay-sandbox-topology.md). The live path,
// /newebpay/notify, stays unrouted until core-v2#1849: it falls through to
// the same 501 every other path gets.
const sandboxNotifyPath = "/newebpay/sandbox/notify"

// maxNoticeBodyBytes caps a notify body at 64 KB. A request over the cap is
// answered 413 without the body being parsed.
const maxNoticeBodyBytes = 64 * 1024

// noticeFieldNames are the only fields this handler reads off a NewebPay
// notify body. Everything else in the form is ignored, read or not.
var noticeFieldNames = []string{"Status", "MerchantID", "TradeInfo", "TradeSha", "Version", "EncryptType"}

// requiredNoticeFields must be non-empty or the request is 400. TradeInfo and
// TradeSha are the encrypted payload and its MAC; MerchantID identifies the
// distributor. This handler does not decrypt TradeInfo or verify TradeSha —
// it only checks that NewebPay sent them.
var requiredNoticeFields = []string{"MerchantID", "TradeInfo", "TradeSha"}

// routeResponse is the transport-neutral answer both the Lambda and the
// local-HTTP handler render into their own response types.
type routeResponse struct {
	status int
	body   []byte
}

func jsonRouteResponse(status int, payload map[string]string) routeResponse {
	body, err := json.Marshal(payload)
	if err != nil {
		return routeResponse{status: http.StatusInternalServerError, body: []byte(`{"error":"internal"}`)}
	}
	return routeResponse{status: status, body: body}
}

// handleSandboxNotify is POST /newebpay/sandbox/notify, shared by the Lambda
// handler and the local HTTP handler so both transports get identical
// behaviour and identical test coverage.
//
// 🔴 NO DECRYPTION, NO VERIFICATION, NO LOOKUP, NO KEY. The ingress only
// shapes the body into a Notice and hands it to sink. Mode is "sandbox"
// because the path matched, never because of anything in the body.
func handleSandboxNotify(ctx context.Context, sink NoticeSink, body []byte, requestID string) routeResponse {
	resp := computeSandboxNoticeResponse(ctx, sink, body, requestID)
	// 🔴 LOG ONLY THESE FIVE FIELDS. Never the body, TradeInfo or TradeSha —
	// TradeInfo is the encrypted payload and TradeSha is its MAC over a
	// shared HashKey/HashIV; logging either turns application logs into a
	// place that shared secret's input leaks from.
	slog.InfoContext(ctx, "newebpay sandbox notice received",
		"method", http.MethodPost,
		"path", sandboxNotifyPath,
		"body_bytes", len(body),
		"status", resp.status,
		"request_id", requestID,
	)
	return resp
}

func computeSandboxNoticeResponse(ctx context.Context, sink NoticeSink, body []byte, requestID string) routeResponse {
	if len(body) > maxNoticeBodyBytes {
		return jsonRouteResponse(http.StatusRequestEntityTooLarge, map[string]string{"error": "request body exceeds 64KB"})
	}
	values, err := url.ParseQuery(string(body))
	if err != nil {
		return jsonRouteResponse(http.StatusBadRequest, map[string]string{"error": "malformed form body"})
	}
	fields := make(map[string]string, len(noticeFieldNames))
	for _, name := range noticeFieldNames {
		if v := values.Get(name); v != "" {
			fields[name] = v
		}
	}
	for _, name := range requiredNoticeFields {
		if fields[name] == "" {
			return jsonRouteResponse(http.StatusBadRequest, map[string]string{"error": "missing required field: " + name})
		}
	}
	notice := Notice{
		Mode:       "sandbox",
		ReceivedAt: time.Now().UTC(),
		Fields:     fields,
		RequestID:  requestID,
	}
	if err := sink.Send(ctx, notice); err != nil {
		return jsonRouteResponse(http.StatusServiceUnavailable, map[string]string{"error": "notice queue unavailable"})
	}
	return jsonRouteResponse(http.StatusOK, map[string]string{"status": "accepted"})
}
