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

// noticeRoute is what a route table entry yields: which provider sent the
// notice and which mode it arrived on. Both are read from the match, never
// from the request body — see handleNotify.
type noticeRoute struct {
	Provider string
	Mode     string
}

// noticeRouteKey packs a method and path into the table's lookup key. The
// convention every entry follows is /{provider}/{mode}/notify (D549).
func noticeRouteKey(method, path string) string {
	return method + " " + path
}

// noticeRoutes is the whole table. Only NewebPay's sandbox route is wired
// today; TapPay will add its own entry later, once its flows are confirmed
// (D549) — this table is what makes that an added row, not a second
// hard-coded path check.
var noticeRoutes = map[string]noticeRoute{
	noticeRouteKey(http.MethodPost, sandboxNotifyPath): {Provider: "newebpay", Mode: "sandbox"},
}

// matchNoticeRoute looks up method and path in the route table.
func matchNoticeRoute(method, path string) (noticeRoute, bool) {
	route, ok := noticeRoutes[noticeRouteKey(method, path)]
	return route, ok
}

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

// handleNotify is the shared body for every route in noticeRoutes — today
// only POST /newebpay/sandbox/notify — used by both the Lambda handler and
// the local HTTP handler so both transports get identical behaviour and
// identical test coverage.
//
// 🔴 NO DECRYPTION, NO VERIFICATION, NO LOOKUP, NO KEY. The ingress only
// shapes the body into a Notice and hands it to sink. Provider and Mode come
// from the matched route, never from anything in the body.
func handleNotify(ctx context.Context, sink NoticeSink, route noticeRoute, path string, body []byte, requestID string) routeResponse {
	resp := computeNoticeResponse(ctx, sink, route, body, requestID)
	// 🔴 LOG ONLY THESE FIVE FIELDS. Never the body, TradeInfo or TradeSha —
	// TradeInfo is the encrypted payload and TradeSha is its MAC over a
	// shared HashKey/HashIV; logging either turns application logs into a
	// place that shared secret's input leaks from.
	slog.InfoContext(ctx, "payment provider notice received",
		"provider", route.Provider,
		"mode", route.Mode,
		"method", http.MethodPost,
		"path", path,
		"body_bytes", len(body),
		"status", resp.status,
		"request_id", requestID,
	)
	return resp
}

func computeNoticeResponse(ctx context.Context, sink NoticeSink, route noticeRoute, body []byte, requestID string) routeResponse {
	// The notice queue isn't deployed yet in this environment
	// (mirrorstack-infra#435 not landed, or landed with the variable still
	// unset): behave like every other not-yet-wired path rather than
	// pretending the route works. See newNoticeSink and unconfiguredSink.
	if _, ok := sink.(unconfiguredSink); ok {
		return jsonRouteResponse(http.StatusNotImplemented, map[string]string{"error": noProviderMessage})
	}
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
		Provider:   route.Provider,
		Mode:       route.Mode,
		ReceivedAt: time.Now().UTC(),
		Fields:     fields,
		RequestID:  requestID,
	}
	if err := sink.Send(ctx, notice); err != nil {
		return jsonRouteResponse(http.StatusServiceUnavailable, map[string]string{"error": "notice queue unavailable"})
	}
	return jsonRouteResponse(http.StatusOK, map[string]string{"status": "accepted"})
}
