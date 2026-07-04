// Command account-api is the billing-engine internal RPC Lambda.
//
// Two transports, one set of handlers:
//
//   - lambda.Invoke (production): payload is the {action, request} RPC
//     envelope; response is the {ok, response | error} envelope.
//   - HTTP (local dev): chi router on ACCOUNT_API_PORT (default 8091).
//     Three routes (one per RPC action); request body is the action's
//     Request struct directly; response is the same {ok, …} envelope.
//
// Auth contract:
//
//   - Production: IAM gates lambda.Invoke (the function URL is not
//     exposed via API Gateway in v1; api-platform invokes by ARN).
//   - Local HTTP: X-MS-Internal-Secret header on every non-/__health
//     route. The secret is fail-closed (empty → 503 secret_unconfigured).
//
// Spec: mirrorstack-docs/api/billing/account-api.md.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"os"
	"time"

	"github.com/aws/aws-lambda-go/lambda"
	"github.com/go-chi/chi/v5"

	"github.com/mirrorstack-ai/billing-engine/internal/account/billing"
	"github.com/mirrorstack-ai/billing-engine/internal/account/budget"
	"github.com/mirrorstack-ai/billing-engine/internal/account/usage"
	"github.com/mirrorstack-ai/billing-engine/internal/shared/auth"
	"github.com/mirrorstack-ai/billing-engine/internal/shared/config"
	"github.com/mirrorstack-ai/billing-engine/internal/shared/httputil"
	billingstripe "github.com/mirrorstack-ai/billing-engine/internal/shared/stripe"
)

// rpcEnvelope is the lambda.Invoke request payload shape.
type rpcEnvelope struct {
	Action  string          `json:"action"`
	Request json.RawMessage `json:"request"`
}

// rpcResponse is the unified response envelope returned to all callers.
type rpcResponse struct {
	OK       bool              `json:"ok"`
	Response any               `json:"response,omitempty"`
	Error    *rpcResponseError `json:"error,omitempty"`
}

type rpcResponseError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

// dispatcher is the action → handler dispatch shared by both transports.
type dispatcher struct {
	svc       *billing.Service
	usageSvc  *usage.Service
	budgetSvc *budget.Service
}

func (d *dispatcher) dispatch(ctx context.Context, action string, requestPayload json.RawMessage) (any, error) {
	switch action {
	case "Ensure":
		var req billing.EnsureRequest
		if err := json.Unmarshal(requestPayload, &req); err != nil {
			return nil, billing.InvalidInput("malformed request payload: " + err.Error())
		}
		return d.svc.Ensure(ctx, req)

	case "PrepareAddPaymentMethod":
		var req billing.PrepareAddPaymentMethodRequest
		if err := json.Unmarshal(requestPayload, &req); err != nil {
			return nil, billing.InvalidInput("malformed request payload: " + err.Error())
		}
		return d.svc.PrepareAddPaymentMethod(ctx, req)

	case "StartAddPaymentMethod":
		var req billing.StartAddPaymentMethodRequest
		if err := json.Unmarshal(requestPayload, &req); err != nil {
			return nil, billing.InvalidInput("malformed request payload: " + err.Error())
		}
		return d.svc.StartAddPaymentMethod(ctx, req)

	case "FinishAddPaymentMethod":
		var req billing.FinishAddPaymentMethodRequest
		if err := json.Unmarshal(requestPayload, &req); err != nil {
			return nil, billing.InvalidInput("malformed request payload: " + err.Error())
		}
		return d.svc.FinishAddPaymentMethod(ctx, req)

	case "GetPaymentMethods":
		var req billing.GetPaymentMethodsRequest
		if err := json.Unmarshal(requestPayload, &req); err != nil {
			return nil, billing.InvalidInput("malformed request payload: " + err.Error())
		}
		return d.svc.GetPaymentMethods(ctx, req)

	case "DetachPaymentMethod":
		var req billing.DetachPaymentMethodRequest
		if err := json.Unmarshal(requestPayload, &req); err != nil {
			return nil, billing.InvalidInput("malformed request payload: " + err.Error())
		}
		return d.svc.DetachPaymentMethod(ctx, req)

	case "SetDefaultPaymentMethod":
		var req billing.SetDefaultPaymentMethodRequest
		if err := json.Unmarshal(requestPayload, &req); err != nil {
			return nil, billing.InvalidInput("malformed request payload: " + err.Error())
		}
		return d.svc.SetDefaultPaymentMethod(ctx, req)

	case "RecordUsage":
		var req usage.RecordUsageRequest
		if err := json.Unmarshal(requestPayload, &req); err != nil {
			return nil, billing.InvalidInput("malformed request payload: " + err.Error())
		}
		return d.usageSvc.RecordUsage(ctx, req)

	case "GetUsageSummary":
		var req usage.GetUsageSummaryRequest
		if err := json.Unmarshal(requestPayload, &req); err != nil {
			return nil, billing.InvalidInput("malformed request payload: " + err.Error())
		}
		return d.usageSvc.GetUsageSummary(ctx, req)

	case "GetUsageHistory":
		var req usage.GetUsageHistoryRequest
		if err := json.Unmarshal(requestPayload, &req); err != nil {
			return nil, billing.InvalidInput("malformed request payload: " + err.Error())
		}
		return d.usageSvc.GetUsageHistory(ctx, req)

	case "GetVersionBreakdown":
		var req usage.GetVersionBreakdownRequest
		if err := json.Unmarshal(requestPayload, &req); err != nil {
			return nil, billing.InvalidInput("malformed request payload: " + err.Error())
		}
		return d.usageSvc.GetVersionBreakdown(ctx, req)

	case "GetAppUsageSummary":
		var req usage.GetAppUsageSummaryRequest
		if err := json.Unmarshal(requestPayload, &req); err != nil {
			return nil, billing.InvalidInput("malformed request payload: " + err.Error())
		}
		return d.usageSvc.GetAppUsageSummary(ctx, req)

	case "GetAppBill":
		var req usage.GetAppBillRequest
		if err := json.Unmarshal(requestPayload, &req); err != nil {
			return nil, billing.InvalidInput("malformed request payload: " + err.Error())
		}
		return d.usageSvc.GetAppBill(ctx, req)

	case "GetBillingPeriods":
		var req usage.GetBillingPeriodsRequest
		if err := json.Unmarshal(requestPayload, &req); err != nil {
			return nil, billing.InvalidInput("malformed request payload: " + err.Error())
		}
		return d.usageSvc.GetBillingPeriods(ctx, req)

	case "ListInvoices":
		var req usage.ListInvoicesRequest
		if err := json.Unmarshal(requestPayload, &req); err != nil {
			return nil, billing.InvalidInput("malformed request payload: " + err.Error())
		}
		return d.usageSvc.ListInvoices(ctx, req)

	case "SetMetricDefinitions":
		var req usage.SetMetricDefinitionsRequest
		if err := json.Unmarshal(requestPayload, &req); err != nil {
			return nil, billing.InvalidInput("malformed request payload: " + err.Error())
		}
		return d.usageSvc.SetMetricDefinitions(ctx, req)

	case "SetInfraPriceOverrides":
		var req usage.SetInfraPriceOverridesRequest
		if err := json.Unmarshal(requestPayload, &req); err != nil {
			return nil, billing.InvalidInput("malformed request payload: " + err.Error())
		}
		return d.usageSvc.SetInfraPriceOverrides(ctx, req)

	case "SetModuleVisibility":
		var req usage.SetModuleVisibilityRequest
		if err := json.Unmarshal(requestPayload, &req); err != nil {
			return nil, billing.InvalidInput("malformed request payload: " + err.Error())
		}
		return d.usageSvc.SetModuleVisibility(ctx, req)

	case "RecordInfraUsage":
		var req usage.RecordInfraUsageRequest
		if err := json.Unmarshal(requestPayload, &req); err != nil {
			return nil, billing.InvalidInput("malformed request payload: " + err.Error())
		}
		return d.usageSvc.RecordInfraUsage(ctx, req)

	case "SetBudget":
		var req budget.SetBudgetRequest
		if err := json.Unmarshal(requestPayload, &req); err != nil {
			return nil, billing.InvalidInput("malformed request payload: " + err.Error())
		}
		return d.budgetSvc.SetBudget(ctx, req)

	case "GetBudgetStatus":
		var req budget.GetBudgetStatusRequest
		if err := json.Unmarshal(requestPayload, &req); err != nil {
			return nil, billing.InvalidInput("malformed request payload: " + err.Error())
		}
		return d.budgetSvc.GetBudgetStatus(ctx, req)

	case "GetBudgetAlerts":
		var req budget.GetBudgetAlertsRequest
		if err := json.Unmarshal(requestPayload, &req); err != nil {
			return nil, billing.InvalidInput("malformed request payload: " + err.Error())
		}
		return d.budgetSvc.GetBudgetAlerts(ctx, req)

	default:
		return nil, billing.InvalidInput("unknown action: " + action)
	}
}

// buildResponse normalizes a service result into the wire envelope.
// Unknown error types collapse to INTERNAL.
func buildResponse(resp any, err error) rpcResponse {
	if err == nil {
		return rpcResponse{OK: true, Response: resp}
	}
	var be *billing.Error
	if errors.As(err, &be) {
		return rpcResponse{OK: false, Error: &rpcResponseError{Code: string(be.Code), Message: be.Message}}
	}
	return rpcResponse{OK: false, Error: &rpcResponseError{Code: string(billing.CodeInternal), Message: err.Error()}}
}

// httpStatusForError maps a billing.Code to the HTTP status the local
// dev path returns. Production lambda.Invoke ignores HTTP status —
// callers read the envelope's OK flag — but the local HTTP path
// surfaces meaningful codes for curl-friendly debugging.
func httpStatusForError(err error) int {
	if err == nil {
		return http.StatusOK
	}
	var be *billing.Error
	if errors.As(err, &be) {
		switch be.Code {
		case billing.CodeInvalidInput:
			return http.StatusBadRequest
		case billing.CodeNotFound:
			return http.StatusNotFound
		case billing.CodeStripeError:
			return http.StatusBadGateway
		case billing.CodeInternal:
			return http.StatusInternalServerError
		}
	}
	return http.StatusInternalServerError
}

// makeHTTPHandler returns a chi handler for the given action. The HTTP
// body is the action's Request struct directly (no envelope) — the path
// identifies the action.
func makeHTTPHandler(d *dispatcher, action string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			httputil.JSON(w, http.StatusBadRequest, buildResponse(nil, billing.InvalidInput("failed to read body: "+err.Error())))
			return
		}
		resp, err := d.dispatch(r.Context(), action, body)
		httputil.JSON(w, httpStatusForError(err), buildResponse(resp, err))
	}
}

// --- lifecycle -----------------------------------------------------------

var disp *dispatcher

// buildDispatcher wires the production dispatcher from the environment. It is
// called from main() (NOT an init()) so the test binary — which imports this
// package to exercise buildRouter — does not trigger the os.Exit-on-missing-env
// config loads at package load time.
func buildDispatcher() *dispatcher {
	pool := config.MustPgxPool()
	stripeKey := config.MustEnv("STRIPE_SECRET_KEY")
	// Post-confirmation redirect target for the setup-mode Checkout
	// Session (the frontend billing page). Required by ui_mode=elements.
	returnURL := config.MustEnv("BILLING_RETURN_URL")

	store := billing.NewStore(pool)
	stripeClient := billingstripe.NewClient(stripeKey)
	svc := billing.NewService(store, stripeClient, returnURL)

	budgetSvc := budget.NewService(budget.NewStore(pool))
	// The ingest path fires the per-app budget hook best-effort on a fresh
	// usage event (design §5 / §10).
	usageSvc := usage.NewService(usage.NewStore(pool)).WithBudgetEvaluator(budgetSvc)

	return &dispatcher{svc: svc, usageSvc: usageSvc, budgetSvc: budgetSvc}
}

func buildRouter(d *dispatcher) *chi.Mux {
	r := chi.NewRouter()
	r.Use(requestLogger)

	// Public health probe — no auth.
	r.Get("/__health", health)

	// Internal-secret-gated RPC routes (api-platform → billing-engine).
	internalSecret := os.Getenv("INTERNAL_SECRET")
	r.Group(func(r chi.Router) {
		r.Use(auth.InternalSecret(internalSecret))
		r.Post("/v1/billing.Ensure", makeHTTPHandler(d, "Ensure"))
		r.Post("/v1/billing.PrepareAddPaymentMethod", makeHTTPHandler(d, "PrepareAddPaymentMethod"))
		r.Post("/v1/billing.StartAddPaymentMethod", makeHTTPHandler(d, "StartAddPaymentMethod"))
		r.Post("/v1/billing.FinishAddPaymentMethod", makeHTTPHandler(d, "FinishAddPaymentMethod"))
		r.Post("/v1/billing.GetPaymentMethods", makeHTTPHandler(d, "GetPaymentMethods"))
		r.Post("/v1/billing.DetachPaymentMethod", makeHTTPHandler(d, "DetachPaymentMethod"))
		r.Post("/v1/billing.SetDefaultPaymentMethod", makeHTTPHandler(d, "SetDefaultPaymentMethod"))
		// Usage RPCs invoked by the platform control plane (manifest metric
		// sync on install/publish, publish-hook visibility, billing-summary
		// read), not the high-volume meter seam — they share the internal
		// secret with the other api-platform → billing calls.
		r.Post("/v1/billing.GetUsageSummary", makeHTTPHandler(d, "GetUsageSummary"))
		// Trend-chart + per-version breakdown reads (this PR): both are
		// read-only surface over the SAME data GetUsageSummary/rollup already
		// produce, so they share its control-plane credential and route group.
		r.Post("/v1/billing.GetUsageHistory", makeHTTPHandler(d, "GetUsageHistory"))
		r.Post("/v1/billing.GetVersionBreakdown", makeHTTPHandler(d, "GetVersionBreakdown"))
		// App-owner per-app bill (this PR): the current-period usage the app
		// owner pays for ONE app (/apps/{appId}/settings/billing). Read-only
		// over the SAME usage data the other summary reads produce, so it shares
		// the control-plane credential + route group.
		r.Post("/v1/billing.GetAppUsageSummary", makeHTTPHandler(d, "GetAppUsageSummary"))
		// Full app-owner bill (this PR): the whole 最終費用 structure for ONE app in
		// ONE period — 基本費用 base fee + 模組使用量 module usage + 基礎設施
		// infrastructure − PaaS 額度 credit — plus the period selector list. Both are
		// read-only over the SAME usage/aggregate/period data, so they share the
		// control-plane credential + route group.
		r.Post("/v1/billing.GetAppBill", makeHTTPHandler(d, "GetAppBill"))
		r.Post("/v1/billing.GetBillingPeriods", makeHTTPHandler(d, "GetBillingPeriods"))
		// Account invoice HISTORY (this PR): the customer's mirrored Stripe
		// invoices for the web-account billing page — a keyset-paged, read-only
		// surface over ms_billing.invoices (the mirror this service already
		// owns; NO Stripe round-trip), so it shares the control-plane
		// credential + route group with the other account-billing reads.
		r.Post("/v1/billing.ListInvoices", makeHTTPHandler(d, "ListInvoices"))
		r.Post("/v1/billing.SetMetricDefinitions", makeHTTPHandler(d, "SetMetricDefinitions"))
		// Per-module infra price OVERRIDES (decision 19 §4.3) — the INVERSE of
		// SetMetricDefinitions: it persists a module's ms.Meter("infra.X",
		// ms.Price(n)) override for a RESERVED platform-infra metric (which
		// SetMetricDefinitions rejects) as a price-only per-(module, metric)
		// catalog row. Control-plane (fired by api-platform's metric sync on
		// publish), so it shares the internal secret + route group.
		r.Post("/v1/billing.SetInfraPriceOverrides", makeHTTPHandler(d, "SetInfraPriceOverrides"))
		r.Post("/v1/billing.SetModuleVisibility", makeHTTPHandler(d, "SetModuleVisibility"))
		// Platform-infra ingest (Plane 1). RecordInfraUsage is the INVERSE of
		// the SDK meter seam: it is called by platform-trusted producers
		// (dispatch compute, cdn-worker egress — deferred PRs), accepts ONLY
		// reserved infra.* / platform.* metrics, and is gated by the INTERNAL
		// secret (NOT the meter secret) so it shares the control-plane
		// credential and a module can never reach it (design §3a / §5).
		r.Post("/v1/billing.RecordInfraUsage", makeHTTPHandler(d, "RecordInfraUsage"))
		// Budget RPCs — control-plane (api-platform writes the cap, reads
		// status/alerts for in-app display). Internal secret, NOT the meter
		// secret: budget config is low-volume and shares the api-platform
		// credential, while evaluation runs internally off the meter ingest.
		r.Post("/v1/billing.SetBudget", makeHTTPHandler(d, "SetBudget"))
		r.Post("/v1/billing.GetBudgetStatus", makeHTTPHandler(d, "GetBudgetStatus"))
		r.Post("/v1/billing.GetBudgetAlerts", makeHTTPHandler(d, "GetBudgetAlerts"))
	})

	// Meter-secret-gated ingest route. RecordUsage is the high-volume
	// dispatch-asserted metering seam; it carries a DEDICATED secret +
	// header (X-MS-Meter-Secret) so it can be rotated independently of the
	// general internal secret and never shares a credential with the
	// Stripe-touching billing RPCs (design §5).
	meterSecret := os.Getenv("METER_SECRET")
	r.Group(func(r chi.Router) {
		r.Use(auth.MeterSecret(meterSecret))
		r.Post("/v1/billing.RecordUsage", makeHTTPHandler(d, "RecordUsage"))
	})

	return r
}

func health(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(`{"status":"ok"}`))
}

func requestLogger(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		sw := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		slog.Info("request.start", "method", r.Method, "path", r.URL.Path)
		next.ServeHTTP(sw, r)
		slog.Info("request.end",
			"method", r.Method,
			"path", r.URL.Path,
			"status", sw.status,
			"duration_ms", time.Since(start).Milliseconds(),
		)
	})
}

type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (s *statusRecorder) WriteHeader(code int) {
	s.status = code
	s.ResponseWriter.WriteHeader(code)
}

// --- lambda.Invoke entry point -------------------------------------------

// lambdaInvokeHandler is the entry point when running inside Lambda.
// Payload is the RPC envelope; response is the marshaled envelope.
// Errors from dispatch flow through the envelope's ok=false path; the
// Go-level error return is reserved for marshaling failures.
//
// AUTH NOTE (plane isolation): in prod the dispatcher is reached only via
// lambda.Invoke — the function is not publicly exposed, so invocation is gated
// by IAM (the caller needs lambda:InvokeFunction on this ARN). That IAM grant,
// not the X-MS-Internal-Secret header (which the local HTTP path checks in
// buildRouter), is what keeps RecordInfraUsage and the other internal RPCs out
// of a module's reach in production. RecordInfraUsage is absent from any
// SDK-accessible / meter-secret surface on BOTH transports (design §3a / §5).
func lambdaInvokeHandler(ctx context.Context, payload json.RawMessage) (json.RawMessage, error) {
	var env rpcEnvelope
	if err := json.Unmarshal(payload, &env); err != nil {
		return json.Marshal(buildResponse(nil, billing.InvalidInput("malformed envelope: "+err.Error())))
	}
	resp, err := disp.dispatch(ctx, env.Action, env.Request)
	return json.Marshal(buildResponse(resp, err))
}

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stderr, nil)))
	disp = buildDispatcher()

	if config.IsLambda() {
		lambda.Start(lambdaInvokeHandler)
		return
	}
	port := config.Port("ACCOUNT_API_PORT", "8091")
	slog.Info("account-api starting", "port", port, "mode", "http-local")
	if err := http.ListenAndServe(":"+port, buildRouter(disp)); err != nil {
		slog.Error("server error", "error", err)
		os.Exit(1)
	}
}
