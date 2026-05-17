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
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/mirrorstack-ai/billing-engine/internal/account/billing"
	"github.com/mirrorstack-ai/billing-engine/internal/shared/middleware"
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
	svc *billing.Service
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

	case "GetPaymentMethods":
		var req billing.GetPaymentMethodsRequest
		if err := json.Unmarshal(requestPayload, &req); err != nil {
			return nil, billing.InvalidInput("malformed request payload: " + err.Error())
		}
		return d.svc.GetPaymentMethods(ctx, req)

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
			writeJSON(w, http.StatusBadRequest, buildResponse(nil, billing.InvalidInput("failed to read body: "+err.Error())))
			return
		}
		resp, err := d.dispatch(r.Context(), action, body)
		writeJSON(w, httpStatusForError(err), buildResponse(resp, err))
	}
}

func writeJSON(w http.ResponseWriter, status int, payload rpcResponse) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(payload); err != nil {
		slog.Error("encode response failed", "error", err)
	}
}

// --- lifecycle -----------------------------------------------------------

var disp *dispatcher

func init() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stderr, nil)))

	pool := mustPgxPool()
	stripeKey := os.Getenv("STRIPE_SECRET_KEY")
	if stripeKey == "" {
		slog.Error("STRIPE_SECRET_KEY not set")
		os.Exit(1)
	}

	store := billing.NewStore(pool)
	stripeClient := billingstripe.NewClient(stripeKey)
	svc := billing.NewService(store, stripeClient)
	disp = &dispatcher{svc: svc}
}

func mustPgxPool() *pgxpool.Pool {
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		slog.Error("DATABASE_URL not set")
		os.Exit(1)
	}
	pool, err := pgxpool.New(context.Background(), dsn)
	if err != nil {
		slog.Error("pgxpool init failed", "error", err)
		os.Exit(1)
	}
	return pool
}

func buildRouter(d *dispatcher) *chi.Mux {
	r := chi.NewRouter()
	r.Use(requestLogger)

	// Public health probe — no auth.
	r.Get("/__health", health)

	// Internal-secret-gated RPC routes.
	internalSecret := os.Getenv("INTERNAL_SECRET")
	r.Group(func(r chi.Router) {
		r.Use(middleware.InternalSecret(internalSecret))
		r.Post("/v1/billing.Ensure", makeHTTPHandler(d, "Ensure"))
		r.Post("/v1/billing.PrepareAddPaymentMethod", makeHTTPHandler(d, "PrepareAddPaymentMethod"))
		r.Post("/v1/billing.GetPaymentMethods", makeHTTPHandler(d, "GetPaymentMethods"))
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

// lambdaInvokeHandler is the entry point when AWS_LAMBDA_FUNCTION_NAME
// is set. Payload is the RPC envelope; response is the marshaled
// envelope. Errors from dispatch flow through the envelope's ok=false
// path; the Go-level error return is reserved for marshaling failures.
func lambdaInvokeHandler(ctx context.Context, payload json.RawMessage) (json.RawMessage, error) {
	var env rpcEnvelope
	if err := json.Unmarshal(payload, &env); err != nil {
		return json.Marshal(buildResponse(nil, billing.InvalidInput("malformed envelope: "+err.Error())))
	}
	resp, err := disp.dispatch(ctx, env.Action, env.Request)
	return json.Marshal(buildResponse(resp, err))
}

func main() {
	if os.Getenv("AWS_LAMBDA_FUNCTION_NAME") != "" {
		lambda.Start(lambdaInvokeHandler)
		return
	}
	port := os.Getenv("ACCOUNT_API_PORT")
	if port == "" {
		port = "8091"
	}
	slog.Info("account-api starting", "port", port, "mode", "http-local")
	if err := http.ListenAndServe(":"+port, buildRouter(disp)); err != nil {
		slog.Error("server error", "error", err)
		os.Exit(1)
	}
}
