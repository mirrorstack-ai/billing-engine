// Command account-api is the billing-engine account-side Lambda.
//
// It serves internal-only endpoints consumed by api-platform's account
// service (subscription state, plan changes, etc.). Every non-public route
// is gated by the shared X-MS-Internal-Secret header.
//
// Runtime mode is auto-detected from the AWS_LAMBDA_FUNCTION_NAME env var:
// in Lambda the chi router is wrapped via aws-lambda-go-api-proxy; locally
// it runs as a plain net/http server on ACCOUNT_API_PORT (default 8091).
package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"time"

	"github.com/aws/aws-lambda-go/lambda"
	chiadapter "github.com/awslabs/aws-lambda-go-api-proxy/chi"
	"github.com/go-chi/chi/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/mirrorstack-ai/billing-engine/internal/account/db"
	"github.com/mirrorstack-ai/billing-engine/internal/account/handler"
	"github.com/mirrorstack-ai/billing-engine/internal/account/service"
	"github.com/mirrorstack-ai/billing-engine/internal/shared/middleware"
	mstripe "github.com/mirrorstack-ai/billing-engine/internal/shared/stripe"
)

// subscriptions is the handler for /subscriptions/* routes. Set by main()
// at startup; unit tests on this package may leave it nil because the
// only routes they exercise (/__health) do not depend on it.
var subscriptions *handler.Subscriptions

func init() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stderr, nil)))
}

// mustBuildSubscriptions wires the subscriptions handler from env. Fails
// fast at cold-start if DATABASE_URL or STRIPE_SECRET_KEY is missing.
func mustBuildSubscriptions() *handler.Subscriptions {
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		slog.Error("DATABASE_URL not set")
		os.Exit(1)
	}
	pool, err := pgxpool.New(context.Background(), dsn)
	if err != nil {
		slog.Error("failed to create pgx pool", "error", err)
		os.Exit(1)
	}
	stripeClient, err := mstripe.NewClient()
	if err != nil {
		slog.Error("failed to create stripe client", "error", err)
		os.Exit(1)
	}
	svc := service.NewSubscriptions(db.NewBillingAccounts(pool), service.NewStripeAdapter(stripeClient))
	return handler.NewSubscriptions(svc)
}

func buildRouter() *chi.Mux {
	r := chi.NewRouter()
	r.Use(requestLogger)

	// Public health probe — no auth, used by load balancers and uptime checks.
	r.Get("/__health", health)

	// Everything else is gated on the internal shared secret.
	r.Group(func(r chi.Router) {
		r.Use(middleware.InternalSecret())
		if subscriptions != nil {
			r.Post("/subscriptions/create", subscriptions.Create)
		}
	})

	return r
}

func health(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(`{"status":"ok"}`))
}

// requestLogger emits a structured log line per request with method, path,
// status, and duration. Status is captured via a thin ResponseWriter wrapper.
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

func main() {
	subscriptions = mustBuildSubscriptions()
	router := buildRouter()

	if os.Getenv("AWS_LAMBDA_FUNCTION_NAME") != "" {
		adapter := chiadapter.New(router)
		lambda.Start(adapter.ProxyWithContext)
		return
	}
	port := os.Getenv("ACCOUNT_API_PORT")
	if port == "" {
		port = "8091"
	}
	slog.Info("account-api starting", "port", port)
	if err := http.ListenAndServe(":"+port, router); err != nil {
		slog.Error("server error", "error", err)
		os.Exit(1)
	}
}
