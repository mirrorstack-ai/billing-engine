// Command account-api is the billing-engine account-side Lambda.
//
// v1 bootstrap: bare HTTP skeleton with /__health and a stub group
// for future internal-secret-gated routes. Real subscription / portal
// / invoice handlers land in subsequent PRs.
//
// Runtime mode is auto-detected from AWS_LAMBDA_FUNCTION_NAME: in
// Lambda the chi router is wrapped via aws-lambda-go-api-proxy;
// locally it runs as a plain net/http server on ACCOUNT_API_PORT
// (default 8091).
package main

import (
	"log/slog"
	"net/http"
	"os"
	"time"

	"github.com/aws/aws-lambda-go/lambda"
	chiadapter "github.com/awslabs/aws-lambda-go-api-proxy/chi"
	"github.com/go-chi/chi/v5"
)

var adapter *chiadapter.ChiLambda

func init() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stderr, nil)))
	adapter = chiadapter.New(buildRouter())
}

func buildRouter() *chi.Mux {
	r := chi.NewRouter()
	r.Use(requestLogger)

	// Public health probe — no auth, used by load balancers and uptime checks.
	r.Get("/__health", health)

	// Internal-secret-gated routes are wired in subsequent PRs.
	return r
}

func health(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(`{"status":"ok"}`))
}

// requestLogger emits a structured log line per request with method,
// path, status, and duration.
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
	if os.Getenv("AWS_LAMBDA_FUNCTION_NAME") != "" {
		lambda.Start(adapter.ProxyWithContext)
		return
	}
	port := os.Getenv("ACCOUNT_API_PORT")
	if port == "" {
		port = "8091"
	}
	slog.Info("account-api starting", "port", port)
	if err := http.ListenAndServe(":"+port, buildRouter()); err != nil {
		slog.Error("server error", "error", err)
		os.Exit(1)
	}
}
