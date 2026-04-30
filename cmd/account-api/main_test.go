package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/mirrorstack-ai/billing-engine/internal/shared/middleware"
)

func TestHealth_OK(t *testing.T) {
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/__health", nil)

	buildRouter().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	var body map[string]string
	if err := json.NewDecoder(rec.Body).Decode(&body); err != nil {
		t.Fatalf("decode body: %v", err)
	}
	if body["status"] != "ok" {
		t.Fatalf("expected status=ok, got %q", body["status"])
	}
}

func TestHealth_NoSecretRequired(t *testing.T) {
	// Even with MS_INTERNAL_SECRET set, /__health stays public.
	t.Setenv(middleware.InternalSecretEnv, "s3cret")

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/__health", nil)

	buildRouter().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected health to bypass auth, got %d", rec.Code)
	}
}

// TestRequestLogger_CapturesStatus verifies the status recorder wraps writes
// without breaking the response body.
func TestRequestLogger_CapturesStatus(t *testing.T) {
	handler := requestLogger(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusTeapot)
		_, _ = w.Write([]byte("brewing"))
	}))

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/x", nil)
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusTeapot {
		t.Fatalf("expected 418, got %d", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "brewing") {
		t.Fatalf("expected body preserved, got %q", rec.Body.String())
	}
}
