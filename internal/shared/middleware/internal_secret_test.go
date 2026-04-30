package middleware

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func okHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`ok`))
	})
}

func TestInternalSecret_MissingEnv_Unauthorized(t *testing.T) {
	t.Setenv(InternalSecretEnv, "")

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/x", nil)
	req.Header.Set(InternalSecretHeader, "anything")

	InternalSecret()(okHandler()).ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401 when env unset, got %d", rec.Code)
	}
}

func TestInternalSecret_MissingHeader_Unauthorized(t *testing.T) {
	t.Setenv(InternalSecretEnv, "s3cret")

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/x", nil)

	InternalSecret()(okHandler()).ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401 with missing header, got %d", rec.Code)
	}
}

func TestInternalSecret_WrongHeader_Unauthorized(t *testing.T) {
	t.Setenv(InternalSecretEnv, "s3cret")

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/x", nil)
	req.Header.Set(InternalSecretHeader, "wrong")

	InternalSecret()(okHandler()).ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401 with wrong secret, got %d", rec.Code)
	}
}

func TestInternalSecret_CorrectHeader_PassThrough(t *testing.T) {
	t.Setenv(InternalSecretEnv, "s3cret")

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/x", nil)
	req.Header.Set(InternalSecretHeader, "s3cret")

	InternalSecret()(okHandler()).ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 with valid secret, got %d", rec.Code)
	}
	if got := rec.Body.String(); got != "ok" {
		t.Fatalf("expected body 'ok', got %q", got)
	}
}
