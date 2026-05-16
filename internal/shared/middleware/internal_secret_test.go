package middleware

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// pass-through next handler used by every test case.
var ok200 = http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(`{"ok":true}`))
})

func TestInternalSecret_Pass(t *testing.T) {
	mw := InternalSecret("dev-secret")(ok200)
	req := httptest.NewRequest(http.MethodPost, "/v1/billing.Ensure", nil)
	req.Header.Set("X-MS-Internal-Secret", "dev-secret")
	rec := httptest.NewRecorder()

	mw.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	require.Contains(t, rec.Body.String(), `"ok":true`)
}

func TestInternalSecret_MissingHeader(t *testing.T) {
	mw := InternalSecret("dev-secret")(ok200)
	req := httptest.NewRequest(http.MethodPost, "/v1/billing.Ensure", nil)
	rec := httptest.NewRecorder()

	mw.ServeHTTP(rec, req)

	require.Equal(t, http.StatusUnauthorized, rec.Code)
	require.Equal(t, "application/json", rec.Header().Get("Content-Type"))
	require.Equal(t, `{"ok":false,"error":{"code":"unauthorized"}}`, rec.Body.String())
}

func TestInternalSecret_WrongValue(t *testing.T) {
	mw := InternalSecret("dev-secret")(ok200)
	req := httptest.NewRequest(http.MethodPost, "/v1/billing.Ensure", nil)
	req.Header.Set("X-MS-Internal-Secret", "wrong")
	rec := httptest.NewRecorder()

	mw.ServeHTTP(rec, req)

	require.Equal(t, http.StatusUnauthorized, rec.Code)
	require.Contains(t, rec.Body.String(), `"unauthorized"`)
}

func TestInternalSecret_WrongLengthDoesNotPanic(t *testing.T) {
	// subtle.ConstantTimeCompare returns 0 for different-length slices.
	// Verify the middleware rejects rather than panicking on a longer-than-expected value.
	mw := InternalSecret("dev-secret")(ok200)
	req := httptest.NewRequest(http.MethodPost, "/v1/billing.Ensure", nil)
	req.Header.Set("X-MS-Internal-Secret", strings.Repeat("a", 100))
	rec := httptest.NewRecorder()

	mw.ServeHTTP(rec, req)

	require.Equal(t, http.StatusUnauthorized, rec.Code)
}

func TestInternalSecret_EmptySecretReturns503(t *testing.T) {
	// Misconfiguration: a wrapper with no secret should refuse all traffic
	// rather than silently allow it. 503 distinguishes from 401 (caller error).
	mw := InternalSecret("")(ok200)
	req := httptest.NewRequest(http.MethodPost, "/v1/billing.Ensure", nil)
	req.Header.Set("X-MS-Internal-Secret", "anything")
	rec := httptest.NewRecorder()

	mw.ServeHTTP(rec, req)

	require.Equal(t, http.StatusServiceUnavailable, rec.Code)
	require.Contains(t, rec.Body.String(), `"secret_unconfigured"`)
}
