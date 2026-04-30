// Package middleware contains HTTP middleware shared across billing-engine
// services.
package middleware

import (
	"crypto/subtle"
	"net/http"
	"os"
)

// InternalSecretHeader is the HTTP header callers attach the shared secret to.
const InternalSecretHeader = "X-MS-Internal-Secret"

// InternalSecretEnv is the environment variable that holds the expected secret.
const InternalSecretEnv = "MS_INTERNAL_SECRET"

// InternalSecret returns middleware that gates a route on a shared secret
// passed via the X-MS-Internal-Secret header. The secret is read from the
// MS_INTERNAL_SECRET env var on every request so a rotation does not require
// a process restart.
//
// If the env var is empty the middleware fails closed (every request → 401).
// Comparison is constant-time to avoid leaking the secret via timing.
func InternalSecret() func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			expected := os.Getenv(InternalSecretEnv)
			provided := r.Header.Get(InternalSecretHeader)

			if expected == "" || provided == "" {
				unauthorized(w)
				return
			}
			if subtle.ConstantTimeCompare([]byte(expected), []byte(provided)) != 1 {
				unauthorized(w)
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}

func unauthorized(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusUnauthorized)
	_, _ = w.Write([]byte(`{"error":{"code":"unauthorized","message":"unauthorized"}}`))
}
