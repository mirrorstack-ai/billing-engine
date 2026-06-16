// Package auth contains service-authentication middleware for
// billing-engine's HTTP transports. Today it gates the X-MS-Internal-
// Secret header used by api-platform → billing-engine internal calls;
// future auth concerns (JWT, OAuth client verification) land here too.
//
// Naming matches api-platform/internal/shared/auth so a developer
// moving between the two repos finds the same package name doing the
// same job.
package auth

import (
	"crypto/subtle"
	"net/http"
)

// InternalSecret returns a chi middleware that gates a request group
// behind the X-MS-Internal-Secret header. The header value is compared
// against the configured secret in constant time to avoid timing leaks.
//
// On miss, the middleware responds 401 with the billing-engine RPC
// error envelope ({"ok": false, "error": {"code": "unauthorized"}})
// to match the contract documented in mirrorstack-docs/api/billing/
// account-api.md.
//
// Local dev: secret value comes from docker-compose.yml as a known
// dev-only string. Production: never used — billing-engine prod is
// invoked via lambda.Invoke and IAM gates the call. The middleware
// is wired only in the HTTP code path.
//
// An empty `secret` argument is treated as a misconfiguration; the
// middleware refuses ALL requests with 503 so an unset secret in
// production (should it ever ship there) can't accidentally allow
// unauthenticated traffic. Callers should fail-fast at startup
// instead of relying on this fallback.
func InternalSecret(secret string) func(http.Handler) http.Handler {
	return secretGuard(headerInternalSecret, secret)
}

// MeterSecret returns a chi middleware identical in behavior to
// InternalSecret but gated behind a DEDICATED X-MS-Meter-Secret header
// and its own secret. The metering ingest path (RecordUsage) is gated by
// this rather than the general internal secret so the high-volume,
// dispatch-asserted meter seam carries a credential the platform can
// rotate independently of every other internal billing RPC (design §5:
// "RecordUsage MUST use a NEW dedicated meter secret/header, not overload
// X-MS-Internal-Secret"). Same fail-closed contract: empty secret → 503.
func MeterSecret(secret string) func(http.Handler) http.Handler {
	return secretGuard(headerMeterSecret, secret)
}

// secretGuard is the shared constant-time header-secret gate backing both
// InternalSecret and MeterSecret. Differing only by header name + secret
// keeps the two surfaces' behavior provably identical.
func secretGuard(headerName, secret string) func(http.Handler) http.Handler {
	expected := []byte(secret)
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if len(expected) == 0 {
				writeError(w, http.StatusServiceUnavailable, "secret_unconfigured")
				return
			}
			got := r.Header.Get(headerName)
			if got == "" {
				writeError(w, http.StatusUnauthorized, "unauthorized")
				return
			}
			// subtle.ConstantTimeCompare returns 1 iff the slices are
			// equal AND have the same length. Differing lengths short-
			// circuit without revealing the length via timing.
			if subtle.ConstantTimeCompare([]byte(got), expected) != 1 {
				writeError(w, http.StatusUnauthorized, "unauthorized")
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}

const (
	headerInternalSecret = "X-MS-Internal-Secret"
	headerMeterSecret    = "X-MS-Meter-Secret"
)

// writeError emits the billing-engine RPC error envelope. Kept inline
// rather than imported from a separate package because the middleware
// is the only consumer; promoting it would invert the dependency.
func writeError(w http.ResponseWriter, status int, code string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	// Hand-rolled JSON keeps this package free of an encoding/json
	// import for a fixed-shape one-line payload. If the error shape
	// ever takes a dynamic message, switch to json.Marshal.
	_, _ = w.Write([]byte(`{"ok":false,"error":{"code":"` + code + `"}}`))
}
