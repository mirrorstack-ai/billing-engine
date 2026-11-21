package tappaygateway

import "fmt"

// GatewayError reports a transport-level failure talking to TapPay:
// building the request, the HTTP round trip, a non-200 status, or a
// response that failed to decode.
//
// It deliberately carries only the endpoint path and a short phase
// label (plus the underlying error's text for a non-200 status, which
// never originates from this package's own request body). Nothing
// that constructs a GatewayError is given the request body, the
// partner key, or the response body — see (*Client).do — so there is
// no field here a future edit could wire up to leak them. A business
// decline (TapPay's own Status field) is not a GatewayError at all: it
// comes back as a normal PayByPrimeResult/QueryRecordResult/RefundResult
// for the caller to branch on.
type GatewayError struct {
	Path  string
	Phase string
	err   error
}

func newGatewayError(path, phase string, cause error) *GatewayError {
	return &GatewayError{Path: path, Phase: phase, err: cause}
}

func (e *GatewayError) Error() string {
	if e.err == nil {
		return fmt.Sprintf("tappaygateway: %s: %s", e.Phase, e.Path)
	}
	return fmt.Sprintf("tappaygateway: %s: %s: %s", e.Phase, e.Path, e.safeCause())
}

func (e *GatewayError) Unwrap() error { return e.err }

// safeCause returns the underlying error's message, which is always
// either a Go stdlib transport error (connection refused, timeout,
// TLS failure) or an encoding/json structural error (both describe
// shapes, not values) — never content read from the request or
// response body.
func (e *GatewayError) safeCause() string {
	if e.err == nil {
		return ""
	}
	return e.err.Error()
}
