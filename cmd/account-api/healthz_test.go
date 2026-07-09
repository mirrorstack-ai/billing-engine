package main

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

// The API Gateway v2 health-probe branch must return before ever touching
// disp (nil in this test) - if it fell through to rpcEnvelope handling, this
// would panic on disp.dispatch's nil pointer instead of returning 200.
func TestLambdaInvokeHandler_HealthProbe(t *testing.T) {
	payload := []byte(`{"rawPath":"/billing/healthz","requestContext":{"http":{"method":"GET"}}}`)

	out, err := lambdaInvokeHandler(context.Background(), payload)
	require.NoError(t, err)

	var resp struct {
		StatusCode int               `json:"statusCode"`
		Headers    map[string]string `json:"headers"`
		Body       string            `json:"body"`
	}
	require.NoError(t, json.Unmarshal(out, &resp))
	require.Equal(t, 200, resp.StatusCode)
	require.Equal(t, `{"status":"ok"}`, resp.Body)
	require.Equal(t, "application/json", resp.Headers["content-type"])
}

// A payload with no rawPath (a real rpcEnvelope) must fall through past the
// health-probe branch and reach disp.dispatch. disp is nil in this test
// (no buildDispatcher call), so reaching that line panics on a nil-pointer
// dereference - which is exactly what proves the fallthrough happened,
// rather than the health branch silently swallowing every payload.
func TestLambdaInvokeHandler_RawPathAbsent_FallsThroughToRPC(t *testing.T) {
	defer func() {
		r := recover()
		require.NotNil(t, r, "expected a nil-pointer panic from disp.dispatch, proving the RPC path was reached")
	}()
	payload := []byte(`{"action":"Ensure","request":{}}`)
	_, _ = lambdaInvokeHandler(context.Background(), payload)
	t.Fatal("expected panic reaching disp.dispatch with nil disp, but none occurred")
}
