package main

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/mirrorstack-ai/billing-engine/internal/account/billing"
)

// The funding-gates wire contract (C1): a PAYMENT_REQUIRED billing error maps
// to HTTP 402 on the local path, and the envelope carries the code verbatim —
// api-platform keys its own 402 {"error":"funding_required"} translation on it.
func TestHTTPStatusForError_PaymentRequired(t *testing.T) {
	err := billing.PaymentRequired("add a card before creating an app")

	require.Equal(t, http.StatusPaymentRequired, httpStatusForError(err))

	resp := buildResponse(nil, err)
	require.False(t, resp.OK)
	require.Equal(t, "PAYMENT_REQUIRED", resp.Error.Code)
	require.Equal(t, "add a card before creating an app", resp.Error.Message)
}
