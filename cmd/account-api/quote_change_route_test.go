package main

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// QuoteChange (billing-engine#255) is dispatched by action name and served at
// /v1/billing.QuoteChange behind the internal secret. A typo in either would
// pass the cycle package's service tests and only fail at api-platform, so this
// pins both without a database: a malformed body stops at the dispatcher's
// decoder, and a well-formed body with no owner reaches Service.QuoteChange's
// first validation (it runs before the service touches its store, so a
// dispatcher with no cycle service is enough). The two answers differ, which is
// what proves the "QuoteChange" case really calls the service.
func TestQuoteChangeRoute_IsInternalSecretGatedAndDispatched(t *testing.T) {
	t.Setenv("INTERNAL_SECRET", "internal-secret")
	t.Setenv("METER_SECRET", "meter-secret")
	router := buildRouter(&dispatcher{})
	const path = "/v1/billing.QuoteChange"

	post := func(body string, secret bool) *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(body))
		if secret {
			req.Header.Set("X-MS-Internal-Secret", "internal-secret")
		}
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, req)
		return rec
	}

	malformed := post("{", true)
	require.Equal(t, http.StatusBadRequest, malformed.Code)
	require.Contains(t, malformed.Body.String(), `"code":"INVALID_INPUT"`)
	require.Contains(t, malformed.Body.String(), "malformed request payload")

	ownerless := post(`{"steps":[{"kind":"create_app","plan":"free"}]}`, true)
	require.Equal(t, http.StatusBadRequest, ownerless.Code)
	require.Contains(t, ownerless.Body.String(), `"code":"INVALID_INPUT"`)
	require.Contains(t, ownerless.Body.String(), "owner_user_id or owner_org_id required",
		"the QuoteChange case must hand the decoded request to Service.QuoteChange")
	require.NotContains(t, ownerless.Body.String(), "malformed request payload")

	require.Equal(t, http.StatusUnauthorized, post(`{}`, false).Code, "the route is internal-secret gated")
}
