package main

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// A malformed body stops at each credit action's decoder, so this pins both
// dispatcher registration and internal-secret route wiring without requiring a
// database-backed billing service.
func TestCreditRPCRoutes_AreInternalSecretGatedAndDispatched(t *testing.T) {
	t.Setenv("INTERNAL_SECRET", "internal-secret")
	t.Setenv("METER_SECRET", "meter-secret")
	router := buildRouter(&dispatcher{})

	for _, action := range []string{
		"GetCreditStanding",
		"ListCreditLedger",
		"StartCreditPurchase",
		"FinishCreditPurchase",
		"SetAutoTopUp",
		"SetCustomerBillingMode",
		"ListDistributorCustomers",
		"GrantCredits",
	} {
		t.Run(action, func(t *testing.T) {
			path := "/v1/billing." + action
			req := httptest.NewRequest(http.MethodPost, path, strings.NewReader("{"))
			req.Header.Set("X-MS-Internal-Secret", "internal-secret")
			rec := httptest.NewRecorder()
			router.ServeHTTP(rec, req)

			require.Equal(t, http.StatusBadRequest, rec.Code)
			require.Contains(t, rec.Body.String(), `"code":"INVALID_INPUT"`)
			require.Contains(t, rec.Body.String(), "malformed request payload")

			unauthorized := httptest.NewRequest(http.MethodPost, path, strings.NewReader("{}"))
			unauthorizedRec := httptest.NewRecorder()
			router.ServeHTTP(unauthorizedRec, unauthorized)
			require.Equal(t, http.StatusUnauthorized, unauthorizedRec.Code)
		})
	}
}
