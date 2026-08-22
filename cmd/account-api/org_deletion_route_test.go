package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/mirrorstack-ai/billing-engine/internal/account/cycle"
)

func TestFinalizeOrgDeletionRoute_IsInternalSecretGatedAndDispatched(t *testing.T) {
	t.Setenv("INTERNAL_SECRET", "internal-secret")
	t.Setenv("METER_SECRET", "meter-secret")
	router := buildRouter(&dispatcher{})
	path := "/v1/billing.FinalizeOrgDeletion"

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
}

func TestFinalizeOrgDeletionLambda_DispatchesMalformedRequest(t *testing.T) {
	previous := disp
	disp = &dispatcher{}
	t.Cleanup(func() { disp = previous })

	payload := json.RawMessage(`{
		"action":"FinalizeOrgDeletion",
		"request":"not-an-object"
	}`)
	out, err := lambdaInvokeHandler(context.Background(), payload)
	require.NoError(t, err)
	require.Contains(t, string(out), `"ok":false`)
	require.Contains(t, string(out), `"code":"INVALID_INPUT"`)
}

// This exact golden shape is shared with api-platform's
// internal/shared/billing.FinalizeOrgDeletion{Request,Response}. It pins UUID
// strings, field names and the response boolean on both transports.
func TestFinalizeOrgDeletionWireContract(t *testing.T) {
	orgID := uuid.MustParse("11111111-1111-4111-8111-111111111111")
	operationID := uuid.MustParse("22222222-2222-4222-8222-222222222222")
	body, err := json.Marshal(cycle.FinalizeOrgDeletionRequest{
		OrgID: orgID, OperationID: operationID,
	})
	require.NoError(t, err)
	require.JSONEq(t, `{
		"org_id":"11111111-1111-4111-8111-111111111111",
		"operation_id":"22222222-2222-4222-8222-222222222222"
	}`, string(body))

	responseBody, err := json.Marshal(cycle.FinalizeOrgDeletionResponse{Finalized: true})
	require.NoError(t, err)
	require.JSONEq(t, `{"finalized":true}`, string(responseBody))
}
