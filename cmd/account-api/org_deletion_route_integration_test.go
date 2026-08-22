//go:build integration

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
	"github.com/mirrorstack-ai/billing-engine/internal/shared/testutil"
)

func TestFinalizeOrgDeletion_HTTPAndLambdaSameOperation(t *testing.T) {
	pool := testutil.NewTestDB(t)
	service := cycle.NewService(cycle.NewStore(pool), nil)
	d := &dispatcher{cycleSvc: service}
	orgID, operationID := uuid.New(), uuid.New()
	body := `{"org_id":"` + orgID.String() + `","operation_id":"` + operationID.String() + `"}`

	t.Setenv("INTERNAL_SECRET", "internal-secret")
	t.Setenv("METER_SECRET", "meter-secret")
	req := httptest.NewRequest(
		http.MethodPost,
		"/v1/billing.FinalizeOrgDeletion",
		strings.NewReader(body),
	)
	req.Header.Set("X-MS-Internal-Secret", "internal-secret")
	rec := httptest.NewRecorder()
	buildRouter(d).ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	require.Contains(t, rec.Body.String(), `"finalized":true`)

	previous := disp
	disp = d
	t.Cleanup(func() { disp = previous })
	lambdaPayload, err := json.Marshal(rpcEnvelope{
		Action: "FinalizeOrgDeletion", Request: json.RawMessage(body),
	})
	require.NoError(t, err)
	out, err := lambdaInvokeHandler(context.Background(), lambdaPayload)
	require.NoError(t, err)
	require.Contains(t, string(out), `"ok":true`)
	require.Contains(t, string(out), `"finalized":true`)

	var count int
	require.NoError(t, pool.QueryRow(context.Background(), `
		SELECT count(*) FROM ms_billing.org_deletion_finalizations
		WHERE org_id=$1 AND operation_id=$2`, orgID, operationID).Scan(&count))
	require.Equal(t, 1, count, "HTTP first call + Lambda replay share one tombstone")
}
