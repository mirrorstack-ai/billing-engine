package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/mirrorstack-ai/billing-engine/internal/account/usage"
)

// stubUsageStore is a minimal usage.Store for the route wiring test: it accepts
// every InsertUsageEvent and reports no account. It exercises the RPC plumbing
// (auth gate → dispatch → service) without a database.
type stubUsageStore struct{}

func (stubUsageStore) LookupMetricDefinition(context.Context, uuid.UUID, string) (usage.MetricDefinition, bool, error) {
	return usage.MetricDefinition{}, false, nil
}
func (stubUsageStore) UpsertMetricDefinitions(context.Context, []usage.MetricDeclaration) error {
	return nil
}
func (stubUsageStore) InsertUsageEvent(context.Context, usage.UsageEvent) (bool, error) {
	return true, nil
}
func (stubUsageStore) AccountByOwner(context.Context, usage.Owner) (uuid.UUID, bool, error) {
	return uuid.Nil, false, nil
}
func (stubUsageStore) CurrentPeriodUsage(context.Context, uuid.UUID, time.Time, time.Time) ([]usage.MetricUsageRaw, error) {
	return nil, nil
}
func (stubUsageStore) UpsertModuleVisibility(context.Context, uuid.UUID, usage.Visibility) error {
	return nil
}

func newRouterForTest(t *testing.T) http.Handler {
	t.Helper()
	t.Setenv("INTERNAL_SECRET", "internal-secret")
	t.Setenv("METER_SECRET", "meter-secret")
	d := &dispatcher{usageSvc: usage.NewService(stubUsageStore{})}
	return buildRouter(d)
}

// infraBody is a valid RecordInfraUsage request body.
const infraBody = `{"event_id":"infra-route-1","app_id":"11111111-1111-1111-1111-111111111111","metric":"infra.compute.ms","value":42}`

func TestRecordInfraUsage_ReachableOnInternalSecret(t *testing.T) {
	r := newRouterForTest(t)
	req := httptest.NewRequest(http.MethodPost, "/v1/billing.RecordInfraUsage", strings.NewReader(infraBody))
	req.Header.Set("X-MS-Internal-Secret", "internal-secret")
	rec := httptest.NewRecorder()

	r.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	require.Contains(t, rec.Body.String(), `"ok":true`)
	require.Contains(t, rec.Body.String(), `"recorded":true`)
}

func TestRecordInfraUsage_NotReachableOnMeterSecret(t *testing.T) {
	// RecordInfraUsage is on the INTERNAL-secret group, NOT the meter-secret
	// group the SDK ingress (RecordUsage) uses. A request carrying only the
	// meter secret must be rejected — the platform-infra ingest never shares the
	// rotatable module-SDK credential.
	r := newRouterForTest(t)
	req := httptest.NewRequest(http.MethodPost, "/v1/billing.RecordInfraUsage", strings.NewReader(infraBody))
	req.Header.Set("X-MS-Meter-Secret", "meter-secret")
	rec := httptest.NewRecorder()

	r.ServeHTTP(rec, req)

	require.Equal(t, http.StatusUnauthorized, rec.Code)
}

func TestRecordInfraUsage_RejectsNonReservedThroughRoute(t *testing.T) {
	// End-to-end through the route: the inverse gate rejects a custom metric.
	r := newRouterForTest(t)
	body := `{"event_id":"infra-route-2","app_id":"11111111-1111-1111-1111-111111111111","metric":"orders.placed","value":1}`
	req := httptest.NewRequest(http.MethodPost, "/v1/billing.RecordInfraUsage", strings.NewReader(body))
	req.Header.Set("X-MS-Internal-Secret", "internal-secret")
	rec := httptest.NewRecorder()

	r.ServeHTTP(rec, req)

	require.Equal(t, http.StatusBadRequest, rec.Code)
	require.Contains(t, rec.Body.String(), "INVALID_INPUT")
}
