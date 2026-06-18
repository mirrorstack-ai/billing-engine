package usage_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/mirrorstack-ai/billing-engine/internal/account/billing"
	"github.com/mirrorstack-ai/billing-engine/internal/account/usage"
)

// validInfra is a well-formed RecordInfraUsage request: a registered infra
// metric, an app, and an owner principal. No metric_definitions row is needed
// (the kind is platform-owned, resolved from the registry, not the catalog).
func validInfra() usage.RecordInfraUsageRequest {
	return usage.RecordInfraUsageRequest{
		EventID:     "infra-evt-1",
		AppID:       uuid.New(),
		OwnerUserID: uuid.New(),
		Metric:      "infra.compute.ms",
		Value:       1500,
		RecordedAt:  time.Date(2026, 6, 15, 12, 0, 0, 0, time.UTC),
	}
}

func TestRecordInfraUsage_AcceptsComputeMS(t *testing.T) {
	store := newFakeStore()
	req := validInfra()
	store.accounts[req.OwnerUserID] = uuid.New()

	resp, err := newService(store).RecordInfraUsage(context.Background(), req)
	require.NoError(t, err)
	require.True(t, resp.Recorded)

	ev := store.events[req.EventID]
	// Kind is platform-owned (sum), set WITHOUT a catalog lookup.
	require.Equal(t, usage.KindSum, ev.Kind)
	// Stamped under the platform-infra sentinel module_id, never a real module.
	require.Equal(t, usage.PlatformInfraModuleID(), ev.ModuleID)
	require.Equal(t, "infra.compute.ms", ev.Metric)
}

func TestRecordInfraUsage_AcceptsEgressBytes(t *testing.T) {
	store := newFakeStore()
	req := validInfra()
	req.EventID = "infra-evt-egress"
	req.Metric = "infra.egress.bytes"

	resp, err := newService(store).RecordInfraUsage(context.Background(), req)
	require.NoError(t, err)
	require.True(t, resp.Recorded)
	require.Equal(t, usage.KindSum, store.events[req.EventID].Kind)
}

func TestRecordInfraUsage_RejectsNonReservedMetric(t *testing.T) {
	// The INVERSE of the SDK gate: a non-reserved (custom) metric is rejected.
	// A registered catalog row for it must NOT let it through this path.
	store := newFakeStore()
	req := validInfra()
	req.Metric = "orders.placed"
	store.defs[defKey(req.AppID, "orders.placed")] = usage.MetricDefinition{Kind: usage.KindCount, Active: true}

	_, err := newService(store).RecordInfraUsage(context.Background(), req)
	requireCode(t, err, billing.CodeInvalidInput)
	require.Empty(t, store.events, "a non-reserved metric must never reach the infra plane")
}

func TestRecordInfraUsage_RejectsUnregisteredReservedMetric(t *testing.T) {
	// A reserved name with no platform-owned kind in the registry is rejected:
	// the platform owns infra semantics, so an unknown infra.* has no kind.
	store := newFakeStore()
	req := validInfra()
	req.Metric = "infra.unknown.thing"

	_, err := newService(store).RecordInfraUsage(context.Background(), req)
	requireCode(t, err, billing.CodeInvalidInput)
	require.Empty(t, store.events)
}

func TestRecordInfraUsage_RejectsUnregisteredPlatformMetric(t *testing.T) {
	// platform.* is a reserved prefix (passes isReservedMetric) but has no
	// platform-owned kind in the registry, so it is rejected at the kind lookup —
	// closing the second gate for the platform.* namespace, not just infra.*.
	store := newFakeStore()
	req := validInfra()
	req.Metric = "platform.tokens"

	_, err := newService(store).RecordInfraUsage(context.Background(), req)
	requireCode(t, err, billing.CodeInvalidInput)
	require.Empty(t, store.events)
}

func TestRecordInfraUsage_IdempotentReplay(t *testing.T) {
	store := newFakeStore()
	req := validInfra()
	svc := newService(store)

	first, err := svc.RecordInfraUsage(context.Background(), req)
	require.NoError(t, err)
	require.True(t, first.Recorded)

	// Same event_id → deduped (ON CONFLICT), still success, no double-count.
	second, err := svc.RecordInfraUsage(context.Background(), req)
	require.NoError(t, err)
	require.False(t, second.Recorded)
	require.Len(t, store.events, 1)
}

func TestRecordInfraUsage_LazyAccountWhenNoBillingAccount(t *testing.T) {
	store := newFakeStore()
	req := validInfra() // owner has no account row

	_, err := newService(store).RecordInfraUsage(context.Background(), req)
	require.NoError(t, err)
	require.Equal(t, uuid.Nil, store.events[req.EventID].AccountID, "lazy infra event records NULL account")
}

func TestRecordInfraUsage_ResolvesAccountFromOwner(t *testing.T) {
	store := newFakeStore()
	req := validInfra()
	acct := uuid.New()
	store.accounts[req.OwnerUserID] = acct

	_, err := newService(store).RecordInfraUsage(context.Background(), req)
	require.NoError(t, err)
	require.Equal(t, acct, store.events[req.EventID].AccountID)
}

func TestRecordInfraUsage_DefaultsRecordedAt(t *testing.T) {
	store := newFakeStore()
	req := validInfra()
	req.RecordedAt = time.Time{} // zero → defaulted to now()

	_, err := newService(store).RecordInfraUsage(context.Background(), req)
	require.NoError(t, err)
	require.False(t, store.events[req.EventID].RecordedAt.IsZero(), "zero recorded_at must default, not drop the fact")
}

func TestRecordInfraUsage_Validation(t *testing.T) {
	base := validInfra()
	cases := []struct {
		name   string
		mutate func(*usage.RecordInfraUsageRequest)
	}{
		{"missing event_id", func(r *usage.RecordInfraUsageRequest) { r.EventID = "" }},
		{"missing app_id", func(r *usage.RecordInfraUsageRequest) { r.AppID = uuid.Nil }},
		{"missing metric", func(r *usage.RecordInfraUsageRequest) { r.Metric = "" }},
		{"negative value", func(r *usage.RecordInfraUsageRequest) { r.Value = -1 }},
		{"nan value", func(r *usage.RecordInfraUsageRequest) { r.Value = nan() }},
		{"inf value", func(r *usage.RecordInfraUsageRequest) { r.Value = inf() }},
		{"both owners set", func(r *usage.RecordInfraUsageRequest) { r.OwnerUserID = uuid.New(); r.OwnerOrgID = uuid.New() }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			store := newFakeStore()
			req := base
			tc.mutate(&req)
			_, err := newService(store).RecordInfraUsage(context.Background(), req)
			requireCode(t, err, billing.CodeInvalidInput)
			require.Empty(t, store.events)
		})
	}
}
