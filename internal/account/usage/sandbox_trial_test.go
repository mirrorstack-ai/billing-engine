package usage_test

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/mirrorstack-ai/billing-engine/internal/account/billing"
	"github.com/mirrorstack-ai/billing-engine/internal/account/usage"
)

// sandboxReq is a well-formed sandbox_trial compute event for an owner that has
// a billing account in store.
func sandboxReq(store *fakeStore) usage.RecordInfraUsageRequest {
	req := validInfra()
	req.EventID, req.Metric, req.Value = "sbx-evt-1", "infra.sandbox.vcpu.hours", 0.5
	req.Source, req.RunID = usage.SourceSandboxTrial, "run-0001"
	store.accounts[req.OwnerUserID] = uuid.New()
	seedSentinelInfra(store, req.Metric, usage.KindSum, "vCPU-hour", 40450)
	return req
}

func TestRecordInfraUsage_SandboxTrial_DevServedTaggedAndPriceStamped(t *testing.T) {
	store := newFakeStore()
	req := sandboxReq(store)
	req.DevServed = false // the tag alone forces dev_served

	resp, err := newService(store).RecordInfraUsage(context.Background(), req)
	require.NoError(t, err)
	require.True(t, resp.Recorded)
	ev := store.events[req.EventID]
	require.Equal(t, usage.SourceSandboxTrial, ev.Source)
	require.Equal(t, "run-0001", ev.RunID)
	require.EqualValues(t, 40450, *ev.StampedPriceMicros)
	require.True(t, ev.DevServed, "recorded and priced, never charged")
}

func TestRecordInfraUsage_SandboxTrial_StampsTheExactModelRow(t *testing.T) {
	for _, tc := range []struct {
		name, metric, model string
		active              bool
		catalog, row, want  int64
	}{
		{"model row beats the catalog fallback", "infra.ai.output.tokens", "m1", true, 3, 15, 15},
		{"a retired row still prices at its own rate", "infra.ai.input.tokens", "m1", false, 1, 9, 9},
		{"requests count: zero catalog price, no model row", "infra.ai.requests", "m1", true, 0, -1, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := newFakeStore()
			req := sandboxReq(store)
			req.Metric, req.Model = tc.metric, tc.model
			seedSentinelInfra(store, tc.metric, usage.KindSum, "token", tc.catalog)
			if tc.row >= 0 {
				store.modelPrices[modelPriceKey(tc.metric, tc.model)] = usage.ModelPrice{UnitPriceMicros: tc.row, Active: tc.active}
			}
			_, err := newService(store).RecordInfraUsage(context.Background(), req)
			require.NoError(t, err)
			require.EqualValues(t, tc.want, *store.events[req.EventID].StampedPriceMicros)
		})
	}
}

func TestRecordInfraUsage_SandboxTrial_RefusedEventsLeaveNothingBehind(t *testing.T) {
	for name, mutate := range map[string]func(*fakeStore, *usage.RecordInfraUsageRequest){
		"no catalog row": func(s *fakeStore, r *usage.RecordInfraUsageRequest) {
			delete(s.defs, defKey(usage.PlatformInfraModuleID(), r.Metric))
		},
		"unpriced catalog row": func(s *fakeStore, r *usage.RecordInfraUsageRequest) {
			s.defs[defKey(usage.PlatformInfraModuleID(), r.Metric)] = usage.MetricDefinition{Kind: usage.KindSum, Active: true}
		},
		"token metric, unknown model (catalog fallback is the cheapest)": func(s *fakeStore, r *usage.RecordInfraUsageRequest) {
			r.Metric, r.Model = "infra.ai.output.tokens", "unknown"
			seedSentinelInfra(s, r.Metric, usage.KindSum, "token", 3)
		},
		"token metric without a model": func(s *fakeStore, r *usage.RecordInfraUsageRequest) {
			r.Metric = "infra.ai.output.tokens"
			seedSentinelInfra(s, r.Metric, usage.KindSum, "token", 3)
		},
		"no billing account (invisible to every cap)": func(s *fakeStore, r *usage.RecordInfraUsageRequest) { delete(s.accounts, r.OwnerUserID) },
		"unknown source":                            func(_ *fakeStore, r *usage.RecordInfraUsageRequest) { r.Source = "free_credit" },
		"no run id":                                 func(_ *fakeStore, r *usage.RecordInfraUsageRequest) { r.RunID = "" },
		"run id with a bad shape":                   func(_ *fakeStore, r *usage.RecordInfraUsageRequest) { r.RunID = "run 1; drop" },
		"run id without a source":                   func(_ *fakeStore, r *usage.RecordInfraUsageRequest) { r.Source = "" },
		"infra.sandbox.* untagged would be charged": func(_ *fakeStore, r *usage.RecordInfraUsageRequest) { r.Source, r.RunID = "", "" },
		"the tag cannot waive another metric":       func(_ *fakeStore, r *usage.RecordInfraUsageRequest) { r.Metric = "infra.task.vcpu.hours" },
	} {
		t.Run(name, func(t *testing.T) {
			store := newFakeStore()
			req := sandboxReq(store)
			mutate(store, &req)
			_, err := newService(store).RecordInfraUsage(context.Background(), req)
			requireCode(t, err, billing.CodeInvalidInput)
			require.Empty(t, store.events)
		})
	}
}

// The fingerprint binds the tag; an ordinary infra.task.vcpu.hours (control) is untouched.
func TestRecordInfraUsage_SandboxTag_FingerprintBindsTheTag_ControlStaysOrdinary(t *testing.T) {
	store := newFakeStore()
	svc, req := newService(store), sandboxReq(store)
	_, err := svc.RecordInfraUsage(context.Background(), req)
	require.NoError(t, err)
	again, err := svc.RecordInfraUsage(context.Background(), req)
	require.NoError(t, err)
	require.False(t, again.Recorded, "an identical retry is deduped")
	req.RunID = "run-0002"
	_, err = svc.RecordInfraUsage(context.Background(), req)
	requireCode(t, err, billing.CodeConflict)

	ctl := validInfra()
	ctl.EventID, ctl.Metric, ctl.Value = "task-evt-1", "infra.task.vcpu.hours", 1
	store.accounts[ctl.OwnerUserID] = uuid.New()
	_, err = svc.RecordInfraUsage(context.Background(), ctl)
	require.NoError(t, err)
	ev := store.events[ctl.EventID]
	require.Empty(t, ev.Source)
	require.Empty(t, ev.RunID)
	require.Nil(t, ev.StampedPriceMicros)
	require.False(t, ev.DevServed, "an ordinary infra event stays chargeable")
}
