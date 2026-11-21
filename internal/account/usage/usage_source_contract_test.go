package usage_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/mirrorstack-ai/billing-engine/internal/account/billing"
	"github.com/mirrorstack-ai/billing-engine/internal/account/usage"
)

// billing-engine#368: the live api-platform tags every infra usage call with the
// JSON field `source`. These are its four values, written out LITERALLY so a
// rename or a removal inside billing-engine cannot move this list with it.
var contractSources = []string{"platform_sidebar", "member", "sandbox_trial", "module"}
var ordinarySources = []string{"platform_sidebar", "member", "module"}

// budgetCalls records which budget families a recorded event woke.
type budgetCalls struct{ app, account, sandbox int }

func (b *budgetCalls) EvaluateAppBudget(context.Context, uuid.UUID, string, time.Time, time.Time) ([]int, error) {
	b.app++
	return nil, nil
}
func (b *budgetCalls) EvaluateAccountBudget(context.Context, uuid.UUID, uuid.UUID, time.Time, time.Time) ([]int, error) {
	b.account++
	return nil, nil
}
func (b *budgetCalls) EvaluateSandboxBudget(context.Context, uuid.UUID, uuid.UUID, time.Time, time.Time) ([]int, error) {
	b.sandbox++
	return nil, nil
}

// aiReq is an ordinary AI token event for an owner that has a billing account.
func aiReq(store *fakeStore, eventID string) usage.RecordInfraUsageRequest {
	req := validInfra()
	req.EventID, req.Metric, req.Value = eventID, "infra.ai.input.tokens", 1000
	store.accounts[req.OwnerUserID] = uuid.New()
	seedSentinelInfra(store, req.Metric, usage.KindSum, "token", 3)
	return req
}

// The contract test: billing-engine holds every name api-platform sends — in the
// Go set, in migration 096's CHECK, and at ingest. It FAILS when any one is missed.
func TestUsageSourceContract_BillingEngineHoldsTheFourNames(t *testing.T) {
	require.ElementsMatch(t, contractSources, usage.KnownSources(), "the Go set is exactly the contract")

	up, err := os.ReadFile(filepath.Join("..", "..", "..", "migrations", "billing", "096_usage_source_contract.up.sql"))
	require.NoError(t, err)
	for _, name := range contractSources {
		require.Contains(t, string(up), "'"+name+"'", "migration 096 must name %s in usage_events_source_known", name)

		store := newFakeStore()
		req := validInfra()
		switch name {
		case "sandbox_trial":
			req = sandboxReq(store)
		default:
			req.Source = name
		}
		resp, err := newService(store).RecordInfraUsage(context.Background(), req)
		require.NoError(t, err, "ingest must accept source %s", name)
		require.True(t, resp.Recorded, name)
		require.Equal(t, name, store.events[req.EventID].Source, "the source is stored on the event")
	}
}

// (a) platform_sidebar, member and module are ORDINARY events: the same event as
// its untagged twin — not dev_served, no stamped price, no run id, the same
// account, the same fingerprint, and the same budget families woken.
func TestRecordInfraUsage_OrdinarySources_AreTheUntaggedTwin(t *testing.T) {
	for _, source := range ordinarySources {
		t.Run(source, func(t *testing.T) {
			untaggedStore, taggedStore := newFakeStore(), newFakeStore()
			untagged, tagged := &budgetCalls{}, &budgetCalls{}

			req := aiReq(untaggedStore, "twin-1")
			twin := req
			taggedStore.accounts[twin.OwnerUserID] = untaggedStore.accounts[req.OwnerUserID]
			seedSentinelInfra(taggedStore, twin.Metric, usage.KindSum, "token", 3)
			twin.Source = source

			_, err := usage.NewService(untaggedStore).WithBudgetEvaluator(untagged).RecordInfraUsage(context.Background(), req)
			require.NoError(t, err)
			resp, err := usage.NewService(taggedStore).WithBudgetEvaluator(tagged).RecordInfraUsage(context.Background(), twin)
			require.NoError(t, err)
			require.True(t, resp.Recorded)

			a, b := untaggedStore.events["twin-1"], taggedStore.events["twin-1"]
			require.Equal(t, source, b.Source)
			require.False(t, b.DevServed, "an ordinary source is charged")
			require.Nil(t, b.StampedPriceMicros, "no stamped price: it prices live at rollup like its twin")
			require.Empty(t, b.RunID)
			require.Equal(t, a.AccountID, b.AccountID)
			require.Equal(t, a.Value, b.Value)
			require.Equal(t, a.Kind, b.Kind)
			require.Equal(t, a.PayloadFingerprint, b.PayloadFingerprint, "the tag is not part of the fingerprint")

			require.Equal(t, 1, tagged.app)
			require.Equal(t, 1, tagged.account, "the AI budgets count it")
			require.Zero(t, tagged.sandbox, "the sandbox budget never sees it")
			require.Equal(t, *untagged, *tagged, "the same budget families as the untagged twin")
		})
	}

	// A module event is accepted with its incurring module id, like any infra event.
	store := newFakeStore()
	req := validInfra()
	req.Source, req.ModuleID = usage.SourceModule, uuid.New()
	_, err := newService(store).RecordInfraUsage(context.Background(), req)
	require.NoError(t, err)
	require.False(t, store.events[req.EventID].DevServed)
}

// (d) The source enters the fingerprint only for sandbox_trial: a retry of an
// event first stored without a source (or with another ordinary one) still dedupes.
func TestRecordInfraUsage_OrdinarySource_RetryStillDedupes(t *testing.T) {
	for _, source := range ordinarySources {
		store := newFakeStore()
		svc, req := newService(store), validInfra()
		_, err := svc.RecordInfraUsage(context.Background(), req)
		require.NoError(t, err)

		req.Source = source
		again, err := svc.RecordInfraUsage(context.Background(), req)
		require.NoError(t, err, source)
		require.False(t, again.Recorded, "%s: the retry of an untagged event dedupes", source)

		req.Source = usage.SourceMember
		again, err = svc.RecordInfraUsage(context.Background(), req)
		require.NoError(t, err)
		require.False(t, again.Recorded)
	}
}

// (c) any other value is refused with InvalidInput, and an ordinary source cannot
// carry a run id or waive the trial-compute tag.
func TestRecordInfraUsage_SourceRefusals(t *testing.T) {
	for name, mutate := range map[string]func(*usage.RecordInfraUsageRequest){
		"unknown source":         func(r *usage.RecordInfraUsageRequest) { r.Source = "free_credit" },
		"case differs":           func(r *usage.RecordInfraUsageRequest) { r.Source = "Member" },
		"padded":                 func(r *usage.RecordInfraUsageRequest) { r.Source = " member" },
		"a near name":            func(r *usage.RecordInfraUsageRequest) { r.Source = "platform-sidebar" },
		"sandbox is not the tag": func(r *usage.RecordInfraUsageRequest) { r.Source = "sandbox" },
		"run id with member":     func(r *usage.RecordInfraUsageRequest) { r.Source, r.RunID = usage.SourceMember, "run-1" },
		"run id with module":     func(r *usage.RecordInfraUsageRequest) { r.Source, r.RunID = usage.SourceModule, "run-1" },
		"run id with no source":  func(r *usage.RecordInfraUsageRequest) { r.RunID = "run-1" },
		"trial compute as member": func(r *usage.RecordInfraUsageRequest) {
			r.Source, r.Metric = usage.SourceMember, "infra.sandbox.vcpu.hours"
		},
		"trial compute as module": func(r *usage.RecordInfraUsageRequest) {
			r.Source, r.Metric = usage.SourceModule, "infra.sandbox.vcpu.hours"
		},
	} {
		t.Run(name, func(t *testing.T) {
			store := newFakeStore()
			req := validInfra()
			seedSentinelInfra(store, "infra.sandbox.vcpu.hours", usage.KindSum, "vCPU-hour", 40450)
			store.accounts[req.OwnerUserID] = uuid.New()
			mutate(&req)
			_, err := newService(store).RecordInfraUsage(context.Background(), req)
			requireCode(t, err, billing.CodeInvalidInput)
			require.Empty(t, store.events)
		})
	}
}

// (b) sandbox_trial behaves as today: stored dev_served with its run id and a
// stamped price, and it wakes only the sandbox cap.
func TestRecordInfraUsage_SandboxTrialStillBehavesAsToday(t *testing.T) {
	store, calls := newFakeStore(), &budgetCalls{}
	req := sandboxReq(store)
	_, err := usage.NewService(store).WithBudgetEvaluator(calls).RecordInfraUsage(context.Background(), req)
	require.NoError(t, err)
	ev := store.events[req.EventID]
	require.True(t, ev.DevServed)
	require.EqualValues(t, 40450, *ev.StampedPriceMicros)
	require.Equal(t, "run-0001", ev.RunID)
	require.Equal(t, budgetCalls{sandbox: 1}, *calls)
}
