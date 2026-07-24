package rollout

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/mirrorstack-ai/billing-engine/internal/account/credit"
)

const (
	testManifestSHA = "1111111111111111111111111111111111111111"
	testBillingSHA  = "2222222222222222222222222222222222222222"
)

func validConfig() Config {
	return Config{
		MasterEnabled: true, SchemaReady: true, Component: ComponentAPI,
		Mode: string(ModeShadow), BasisPoints: "1000",
		CoreManifestSHA: testManifestSHA, BillingSHA: testBillingSHA,
	}
}

func TestParseFailsClosedAtomically(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*Config)
	}{
		{name: "master off", mutate: func(c *Config) { c.MasterEnabled = false }},
		{name: "schema unready", mutate: func(c *Config) { c.SchemaReady = false }},
		{name: "unknown component", mutate: func(c *Config) { c.Component = "webhook" }},
		{name: "missing mode", mutate: func(c *Config) { c.Mode = "" }},
		{name: "invalid mode", mutate: func(c *Config) { c.Mode = "on" }},
		{name: "missing bps", mutate: func(c *Config) { c.BasisPoints = "" }},
		{name: "negative bps", mutate: func(c *Config) { c.BasisPoints = "-1" }},
		{name: "bps overflow", mutate: func(c *Config) { c.BasisPoints = "10001" }},
		{name: "bps whitespace", mutate: func(c *Config) { c.BasisPoints = " 1" }},
		{name: "bps leading zero", mutate: func(c *Config) { c.BasisPoints = "01" }},
		{name: "invalid allowlist", mutate: func(c *Config) { c.Allowlist = "not-a-uuid" }},
		{name: "noncanonical allowlist", mutate: func(c *Config) {
			c.Allowlist = "AAAAAAAA-AAAA-4AAA-8AAA-AAAAAAAAAAAA"
		}},
		{name: "duplicate allowlist", mutate: func(c *Config) {
			c.Allowlist = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa,aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
		}},
		{name: "missing manifest", mutate: func(c *Config) { c.CoreManifestSHA = "" }},
		{name: "short billing sha", mutate: func(c *Config) { c.BillingSHA = "1234" }},
		{name: "uppercase sha", mutate: func(c *Config) {
			c.BillingSHA = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
		}},
	}

	accountID := uuid.MustParse("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa")
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cfg := validConfig()
			tc.mutate(&cfg)
			decision := Parse(cfg).Decide(accountID)
			require.Equal(t, ModeOff, decision.Mode)
			require.False(t, decision.Selected)
			require.Zero(t, decision.BasisPoints)
			require.Empty(t, decision.CoreManifestSHA)
			require.Empty(t, decision.BillingSHA)
		})
	}
}

func TestExplicitOffDoesNotRequireActiveConfiguration(t *testing.T) {
	policy := Parse(Config{
		MasterEnabled: true, SchemaReady: true,
		Component: ComponentWorker, Mode: string(ModeOff),
		BasisPoints: "invalid", Allowlist: "invalid",
	})
	require.Equal(t, ModeOff, policy.Decide(uuid.New()).Mode)
}

func TestAllowlistSelectsAtZeroPercent(t *testing.T) {
	accountID := uuid.MustParse("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa")
	cfg := validConfig()
	cfg.Mode = string(ModeEnforce)
	cfg.BasisPoints = "0"
	cfg.Allowlist = accountID.String()

	decision := Parse(cfg).Decide(accountID)
	require.Equal(t, ModeEnforce, decision.Mode)
	require.True(t, decision.Selected)
	require.True(t, decision.Allowlisted)
	require.Zero(t, decision.BasisPoints)
}

func TestPercentageMembershipIsMonotonic(t *testing.T) {
	accountID := uuid.MustParse("01234567-89ab-4cde-8fab-0123456789ab")
	var selected bool
	for _, bps := range []string{"0", "1", "100", "500", "1000", "2500", "5000", "10000"} {
		cfg := validConfig()
		cfg.BasisPoints = bps
		current := Parse(cfg).Decide(accountID).Selected
		require.False(t, selected && !current, "increasing bps must never evict an account")
		selected = current
	}
	require.True(t, selected, "10,000 bps selects every account")
}

func TestBucketFixedVectorsAndComponentSeparation(t *testing.T) {
	tests := []struct {
		account string
		api     uint16
		worker  uint16
	}{
		{account: "00000000-0000-4000-8000-000000000001", api: 9303, worker: 6729},
		{account: "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa", api: 9389, worker: 9320},
		{account: "ffffffff-ffff-4fff-bfff-ffffffffffff", api: 3035, worker: 2785},
	}
	for _, tc := range tests {
		id := uuid.MustParse(tc.account)
		require.Equal(t, tc.api, Bucket(ComponentAPI, id))
		require.Equal(t, tc.worker, Bucket(ComponentWorker, id))
		require.NotEqual(t, Bucket(ComponentAPI, id), Bucket(ComponentWorker, id),
			"component must participate in the stable hash")
	}
}

func TestFromEnvUsesIndependentComponentControls(t *testing.T) {
	t.Setenv(envMaster, "true")
	t.Setenv(envAllowlist, "")
	t.Setenv(envManifest, testManifestSHA)
	t.Setenv(envBillingSHA, testBillingSHA)
	t.Setenv("CREDIT_WALLET_API_MODE", "shadow")
	t.Setenv("CREDIT_WALLET_API_BPS", "10000")
	t.Setenv("CREDIT_WALLET_WORKER_MODE", "enforce")
	t.Setenv("CREDIT_WALLET_WORKER_BPS", "0")

	accountID := uuid.New()
	api := FromEnv(ComponentAPI, true).Decide(accountID)
	worker := FromEnv(ComponentWorker, true).Decide(accountID)
	require.Equal(t, ModeShadow, api.Mode)
	require.True(t, api.Selected)
	require.Equal(t, ModeEnforce, worker.Mode)
	require.False(t, worker.Selected)
}

func TestFromEnvUnsetMasterIsOff(t *testing.T) {
	t.Setenv(envMaster, "temporary")
	require.NoError(t, os.Unsetenv(envMaster))
	require.Equal(t, ModeOff, FromEnv(ComponentAPI, true).Decide(uuid.New()).Mode)
}

func TestFromEnvMissingActiveBPSIsOffEvenWithAllowlist(t *testing.T) {
	accountID := uuid.MustParse("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa")
	t.Setenv(envMaster, "true")
	t.Setenv(envAllowlist, accountID.String())
	t.Setenv(envManifest, testManifestSHA)
	t.Setenv(envBillingSHA, testBillingSHA)
	t.Setenv("CREDIT_WALLET_API_MODE", "enforce")
	t.Setenv("CREDIT_WALLET_API_BPS", "temporary")
	require.NoError(t, os.Unsetenv("CREDIT_WALLET_API_BPS"))

	decision := FromEnv(ComponentAPI, true).Decide(accountID)
	require.Equal(t, ModeOff, decision.Mode)
	require.False(t, decision.Selected,
		"a missing percentage cannot leave an allowlisted account active")
}

func TestRolloutIDBindsExactReleaseAndStageButNotAccount(t *testing.T) {
	cfg := validConfig()
	firstAccount := uuid.MustParse("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa")
	secondAccount := uuid.MustParse("bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb")
	baseline := Parse(cfg)
	baselineID := baseline.Decide(firstAccount).RolloutID
	require.NotEmpty(t, baselineID)
	require.Equal(t, "61b8948f50fb34423a15621d", baselineID,
		"release-stage identity is a reviewed stable vector")
	require.Equal(t, baselineID, baseline.Decide(secondAccount).RolloutID,
		"account and cohort bucket must not create metric cardinality")

	releaseChanged := cfg
	releaseChanged.BillingSHA = "3333333333333333333333333333333333333333"
	require.NotEqual(t, baselineID, Parse(releaseChanged).Decide(firstAccount).RolloutID)

	manifestChanged := cfg
	manifestChanged.CoreManifestSHA = "4444444444444444444444444444444444444444"
	require.NotEqual(t, baselineID, Parse(manifestChanged).Decide(firstAccount).RolloutID)

	stageChanged := cfg
	stageChanged.BasisPoints = "1001"
	require.NotEqual(t, baselineID, Parse(stageChanged).Decide(firstAccount).RolloutID)

	componentChanged := cfg
	componentChanged.Component = ComponentWorker
	require.NotEqual(t, baselineID, Parse(componentChanged).Decide(firstAccount).RolloutID)

	modeChanged := cfg
	modeChanged.Mode = string(ModeEnforce)
	require.NotEqual(t, baselineID, Parse(modeChanged).Decide(firstAccount).RolloutID)
}

func TestReporterHasLowCardinalityDimensionsAndNoAccountIdentifier(t *testing.T) {
	var out bytes.Buffer
	accountID := uuid.MustParse("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa")
	decision := Parse(validConfig()).Decide(
		accountID,
	)
	err := NewReporter(&out).
		WithNow(func() time.Time {
			return time.Date(2026, time.July, 25, 1, 2, 3, 0, time.UTC)
		}).
		Emit(Observation{
			Decision: decision, Duration: 1250 * time.Microsecond,
			Diverged: true, EvaluatorError: true,
			MoneyInvariantFailure: true, ShadowMutation: true,
			DuplicateMutation: true,
		})
	require.NoError(t, err)
	require.NotContains(t, strings.ToLower(out.String()), "account")
	require.NotContains(t, out.String(), accountID.String(),
		"account UUID must never enter the EMF event")

	var event map[string]any
	require.NoError(t, json.Unmarshal(out.Bytes(), &event))
	require.Equal(t, "api", event["Component"])
	require.Equal(t, "shadow", event["Mode"])
	require.Equal(t, testManifestSHA, event["CoreManifestSHA"])
	require.Equal(t, testBillingSHA, event["BillingEngineSHA"])
	require.Equal(t, decision.RolloutID, event["RolloutID"])
	require.EqualValues(t, 1, event["EvaluationCount"])
	require.EqualValues(t, 1, event["DivergenceCount"])
	require.EqualValues(t, 1, event["EvaluatorErrorCount"])
	require.EqualValues(t, 1.25, event["LatencyMs"])

	metadata := event["_aws"].(map[string]any)
	directive := metadata["CloudWatchMetrics"].([]any)[0].(map[string]any)
	require.Equal(t, metricsNamespace, directive["Namespace"])
	require.Equal(t, []any{[]any{"Component", "Mode", "RolloutID"}}, directive["Dimensions"])
	require.NotContains(t, directive["Dimensions"], "CohortBucket")
}

func TestControllerOffAndExcludedNeverInvokeWalletEvaluator(t *testing.T) {
	accountID := uuid.MustParse("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa")
	var calls int
	evaluator := ReadOnlyBooleanEvaluatorFunc(func(context.Context, uuid.UUID) (bool, error) {
		calls++
		return true, nil
	})

	off := NewController(offPolicy(ComponentAPI), nil)
	result := off.CompareBoolean(context.Background(), accountID, false, evaluator)
	require.False(t, result.Evaluated)
	require.False(t, result.Effective)

	cfg := validConfig()
	cfg.Mode = string(ModeEnforce)
	cfg.BasisPoints = "0"
	excluded := NewController(Parse(cfg), nil)
	result = excluded.CompareBoolean(context.Background(), accountID, true, evaluator)
	require.False(t, result.Evaluated)
	require.True(t, result.Effective)
	require.Zero(t, calls, "off and excluded paths must make zero wallet calls")
}

func TestControllerShadowEvaluatesReadOnlyAndPreservesLegacy(t *testing.T) {
	accountID := uuid.MustParse("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa")
	cfg := validConfig()
	cfg.BasisPoints = "0"
	cfg.Allowlist = accountID.String()

	var out bytes.Buffer
	now := time.Date(2026, time.July, 25, 1, 2, 3, 0, time.UTC)
	reporter := NewReporter(&out).WithNow(func() time.Time { return now })
	controller := NewController(Parse(cfg), reporter).WithNow(func() time.Time {
		current := now
		now = now.Add(25 * time.Millisecond)
		return current
	})

	result := controller.CompareBoolean(
		context.Background(),
		accountID,
		false,
		ReadOnlyBooleanEvaluatorFunc(func(context.Context, uuid.UUID) (bool, error) {
			return true, nil
		}),
	)
	require.True(t, result.Decision.Shadowed())
	require.True(t, result.Evaluated)
	require.True(t, result.Wallet)
	require.True(t, result.Diverged)
	require.False(t, result.Effective, "shadow must preserve the legacy verdict")
	require.Contains(t, out.String(), `"DivergenceCount":1`)
	require.Contains(t, out.String(), `"LatencyMs":25`)
}

func TestControllerSelectedNilEvaluatorIsVisibleAndPreservesLegacy(t *testing.T) {
	accountID := uuid.MustParse("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa")
	cfg := validConfig()
	cfg.BasisPoints = "0"
	cfg.Allowlist = accountID.String()

	var out bytes.Buffer
	controller := NewController(Parse(cfg), NewReporter(&out))
	result := controller.CompareBoolean(
		context.Background(),
		accountID,
		true,
		nil,
	)

	require.ErrorIs(t, result.Err, ErrEvaluatorUnavailable)
	require.False(t, result.Evaluated)
	require.True(t, result.Effective, "wiring failure must preserve legacy")
	require.Contains(t, out.String(), `"EvaluationCount":1`)
	require.Contains(t, out.String(), `"EvaluatorErrorCount":1`)

	out.Reset()
	off := NewController(offPolicy(ComponentAPI), NewReporter(&out))
	result = off.CompareBoolean(context.Background(), accountID, false, nil)
	require.NoError(t, result.Err)
	require.False(t, result.Evaluated)
	require.Empty(t, out.String(), "off must not emit or call wallet evaluation")

	excludedCfg := validConfig()
	excludedCfg.BasisPoints = "0"
	excluded := NewController(Parse(excludedCfg), NewReporter(&out))
	result = excluded.CompareBoolean(context.Background(), accountID, true, nil)
	require.NoError(t, result.Err)
	require.True(t, result.Effective)
	require.Empty(t, out.String(), "excluded must not emit or call wallet evaluation")
}

func TestControllerEnforceOnlyAppliesSuccessfulSelectedEvaluation(t *testing.T) {
	accountID := uuid.MustParse("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa")
	cfg := validConfig()
	cfg.Mode = string(ModeEnforce)
	cfg.BasisPoints = "0"
	cfg.Allowlist = accountID.String()
	controller := NewController(Parse(cfg), nil)

	result := controller.CompareBoolean(
		context.Background(),
		accountID,
		false,
		ReadOnlyBooleanEvaluatorFunc(func(context.Context, uuid.UUID) (bool, error) {
			return true, nil
		}),
	)
	require.True(t, result.Decision.Enforced())
	require.True(t, result.Effective)

	sentinel := errors.New("wallet unavailable")
	result = controller.CompareBoolean(
		context.Background(),
		accountID,
		false,
		ReadOnlyBooleanEvaluatorFunc(func(context.Context, uuid.UUID) (bool, error) {
			return true, sentinel
		}),
	)
	require.ErrorIs(t, result.Err, sentinel)
	require.False(t, result.Diverged)
	require.False(t, result.Effective, "evaluator failure must preserve legacy")
}

type countingUsageEvaluator struct {
	calls int
}

func (e *countingUsageEvaluator) EvaluateCreditUsage(context.Context, credit.UsageEvent) error {
	e.calls++
	return nil
}

func TestUsageEvaluatorRoutesOffShadowAndEnforceWithoutCrossingSeams(t *testing.T) {
	accountID := uuid.MustParse("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa")
	event := credit.UsageEvent{AccountID: accountID}
	var shadowCalls int
	shadow := ReadOnlyUsageEvaluatorFunc(func(context.Context, credit.UsageEvent) error {
		shadowCalls++
		return nil
	})
	enforce := &countingUsageEvaluator{}

	cfg := validConfig()
	cfg.BasisPoints = "0"
	adapter := NewUsageEvaluator(NewController(Parse(cfg), nil), shadow, enforce)
	require.NoError(t, adapter.EvaluateCreditUsage(context.Background(), event))
	require.Zero(t, shadowCalls)
	require.Zero(t, enforce.calls, "excluded account must make zero wallet calls")

	cfg.Allowlist = accountID.String()
	adapter = NewUsageEvaluator(NewController(Parse(cfg), nil), shadow, enforce)
	require.NoError(t, adapter.EvaluateCreditUsage(context.Background(), event))
	require.Equal(t, 1, shadowCalls)
	require.Zero(t, enforce.calls, "shadow must never enter the mutation-capable seam")

	cfg.Mode = string(ModeEnforce)
	adapter = NewUsageEvaluator(NewController(Parse(cfg), nil), shadow, enforce)
	require.NoError(t, adapter.EvaluateCreditUsage(context.Background(), event))
	require.Equal(t, 1, shadowCalls)
	require.Equal(t, 1, enforce.calls)
}
