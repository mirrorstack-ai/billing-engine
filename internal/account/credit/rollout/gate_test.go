package rollout

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

type gateSpy struct {
	calls   atomic.Int32
	blocked bool
	err     error
}

func (g *gateSpy) OutOfCredits(context.Context, uuid.UUID) (bool, error) {
	g.calls.Add(1)
	return g.blocked, g.err
}

func selectedGateController(mode Mode, accountID uuid.UUID, reporter *Reporter) *Controller {
	cfg := validConfig()
	cfg.Mode = string(mode)
	cfg.BasisPoints = "0"
	setTestAllowlist(&cfg, accountID.String())
	return NewController(Parse(cfg), reporter)
}

func readEMFEvents(t *testing.T, out *bytes.Buffer) []map[string]any {
	t.Helper()
	raw := strings.TrimSpace(out.String())
	if raw == "" {
		return nil
	}
	lines := strings.Split(raw, "\n")
	events := make([]map[string]any, 0, len(lines))
	for _, line := range lines {
		var event map[string]any
		require.NoError(t, json.Unmarshal([]byte(line), &event))
		events = append(events, event)
	}
	return events
}

func TestGateOffAndExcludedMakeZeroWalletCallsAndEmitNothing(t *testing.T) {
	accountID := uuid.MustParse("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa")
	tests := []struct {
		name       string
		controller func(*Reporter) *Controller
	}{
		{
			name: "off",
			controller: func(reporter *Reporter) *Controller {
				return NewController(offPolicy(ComponentAPI), reporter)
			},
		},
		{
			name: "excluded",
			controller: func(reporter *Reporter) *Controller {
				cfg := validConfig()
				cfg.Mode = string(ModeEnforce)
				cfg.BasisPoints = "0"
				return NewController(Parse(cfg), reporter)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var (
				out         bytes.Buffer
				shadowCalls atomic.Int32
			)
			shadow := ReadOnlyBooleanEvaluatorFunc(func(context.Context, uuid.UUID) (bool, error) {
				shadowCalls.Add(1)
				return true, nil
			})
			enforce := &gateSpy{blocked: true}
			gate := NewGate(tc.controller(NewReporter(&out)), shadow, enforce)

			blocked, err := gate.OutOfCredits(context.Background(), accountID)

			require.NoError(t, err)
			require.False(t, blocked)
			require.Zero(t, shadowCalls.Load())
			require.Zero(t, enforce.calls.Load())
			require.Empty(t, out.String())
		})
	}
}

func TestGateShadowUsesOnlyReadOnlyEvaluatorAndPreservesLegacyFalse(t *testing.T) {
	accountID := uuid.MustParse("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa")
	var (
		out         bytes.Buffer
		shadowCalls atomic.Int32
	)
	reporter := NewReporter(&out)
	controller := selectedGateController(ModeShadow, accountID, reporter)
	shadow := ReadOnlyBooleanEvaluatorFunc(func(context.Context, uuid.UUID) (bool, error) {
		shadowCalls.Add(1)
		return true, nil
	})
	enforce := &gateSpy{blocked: true}

	blocked, err := NewGate(controller, shadow, enforce).
		OutOfCredits(context.Background(), accountID)

	require.NoError(t, err)
	require.False(t, blocked, "shadow must preserve the legacy false verdict")
	require.EqualValues(t, 1, shadowCalls.Load())
	require.Zero(t, enforce.calls.Load(), "shadow must not enter the mutation-capable seam")
	events := readEMFEvents(t, &out)
	require.Len(t, events, 1)
	require.Equal(t, "shadow", events[0]["Mode"])
	require.EqualValues(t, 1, events[0]["EvaluationCount"])
	require.EqualValues(t, 0, events[0]["DivergenceCount"],
		"gate cannot infer divergence from the caller's legacy standing verdict")
	require.EqualValues(t, 0, events[0]["EvaluatorErrorCount"])
	require.NotContains(t, strings.ToLower(out.String()), "account")
	require.NotContains(t, out.String(), accountID.String())
}

func TestGateShadowErrorIsVisibleButCannotChangeLegacyFalse(t *testing.T) {
	accountID := uuid.MustParse("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa")
	sentinel := errors.New("shadow read failed")
	var out bytes.Buffer
	shadow := ReadOnlyBooleanEvaluatorFunc(func(context.Context, uuid.UUID) (bool, error) {
		return true, sentinel
	})
	enforce := &gateSpy{blocked: true}

	blocked, err := NewGate(
		selectedGateController(ModeShadow, accountID, NewReporter(&out)),
		shadow,
		enforce,
	).OutOfCredits(context.Background(), accountID)

	require.False(t, blocked)
	require.ErrorIs(t, err, sentinel)
	require.Zero(t, enforce.calls.Load())
	events := readEMFEvents(t, &out)
	require.Len(t, events, 1)
	require.EqualValues(t, 0, events[0]["DivergenceCount"])
	require.EqualValues(t, 1, events[0]["EvaluatorErrorCount"])
}

func TestGateEnforceCallsCoordinatorExactlyOnceAndReportsOutcome(t *testing.T) {
	accountID := uuid.MustParse("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa")
	sentinel := errors.New("coordinator unavailable")
	tests := []struct {
		name            string
		coordinatorBool bool
		coordinatorErr  error
		wantBlocked     bool
		wantError       error
		wantErrorCount  float64
	}{
		{
			name:            "successful blocked verdict",
			coordinatorBool: true,
			wantBlocked:     true,
		},
		{
			name:            "successful eligible verdict",
			coordinatorBool: false,
			wantBlocked:     false,
		},
		{
			name:            "error cannot apply a returned blocked verdict",
			coordinatorBool: true,
			coordinatorErr:  sentinel,
			wantBlocked:     false,
			wantError:       sentinel,
			wantErrorCount:  1,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var (
				out         bytes.Buffer
				shadowCalls atomic.Int32
			)
			shadow := ReadOnlyBooleanEvaluatorFunc(func(context.Context, uuid.UUID) (bool, error) {
				shadowCalls.Add(1)
				return true, nil
			})
			enforce := &gateSpy{blocked: tc.coordinatorBool, err: tc.coordinatorErr}
			controller := selectedGateController(ModeEnforce, accountID, NewReporter(&out))

			blocked, err := NewGate(controller, shadow, enforce).
				OutOfCredits(context.Background(), accountID)

			require.Equal(t, tc.wantBlocked, blocked)
			if tc.wantError == nil {
				require.NoError(t, err)
			} else {
				require.ErrorIs(t, err, tc.wantError)
			}
			require.Zero(t, shadowCalls.Load(), "enforce must not invoke the shadow seam")
			require.EqualValues(t, 1, enforce.calls.Load())
			events := readEMFEvents(t, &out)
			require.Len(t, events, 1)
			require.Equal(t, "enforce", events[0]["Mode"])
			require.EqualValues(t, 0, events[0]["DivergenceCount"],
				"gate cannot infer divergence from the caller's legacy standing verdict")
			require.EqualValues(t, tc.wantErrorCount, events[0]["EvaluatorErrorCount"])
			require.NotContains(t, strings.ToLower(out.String()), "account")
			require.NotContains(t, out.String(), accountID.String())
		})
	}
}

func TestGateSelectedMissingDependenciesFailSafeAndReport(t *testing.T) {
	accountID := uuid.MustParse("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa")
	for _, mode := range []Mode{ModeShadow, ModeEnforce} {
		t.Run(string(mode), func(t *testing.T) {
			var out bytes.Buffer
			blocked, err := NewGate(
				selectedGateController(mode, accountID, NewReporter(&out)),
				nil,
				nil,
			).OutOfCredits(context.Background(), accountID)

			require.False(t, blocked)
			require.ErrorIs(t, err, ErrEvaluatorUnavailable)
			events := readEMFEvents(t, &out)
			require.Len(t, events, 1)
			require.EqualValues(t, 1, events[0]["EvaluatorErrorCount"])

			var standingOut bytes.Buffer
			result := NewGate(
				selectedGateController(mode, accountID, NewReporter(&standingOut)),
				nil,
				nil,
			).EvaluateStanding(context.Background(), accountID, true)
			require.ErrorIs(t, result.Err, ErrEvaluatorUnavailable)
			require.True(t, result.Effective,
				"a missing dependency cannot clear the legacy block")
			require.False(t, result.Evaluated)
			require.Len(t, readEMFEvents(t, &standingOut), 1)
		})
	}

	blocked, err := (*Gate)(nil).OutOfCredits(context.Background(), accountID)
	require.NoError(t, err)
	require.False(t, blocked)
	blocked, err = NewGate(nil, nil, nil).OutOfCredits(context.Background(), accountID)
	require.NoError(t, err)
	require.False(t, blocked)
	require.True(t, (*Gate)(nil).
		EvaluateStanding(context.Background(), accountID, true).
		Effective)
}

func TestGateEvaluateStandingUsesAdditiveLegacyComposition(t *testing.T) {
	accountID := uuid.MustParse("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa")
	tests := []struct {
		name          string
		mode          Mode
		legacyBlocked bool
		walletBlocked bool
		wantEffective bool
		wantDiverged  bool
	}{
		{
			name:          "shadow observes new wallet block but preserves eligible legacy",
			mode:          ModeShadow,
			walletBlocked: true,
			wantEffective: false,
			wantDiverged:  true,
		},
		{
			name:          "shadow cannot unblock legacy",
			mode:          ModeShadow,
			legacyBlocked: true,
			walletBlocked: false,
			wantEffective: true,
		},
		{
			name:          "shadow matching block is not a final divergence",
			mode:          ModeShadow,
			legacyBlocked: true,
			walletBlocked: true,
			wantEffective: true,
		},
		{
			name:          "enforce adds wallet block",
			mode:          ModeEnforce,
			walletBlocked: true,
			wantEffective: true,
			wantDiverged:  true,
		},
		{
			name:          "enforce cannot unblock legacy",
			mode:          ModeEnforce,
			legacyBlocked: true,
			walletBlocked: false,
			wantEffective: true,
		},
		{
			name:          "enforce matching block is not a final divergence",
			mode:          ModeEnforce,
			legacyBlocked: true,
			walletBlocked: true,
			wantEffective: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var (
				out         bytes.Buffer
				shadowCalls atomic.Int32
			)
			shadow := ReadOnlyBooleanEvaluatorFunc(func(context.Context, uuid.UUID) (bool, error) {
				shadowCalls.Add(1)
				return tc.walletBlocked, nil
			})
			enforce := &gateSpy{blocked: tc.walletBlocked}
			gate := NewGate(
				selectedGateController(tc.mode, accountID, NewReporter(&out)),
				shadow,
				enforce,
			)

			result := gate.EvaluateStanding(
				context.Background(),
				accountID,
				tc.legacyBlocked,
			)

			require.NoError(t, result.Err)
			require.True(t, result.Evaluated)
			require.Equal(t, tc.legacyBlocked, result.Legacy)
			require.Equal(t, tc.walletBlocked, result.Wallet)
			require.Equal(t, tc.wantEffective, result.Effective)
			require.Equal(t, tc.wantDiverged, result.Diverged)
			if tc.mode == ModeShadow {
				require.EqualValues(t, 1, shadowCalls.Load())
				require.Zero(t, enforce.calls.Load())
			} else {
				require.Zero(t, shadowCalls.Load())
				require.EqualValues(t, 1, enforce.calls.Load())
			}
			events := readEMFEvents(t, &out)
			require.Len(t, events, 1)
			require.EqualValues(t, count(tc.wantDiverged), events[0]["DivergenceCount"])
		})
	}
}

func TestGateEvaluateStandingErrorsPreserveLegacy(t *testing.T) {
	accountID := uuid.MustParse("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa")
	sentinel := errors.New("wallet evaluator failed")
	for _, mode := range []Mode{ModeShadow, ModeEnforce} {
		for _, legacyBlocked := range []bool{false, true} {
			t.Run(string(mode)+"/legacy="+strings.ToLower(strconv.FormatBool(legacyBlocked)), func(t *testing.T) {
				var (
					out         bytes.Buffer
					shadowCalls atomic.Int32
					shadow      ReadOnlyBooleanEvaluator
					enforce     *gateSpy
				)
				if mode == ModeShadow {
					shadow = ReadOnlyBooleanEvaluatorFunc(func(context.Context, uuid.UUID) (bool, error) {
						shadowCalls.Add(1)
						return !legacyBlocked, sentinel
					})
					enforce = &gateSpy{blocked: !legacyBlocked}
				} else {
					shadow = ReadOnlyBooleanEvaluatorFunc(func(context.Context, uuid.UUID) (bool, error) {
						shadowCalls.Add(1)
						return !legacyBlocked, nil
					})
					enforce = &gateSpy{blocked: !legacyBlocked, err: sentinel}
				}

				result := NewGate(
					selectedGateController(mode, accountID, NewReporter(&out)),
					shadow,
					enforce,
				).EvaluateStanding(context.Background(), accountID, legacyBlocked)

				require.ErrorIs(t, result.Err, sentinel)
				require.Equal(t, legacyBlocked, result.Effective)
				require.False(t, result.Diverged)
				if mode == ModeShadow {
					require.EqualValues(t, 1, shadowCalls.Load())
					require.Zero(t, enforce.calls.Load())
				} else {
					require.Zero(t, shadowCalls.Load())
					require.EqualValues(t, 1, enforce.calls.Load())
				}
				events := readEMFEvents(t, &out)
				require.Len(t, events, 1)
				require.EqualValues(t, 1, events[0]["EvaluatorErrorCount"])
				require.EqualValues(t, 0, events[0]["DivergenceCount"])
			})
		}
	}
}

func TestGateEvaluateStandingOffAndExcludedMakeZeroCalls(t *testing.T) {
	accountID := uuid.MustParse("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa")
	var (
		out         bytes.Buffer
		shadowCalls atomic.Int32
	)
	shadow := ReadOnlyBooleanEvaluatorFunc(func(context.Context, uuid.UUID) (bool, error) {
		shadowCalls.Add(1)
		return true, nil
	})
	enforce := &gateSpy{blocked: true}

	off := NewGate(
		NewController(offPolicy(ComponentAPI), NewReporter(&out)),
		shadow,
		enforce,
	).EvaluateStanding(context.Background(), accountID, true)
	require.True(t, off.Effective)
	require.False(t, off.Evaluated)

	cfg := validConfig()
	cfg.Mode = string(ModeEnforce)
	cfg.BasisPoints = "0"
	excluded := NewGate(
		NewController(Parse(cfg), NewReporter(&out)),
		shadow,
		enforce,
	).EvaluateStanding(context.Background(), accountID, false)
	require.False(t, excluded.Effective)
	require.False(t, excluded.Evaluated)
	require.Zero(t, shadowCalls.Load())
	require.Zero(t, enforce.calls.Load())
	require.Empty(t, out.String())
}

func TestControllerObserveReportsOnlyMatchingSelectedDecision(t *testing.T) {
	accountID := uuid.MustParse("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa")
	sentinel := errors.New("cycle mutation failed")
	var out bytes.Buffer
	controller := selectedGateController(ModeEnforce, accountID, NewReporter(&out))
	selected := controller.Decide(accountID)

	controller.Observe(selected, 25*time.Millisecond, sentinel)
	events := readEMFEvents(t, &out)
	require.Len(t, events, 1)
	require.EqualValues(t, 1, events[0]["EvaluationCount"])
	require.EqualValues(t, 1, events[0]["EvaluatorErrorCount"])
	require.EqualValues(t, 25, events[0]["LatencyMs"])

	cfg := validConfig()
	cfg.Mode = string(ModeEnforce)
	cfg.BasisPoints = "0"
	excluded := NewController(Parse(cfg), NewReporter(&out))
	excluded.Observe(excluded.Decide(accountID), time.Second, sentinel)
	NewController(offPolicy(ComponentAPI), NewReporter(&out)).
		Observe(selected, time.Second, sentinel)
	require.Len(t, readEMFEvents(t, &out), 1,
		"excluded and off controllers must not report observations")
}

func TestGateReporterAndEnforceAdapterAreRaceSafe(t *testing.T) {
	accountID := uuid.MustParse("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa")
	var out bytes.Buffer
	enforce := &gateSpy{blocked: true}
	gate := NewGate(
		selectedGateController(ModeEnforce, accountID, NewReporter(&out)),
		nil,
		enforce,
	)

	const calls = 32
	var wg sync.WaitGroup
	results := make(chan error, calls)
	wg.Add(calls)
	for range calls {
		go func() {
			defer wg.Done()
			blocked, err := gate.OutOfCredits(context.Background(), accountID)
			if err != nil {
				results <- err
				return
			}
			if !blocked {
				results <- errors.New("enforce gate unexpectedly returned eligible")
			}
		}()
	}
	wg.Wait()
	close(results)
	for err := range results {
		require.NoError(t, err)
	}

	require.EqualValues(t, calls, enforce.calls.Load())
	require.Len(t, readEMFEvents(t, &out), calls)
}
