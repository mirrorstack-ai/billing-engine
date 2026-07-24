package legacyrestamp_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/mirrorstack-ai/billing-engine/internal/account/legacyrestamp"
)

const (
	testCoreSHA    = "1111111111111111111111111111111111111111"
	testBillingSHA = "2222222222222222222222222222222222222222"
)

func validEnvironment() legacyrestamp.Environment {
	return legacyrestamp.Environment{
		RestampMode: "1",
		Master:      "0",
		WorkerMode:  "off",
		WorkerBPS:   "0",
		CoreSHA:     testCoreSHA,
		BillingSHA:  testBillingSHA,
	}
}

func TestParseEnvironmentExplicitSafeMode(t *testing.T) {
	cfg, err := legacyrestamp.ParseEnvironment(validEnvironment())
	require.NoError(t, err)
	require.Equal(t, legacyrestamp.Config{
		Enabled:    true,
		CoreSHA:    testCoreSHA,
		BillingSHA: testBillingSHA,
	}, cfg)
}

func TestParseEnvironmentEmptyIsOrdinaryDarkPath(t *testing.T) {
	cfg, err := legacyrestamp.ParseEnvironment(legacyrestamp.Environment{
		Master:     "0",
		WorkerMode: "off",
		WorkerBPS:  "0",
		CoreSHA:    testCoreSHA,
		BillingSHA: testBillingSHA,
	})
	require.NoError(t, err)
	require.False(t, cfg.Enabled)
}

func TestParseEnvironmentRejectsMalformedIntentAndEveryUnsafeControl(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*legacyrestamp.Environment)
	}{
		{
			name: "malformed explicit value",
			mutate: func(env *legacyrestamp.Environment) {
				env.RestampMode = "true"
			},
		},
		{
			name: "master absent is not exact off",
			mutate: func(env *legacyrestamp.Environment) {
				env.Master = ""
			},
		},
		{
			name: "master truthy",
			mutate: func(env *legacyrestamp.Environment) {
				env.Master = "1"
			},
		},
		{
			name: "worker shadow",
			mutate: func(env *legacyrestamp.Environment) {
				env.WorkerMode = "shadow"
			},
		},
		{
			name: "worker bps absent",
			mutate: func(env *legacyrestamp.Environment) {
				env.WorkerBPS = ""
			},
		},
		{
			name: "worker bps nonzero",
			mutate: func(env *legacyrestamp.Environment) {
				env.WorkerBPS = "1"
			},
		},
		{
			name: "short core sha",
			mutate: func(env *legacyrestamp.Environment) {
				env.CoreSHA = "1234"
			},
		},
		{
			name: "uppercase core sha",
			mutate: func(env *legacyrestamp.Environment) {
				env.CoreSHA = strings.ToUpper(testCoreSHA[:20] + "abcdefabcdefabcdefab")
			},
		},
		{
			name: "short billing sha",
			mutate: func(env *legacyrestamp.Environment) {
				env.BillingSHA = "1234"
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			env := validEnvironment()
			tc.mutate(&env)
			cfg, err := legacyrestamp.ParseEnvironment(env)
			require.Error(t, err)
			require.False(t, cfg.Enabled)
		})
	}
}
