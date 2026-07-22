package config_test

import (
	"os"
	"testing"

	"github.com/mirrorstack-ai/billing-engine/internal/shared/config"
)

// IsLambda and Port are the testable functions; MustEnv and MustPgxPool
// call os.Exit on failure, which would terminate the test binary —
// covering those paths would require a subprocess pattern, deferred
// until the os.Exit branches grow more interesting than "log and exit".

func TestIsLambda_TrueWhenLambdaEnvSet(t *testing.T) {
	t.Setenv("AWS_LAMBDA_FUNCTION_NAME", "billing-engine-account-webhook")
	if !config.IsLambda() {
		t.Errorf("IsLambda() = false; want true when AWS_LAMBDA_FUNCTION_NAME is set")
	}
}

func TestIsLambda_FalseWhenLambdaEnvUnset(t *testing.T) {
	t.Setenv("AWS_LAMBDA_FUNCTION_NAME", "")
	if config.IsLambda() {
		t.Errorf("IsLambda() = true; want false when AWS_LAMBDA_FUNCTION_NAME is empty")
	}
}

func TestPort_UsesServiceEnvWhenSet(t *testing.T) {
	t.Setenv("SVC_PORT", "9999")
	t.Setenv("PORT", "7777")
	if got := config.Port("SVC_PORT", "8000"); got != "9999" {
		t.Errorf("Port() = %q; want %q (service-specific takes precedence)", got, "9999")
	}
}

func TestPort_FallsThroughToPortEnv(t *testing.T) {
	t.Setenv("SVC_PORT", "")
	t.Setenv("PORT", "7777")
	if got := config.Port("SVC_PORT", "8000"); got != "7777" {
		t.Errorf("Port() = %q; want %q (PORT fallback)", got, "7777")
	}
}

func TestPort_UsesFallbackWhenBothUnset(t *testing.T) {
	t.Setenv("SVC_PORT", "")
	t.Setenv("PORT", "")
	if got := config.Port("SVC_PORT", "8000"); got != "8000" {
		t.Errorf("Port() = %q; want %q (final fallback)", got, "8000")
	}
}

func TestMustEnv_ReturnsValueWhenSet(t *testing.T) {
	t.Setenv("TEST_REQUIRED", "hello")
	if got := config.MustEnv("TEST_REQUIRED"); got != "hello" {
		t.Errorf("MustEnv() = %q; want %q", got, "hello")
	}
}

func TestCreditWalletEnabled_FailClosedTruthTable(t *testing.T) {
	value := func(v string) *string { return &v }
	tests := []struct {
		name  string
		value *string
		want  bool
	}{
		{name: "unset", value: nil, want: false},
		{name: "empty", value: value(""), want: false},
		{name: "one", value: value("1"), want: true},
		{name: "lowercase true", value: value("true"), want: true},
		{name: "uppercase true", value: value("TRUE"), want: true},
		{name: "titlecase true", value: value("True"), want: true},
		{name: "zero", value: value("0"), want: false},
		{name: "false", value: value("false"), want: false},
		{name: "other", value: value("yes"), want: false},
		{name: "whitespace is not accepted", value: value(" true "), want: false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if tc.value == nil {
				t.Setenv("CREDIT_WALLET_ENABLED", "temporary")
				if err := os.Unsetenv("CREDIT_WALLET_ENABLED"); err != nil {
					t.Fatalf("Unsetenv() error = %v", err)
				}
			} else {
				t.Setenv("CREDIT_WALLET_ENABLED", *tc.value)
			}
			if got := config.CreditWalletEnabled(); got != tc.want {
				t.Errorf("CreditWalletEnabled() = %v; want %v", got, tc.want)
			}
		})
	}
}
