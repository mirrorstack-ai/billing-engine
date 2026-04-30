package stripe

import (
	"errors"
	"testing"
)

func TestNewClient_MissingEnv(t *testing.T) {
	t.Setenv(envSecretKey, "")
	c, err := NewClient()
	if !errors.Is(err, ErrMissingSecretKey) {
		t.Fatalf("err = %v, want ErrMissingSecretKey", err)
	}
	if c != nil {
		t.Fatalf("client = %v, want nil", c)
	}
}

func TestNewClient_WithEnv(t *testing.T) {
	t.Setenv(envSecretKey, "sk_test_dummy")
	c, err := NewClient()
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	if c == nil || c.API == nil {
		t.Fatal("expected non-nil client and API")
	}
}
