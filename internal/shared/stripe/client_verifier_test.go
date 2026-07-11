package stripe

import (
	"strings"
	"testing"
)

// An EMPTY signing secret must yield the fail-closed verifier: a
// realVerifier with secret "" would be fail-OPEN (ConstructEvent HMACs
// with the empty key, so an attacker who guesses the secret is unset
// can mint passing signatures).
func TestNewVerifierEmptySecretRejectsEverything(t *testing.T) {
	v := NewVerifier("")
	if _, ok := v.(rejectAllVerifier); !ok {
		t.Fatalf("NewVerifier(\"\") = %T, want rejectAllVerifier", v)
	}
	_, err := v.Verify([]byte(`{"id":"evt_1"}`), "t=1,v1=deadbeef")
	if err == nil || !strings.Contains(err.Error(), "not configured") {
		t.Fatalf("Verify with empty secret must fail closed, got err=%v", err)
	}
}

// A configured secret keeps returning the real verifier.
func TestNewVerifierNonEmptySecretIsReal(t *testing.T) {
	if _, ok := NewVerifier("whsec_x").(*realVerifier); !ok {
		t.Fatal("non-empty secret must construct the real verifier")
	}
}
