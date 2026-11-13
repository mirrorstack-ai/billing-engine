package systemleadadapter_test

import (
	"errors"
	"testing"

	"github.com/mirrorstack-ai/billing-engine/internal/provider/systemleadadapter"
)

// TestSignIsUnimplemented pins the honest refusal: this library must
// not guess the vendor's signature concatenation. If this test starts
// failing because Sign now returns a real signature, that is the
// Swagger having been read — update this test to a known-answer check
// against the vendor's own example pair instead of deleting it.
func TestSignIsUnimplemented(t *testing.T) {
	_, err := systemleadadapter.Sign("company-1", "2026-01-01T00:00:00Z", "salt")
	if !errors.Is(err, systemleadadapter.ErrSignatureSchemeUnverified) {
		t.Fatalf("Sign = %v, want ErrSignatureSchemeUnverified", err)
	}
}
