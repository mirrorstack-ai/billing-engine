package systemleadadapter_test

import (
	"testing"
	"time"

	"github.com/mirrorstack-ai/billing-engine/internal/einvoice"
	"github.com/mirrorstack-ai/billing-engine/internal/provider/systemleadadapter"
)

func TestVoidWindowMatchesPortDefault(t *testing.T) {
	issuedAt := time.Date(2026, time.May, 5, 12, 0, 0, 0, time.UTC)
	got := systemleadadapter.VoidWindow(issuedAt)
	want := einvoice.DefaultVoidWindow(issuedAt)
	if !got.Equal(want) {
		t.Fatalf("VoidWindow(%s) = %s, want %s (the port default)", issuedAt, got, want)
	}
}
