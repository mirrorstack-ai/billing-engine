package systemleadadapter_test

import (
	"errors"
	"testing"

	"github.com/mirrorstack-ai/billing-engine/internal/einvoice"
	"github.com/mirrorstack-ai/billing-engine/internal/provider/systemleadadapter"
)

func TestHostForModeSandbox(t *testing.T) {
	host, err := systemleadadapter.HostForMode(einvoice.ModeSandbox)
	if err != nil {
		t.Fatalf("HostForMode(sandbox): %v", err)
	}
	if host != systemleadadapter.StageBaseURL {
		t.Fatalf("host = %q, want %q", host, systemleadadapter.StageBaseURL)
	}
}

// TestHostForModeLiveRefuses is D524 made concrete: a sandbox-mode
// request can never reach a live host, which this library enforces by
// having no live host to return at all.
func TestHostForModeLiveRefuses(t *testing.T) {
	_, err := systemleadadapter.HostForMode(einvoice.ModeLive)
	if !errors.Is(err, systemleadadapter.ErrNoLiveHost) {
		t.Fatalf("HostForMode(live) = %v, want ErrNoLiveHost", err)
	}
}

func TestHostForModeUnknown(t *testing.T) {
	_, err := systemleadadapter.HostForMode(einvoice.Mode("staging"))
	if err == nil {
		t.Fatal("HostForMode(unknown mode) = nil error, want a refusal")
	}
}
