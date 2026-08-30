package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/mirrorstack-ai/billing-engine/internal/account/capabilities"
	"github.com/mirrorstack-ai/billing-engine/internal/shared/buildinfo"
)

// docs/VERIFICATION.md §2: Health "must return the same identity fields
// whether the service is healthy or not." The identity must therefore
// not be conditional on anything, including on the build having been
// stamped — an unstamped build has to say "unknown" out loud, because
// that literal is what tells a reader the answer cannot be tied to a
// revision.
func TestHealthAlwaysCarriesBuildIdentity(t *testing.T) {
	rec := httptest.NewRecorder()
	health(rec, httptest.NewRequest(http.MethodGet, "/__health", nil))

	if rec.Code != http.StatusOK {
		t.Fatalf("health returned %d, want 200", rec.Code)
	}

	var body struct {
		Status string         `json:"status"`
		Build  buildinfo.Info `json:"build"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("health body is not JSON: %v (%s)", err, rec.Body.String())
	}
	if body.Status != "ok" {
		t.Errorf("status = %q, want %q", body.Status, "ok")
	}
	// The test binary is not stamped, so this asserts the honest answer
	// rather than a stamped one.
	if body.Build.Commit != buildinfo.Unknown {
		t.Errorf("commit = %q, want %q on an unstamped build", body.Build.Commit, buildinfo.Unknown)
	}
	if body.Build.Identified {
		t.Error("an unstamped build reported itself as identified")
	}
	if !strings.Contains(rec.Body.String(), `"build"`) {
		t.Error("health body carries no build object at all")
	}
}

// The Capabilities action exists, needs no payload, and reports the
// legacy money paths honestly. docs/VERIFICATION.md §2 records that the
// dispatcher's action table had no case for it.
func TestCapabilitiesActionReportsLegacyMoneyPaths(t *testing.T) {
	d := &dispatcher{}

	out, err := d.dispatch(context.Background(), "Capabilities", nil)
	if err != nil {
		t.Fatalf("Capabilities dispatch failed: %v", err)
	}
	report, ok := out.(capabilities.Report)
	if !ok {
		t.Fatalf("Capabilities returned %T, want capabilities.Report", out)
	}

	if report.LegacyMoneyPaths != capabilities.LegacyMoneyPaths {
		t.Errorf("reported %d legacy money paths, want %d",
			report.LegacyMoneyPaths, capabilities.LegacyMoneyPaths)
	}

	// The intent-only claim must be computed, never configured, so no
	// deployment can assert it while legacy collectors remain.
	wantIntentOnly := capabilities.LegacyMoneyPaths == 0
	if report.IntentOnly != wantIntentOnly {
		t.Errorf("intent_only = %v with %d legacy paths, want %v",
			report.IntentOnly, report.LegacyMoneyPaths, wantIntentOnly)
	}

	// An unstamped build must not be allowed to execute an intent even
	// once the legacy paths are gone.
	if report.IntentExecutionReady {
		t.Error("an unstamped build with legacy money paths reported itself ready to execute intents")
	}
}

// Capabilities is a statement about the build, so it must answer
// without a database, a provider client, or an account. A dispatcher
// with every service nil is the strongest way to say that.
func TestCapabilitiesNeedsNoDependencies(t *testing.T) {
	d := &dispatcher{}
	if _, err := d.dispatch(context.Background(), "Capabilities", json.RawMessage(`{"ignored":true}`)); err != nil {
		t.Fatalf("Capabilities must answer from a bare dispatcher, got: %v", err)
	}
}
