// Package pftest is the test seam for platformfunded's audit trail: a test asks
// "was the attribute used, at which gate, for which org" instead of inferring a
// use from a side effect. Test-only; no production code imports it.
package pftest

import (
	"context"
	"log/slog"
	"sync"
	"testing"

	"github.com/mirrorstack-ai/billing-engine/internal/account/platformfunded"
)

// Use is one audit line: the gate that relied on the attribute and the org.
type Use struct{ Gate, OrgID string }

// Capture routes the default slog logger into memory for the rest of the test
// (restored on cleanup; not for parallel tests) and returns a func listing every
// platform_funded audit line logged so far.
func Capture(t testing.TB) func() []Use {
	t.Helper()
	h := &handler{}
	prev := slog.Default()
	slog.SetDefault(slog.New(h))
	t.Cleanup(func() { slog.SetDefault(prev) })
	return func() []Use {
		h.mu.Lock()
		defer h.mu.Unlock()
		return append([]Use(nil), h.uses...)
	}
}

type handler struct {
	mu   sync.Mutex
	uses []Use
}

func (h *handler) Enabled(context.Context, slog.Level) bool { return true }
func (h *handler) WithAttrs([]slog.Attr) slog.Handler       { return h }
func (h *handler) WithGroup(string) slog.Handler            { return h }

func (h *handler) Handle(_ context.Context, r slog.Record) error {
	attrs := map[string]string{}
	r.Attrs(func(a slog.Attr) bool { attrs[a.Key] = a.Value.String(); return true })
	if attrs["event"] == platformfunded.AuditEvent {
		h.mu.Lock()
		h.uses = append(h.uses, Use{Gate: attrs["gate"], OrgID: attrs["org_id"]})
		h.mu.Unlock()
	}
	return nil
}
