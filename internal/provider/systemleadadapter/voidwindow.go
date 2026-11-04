package systemleadadapter

import (
	"time"

	"github.com/mirrorstack-ai/billing-engine/internal/einvoice"
)

// VoidWindow reports the last instant a Void is possible for an invoice
// issued at issuedAt. It is the port-level default
// (einvoice.DefaultVoidWindow): e首發票's own cutoff is UNVERIFIED
// (mirrorstack-ai/billing-engine#286 item 7), so this function is the
// one place to change when the Swagger or a stage run says otherwise —
// everything that calls VoidWindow goes through here rather than
// inlining the default a second time.
func VoidWindow(issuedAt time.Time) time.Time {
	return einvoice.DefaultVoidWindow(issuedAt)
}
