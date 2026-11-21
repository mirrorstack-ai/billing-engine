package systemleadadapter

import (
	"errors"

	"github.com/mirrorstack-ai/billing-engine/internal/einvoice"
)

// StageBaseURL is e首發票's public stage host, used for Swagger reads
// and contract tests (mirrorstack-ai/billing-engine#286 item 2). It is
// public information, not a credential.
const StageBaseURL = "https://jpe-sl-einvoice-erpapi-stage.azurewebsites.net"

// ErrNoLiveHost is returned by HostForMode for einvoice.ModeLive. There
// is deliberately no live host constant in this package: D504 and D520
// keep this issue sandbox-and-vendor-stage only until the Taiwan
// company exists, and the production URL and keys arrive only at
// vendor activation, which a later issue adds.
var ErrNoLiveHost = errors.New("systemleadadapter: no live host is configured; this library is stage-only (D504, D520)")

// HostForMode asserts a host against mode rather than taking one from a
// caller — D524: "a sandbox request can never reach a live host." A
// request built for einvoice.ModeLive refuses outright, because the
// only alternative to refusing is accepting a caller-supplied host,
// which is exactly the hole D524 closes.
func HostForMode(mode einvoice.Mode) (string, error) {
	switch mode {
	case einvoice.ModeSandbox:
		return StageBaseURL, nil
	case einvoice.ModeLive:
		return "", ErrNoLiveHost
	default:
		return "", errors.New("systemleadadapter: unknown mode " + string(mode))
	}
}
