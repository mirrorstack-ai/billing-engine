package stripeadapter

import (
	"strings"
	"testing"
	"time"

	"github.com/mirrorstack-ai/billing-engine/internal/intent"
)

// Seal stores the canonical upper-case ISO 4217 code. Every legacy call site
// in this repo sends the lower-case constant to Stripe. If the adapter
// forwards the sealed value verbatim, the intent rail describes the same money
// differently to the same provider than the legacy rail does — which surfaces
// as an unexplained reconciliation mismatch rather than an error.
func TestSealNormalizesUpAndTheWireNeedsItDown(t *testing.T) {
	sealed, err := intent.Seal(intent.Draft{
		Payer:             intent.Subject{Kind: "user", ID: "acct-1"},
		Currency:          "usd", // what every legacy call site uses
		Lines:             []intent.Line{intent.NewLine("d", "m", "1", 1, 10_000)},
		Kind:              "domain.custom",
		PriceBookRevision: "pb-1",
		TermsRevision:     "terms-1",
		Tax:               intent.TaxDetermination{Resolved: true, Jurisdiction: "TW", RuleRevision: "tax-1"},
		AuthorizationID:   "auth-1",
		NoticePolicy:      "email/v1",
		ExecuteNotBefore:  time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC),
		ExecuteNotAfter:   time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC),
		SourceFactKeys:    []string{"f"},
	})
	if err != nil {
		t.Fatalf("Seal: %v", err)
	}

	// The document holds the canonical form. That is correct and must not change.
	if sealed.Currency() != "USD" {
		t.Fatalf("Seal stored %q, expected the canonical upper-case code", sealed.Currency())
	}

	// The wire needs the other one.
	if got := strings.ToLower(strings.TrimSpace(sealed.Currency())); got != "usd" {
		t.Fatalf("wire form is %q, want %q", got, "usd")
	}
}
