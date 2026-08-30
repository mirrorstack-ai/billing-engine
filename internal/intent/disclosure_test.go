package intent

import (
	"errors"
	"testing"
	"time"
)

func fullDisclosure() Disclosure {
	return Disclosure{
		Currency:     "usd",
		AmountMicros: 20_000_000,
		CreditMicros: 22_000_000, // a bonus tier: paid != received
		Restrictions: "credit is usable only on module usage, and is not transferable",
		RefundTerms:  "refundable in full within 14 days if unspent",
		ExpiresAt:    time.Date(2027, 1, 1, 0, 0, 0, 0, time.UTC),
		Rail:         "stripe",
		IntentDigest: "abc123",
	}
}

// §6 names eight things a credit-purchase disclosure must state. A disclosure
// missing any of them is not a shorter document — it is one that failed to say
// something the customer needed, and an acceptance of it attests to a hole.
func TestEveryFieldSection6NamesIsRequired(t *testing.T) {
	for _, tc := range []struct {
		name   string
		break_ func(*Disclosure)
		want   error
	}{
		{"currency", func(d *Disclosure) { d.Currency = "" }, ErrDisclosureIncomplete},
		{"restrictions", func(d *Disclosure) { d.Restrictions = "  " }, ErrDisclosureIncomplete},
		{"refund terms", func(d *Disclosure) { d.RefundTerms = "" }, ErrDisclosureIncomplete},
		{"rail", func(d *Disclosure) { d.Rail = "" }, ErrDisclosureIncomplete},
		{"expiry", func(d *Disclosure) { d.ExpiresAt = time.Time{} }, ErrDisclosureIncomplete},
		{"intent digest", func(d *Disclosure) { d.IntentDigest = "" }, ErrDisclosureUnboundIntent},
		{"amount", func(d *Disclosure) { d.AmountMicros = 0 }, ErrDisclosureNonPositive},
		{"credit received", func(d *Disclosure) { d.CreditMicros = 0 }, ErrDisclosureNonPositive},
	} {
		t.Run(tc.name, func(t *testing.T) {
			d := fullDisclosure()
			tc.break_(&d)
			if _, err := d.Digest(); !errors.Is(err, tc.want) {
				t.Fatalf("a disclosure with no %s produced a digest (err=%v)", tc.name, err)
			}
		})
	}

	if _, err := fullDisclosure().Digest(); err != nil {
		t.Fatalf("a complete disclosure was refused: %v", err)
	}
}

// 🔴 The encoding must be injective. The acceptance references only the
// digest, so two different disclosures sharing one would mean accepting either
// attests to both — and "restrictions" is free prose, exactly the kind of
// field a separator-joined encoding collides on.
func TestEveryFieldChangesTheDisclosureDigest(t *testing.T) {
	base, err := fullDisclosure().Digest()
	if err != nil {
		t.Fatalf("Digest: %v", err)
	}

	for _, tc := range []struct {
		name   string
		change func(*Disclosure)
	}{
		{"currency", func(d *Disclosure) { d.Currency = "eur" }},
		{"amount", func(d *Disclosure) { d.AmountMicros = 20_000_001 }},
		{"credit received", func(d *Disclosure) { d.CreditMicros = 22_000_001 }},
		{"restrictions", func(d *Disclosure) { d.Restrictions += "." }},
		{"refund terms", func(d *Disclosure) { d.RefundTerms += "." }},
		{"expiry", func(d *Disclosure) { d.ExpiresAt = d.ExpiresAt.Add(time.Second) }},
		{"rail", func(d *Disclosure) { d.Rail = "newebpay" }},
		{"intent digest", func(d *Disclosure) { d.IntentDigest = "def456" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			d := fullDisclosure()
			tc.change(&d)
			got, err := d.Digest()
			if err != nil {
				t.Fatalf("Digest: %v", err)
			}
			if got == base {
				t.Fatalf("changing the %s did not change the digest — an acceptance of one "+
					"disclosure attests to the other", tc.name)
			}
		})
	}
}

// The collision a separator-joined encoding would produce, stated directly:
// moving a character across the restrictions/refund-terms boundary must not
// leave the digest unchanged.
func TestTheEncodingDoesNotCollideAcrossFieldBoundaries(t *testing.T) {
	a := fullDisclosure()
	a.Restrictions, a.RefundTerms = "ab", "c"

	b := fullDisclosure()
	b.Restrictions, b.RefundTerms = "a", "bc"

	da, err := a.Digest()
	if err != nil {
		t.Fatalf("Digest(a): %v", err)
	}
	db, err := b.Digest()
	if err != nil {
		t.Fatalf("Digest(b): %v", err)
	}
	if da == db {
		t.Fatal(`"ab"+"c" and "a"+"bc" produced one digest — the encoding is not injective`)
	}
}

// Currency is normalised the way Seal normalises it, so a disclosure and the
// intent it binds to cannot disagree about the same money for want of a case.
func TestCurrencyIsNormalisedLikeTheIntent(t *testing.T) {
	lower := fullDisclosure()
	lower.Currency = "usd"
	upper := fullDisclosure()
	upper.Currency = "USD"

	dl, err := lower.Digest()
	if err != nil {
		t.Fatalf("Digest: %v", err)
	}
	du, err := upper.Digest()
	if err != nil {
		t.Fatalf("Digest: %v", err)
	}
	if dl != du {
		t.Fatal("usd and USD produced different disclosure digests")
	}
}
