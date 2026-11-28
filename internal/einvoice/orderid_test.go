package einvoice_test

import (
	"errors"
	"strings"
	"testing"

	"github.com/mirrorstack-ai/billing-engine/internal/einvoice"
)

func TestValidateOrderNumberDefaultLimit(t *testing.T) {
	cases := []struct {
		name string
		n    string
		ok   bool
	}{
		{"ordinary", "ORDER_2026_000123", true},
		{"exactly 20", strings.Repeat("a", 20), true},
		{"21 characters", strings.Repeat("a", 21), false},
		{"empty", "", false},
		{"disallowed char", "order-123", false},
		{"unicode", "訂單123", false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			n := einvoice.ProviderOrderNumber(c.n)
			for _, err := range []error{
				einvoice.ValidateOrderNumber(n),
				einvoice.Capabilities{}.ValidateOrderNumber(n),
			} {
				if c.ok && err != nil {
					t.Errorf("ValidateOrderNumber(%q) = %v, want nil", c.n, err)
				}
				if !c.ok && !errors.Is(err, einvoice.ErrInvalidOrderNumber) {
					t.Errorf("ValidateOrderNumber(%q) = %v, want ErrInvalidOrderNumber", c.n, err)
				}
			}
		})
	}
}

// The longer key is accepted only by a provider that declares it.
func TestValidateOrderNumberPerProviderLimit(t *testing.T) {
	selfNumbering := einvoice.Capabilities{
		SelfNumbering:     true,
		MaxOrderNumberLen: einvoice.SelfNumberingMaxOrderNumberLen,
	}
	long := einvoice.ProviderOrderNumber(strings.Repeat("a", einvoice.SelfNumberingMaxOrderNumberLen))

	if err := selfNumbering.ValidateOrderNumber(long); err != nil {
		t.Fatalf("self-numbering provider refused a %d-character key: %v", len(long), err)
	}
	if err := selfNumbering.ValidateOrderNumber(long + "a"); !errors.Is(err, einvoice.ErrInvalidOrderNumber) {
		t.Fatalf("self-numbering provider accepted %d characters: %v", len(long)+1, err)
	}
	if err := einvoice.ValidateOrderNumber(long); !errors.Is(err, einvoice.ErrInvalidOrderNumber) {
		t.Fatalf("default limit accepted a %d-character key: %v", len(long), err)
	}
	if err := selfNumbering.ValidateOrderNumber("bad-char"); !errors.Is(err, einvoice.ErrInvalidOrderNumber) {
		t.Fatalf("the longer limit relaxed the character set: %v", err)
	}
}

func TestCapabilitiesValidate(t *testing.T) {
	cases := []struct {
		name string
		c    einvoice.Capabilities
		ok   bool
	}{
		{"zero value", einvoice.Capabilities{}, true},
		{"self-numbering, long keys", einvoice.Capabilities{SelfNumbering: true, MaxOrderNumberLen: 64}, true},
		{"vendor-style pending", einvoice.Capabilities{SupportsAsyncPending: true}, true},
		{"self-numbering cannot be pending", einvoice.Capabilities{SelfNumbering: true, SupportsAsyncPending: true}, false},
		{"limit below the default", einvoice.Capabilities{MaxOrderNumberLen: 19}, false},
		{"limit above the ceiling", einvoice.Capabilities{MaxOrderNumberLen: einvoice.MaxOrderNumberLenCeiling + 1}, false},
		{"negative limit", einvoice.Capabilities{MaxOrderNumberLen: -1}, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			err := c.c.Validate()
			if c.ok != (err == nil) {
				t.Fatalf("Validate(%+v) = %v, want ok=%v", c.c, err, c.ok)
			}
		})
	}
}
