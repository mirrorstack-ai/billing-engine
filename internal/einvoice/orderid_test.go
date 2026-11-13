package einvoice_test

import (
	"errors"
	"strings"
	"testing"

	"github.com/mirrorstack-ai/billing-engine/internal/einvoice"
)

func TestValidateOrderNumber(t *testing.T) {
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
			err := einvoice.ValidateOrderNumber(einvoice.ProviderOrderNumber(c.n))
			if c.ok && err != nil {
				t.Errorf("ValidateOrderNumber(%q) = %v, want nil", c.n, err)
			}
			if !c.ok && !errors.Is(err, einvoice.ErrInvalidOrderNumber) {
				t.Errorf("ValidateOrderNumber(%q) = %v, want ErrInvalidOrderNumber", c.n, err)
			}
		})
	}
}
