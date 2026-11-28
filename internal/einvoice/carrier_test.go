package einvoice_test

import (
	"errors"
	"testing"

	"github.com/mirrorstack-ai/billing-engine/internal/einvoice"
)

func TestValidateMobileBarcode(t *testing.T) {
	cases := []struct {
		code string
		ok   bool
	}{
		{"/ABCD123", true},
		{"/AB.+-12", true},
		{"ABCD1234", false}, // missing leading slash
		{"/AB", false},      // too short
		{"", false},
	}
	for _, c := range cases {
		err := einvoice.ValidateMobileBarcode(c.code)
		if c.ok && err != nil {
			t.Errorf("ValidateMobileBarcode(%q) = %v, want nil", c.code, err)
		}
		if !c.ok && !errors.Is(err, einvoice.ErrInvalidCarrier) {
			t.Errorf("ValidateMobileBarcode(%q) = %v, want ErrInvalidCarrier", c.code, err)
		}
	}
}

func TestValidateLoveCode(t *testing.T) {
	cases := []struct {
		code string
		ok   bool
	}{
		{"919", true},
		{"1234567", true},
		{"12", false},
		{"12345678", false},
		{"abc", false},
	}
	for _, c := range cases {
		err := einvoice.ValidateLoveCode(c.code)
		if c.ok && err != nil {
			t.Errorf("ValidateLoveCode(%q) = %v, want nil", c.code, err)
		}
		if !c.ok && !errors.Is(err, einvoice.ErrInvalidLoveCode) {
			t.Errorf("ValidateLoveCode(%q) = %v, want ErrInvalidLoveCode", c.code, err)
		}
	}
}
