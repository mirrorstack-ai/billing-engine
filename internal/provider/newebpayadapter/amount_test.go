package newebpayadapter

import (
	"errors"
	"testing"
)

func TestParseAmtValid(t *testing.T) {
	cases := map[string]int64{
		"0":          0,
		"100":        100,
		"0100":       100,
		"9999999999": 9999999999,
	}
	for in, want := range cases {
		got, err := ParseAmt(in)
		if err != nil {
			t.Fatalf("ParseAmt(%q): %v", in, err)
		}
		if got != want {
			t.Errorf("ParseAmt(%q) = %d, want %d", in, got, want)
		}
	}
}

func TestParseAmtInvalid(t *testing.T) {
	for _, in := range []string{
		"",
		"-100",
		"100.0",
		"+100",
		"1e2",
		"100 ",
		" 100",
		"10,000",
		"10000000000", // 11 digits
		"abc",
	} {
		if _, err := ParseAmt(in); !errors.Is(err, ErrInvalidAmount) {
			t.Errorf("ParseAmt(%q): got %v, want error wrapping ErrInvalidAmount", in, err)
		}
	}
}
