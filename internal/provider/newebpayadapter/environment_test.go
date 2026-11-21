package newebpayadapter

import (
	"errors"
	"testing"
)

func TestEnvironmentURLs(t *testing.T) {
	cases := []struct {
		env       Environment
		wantMPG   string
		wantQuery string
	}{
		{Sandbox, "https://ccore.newebpay.com/MPG/mpg_gateway", "https://ccore.newebpay.com/API/QueryTradeInfo"},
		{Live, "https://core.newebpay.com/MPG/mpg_gateway", "https://core.newebpay.com/API/QueryTradeInfo"},
	}
	for _, tc := range cases {
		mpg, err := tc.env.MPGURL()
		if err != nil {
			t.Fatalf("MPGURL(%q): %v", tc.env, err)
		}
		if mpg != tc.wantMPG {
			t.Errorf("MPGURL(%q) = %q, want %q", tc.env, mpg, tc.wantMPG)
		}
		query, err := tc.env.QueryTradeInfoURL()
		if err != nil {
			t.Fatalf("QueryTradeInfoURL(%q): %v", tc.env, err)
		}
		if query != tc.wantQuery {
			t.Errorf("QueryTradeInfoURL(%q) = %q, want %q", tc.env, query, tc.wantQuery)
		}
	}
}

func TestEnvironmentUnknownIsError(t *testing.T) {
	unknown := Environment("staging")
	if _, err := unknown.MPGURL(); !errors.Is(err, ErrUnknownEnvironment) {
		t.Fatalf("MPGURL(staging): got %v, want error wrapping ErrUnknownEnvironment", err)
	}
	if _, err := unknown.QueryTradeInfoURL(); !errors.Is(err, ErrUnknownEnvironment) {
		t.Fatalf("QueryTradeInfoURL(staging): got %v, want error wrapping ErrUnknownEnvironment", err)
	}
	if _, err := Environment("").MPGURL(); !errors.Is(err, ErrUnknownEnvironment) {
		t.Fatalf("MPGURL(\"\"): got %v, want error wrapping ErrUnknownEnvironment", err)
	}
}
