package newebpayadapter

import (
	"fmt"
	"strconv"
)

// maxAmtDigits is NewebPay's own limit on Amt: at most 10 decimal digits
// (NT$9,999,999,999), comfortably above anything this engine will ever
// route through NewebPay.
const maxAmtDigits = 10

// ParseAmt parses NewebPay's Amt field strictly: digits only, no sign, no
// decimal point, no leading '+', at most 10 digits. "0100" is accepted —
// NewebPay does not forbid a leading zero — but "-100", "100.0" and "+100"
// are not integers in NewebPay's own sense and are refused rather than
// coerced.
func ParseAmt(s string) (int64, error) {
	if s == "" || len(s) > maxAmtDigits {
		return 0, fmt.Errorf("%w: %q", ErrInvalidAmount, s)
	}
	for i := 0; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return 0, fmt.Errorf("%w: %q", ErrInvalidAmount, s)
		}
	}
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("%w: %q", ErrInvalidAmount, s)
	}
	return n, nil
}
