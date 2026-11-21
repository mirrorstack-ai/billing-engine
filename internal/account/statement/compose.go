package statement

import (
	"fmt"
	"math/big"
	"strconv"
	"strings"
	"time"
)

// Compose builds a Statement from Input.
//
// Arithmetic (every step below rounds AT MOST once, and the rounding is
// always round-half-up — a .5 remainder rounds to the larger magnitude):
//
//	subtotal_usd_micros = Σ Lines[].AmountMicros                   (exact)
//	rate_scaled          = Rate parsed as Rate × 10^4                (exact)
//	subtotal_twd         = round_half_up(subtotal_usd_micros × rate_scaled / 10^10)
//	tax_twd              = round_half_up(subtotal_twd × tax_rate_bps / 10000)
//	total_twd            = subtotal_twd + tax_twd
//
// 10^10 is 10^6 (the USD-micros scale) × 10^4 (the rate scale). subtotal_twd
// rounds ONCE, on the summed subtotal — never per line: three USD $0.01 lines
// at rate 32.1500 sum to USD $0.03 (NT$0.9645, rounds to NT$1); rounding each
// $0.01 line first would round every one of them down to NT$0.
//
// Compose refuses:
//   - a total_twd at or below zero,
//   - a subtotal_usd_micros above MaxUSDSubtotalMicros (US$1,000,000),
//   - any FX problem (see FX's doc comment: a non-positive rate, a rate with
//     more than 4 decimal digits, a rate date that is not strictly before
//     StatementDate, or a rate date that falls on a Saturday or Sunday),
//   - a Line with an unlisted Kind,
//   - a Line whose Quantity × UnitMicros (when both are given) disagrees
//     with its AmountMicros.
func Compose(in Input) (Statement, error) {
	rateScaled, err := parseRateScaled(in.FX.Rate)
	if err != nil {
		return Statement{}, fmt.Errorf("fx: %w", err)
	}
	if !in.FX.RateDate.Before(in.StatementDate) {
		return Statement{}, fmt.Errorf(
			"fx: rate date %s must be strictly before the statement date %s",
			in.FX.RateDate.Format("2006-01-02"), in.StatementDate.Format("2006-01-02"))
	}
	if wd := in.FX.RateDate.UTC().Weekday(); wd == time.Saturday || wd == time.Sunday {
		return Statement{}, fmt.Errorf(
			"fx: rate date %s is a %s; confirm a business day against the bank's table",
			in.FX.RateDate.Format("2006-01-02"), wd)
	}

	for i, l := range in.Lines {
		if !l.Kind.valid() {
			return Statement{}, fmt.Errorf("line %d: unknown kind %q", i, l.Kind)
		}
		if l.Quantity != nil && l.UnitMicros != nil {
			product := new(big.Int).Mul(big.NewInt(*l.Quantity), big.NewInt(*l.UnitMicros))
			if !product.IsInt64() || product.Int64() != l.AmountMicros {
				return Statement{}, fmt.Errorf(
					"line %d: quantity %d × unit %d micros = %s, not amount %d micros",
					i, *l.Quantity, *l.UnitMicros, product.String(), l.AmountMicros)
			}
		}
	}

	sum := new(big.Int)
	for _, l := range in.Lines {
		sum.Add(sum, big.NewInt(l.AmountMicros))
	}
	if !sum.IsInt64() {
		return Statement{}, fmt.Errorf("usd subtotal %s micros overflows int64", sum.String())
	}
	subtotalMicros := sum.Int64()
	if subtotalMicros > MaxUSDSubtotalMicros {
		return Statement{}, fmt.Errorf(
			"usd subtotal %d micros exceeds the US$1,000,000 cap", subtotalMicros)
	}

	// subtotal_twd = round_half_up(subtotal_usd_micros × rate_scaled / 10^10)
	twdNum := new(big.Int).Mul(big.NewInt(subtotalMicros), big.NewInt(rateScaled))
	twdDen := new(big.Int).Exp(big.NewInt(10), big.NewInt(10), nil)
	subtotalTWD, err := roundHalfUp(twdNum, twdDen)
	if err != nil {
		return Statement{}, err
	}

	taxRateBps := in.TaxRateBps
	if taxRateBps == 0 {
		taxRateBps = DefaultTaxRateBps
	}
	if taxRateBps < 0 {
		return Statement{}, fmt.Errorf("tax_rate_bps must not be negative, got %d", taxRateBps)
	}

	// tax_twd = round_half_up(subtotal_twd × tax_rate_bps / 10000)
	taxNum := new(big.Int).Mul(big.NewInt(subtotalTWD), big.NewInt(int64(taxRateBps)))
	taxTWD, err := roundHalfUp(taxNum, big.NewInt(10_000))
	if err != nil {
		return Statement{}, err
	}

	totalTWD := subtotalTWD + taxTWD
	if totalTWD <= 0 {
		return Statement{}, fmt.Errorf("total %d (NT$) must be positive", totalTWD)
	}

	due, err := DueDate(in.StatementDate, in.DueRule)
	if err != nil {
		return Statement{}, fmt.Errorf("due rule: %w", err)
	}

	outFX := in.FX
	outFX.RateScaled = rateScaled

	return Statement{
		StatementDate:     in.StatementDate,
		UsageWindowFrom:   in.UsageWindowFrom,
		UsageWindowTo:     in.UsageWindowTo,
		Lines:             in.Lines,
		SubtotalUSDMicros: subtotalMicros,
		SubtotalTWD:       subtotalTWD,
		TaxTWD:            taxTWD,
		TotalTWD:          totalTWD,
		DueDate:           due,
		FX:                outFX,
	}, nil
}

// parseRateScaled parses a decimal string with at most 4 fractional digits
// into rate × 10^4, exactly, as an int64 — no float ever touches this number.
func parseRateScaled(s string) (int64, error) {
	if s == "" {
		return 0, fmt.Errorf("rate is empty")
	}
	intPart, fracPart, hasDot := strings.Cut(s, ".")
	if hasDot {
		if len(fracPart) > 4 {
			return 0, fmt.Errorf("rate %q has more than 4 decimal places", s)
		}
		if _, err := strconv.ParseUint(fracPart, 10, 64); fracPart != "" && err != nil {
			return 0, fmt.Errorf("rate %q is not a valid decimal: %w", s, err)
		}
		fracPart += strings.Repeat("0", 4-len(fracPart))
	} else {
		fracPart = "0000"
	}
	if intPart == "" {
		return 0, fmt.Errorf("rate %q is not a valid decimal", s)
	}
	combined := intPart + fracPart
	v, err := strconv.ParseInt(combined, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("rate %q is not a valid decimal: %w", s, err)
	}
	if v <= 0 {
		return 0, fmt.Errorf("rate %q must be positive", s)
	}
	return v, nil
}

// roundHalfUp rounds num/den to the nearest integer, half rounding up (toward
// +∞ on an exact .5 remainder). den must be positive. Returns an error rather
// than silently truncating when the result does not fit in int64 — the same
// guard internal/account/cycle's roundRatHalfUp uses, reimplemented here so
// this package imports nothing beyond the standard library.
func roundHalfUp(num, den *big.Int) (int64, error) {
	q := new(big.Int)
	rem := new(big.Int)
	q.QuoRem(num, den, rem) // truncates toward zero
	if rem.Sign() < 0 {
		// Normalize toward floor for a negative numerator (a net-credit
		// subtotal); den is always positive here.
		q.Sub(q, big.NewInt(1))
		rem.Add(rem, den)
	}
	if new(big.Int).Mul(rem, big.NewInt(2)).Cmp(den) >= 0 {
		q.Add(q, big.NewInt(1))
	}
	if q.BitLen() > 63 {
		return 0, fmt.Errorf("value %s overflows int64", q.String())
	}
	return q.Int64(), nil
}
