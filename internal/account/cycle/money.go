package cycle

import (
	"fmt"
	"math/big"
	"strings"

	"github.com/jackc/pgx/v5/pgtype"
)

// Money in this package is integer micro-dollars (1e-6 USD), end to end. The
// only fractional arithmetic happens inside big.Rat (exact rationals); every
// value that crosses a boundary is a rounded int64. NEVER float for money —
// this matches the #16 (usage/budget) precedent (usage.MicrosFromNumeric uses
// the same big.Rat round-half-up).

// rawCostMicros computes raw_cost = billable_quantity × unit_price_micros,
// rounded half-up to the whole micro. billable_quantity may be fractional (a
// time-weighted integral yields byte-hours), so the product is taken in big.Rat
// and rounded once at the boundary. unit_price_micros is a whole-micro int64.
func rawCostMicros(quantity string, unitPriceMicros int64) (int64, error) {
	qty, ok := new(big.Rat).SetString(quantity)
	if !ok {
		return 0, fmt.Errorf("parse billable quantity %q", quantity)
	}
	product := new(big.Rat).Mul(qty, new(big.Rat).SetInt64(unitPriceMicros))
	return roundRatHalfUp(product)
}

// chargedMicros computes the customer charge in ONE rounding pass:
//
//	charged = round_half_up(quantity × unit_price × num/den)
//
// It is deliberately NOT round_half_up(rawCost × num/den): a second rounding on
// an already-rounded rawCost diverges from the single declared round for
// fractional quantities (e.g. qty=0.1, price=13, 12/10 → single-pass 2 micros,
// two-pass 1). The whole product is held in exact big.Rat and rounded once.
// For the identity multiplier (num==den, custom metrics) the result equals
// rawCost exactly, so the snapshot stays consistent. den must be > 0 (enforced
// by the DB CHECK and the call sites).
func chargedMicros(quantity string, unitPriceMicros int64, num, den int) (int64, error) {
	if den == 0 || num == den {
		// Identity multiplier: charged == rawCost. Reuse the single-round
		// rawCost so the two snapshotted columns agree exactly.
		return rawCostMicros(quantity, unitPriceMicros)
	}
	qty, ok := new(big.Rat).SetString(quantity)
	if !ok {
		return 0, fmt.Errorf("parse billable quantity %q", quantity)
	}
	product := new(big.Rat).Mul(qty, new(big.Rat).SetInt64(unitPriceMicros))
	scaled := new(big.Rat).Mul(product, big.NewRat(int64(num), int64(den)))
	return roundRatHalfUp(scaled)
}

// takeMicros computes platform_take = round_half_up(num/den × base). base is
// (income − infra) in whole micros; the take is a fraction of it (15/100 or
// 30/100). den must be > 0.
func takeMicros(base int64, num, den int) (int64, error) {
	if den == 0 {
		return 0, nil
	}
	scaled := new(big.Rat).SetFrac(
		new(big.Int).Mul(big.NewInt(base), big.NewInt(int64(num))),
		big.NewInt(int64(den)),
	)
	return roundRatHalfUp(scaled)
}

// roundRatHalfUp rounds a big.Rat to the nearest integer, halves up (toward +∞
// on a .5 tie). Identical rounding to usage.roundRatHalfUp — money rounds at one
// deterministic point so the rollup and the live summary agree to the micro.
// Costs are non-negative, so half-up is the conventional merchant rounding.
//
// It returns an error rather than silently wrapping when the result does not
// fit in int64: big.Int.Int64() returns the low 64 bits on overflow, which
// would produce a silently-wrong (possibly negative) charge. billable_quantity
// (unbounded NUMERIC) × unit_price_micros (BIGINT) has no DB-side cap here (the
// budget engine caps limit_micros at 1e15; the rollup has no equivalent), so a
// large quantity × price can exceed int64 — guard it.
func roundRatHalfUp(r *big.Rat) (int64, error) {
	num := new(big.Int).Set(r.Num())
	den := r.Denom()
	q := new(big.Int)
	rem := new(big.Int)
	q.QuoRem(num, den, rem) // truncates toward zero
	// Normalize toward floor for negative values (defensive; costs ≥ 0).
	if rem.Sign() < 0 {
		q.Sub(q, big.NewInt(1))
		rem.Add(rem, den)
	}
	// remainder×2 ≥ denom → round up.
	if new(big.Int).Mul(rem, big.NewInt(2)).Cmp(den) >= 0 {
		q.Add(q, big.NewInt(1))
	}
	// int64 holds values whose magnitude needs ≤ 63 bits; anything wider would
	// wrap silently in Int64().
	if q.BitLen() > 63 {
		return 0, fmt.Errorf("money value %s overflows int64 micros", q.String())
	}
	return q.Int64(), nil
}

// numericString renders a pgtype.Numeric to its exact decimal string for
// carrying the billable_quantity through the Service without a float round-trip.
// An invalid/NULL numeric renders "0".
func numericString(n pgtype.Numeric) string {
	if !n.Valid || n.NaN || n.InfinityModifier != 0 || n.Int == nil {
		return "0"
	}
	// Reconstruct Int × 10^Exp as a big.Rat, then render exactly.
	rat := new(big.Rat).SetInt(n.Int)
	if n.Exp >= 0 {
		mul := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(n.Exp)), nil)
		rat.Mul(rat, new(big.Rat).SetInt(mul))
	} else {
		div := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(-n.Exp)), nil)
		rat.Quo(rat, new(big.Rat).SetInt(div))
	}
	if rat.IsInt() {
		return rat.Num().String()
	}
	// A finite decimal renders exactly with enough digits; -1 prec emits the
	// shortest exact decimal for a terminating fraction and a faithful value
	// otherwise (quantities are display values — money never flows through here).
	return rat.FloatString(ratStringPrec)
}

// ratStringPrec bounds the decimal places when a quantity is a non-terminating
// fraction. byte-hours / integrals are the only fractional quantities and are
// far below this precision; money never uses this path.
const ratStringPrec = 12

// numericFromString builds the pgtype.Numeric the generated upsert expects for
// the NUMERIC billable_quantity column, from the exact decimal string.
func numericFromString(s string) (pgtype.Numeric, error) {
	if s == "" {
		s = "0"
	}
	var n pgtype.Numeric
	if err := n.Scan(s); err != nil {
		return pgtype.Numeric{}, fmt.Errorf("encode numeric from %q: %w", s, err)
	}
	return n, nil
}

// reservedMetricPrefixes are the platform-measured namespaces (design §3a).
// A reserved-name aggregate is a platform-infra / built-in line: it carries the
// 12/10 customer markup (cost × 1.2). These metrics are not ingested until PR
// #10 — RecordUsage rejects them from the SDK — so in practice the rollup never
// sees one today; the plane logic is implemented + defaults safely. Mirrors
// usage.reservedMetricPrefixes (kept local to avoid widening the usage package
// API for a single internal consumer).
var reservedMetricPrefixes = []string{"platform.", "infra."}

// isReservedMetric reports whether the metric falls in a platform-measured
// namespace (→ the 12/10 markup plane). Case-sensitive, matching the ingest
// gate.
func isReservedMetric(metric string) bool {
	for _, p := range reservedMetricPrefixes {
		if strings.HasPrefix(metric, p) {
			return true
		}
	}
	return false
}
