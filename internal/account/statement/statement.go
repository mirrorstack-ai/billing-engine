// Package statement composes a payer's monthly statement: the fixed fees the
// operator already priced, plus the previous month's usage read verbatim from
// the engine, into a set of printable lines and a TWD total — nothing else.
//
// This is a payment notice, not a charge path. Compose has no I/O, no clock,
// and no provider client; it creates no intent, authorizes nothing, and
// collects nothing. Every number it needs — the lines, the FX rate, the tax
// rate, the due-date rule — arrives as Input, and every number it produces
// comes back in the return value. The owner reads the result and sends it by
// hand (D493); nothing in this package or in cmd/statement-lines sends or
// collects on its own.
//
// Only integers and math/big ever carry money here. No 32- or 64-bit
// floating-point type appears in this package (see the grep test), because a
// repeating binary fraction is exactly the kind of silent drift a statement
// total must not have.
package statement

import "time"

// LineKind is the closed set of statement line kinds. An unlisted kind is
// refused by Compose rather than accepted as free text — see
// internal/intent/catalog.go for the same posture applied to charge kinds.
type LineKind string

const (
	// KindPlatformFee is the payer's flat platform fee for the month, priced
	// by the operator outside this package.
	KindPlatformFee LineKind = "platform_fee"
	// KindOrgSeats is the org's seat fee, priced by billing-engine#228.
	KindOrgSeats LineKind = "org_seats"
	// KindAppPlan is a per-app plan fee, priced by billing-engine#258.
	KindAppPlan LineKind = "app_plan"
	// KindUsage is metered usage read from the engine's own bill, never
	// multiplied by a payer price basis — see the package doc on #258/#228.
	KindUsage LineKind = "usage"
	// KindManual is a one-off line the operator typed by hand (e.g. a single
	// Alpha cost-share line; the Alpha instalments themselves are separate
	// bills, never a line here — D405, D463, D473).
	KindManual LineKind = "manual"
)

var lineKinds = map[LineKind]struct{}{
	KindPlatformFee: {},
	KindOrgSeats:    {},
	KindAppPlan:     {},
	KindUsage:       {},
	KindManual:      {},
}

func (k LineKind) valid() bool {
	_, ok := lineKinds[k]
	return ok
}

// Line is one row of a statement. AmountMicros is USD micros (1e-6 USD);
// negative is a credit. Quantity and UnitMicros are both optional display
// detail — when BOTH are given, Compose refuses the line unless
// Quantity × UnitMicros == AmountMicros exactly, so a line can never show
// arithmetic that disagrees with what it charges.
//
// Group is empty for a line that belongs to the payer issuing the statement.
// billing-engine#262 (the engine-made bill-to-distributor statement) reuses
// this exact shape for its per-customer groups, so a non-empty Group there
// names the customer the line rolls up from — the hand statement built by
// cmd/statement-lines never sets it.
type Line struct {
	Kind         LineKind `json:"kind"`
	Subject      string   `json:"subject"`
	Group        string   `json:"group,omitempty"`
	Quantity     *int64   `json:"quantity,omitempty"`
	UnitMicros   *int64   `json:"unit_micros,omitempty"`
	AmountMicros int64    `json:"amount_micros"`
	Note         string   `json:"note,omitempty"`
}

// FX is the rate quote an operator types in for one statement: the Bank of
// Taiwan spot selling rate of the business day before the statement date,
// that day's date, and a citation of where it was read. There is no FX code
// anywhere in this system, and this package fetches nothing — it only
// validates and converts a rate the operator already looked up.
type FX struct {
	// Rate is the spot selling rate as a decimal string with AT MOST 4
	// fractional digits (e.g. "32.1500"). Compose parses it into RateScaled
	// (Rate × 10^4, exact — no float ever touches this number); more than 4
	// decimal digits, a non-numeric value, or a value ≤ 0 is refused.
	Rate string `json:"rate"`
	// RateDate is the day the rate was quoted for. Compose requires it
	// strictly before StatementDate and not a Saturday or Sunday.
	//
	// 🔴 This package cannot know Taiwan public holidays. The weekday check
	// catches a weekend; it does not catch a holiday the bank was closed on.
	// The operator must confirm RateDate against the bank's own published
	// table before calling Compose.
	RateDate time.Time `json:"rate_date"`
	// Source cites where the rate was read, e.g. "Bank of Taiwan, spot
	// selling, published table, 2026-12-31".
	Source string `json:"source"`

	// RateScaled is Rate parsed as Rate × 10^4, an exact int64. Ignored on
	// Input; set by Compose on the echoed output FX.
	RateScaled int64 `json:"rate_scaled,omitempty"`
}

// DueRuleKind selects how DueDate computes a due date.
type DueRuleKind string

const (
	// DueRuleDayOfMonth is the contract due date for the first distributor:
	// the 15th of the statement's month when the statement date is on or
	// before the 15th, else the 15th of the next month.
	DueRuleDayOfMonth DueRuleKind = "day_of_month"
	// DueRuleAfterDays is the public-Terms due date for everyone else: N
	// days after the statement date.
	DueRuleAfterDays DueRuleKind = "after_days"
)

// DueRule picks the due-date rule and its one parameter.
type DueRule struct {
	Kind DueRuleKind `json:"kind"`
	// DayOfMonth is used when Kind == DueRuleDayOfMonth (the contract value
	// is 15).
	DayOfMonth int `json:"day_of_month,omitempty"`
	// AfterDays is used when Kind == DueRuleAfterDays (the public-Terms
	// value is 30).
	AfterDays int `json:"after_days,omitempty"`
}

// NewDueDayOfMonth builds the contract due-date rule (the first
// distributor's 15th-of-the-month).
func NewDueDayOfMonth(day int) DueRule {
	return DueRule{Kind: DueRuleDayOfMonth, DayOfMonth: day}
}

// NewDueAfterDays builds the public-Terms due-date rule (N days after the
// statement date).
func NewDueAfterDays(days int) DueRule {
	return DueRule{Kind: DueRuleAfterDays, AfterDays: days}
}

// DefaultTaxRateBps is D502's 5% business tax, in basis points. Input.TaxRateBps
// of 0 means "use this default" — this package has no caller that wants a
// genuine 0% statement tax line today.
const DefaultTaxRateBps = 500

// MaxUSDSubtotalMicros is the sanity ceiling Compose refuses above: a USD
// subtotal over US$1,000,000. It is checked in USD, before conversion, so it
// does not drift with whatever FX rate the operator typed.
const MaxUSDSubtotalMicros = 1_000_000 * 1_000_000

// Input is everything Compose needs to build one statement. UsageWindowFrom
// and UsageWindowTo are recorded on the Statement verbatim — Compose does not
// interpret or validate them against StatementDate, because the engine's
// anchored billing-period window and the plan's "last day 24:00 Asia/Taipei"
// wording do not always coincide (see cmd/statement-lines's package doc).
type Input struct {
	StatementDate   time.Time `json:"statement_date"`
	UsageWindowFrom time.Time `json:"usage_window_from"`
	UsageWindowTo   time.Time `json:"usage_window_to"`
	Lines           []Line    `json:"lines"`
	FX              FX        `json:"fx"`
	// TaxRateBps is the business-tax rate in basis points; 0 means
	// DefaultTaxRateBps.
	TaxRateBps int     `json:"tax_rate_bps,omitempty"`
	DueRule    DueRule `json:"due_rule"`
}

// Statement is what Compose produces: the lines it was given, the USD
// subtotal they sum to, the converted TWD subtotal, the tax, the total, the
// due date, and the FX quote actually applied (with RateScaled filled in).
type Statement struct {
	StatementDate   time.Time `json:"statement_date"`
	UsageWindowFrom time.Time `json:"usage_window_from"`
	UsageWindowTo   time.Time `json:"usage_window_to"`
	Lines           []Line    `json:"lines"`

	SubtotalUSDMicros int64 `json:"subtotal_usd_micros"`
	// SubtotalTWD, TaxTWD and TotalTWD are whole New Taiwan dollars.
	SubtotalTWD int64 `json:"subtotal_twd"`
	TaxTWD      int64 `json:"tax_twd"`
	TotalTWD    int64 `json:"total_twd"`

	DueDate time.Time `json:"due_date"`
	FX      FX        `json:"fx"`
}
