package einvoice

import "time"

// Taipei is the zone every invoice calendar rule is read in. Taiwan has
// kept UTC+8 without daylight saving since 1979, so a fixed zone is
// exact and does not depend on the tzdata the Lambda image carries.
var Taipei = time.FixedZone("Asia/Taipei", 8*60*60)

// PeriodStart returns 00:00 Asia/Taipei on the first day of the
// two-month invoice period t falls in. Periods are Jan-Feb, Mar-Apr,
// May-Jun, Jul-Aug, Sep-Oct and Nov-Dec, so each starts in an odd month.
// The 字軌 ledger (mirrorstack-ai/billing-engine#338) keys its ranges on
// this value.
func PeriodStart(t time.Time) time.Time {
	local := t.In(Taipei)
	month := local.Month()
	if month%2 == 0 {
		month--
	}
	return time.Date(local.Year(), month, 1, 0, 0, 0, 0, Taipei)
}

// DefaultVoidWindow is our own void rule, one constant for every
// provider: void is allowed until 23:59:59 Asia/Taipei on the 13th of
// the odd-numbered month after the invoice's two-month period; after it
// only an allowance is possible. The Nov-Dec period wraps into January
// of the following year.
//
// UNVERIFIED (assumption D526): the 13th is the earliest cutoff found in
// published practice, taken as the conservative default; the MOF rule
// for a self-issued invoice is not yet confirmed. If it differs, change
// this one function.
func DefaultVoidWindow(issuedAt time.Time) time.Time {
	deadline := PeriodStart(issuedAt).AddDate(0, 2, 12)
	return time.Date(deadline.Year(), deadline.Month(), deadline.Day(), 23, 59, 59, 0, Taipei)
}

// Upload deadlines in days, counted from the day after the invoice date
// (MOF time table 台財稅字第11304654280號, effective 2025-01-01).
const (
	uploadDaysB2B = 7 // the buyer is a business (營業人): a B2B document
	uploadDaysB2C = 2 // the buyer is not a business: a B2C document
)

// UploadDeadline is the last instant the MOF platform must hold an
// invoice dated invoiceDate: 23:59:59 Asia/Taipei on the 7th day after
// the invoice date for B2B, the 2nd day for B2C. The clock starts at the
// invoice date, not at queue time, which is why Result keeps Date. An
// unknown kind gets the shorter B2C deadline, so an alarm fires early
// rather than late.
func UploadDeadline(invoiceDate time.Time, kind DocKind) time.Time {
	days := uploadDaysB2C
	if kind == KindB2B {
		days = uploadDaysB2B
	}
	local := invoiceDate.In(Taipei)
	return time.Date(local.Year(), local.Month(), local.Day()+days, 23, 59, 59, 0, Taipei)
}
