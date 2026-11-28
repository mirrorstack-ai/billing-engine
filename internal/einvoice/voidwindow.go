package einvoice

import "time"

// DefaultVoidWindow is item 7 of mirrorstack-ai/billing-engine#286,
// D526's assumption pending vendor verification: void is allowed until
// 23:59:59 on the 13th of the odd-numbered month after the invoice's
// two-month period. That is ECPay's documented rule
// (developers.ecpay.com.tw/7906/); other vendor pages say the 14th or
// 15th, so the 13th is the earliest and the conservative default.
//
// Taiwan e-invoices are issued in bimonthly periods: Jan-Feb, Mar-Apr,
// May-Jun, Jul-Aug, Sep-Oct, Nov-Dec. Each period's own start month is
// odd, so "the odd month after the period" is always the period's start
// month plus two, wrapping into January of the following year for the
// Nov-Dec period.
//
// A real adapter's VoidWindow should call this and change only this one
// constant if e首發票's Swagger or stage documents a different cutoff —
// item 7 says so explicitly: "verify against the e首發票 Swagger or
// stage and change the one constant if wrong."
func DefaultVoidWindow(issuedAt time.Time) time.Time {
	periodStartMonth := issuedAt.Month()
	// Round down to the odd month starting this invoice's period: 1,2->1;
	// 3,4->3; 5,6->5; 7,8->7; 9,10->9; 11,12->11.
	if periodStartMonth%2 == 0 {
		periodStartMonth--
	}
	deadlineMonth := periodStartMonth + 2
	year := issuedAt.Year()
	if deadlineMonth > 12 {
		deadlineMonth -= 12
		year++
	}
	return time.Date(year, deadlineMonth, 13, 23, 59, 59, 0, issuedAt.Location())
}
