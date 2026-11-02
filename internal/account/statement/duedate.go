package statement

import (
	"fmt"
	"time"
)

// DueDate computes a statement's due date from its statement date and a rule.
//
//   - DueRuleDayOfMonth: the rule's DayOfMonth in the statement's own month
//     when the statement date falls on or before that day, else DayOfMonth in
//     the NEXT month. This is the first distributor's contract due date (the
//     15th): a statement dated the 1st through the 15th is due the 15th of
//     that same month; a statement dated after the 15th is due the 15th of
//     the month after.
//   - DueRuleAfterDays: the statement date plus AfterDays calendar days. This
//     is the public-Terms due date for everyone else (30 days).
//
// Both rules only add whole days or reuse a fixed day-of-month (15), so
// neither needs the anchor-day clamping internal/billingperiod uses for
// 29/30/31-anchored windows.
func DueDate(statementDate time.Time, rule DueRule) (time.Time, error) {
	statementDate = statementDate.UTC()
	switch rule.Kind {
	case DueRuleAfterDays:
		if rule.AfterDays <= 0 {
			return time.Time{}, fmt.Errorf("after_days must be positive, got %d", rule.AfterDays)
		}
		return statementDate.AddDate(0, 0, rule.AfterDays), nil
	case DueRuleDayOfMonth:
		if rule.DayOfMonth <= 0 || rule.DayOfMonth > 28 {
			return time.Time{}, fmt.Errorf("day_of_month must be in 1..28, got %d", rule.DayOfMonth)
		}
		y, m, d := statementDate.Date()
		if d <= rule.DayOfMonth {
			return time.Date(y, m, rule.DayOfMonth, 0, 0, 0, 0, time.UTC), nil
		}
		// time.Date normalizes a 13th month into January of the next year, so
		// this needs no manual month-rollover arithmetic.
		return time.Date(y, m+1, rule.DayOfMonth, 0, 0, 0, 0, time.UTC), nil
	default:
		return time.Time{}, fmt.Errorf("unknown due rule kind %q", rule.Kind)
	}
}
