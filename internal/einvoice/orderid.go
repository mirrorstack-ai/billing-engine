package einvoice

import (
	"fmt"
	"regexp"
)

// orderNumberPattern is item 8 of mirrorstack-ai/billing-engine#286: at
// most 20 characters of [A-Za-z0-9_], the tightest known vendor limit
// (ezPay's MerchantOrderNo). A port-level document that violates a
// looser vendor's own limit (Amego 40, ECPay 50) still validates here,
// because the port picks the tightest constraint so a document built
// once can move between vendors without being rebuilt narrower.
var orderNumberPattern = regexp.MustCompile(`^[A-Za-z0-9_]{1,20}$`)

// ErrInvalidOrderNumber is returned by ValidateOrderNumber.
var ErrInvalidOrderNumber = fmt.Errorf("einvoice: order number must be 1-20 characters of [A-Za-z0-9_]")

// ValidateOrderNumber checks the port-level shape. It does not check
// uniqueness — that is a Provider's Issue, keyed on the same value.
func ValidateOrderNumber(n ProviderOrderNumber) error {
	if !orderNumberPattern.MatchString(string(n)) {
		return ErrInvalidOrderNumber
	}
	return nil
}
