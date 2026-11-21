package einvoice

import (
	"regexp"
	"strings"

	"github.com/mirrorstack-ai/billing-engine/internal/account/billing"
)

// taxIDShape matches an 8-digit 統編 before the checksum runs.
var taxIDShape = regexp.MustCompile(`^[0-9]{8}$`)

// mobileBarcodeShape: '/' plus 7 characters of 0-9 A-Z + - . (researched
// default from the vendors' public docs and the MOF rules, D526 — see
// docs/adr/0001-taiwan-einvoice-platform-service.md). citizen_cert and
// vendor_member have no verified shape here; CarrierNumValid only checks
// those are non-empty, and the adapter (#286) does the vendor-specific check
// before issuance.
var mobileBarcodeShape = regexp.MustCompile(`^/[0-9A-Z+.-]{7}$`)

// donationCodeShape: 3 to 7 digits (researched default, D526).
var donationCodeShape = regexp.MustCompile(`^[0-9]{3,7}$`)

// taiwanTaxIDWeights is the published 財政部 統一編號 check-digit weight
// table. Mirrors ms_billing.taiwan_tax_id_checksum_valid
// (migrations/billing/093_buyer_invoice_profiles.up.sql) exactly — the two
// MUST agree, because the Go side validates before a write and the SQL side
// is what actually enforces it; a table test for one without the other would
// let them drift apart silently.
var taiwanTaxIDWeights = [8]int{1, 2, 1, 2, 1, 2, 4, 1}

// TaiwanTaxIDValid checks an 8-digit 統編 against the MOF check-digit
// algorithm: each digit times its weight, the two digits of that product
// summed, the eight sums totalled; valid at total%10==0, OR — the documented
// exception — the 7th digit is 7 and (total+1)%10==0.
func TaiwanTaxIDValid(taxID string) bool {
	if !taxIDShape.MatchString(taxID) {
		return false
	}
	total := 0
	for i, w := range taiwanTaxIDWeights {
		digit := int(taxID[i] - '0')
		product := digit * w
		total += product/10 + product%10
	}
	if total%10 == 0 {
		return true
	}
	seventhDigit := int(taxID[6] - '0')
	return seventhDigit == 7 && (total+1)%10 == 0
}

// CarrierNumValid checks a carrier number's shape for the given carrier
// type. Only mobile_barcode has a verified public shape; the other two just
// require a non-empty value here (the adapter does the rest, #286).
func CarrierNumValid(carrierType CarrierType, num string) bool {
	switch carrierType {
	case CarrierMobileBarcode:
		return mobileBarcodeShape.MatchString(num)
	case CarrierCitizenCert, CarrierVendorMember:
		return strings.TrimSpace(num) != ""
	default:
		return false
	}
}

// DonationCodeValid checks a 愛心碼's shape: 3 to 7 digits.
func DonationCodeValid(code string) bool {
	return donationCodeShape.MatchString(code)
}

// Validate applies every rule migration 093's CHECK constraints enforce, as a
// pure function #287's issuance path and the adapters' pre-issue checks (#286)
// can call before a write ever reaches the database. It intentionally mirrors
// the CHECK constraints rather than replacing them: the database refuses the
// same rows even if a future caller skips this function.
func Validate(p Profile) error {
	switch p.Kind {
	case KindCompany:
		if p.TaxID == "" || !TaiwanTaxIDValid(p.TaxID) {
			return billing.InvalidInput("company profile needs a valid 統一編號 (8 digits, MOF checksum)")
		}
		if strings.TrimSpace(p.CompanyName) == "" {
			return billing.InvalidInput("company profile needs a company_name")
		}
		if p.CarrierType != "" || p.DonationCode != "" {
			return billing.InvalidInput("a company profile carries no carrier or donation code")
		}
	case KindPersonal:
		if p.TaxID != "" {
			return billing.InvalidInput("a personal profile carries no 統一編號")
		}
	default:
		return billing.InvalidInput("kind must be personal or company")
	}

	if p.CarrierType != "" && p.DonationCode != "" {
		return billing.InvalidInput("a carrier and a donation code are mutually exclusive")
	}
	if p.CarrierType != "" {
		switch p.CarrierType {
		case CarrierMobileBarcode, CarrierCitizenCert, CarrierVendorMember:
		default:
			return billing.InvalidInput("unknown carrier_type: " + string(p.CarrierType))
		}
		if !CarrierNumValid(p.CarrierType, p.CarrierNum) {
			return billing.InvalidInput("carrier_num does not match carrier_type's shape")
		}
	} else if p.CarrierNum != "" {
		return billing.InvalidInput("carrier_num set without a carrier_type")
	}
	if p.DonationCode != "" && !DonationCodeValid(p.DonationCode) {
		return billing.InvalidInput("donation_code must be 3 to 7 digits")
	}
	return nil
}
