package einvoice

import "regexp"

// mobileBarcodePattern is the MOF mobile-barcode carrier shape: a
// leading "/" followed by seven characters from [0-9A-Z+-.]. This is
// the national e-invoice standard, not a vendor-specific wire detail,
// so it is safe to encode here rather than waiting on any one vendor's
// Swagger.
var mobileBarcodePattern = regexp.MustCompile(`^/[0-9A-Z+\-.]{7}$`)

// loveCodePattern is the MOF love-code (愛心碼) shape: 3 to 7 digits.
var loveCodePattern = regexp.MustCompile(`^[0-9]{3,7}$`)

// ValidateMobileBarcode checks the MOF national format. A provider's
// own ValidateCarrier should call this for CarrierMobile rather than
// reimplementing it, so the one shared rule does not drift per adapter.
func ValidateMobileBarcode(code string) error {
	if !mobileBarcodePattern.MatchString(code) {
		return ErrInvalidCarrier
	}
	return nil
}

// ValidateLoveCode checks the MOF national format.
func ValidateLoveCode(loveCode string) error {
	if !loveCodePattern.MatchString(loveCode) {
		return ErrInvalidLoveCode
	}
	return nil
}
