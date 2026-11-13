package invoice

// ValidateTaxID checks an 8-digit 統一編號 (Taiwan unified business number)
// against the standard MOF checksum: weight each digit, fold a two-digit
// product to the sum of its digits, and the total must be a multiple of 10 —
// except when the 7th digit is '7', which also accepts a total one short of
// a multiple of 10 (a documented exception for IDs whose 7th digit is 7).
//
// This is a pure format/checksum check. It says nothing about whether the
// number is actually registered with Taiwan's tax authority — only that it
// is well-formed enough to store as "registered".
func ValidateTaxID(id string) bool {
	if len(id) != 8 {
		return false
	}
	digits := make([]int, 8)
	for i := 0; i < 8; i++ {
		c := id[i]
		if c < '0' || c > '9' {
			return false
		}
		digits[i] = int(c - '0')
	}

	weights := [8]int{1, 2, 1, 2, 1, 2, 4, 1}
	sum := 0
	for i, w := range weights {
		product := digits[i] * w
		sum += product/10 + product%10
	}
	if sum%10 == 0 {
		return true
	}
	// The documented exception: a 7th digit of 7 also accepts a total that
	// is one short of a multiple of 10.
	return digits[6] == 7 && (sum+1)%10 == 0
}
