package invoice

import "testing"

// Test fixtures are derived directly from the documented MOF checksum
// algorithm (weights [1,2,1,2,1,2,4,1], fold each product's digits, total
// must be a multiple of 10, with the digit-7 exception on the 7th digit) —
// see taxid.go for the computation this test is checking a Go
// implementation of.
func TestValidateTaxID(t *testing.T) {
	cases := []struct {
		name string
		id   string
		want bool
	}{
		{"valid, no exception needed (sum=30)", "53212539", true},
		{"same prefix, wrong check digit (sum=21)", "53212530", false},
		{"digit-7 exception applies (sum=19, 7th digit is 7)", "00000079", true},
		{"same shape, exception does not apply (sum=18)", "00000078", false},
		{"too short", "5321253", false},
		{"too long", "532125390", false},
		{"non-digit", "5321253X", false},
		{"empty", "", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := ValidateTaxID(tc.id); got != tc.want {
				t.Errorf("ValidateTaxID(%q) = %v, want %v", tc.id, got, tc.want)
			}
		})
	}
}
