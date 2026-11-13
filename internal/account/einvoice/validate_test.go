package einvoice

import "testing"

func TestTaiwanTaxIDValid(t *testing.T) {
	cases := []struct {
		name  string
		taxID string
		want  bool
	}{
		{"valid canonical example", "04595257", true},
		{"invalid checksum", "12345678", false},
		{"too short", "1234567", false},
		{"too long", "123456789", false},
		{"non-numeric", "abcdefgh", false},
		{"empty", "", false},
		{"off by one from the valid example", "04595258", false},
		{"seventh-digit-is-7 exception path", "00000079", true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := TaiwanTaxIDValid(c.taxID); got != c.want {
				t.Errorf("TaiwanTaxIDValid(%q) = %v, want %v", c.taxID, got, c.want)
			}
		})
	}
}

func TestCarrierNumValid(t *testing.T) {
	cases := []struct {
		name        string
		carrierType CarrierType
		num         string
		want        bool
	}{
		{"good mobile barcode", CarrierMobileBarcode, "/ABC123.", true},
		{"good mobile barcode with plus and dash", CarrierMobileBarcode, "/A+B-C.1", true},
		{"mobile barcode missing slash", CarrierMobileBarcode, "ABC123.4", false},
		{"mobile barcode wrong length", CarrierMobileBarcode, "/ABC12", false},
		{"mobile barcode lowercase refused", CarrierMobileBarcode, "/abc123.", false},
		{"citizen cert non-empty accepted", CarrierCitizenCert, "AB12345678901234", true},
		{"citizen cert empty refused", CarrierCitizenCert, "", false},
		{"citizen cert whitespace-only refused", CarrierCitizenCert, "   ", false},
		{"vendor member non-empty accepted", CarrierVendorMember, "member-123", true},
		{"vendor member empty refused", CarrierVendorMember, "", false},
		{"unknown carrier type refused", CarrierType("unknown"), "anything", false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := CarrierNumValid(c.carrierType, c.num); got != c.want {
				t.Errorf("CarrierNumValid(%q, %q) = %v, want %v", c.carrierType, c.num, got, c.want)
			}
		})
	}
}

func TestDonationCodeValid(t *testing.T) {
	cases := []struct {
		name string
		code string
		want bool
	}{
		{"3 digits", "123", true},
		{"7 digits", "1234567", true},
		{"2 digits too short", "12", false},
		{"8 digits too long", "12345678", false},
		{"non-numeric", "12a", false},
		{"empty", "", false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := DonationCodeValid(c.code); got != c.want {
				t.Errorf("DonationCodeValid(%q) = %v, want %v", c.code, got, c.want)
			}
		})
	}
}

func TestValidate(t *testing.T) {
	cases := []struct {
		name    string
		profile Profile
		wantErr bool
	}{
		{
			name:    "valid company",
			profile: Profile{Kind: KindCompany, TaxID: "04595257", CompanyName: "Acme Co"},
			wantErr: false,
		},
		{
			name:    "company missing tax id",
			profile: Profile{Kind: KindCompany, CompanyName: "Acme Co"},
			wantErr: true,
		},
		{
			name:    "company invalid tax id checksum",
			profile: Profile{Kind: KindCompany, TaxID: "12345678", CompanyName: "Acme Co"},
			wantErr: true,
		},
		{
			name:    "company missing name",
			profile: Profile{Kind: KindCompany, TaxID: "04595257"},
			wantErr: true,
		},
		{
			name:    "company with carrier refused",
			profile: Profile{Kind: KindCompany, TaxID: "04595257", CompanyName: "Acme Co", CarrierType: CarrierMobileBarcode, CarrierNum: "/ABC123."},
			wantErr: true,
		},
		{
			name:    "company with donation code refused",
			profile: Profile{Kind: KindCompany, TaxID: "04595257", CompanyName: "Acme Co", DonationCode: "123"},
			wantErr: true,
		},
		{
			name:    "valid personal, no carrier",
			profile: Profile{Kind: KindPersonal},
			wantErr: false,
		},
		{
			name:    "personal with tax id refused",
			profile: Profile{Kind: KindPersonal, TaxID: "04595257"},
			wantErr: true,
		},
		{
			name:    "personal with valid mobile barcode carrier",
			profile: Profile{Kind: KindPersonal, CarrierType: CarrierMobileBarcode, CarrierNum: "/ABC123."},
			wantErr: false,
		},
		{
			name:    "personal with invalid carrier shape",
			profile: Profile{Kind: KindPersonal, CarrierType: CarrierMobileBarcode, CarrierNum: "nope"},
			wantErr: true,
		},
		{
			name:    "personal with valid donation code",
			profile: Profile{Kind: KindPersonal, DonationCode: "123"},
			wantErr: false,
		},
		{
			name:    "personal with invalid donation code shape",
			profile: Profile{Kind: KindPersonal, DonationCode: "1"},
			wantErr: true,
		},
		{
			name:    "carrier and donation code mutually exclusive",
			profile: Profile{Kind: KindPersonal, CarrierType: CarrierMobileBarcode, CarrierNum: "/ABC123.", DonationCode: "123"},
			wantErr: true,
		},
		{
			name:    "carrier_num without carrier_type refused",
			profile: Profile{Kind: KindPersonal, CarrierNum: "/ABC123."},
			wantErr: true,
		},
		{
			name:    "unknown kind refused",
			profile: Profile{Kind: Kind("business")},
			wantErr: true,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			err := Validate(c.profile)
			if c.wantErr && err == nil {
				t.Errorf("Validate(%+v) = nil, want error", c.profile)
			}
			if !c.wantErr && err != nil {
				t.Errorf("Validate(%+v) = %v, want nil", c.profile, err)
			}
		})
	}
}
