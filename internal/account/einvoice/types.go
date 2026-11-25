package einvoice

import "github.com/google/uuid"

// Kind is who the invoice document names — never the seller's tax-bureau
// document kind (tax_invoice | receipt | none, billing-engine#312), which is
// a seller-level fact this package does not hold.
type Kind string

const (
	KindPersonal Kind = "personal"
	KindCompany  Kind = "company"
)

// CarrierType is a vendor-neutral enum; each invoice-provider adapter (#286)
// maps it to its own wire codes.
type CarrierType string

const (
	CarrierMobileBarcode CarrierType = "mobile_barcode"
	CarrierCitizenCert   CarrierType = "citizen_cert"
	CarrierVendorMember  CarrierType = "vendor_member"
)

// Profile is the buyer invoice profile for one billing account (migration
// 093). Default when none exists: Kind "" reads back as KindPersonal with no
// carrier, no donation code, and Email as known from the account — what each
// vendor needs for that default case lives in the adapter, not here.
type Profile struct {
	AccountID    uuid.UUID
	Kind         Kind
	TaxID        string // company only (統編)
	CompanyName  string // company only
	Address      string // optional
	Email        string
	CarrierType  CarrierType // "" = none
	CarrierNum   string
	DonationCode string // 愛心碼; mutually exclusive with CarrierType
}

// GetProfileRequest selects a billing account's buyer invoice profile.
type GetProfileRequest struct {
	AccountID uuid.UUID `json:"account_id"`
}

// GetProfileResponse echoes the stored profile, or the documented default
// (Exists=false, Kind=KindPersonal, every other field empty) when the
// account has never set one.
type GetProfileResponse struct {
	Exists  bool    `json:"exists"`
	Profile Profile `json:"profile"`
}

// PutProfileRequest replaces a billing account's buyer invoice profile in
// full (no partial update — see PutBuyerInvoiceProfile in
// internal/account/db/queries/buyer_invoice_profiles.sql).
type PutProfileRequest struct {
	AccountID    uuid.UUID   `json:"account_id"`
	Kind         Kind        `json:"kind"`
	TaxID        string      `json:"tax_id,omitempty"`
	CompanyName  string      `json:"company_name,omitempty"`
	Address      string      `json:"address,omitempty"`
	Email        string      `json:"email,omitempty"`
	CarrierType  CarrierType `json:"carrier_type,omitempty"`
	CarrierNum   string      `json:"carrier_num,omitempty"`
	DonationCode string      `json:"donation_code,omitempty"`
}

// PutProfileResponse echoes the persisted profile.
type PutProfileResponse struct {
	Profile Profile `json:"profile"`
}
