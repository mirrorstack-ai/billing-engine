package einvoice

import "time"

// Mode selects stage or production. It never arrives on a request: a
// Provider picks its host from Mode, and D524 requires that a sandbox
// request can never reach a live host.
type Mode string

const (
	ModeSandbox Mode = "sandbox"
	ModeLive    Mode = "live"
)

// DocKind is B2B or B2C, per D547: the invoice goes from a seller to
// that seller's customer, and which of the two governs which buyer
// fields are required and which rounding tolerance applies (item 6 of
// mirrorstack-ai/billing-engine#286).
type DocKind string

const (
	KindB2B DocKind = "b2b"
	KindB2C DocKind = "b2c"
)

// CarrierKind names a B2C e-invoice delivery carrier. The engine never
// invents a third kind: an invoice is either carried, loved (donated),
// or printed, and printed is the zero value (no carrier, no love code).
type CarrierKind string

const (
	// CarrierMobile is the MOF mobile barcode carrier, "/" plus seven
	// characters from [0-9A-Z.+-].
	CarrierMobile CarrierKind = "mobile_barcode"
	// CarrierCitizenDigital is the MOF natural-person certificate carrier.
	CarrierCitizenDigital CarrierKind = "citizen_digital"
	// CarrierNone means no carrier: printed, or donated via LoveCode.
	CarrierNone CarrierKind = ""
)

// Buyer is the normalized counterparty snapshot the port needs to
// issue. It is deliberately smaller than the buyer invoice profile
// mirrorstack-ai/billing-engine#285 owns: that issue's record is the
// source, this is the projection an adapter signs.
//
// No field here is a secret. A seller's vendor credential never travels
// in a Document — see RegisterCounterparty and the broker split in
// mirrorstack-ai/billing-engine#286's fix direction item 3.
type Buyer struct {
	Name string
	// TaxID is the buyer's 統編 for a B2B document. Empty for B2C.
	TaxID string
	// Carrier and LoveCode are mutually exclusive B2C delivery choices.
	// Both empty means printed.
	Carrier     CarrierKind
	CarrierCode string
	LoveCode    string
	Email       string
}

// Line is one normalized invoice line. Amounts are integer NT$ — Taiwan
// e-invoices carry no subunit — and all three are carried so an adapter
// can refuse rather than silently re-deriving one from the other two
// (item 6: "the adapter computes what its vendor will compute and
// refuses to issue when that differs from the engine's net, tax and
// gross").
type Line struct {
	Description string
	NetNTD      int64
	TaxNTD      int64
	GrossNTD    int64
}

// RoundHalfUpNTD applies the engine's own tax rule — net times 500 bps,
// rounded half-up — to one net amount.
//
// internal/account/usage/tax.go owns the live tax-estimate rule this
// mirrors (twVATRateBps); mirrorstack-ai/billing-engine#293 is expected
// to export a shared NT$ rounding helper, and when it lands this
// function should be replaced by a call to it rather than kept beside
// it, per item 6's equality-test requirement.
func RoundHalfUpNTD(netNTD int64) int64 {
	const (
		rateBps  = 500
		denomBps = 10_000
	)
	neg := netNTD < 0
	if neg {
		netNTD = -netNTD
	}
	numer := netNTD*rateBps + denomBps/2
	tax := numer / denomBps
	if neg {
		return -tax
	}
	return tax
}

// ProviderOrderNumber is the port-level idempotency key: at most 20
// characters of [A-Za-z0-9_], the tightest known vendor limit (ezPay's
// MerchantOrderNo). The deterministic generator that produces one lives
// in mirrorstack-ai/billing-engine#287; this package only validates the
// shape and uses it to key idempotency.
type ProviderOrderNumber string

// Document is what a caller hands a Provider to issue. It carries no
// seller credential and no free amount: amounts are the sealed lines
// the caller computed, and the adapter's job is to refuse rather than
// recompute them from something looser.
type Document struct {
	// SellerRef identifies the seller whose 統編 and 字軌 this invoice
	// is issued under (D533). The engine resolves it; a caller never
	// supplies which seller, only which seller's sale this is.
	SellerRef string
	Buyer     Buyer
	Lines     []Line
	Kind      DocKind
	// OrderNumber is this document's idempotency key at the provider.
	OrderNumber ProviderOrderNumber
	Mode        Mode
}

// TotalNTD sums the document's lines.
func (d Document) TotalNTD() (net, tax, gross int64) {
	for _, l := range d.Lines {
		net += l.NetNTD
		tax += l.TaxNTD
		gross += l.GrossNTD
	}
	return net, tax, gross
}

// Status is what a Provider reports an invoice to be.
type Status string

const (
	// StatusIssued means the vendor accepted and numbered the invoice.
	StatusIssued Status = "issued"
	// StatusPending means the vendor accepted the request but has not
	// numbered it yet — ECPay issues B2B the following day. InvoiceNumber
	// is empty on a pending Result; a caller rereads with Query.
	StatusPending Status = "pending"
	// StatusFailed means the vendor refused the request outright.
	StatusFailed Status = "failed"
	// StatusVoided means a previously issued invoice was voided.
	StatusVoided Status = "voided"
)

// Result is what a Provider hands back for an issue, void, allowance or
// query call.
type Result struct {
	Status        Status
	InvoiceNumber string
	// RandomNumber is the MOF four-digit check number printed beside the
	// invoice number.
	RandomNumber string
	Date         time.Time
	// UploadStatus is the vendor's report of submission to the MOF
	// platform, when it reports one separately from Status.
	UploadStatus string
	OrderNumber  ProviderOrderNumber
}

// Ambiguous is its own typed outcome, mirroring
// executor.CollectResult.Ambiguous (internal/intent/executor/executor.go:90-110):
// a timeout or a lost answer never establishes whether the vendor
// issued, so it is never a failure and never an invitation to reissue.
// A caller that gets Ambiguous must re-read with Query, keyed on the
// same OrderNumber, not retry Issue.
type Ambiguous struct {
	OrderNumber ProviderOrderNumber
	// Reason is a short, non-secret description of what was lost — a
	// timeout, a malformed answer — never vendor response bytes.
	Reason string
}

func (a Ambiguous) Error() string {
	return "einvoice: ambiguous answer for order " + string(a.OrderNumber) + ": " + a.Reason
}

// Capabilities tells a worker what one provider supports, so a caller
// does not have to call a method to learn it is unsupported.
type Capabilities struct {
	SupportsAllowance                bool
	SupportsAsyncPending             bool
	SupportsCounterpartyRegistration bool
	SupportsLoveCode                 bool
	SupportsCarrier                  bool
}
