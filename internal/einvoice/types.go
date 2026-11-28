package einvoice

import "time"

// Mode selects stage or production. It never arrives on a request: a
// Provider picks its target from Mode, and D524 requires that a sandbox
// request can never reach a live host (a sandbox payment gets a test
// invoice from the fake only).
type Mode string

const (
	ModeSandbox Mode = "sandbox"
	ModeLive    Mode = "live"
)

// DocKind is B2B or B2C, per D547: the invoice goes from a seller to
// that seller's customer, and which of the two governs which buyer
// fields are required and which upload deadline applies.
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
// source, this is the projection an adapter writes into the invoice.
//
// No field here is a secret. A seller's Turnkey sender credential never
// travels in a Document (mirrorstack-ai/billing-engine#312 owns it).
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
// can refuse rather than silently re-deriving one from the other two:
// a document whose net, tax and gross disagree with the engine's rule is
// refused with ErrAmountMismatch, never corrected.
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
// function should be replaced by a call to it rather than kept beside it.
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

// ProviderOrderNumber is the port-level idempotency key. It is ours, not
// the invoice number: a self-numbering provider assigns the invoice
// number from the seller's 字軌 ledger, and this key only makes a second
// Issue of the same sale return the first invoice. Its length limit is a
// per-provider capability (Capabilities.MaxOrderNumberLen, checked by
// Capabilities.ValidateOrderNumber). The deterministic generator that
// produces one lives in mirrorstack-ai/billing-engine#287.
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

// Status is what a Provider reports an invoice to be. It answers "does
// this sale have an invoice number", not "does MOF have the invoice yet"
// — the second question is Transmission.
type Status string

const (
	// StatusIssued means the invoice has its number. For a self-numbering
	// provider that is true the moment Issue returns (D550: issue
	// immediately), even while the MOF upload is still in flight; read
	// Result.Transmission for that.
	StatusIssued Status = "issued"
	// StatusPending means the provider accepted the request but has not
	// numbered it yet. Only a provider that does NOT self-number may
	// return it (Capabilities.SupportsAsyncPending); a self-numbering
	// provider never does. InvoiceNumber is empty on a pending Result; a
	// caller rereads with Query.
	StatusPending Status = "pending"
	// StatusFailed means the provider refused the request outright, and
	// no number was consumed for it.
	StatusFailed Status = "failed"
	// StatusVoided means a previously issued invoice was voided.
	StatusVoided Status = "voided"
)

// TransmissionState is where an issued invoice's upload to the MOF
// platform stands. Under D627 the number is ours at once and the upload
// is asynchronous (written to an outbox, picked up by the Turnkey host,
// signed and sent, then answered in Turnkey's result files), so it is a
// separate field from Status rather than a reason to report "pending".
//
// The zero value means the provider does not report a transmission
// state (the fake in vendor mode, a future vendor adapter).
type TransmissionState string

const (
	// TransmissionQueued: the invoice file is written for the Turnkey host
	// but not yet handed to MOF.
	TransmissionQueued TransmissionState = "queued"
	// TransmissionSent: Turnkey has sent the file; MOF has not answered.
	TransmissionSent TransmissionState = "sent"
	// TransmissionAccepted: MOF accepted the invoice. Terminal.
	TransmissionAccepted TransmissionState = "accepted"
	// TransmissionRejected: MOF rejected the invoice. Terminal and
	// visible: the issuing worker must act on it (correct and resend
	// under the same number, void, alarm), and it is never dropped or
	// turned into a second invoice.
	TransmissionRejected TransmissionState = "rejected"
)

// transmissionRank orders the states. Accepted and rejected share the
// last rank: both are answers, and neither follows the other.
var transmissionRank = map[TransmissionState]int{
	TransmissionQueued:   1,
	TransmissionSent:     2,
	TransmissionAccepted: 3,
	TransmissionRejected: 3,
}

// Valid reports whether s is one of the four states. The zero value is
// not Valid: "not reported" is not a state a transition can reach.
func (s TransmissionState) Valid() bool {
	_, ok := transmissionRank[s]
	return ok
}

// Terminal reports whether MOF has answered (accepted or rejected).
func (s TransmissionState) Terminal() bool {
	return transmissionRank[s] == 3
}

// Advances reports whether moving from s to next is a forward move. A
// poll may skip a state (queued straight to accepted), and a reread of
// the same state is not an advance. Nothing moves out of a terminal
// state, so a late or duplicated "sent" message can never overwrite an
// answer; a resend of a rejected invoice is a new transmission of the
// same number, which the Turnkey adapter (mirrorstack-ai/billing-engine#341)
// records, not a transition of this one.
func (s TransmissionState) Advances(next TransmissionState) bool {
	if !next.Valid() || s.Terminal() {
		return false
	}
	return transmissionRank[next] > transmissionRank[s]
}

// Result is what a Provider hands back for an issue, void, allowance or
// query call.
type Result struct {
	Status        Status
	InvoiceNumber string
	// RandomNumber is the MOF four-digit check number printed beside the
	// invoice number.
	RandomNumber string
	// Date is the invoice date. The MOF upload clock starts here, not at
	// queue time — see UploadDeadline.
	Date time.Time
	// Transmission is the MOF upload state, empty when the provider does
	// not report one (Capabilities.SelfNumbering is false).
	Transmission TransmissionState
	OrderNumber  ProviderOrderNumber
}

// Ambiguous is its own typed outcome, mirroring
// executor.CollectResult.Ambiguous (internal/intent/executor/executor.go:90-110):
// a timeout or a lost answer never establishes whether the provider
// issued, so it is never a failure and never an invitation to reissue.
// A caller that gets Ambiguous must re-read with Query, keyed on the
// same OrderNumber, not retry Issue under a new one.
type Ambiguous struct {
	OrderNumber ProviderOrderNumber
	// Reason is a short, non-secret description of what was lost — a
	// timeout, a malformed answer — never response bytes.
	Reason string
}

func (a Ambiguous) Error() string {
	return "einvoice: ambiguous answer for order " + string(a.OrderNumber) + ": " + a.Reason
}

// Capabilities tells a worker what one provider supports, so a caller
// does not have to call a method to learn it is unsupported.
type Capabilities struct {
	// SelfNumbering: the provider assigns the invoice number from our own
	// per-seller 字軌 ledger (mirrorstack-ai/billing-engine#338) inside
	// Issue. Issue then returns StatusIssued with a non-empty
	// InvoiceNumber and a Transmission state at once, and LookupTrack
	// answers from that ledger rather than from a remote call. The
	// Turnkey provider (D627) and the fake in self-numbering mode set it.
	SelfNumbering bool
	// SupportsAsyncPending: Issue may return StatusPending (the number
	// comes later). Mutually exclusive with SelfNumbering.
	SupportsAsyncPending             bool
	SupportsAllowance                bool
	SupportsCounterpartyRegistration bool
	SupportsLoveCode                 bool
	SupportsCarrier                  bool
	// MaxOrderNumberLen is the longest ProviderOrderNumber this provider
	// accepts. Zero means DefaultMaxOrderNumberLen.
	MaxOrderNumberLen int
}

// Validate rejects a contradictory capability set. Registry.Register
// calls it, so a provider that claims both to number at once and to
// number later never reaches a caller.
func (c Capabilities) Validate() error {
	if c.SelfNumbering && c.SupportsAsyncPending {
		return errSelfNumberingPending
	}
	if c.MaxOrderNumberLen != 0 &&
		(c.MaxOrderNumberLen < DefaultMaxOrderNumberLen || c.MaxOrderNumberLen > MaxOrderNumberLenCeiling) {
		return errOrderNumberLenRange
	}
	return nil
}
