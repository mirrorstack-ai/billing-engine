package tappaygateway

// Mode selects which TapPay host a Client talks to.
type Mode int

const (
	// ModeSandbox is the only mode NewClient accepts today.
	ModeSandbox Mode = iota
	// ModeLive is accepted by the type but refused by NewClient until
	// billing-engine#318 lands. See doc.go.
	ModeLive
)

func (m Mode) baseURL() (string, bool) {
	switch m {
	case ModeSandbox:
		return sandboxBaseURL, true
	case ModeLive:
		return liveBaseURL, true
	default:
		return "", false
	}
}

func (m Mode) String() string {
	switch m {
	case ModeSandbox:
		return "sandbox"
	case ModeLive:
		return "live"
	default:
		return "unknown"
	}
}

// Cardholder is the minimum cardholder detail TapPay's Pay by Prime
// requires. It is sent on every request; none of it is card data.
type Cardholder struct {
	PhoneNumber string
	Name        string
	Email       string
	ZipCode     string
	Address     string
	NationalID  string
}

// ResultURL carries the redirect targets TapPay requires for e-payment
// and Direct Pay 3D Secure.
type ResultURL struct {
	FrontendRedirectURL string
	BackendNotifyURL    string
	GoBackURL           string
}

// PayByPrimeParams is one Pay by Prime attempt.
//
// BankTransactionID is set by the caller on every attempt (never left
// empty): TapPay's docs say a trade with no bank_transaction_id cannot
// be looked up after a 421 gateway timeout, so Record read-back would
// have nothing to search on. OrderNumber is MirrorStack's own payment
// id, never empty when sent, per TapPay's own constraint.
type PayByPrimeParams struct {
	Prime             string
	MerchantID        string
	AmountTWD         int64
	Cardholder        Cardholder
	OrderNumber       string
	BankTransactionID string
	// Details is the item name TapPay's PCI requirement needs described
	// on every charge; it must be non-empty.
	Details string
	// ResultURL is required whenever ThreeDomainSecure is true, or the
	// charge is an e-payment instrument.
	ResultURL         *ResultURL
	ThreeDomainSecure bool
	// Remember is intentionally not exposed: Client.PayByPrime always
	// sends false. See (*Client).PayByPrime.
}

// PayByPrimeResult is TapPay's Pay by Prime response, reduced to the
// fields this adapter needs.
//
// A non-empty PaymentURL means TapPay is asking for a 3D Secure
// redirect; Status is 0 (TapPay's success code) in that case too, so
// callers must check PaymentURL before treating Status==0 as a
// finished charge. Nothing here is settlement-grade on its own — see
// doc.go and billing-engine#310 item 6: only a later QueryRecord
// read-back is.
type PayByPrimeResult struct {
	Status            int
	Msg               string
	RecTradeID        string
	BankTransactionID string
	OrderNumber       string
	AmountTWD         int64
	// PaymentURL is non-empty when the bank requires a 3D Secure
	// redirect before the charge can complete.
	PaymentURL string
}

// QueryRecordParams asks TapPay for the current state of trades
// matching a filter. The engine uses it keyed to one BankTransactionID
// or RecTradeID, never a bare time-range scan, so a single read-back
// names the one trade it is settling.
type QueryRecordParams struct {
	RecTradeIDs        []string
	BankTransactionIDs []string
	OrderNumber        string
	// Records bounds the page size; TapPay's own ceiling is 200.
	Records int
}

// QueryRecordResult is one matched trade from Record.
//
// Status 0 found the trade; Status 2 means Record itself succeeded but
// found nothing (TapPay's "no more records", not an error) — callers
// must branch on this before treating an empty TradeRecords as a
// problem.
type QueryRecordResult struct {
	Status       int
	Msg          string
	TradeRecords []TradeRecord
}

// TradeRecord is one trade as TapPay's Record reports it.
type TradeRecord struct {
	RecTradeID        string
	BankTransactionID string
	OrderNumber       string
	AmountTWD         int64
	// RecordStatus is TapPay's per-trade status (e.g. captured,
	// refunded), distinct from the envelope Status above.
	RecordStatus int
}

// RefundParams asks TapPay to refund a trade. Amount is optional: a
// nil Amount is a full refund, a non-nil Amount is partial. A refund
// cancels both the authorization and the capture, per TapPay's docs.
type RefundParams struct {
	RecTradeID string
	AmountTWD  *int64
}

// RefundResult is TapPay's refund response.
type RefundResult struct {
	Status       int
	Msg          string
	RecTradeID   string
	RefundAmount int64
}
