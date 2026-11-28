// Package paymentfact is the one neutral record every payment rail writes.
//
// billing-engine#313 (core-v2#1850, D530): invoicing is a platform service,
// separate from every payment rail. Stripe, NewebPay, TapPay and manual bank
// transfer each write a payment Fact in the SAME transaction as the step that
// makes the money real — the executor's settle step, a rail's notice worker,
// an operator's paid-mark, a finalized statement — and invoice code never
// again needs to know which rail paid.
//
// This package is deliberately independent of internal/intent and
// internal/account: it is imported BY both (the intent executor writes an
// engine_settlement fact; an account-side rail notice worker writes a
// rail_notice fact), and importing either back would be the wrong direction.
//
// # What this package does NOT do
//
// It never calls a vendor and never answers on a public route (D550 — issue
// synchronously, no callback). It does not resolve a seller (#312) or build a
// buyer snapshot (#285); both are carried as opaque, caller-supplied,
// nullable fields until those land. It does not decide an invoice (#287) —
// InvoiceDecision/DecisionReason are set at write time by a writer that
// already knows the answer (Stripe's foreign_seller_usd case) or left
// 'pending' for the invoice worker to pick up later; this package never
// revises a decision after writing one, because RecordTx's whole contract is
// idempotent replay, not amendment.
package paymentfact

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
)

// Rail is which payment rail settled the payment. Extensible: a new rail is a
// new constant and a new value in the migration's CHECK, not a new table.
type Rail string

const (
	RailStripe       Rail = "stripe"
	RailNewebPay     Rail = "newebpay"
	RailTapPay       Rail = "tappay"
	RailBankTransfer Rail = "bank_transfer"
)

var validRails = map[Rail]bool{
	RailStripe: true, RailNewebPay: true, RailTapPay: true, RailBankTransfer: true,
}

// Mode is sandbox-or-live, copied from the payment at write time and never
// changed afterward (D524: a sandbox payment must never reach a production
// invoice call).
type Mode string

const (
	ModeSandbox Mode = "sandbox"
	ModeLive    Mode = "live"
)

var validModes = map[Mode]bool{ModeSandbox: true, ModeLive: true}

// SourceKind says which step on the rail observed the payment. The rail
// already says WHICH payment method; this says HOW this particular fact came
// to be written.
type SourceKind string

const (
	// SourceEngineSettlement is the intent executor's own settle step.
	SourceEngineSettlement SourceKind = "engine_settlement"
	// SourceRailNotice is an asynchronous notice from a rail (NewebPay,
	// TapPay) verified by that rail's own worker.
	SourceRailNotice SourceKind = "rail_notice"
	// SourceManualPaidMark is an operator's paid-mark verb on a persisted
	// statement, once recorded payments reach the statement's total.
	SourceManualPaidMark SourceKind = "manual_paid_mark"
	// SourceStatement is a finalized statement standing in for payment not
	// yet received, where the payer's contract lets it hold payment until a
	// 統一發票 exists.
	SourceStatement SourceKind = "statement"
)

var validSourceKinds = map[SourceKind]bool{
	SourceEngineSettlement: true, SourceRailNotice: true,
	SourceManualPaidMark: true, SourceStatement: true,
}

// InvoiceDecision is the invoice worker's (#287) verdict, or the absence of
// one yet.
type InvoiceDecision string

const (
	// DecisionPending means no verdict has been reached. The only state a
	// freshly written fact may carry.
	DecisionPending InvoiceDecision = "pending"
	// DecisionNotApplicable means this payment cannot or must not produce a
	// 統一發票 — e.g. a USD charge by the US C-corp (reason
	// "foreign_seller_usd": no FX in v1, and a Taiwan invoice is NT$ only).
	DecisionNotApplicable InvoiceDecision = "not_applicable"
	// DecisionDeferred means an invoice is owed but cannot be issued yet —
	// e.g. a distributor's facts before the Taiwan company is registered
	// (D494, D503).
	DecisionDeferred InvoiceDecision = "deferred"
)

var validDecisions = map[InvoiceDecision]bool{
	DecisionPending: true, DecisionNotApplicable: true, DecisionDeferred: true,
}

// New is what a writer supplies. ID and CreatedAt are assigned by the store.
type New struct {
	Rail       Rail
	RailRef    string
	Mode       Mode
	SourceKind SourceKind

	// PayerAccountID is the ms_billing.accounts row that paid.
	PayerAccountID uuid.UUID
	// SellerID is the issuing seller (#312). Nil while unresolved or while
	// that issue has not merged.
	SellerID *uuid.UUID

	Currency string
	// NetAmountMicros + TaxAmountMicros must equal GrossAmountMicros — the
	// database enforces this too, so a caller's arithmetic mistake is a
	// write that fails rather than a row that misstates what was collected.
	NetAmountMicros   int64
	TaxAmountMicros   int64
	GrossAmountMicros int64

	PaidAt time.Time

	// BuyerSnapshot is the buyer invoice profile at payment time (#285).
	// Nil until that issue lands or the buyer has not completed one.
	BuyerSnapshot []byte

	// InvoiceDecision is 'pending' for a writer that does not yet know the
	// answer (the ordinary case: the invoice worker decides later), or a
	// decided value for a writer that already knows it (Stripe's
	// not_applicable). DecisionReason is required exactly when this is not
	// DecisionPending.
	InvoiceDecision InvoiceDecision
	DecisionReason  string
}

// Fact is a stored payment fact.
type Fact struct {
	New
	ID        uuid.UUID
	CreatedAt time.Time
}

// Errors from New.validate.
var (
	ErrUnknownRail       = errors.New("paymentfact: not one of the known rails")
	ErrUnknownMode       = errors.New("paymentfact: not sandbox or live")
	ErrUnknownSourceKind = errors.New("paymentfact: not one of the known source kinds")
	ErrUnknownDecision   = errors.New("paymentfact: not one of the known invoice decisions")
	ErrIncomplete        = errors.New("paymentfact: the fact omits something a record must name")
	ErrAmountsDisagree   = errors.New("paymentfact: net + tax must equal gross")
)

// validate checks everything RecordTx cannot delegate to a database
// CHECK — mainly because a clear Go error at the call site is more useful to
// a writer than a constraint-violation message from the pool.
func (n New) validate() error {
	if !validRails[n.Rail] {
		return fmt.Errorf("%w: %q", ErrUnknownRail, n.Rail)
	}
	if strings.TrimSpace(n.RailRef) == "" {
		return fmt.Errorf("%w: rail_ref", ErrIncomplete)
	}
	if !validModes[n.Mode] {
		return fmt.Errorf("%w: %q", ErrUnknownMode, n.Mode)
	}
	if !validSourceKinds[n.SourceKind] {
		return fmt.Errorf("%w: %q", ErrUnknownSourceKind, n.SourceKind)
	}
	if n.PayerAccountID == uuid.Nil {
		return fmt.Errorf("%w: payer_account_id", ErrIncomplete)
	}
	if strings.TrimSpace(n.Currency) == "" {
		return fmt.Errorf("%w: currency", ErrIncomplete)
	}
	if n.NetAmountMicros < 0 || n.TaxAmountMicros < 0 || n.GrossAmountMicros < 0 {
		return fmt.Errorf("%w: a negative amount", ErrIncomplete)
	}
	if n.NetAmountMicros+n.TaxAmountMicros != n.GrossAmountMicros {
		return ErrAmountsDisagree
	}
	if n.PaidAt.IsZero() {
		return fmt.Errorf("%w: paid_at", ErrIncomplete)
	}
	if !validDecisions[n.InvoiceDecision] {
		return fmt.Errorf("%w: %q", ErrUnknownDecision, n.InvoiceDecision)
	}
	hasReason := strings.TrimSpace(n.DecisionReason) != ""
	if n.InvoiceDecision == DecisionPending && hasReason {
		return fmt.Errorf("%w: a pending fact must not carry a decision reason", ErrIncomplete)
	}
	if n.InvoiceDecision != DecisionPending && !hasReason {
		return fmt.Errorf("%w: decision_reason", ErrIncomplete)
	}
	return nil
}
