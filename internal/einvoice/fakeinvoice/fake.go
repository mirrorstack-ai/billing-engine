// Package fakeinvoice is an in-memory einvoice.Provider. It is the
// default for unit tests and for the sandbox path until a stage account
// is active (mirrorstack-ai/billing-engine#286, item 4).
//
// Everything here is deterministic and process-local: no network, no
// clock reads beyond what a caller injects, and no vendor credential of
// any kind — there is no vendor to hold a credential for.
package fakeinvoice

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"sync"
	"time"

	"github.com/mirrorstack-ai/billing-engine/internal/einvoice"
)

// Name is the registry key this package registers itself under.
const Name = "fake"

// invoiceRecord is the fake's whole notion of an issued invoice.
type invoiceRecord struct {
	doc           einvoice.Document
	invoiceNumber string
	randomNumber  string
	issuedAt      time.Time
	voided        bool
	allowedNTD    int64 // cumulative allowance already issued against this invoice
}

// Fake is an in-memory einvoice.Provider, safe for concurrent use.
type Fake struct {
	mu sync.Mutex

	byOrderNumber map[einvoice.ProviderOrderNumber]*invoiceRecord
	byInvoiceNum  map[string]*invoiceRecord
	allowanceSeq  map[string]int // invoice number -> next allowance sequence
	now           func() time.Time

	// Failure injection, keyed on the order number the caller is about
	// to issue. Set before calling Issue; the fake reads and then
	// forgets these, so a scenario needs to be re-armed per call.
	injectTimeout      map[einvoice.ProviderOrderNumber]bool
	injectAsyncPending map[einvoice.ProviderOrderNumber]bool
	pendingResolved    map[einvoice.ProviderOrderNumber]bool
}

// New builds an empty fake. now defaults to time.Now when nil.
func New(now func() time.Time) *Fake {
	if now == nil {
		now = time.Now
	}
	return &Fake{
		byOrderNumber:      make(map[einvoice.ProviderOrderNumber]*invoiceRecord),
		byInvoiceNum:       make(map[string]*invoiceRecord),
		allowanceSeq:       make(map[string]int),
		now:                now,
		injectTimeout:      make(map[einvoice.ProviderOrderNumber]bool),
		injectAsyncPending: make(map[einvoice.ProviderOrderNumber]bool),
		pendingResolved:    make(map[einvoice.ProviderOrderNumber]bool),
	}
}

// InjectTimeout arms Issue to return einvoice.Ambiguous the next time it
// is called with this order number, instead of issuing.
func (f *Fake) InjectTimeout(orderNumber einvoice.ProviderOrderNumber) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.injectTimeout[orderNumber] = true
}

// InjectAsyncPending arms Issue to answer StatusPending the first time
// it is called with this order number — like ECPay's next-day B2B
// issuance — and StatusIssued on a later Query or re-Issue of the same
// order number.
func (f *Fake) InjectAsyncPending(orderNumber einvoice.ProviderOrderNumber) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.injectAsyncPending[orderNumber] = true
}

func (f *Fake) Name() string { return Name }

func (f *Fake) Capabilities() einvoice.Capabilities {
	return einvoice.Capabilities{
		SupportsAllowance:                true,
		SupportsAsyncPending:             true,
		SupportsCounterpartyRegistration: false,
		SupportsLoveCode:                 true,
		SupportsCarrier:                  true,
	}
}

// Issue is idempotent on doc.OrderNumber: calling it twice with the
// same order number returns the same invoice rather than creating a
// second one.
func (f *Fake) Issue(ctx context.Context, doc einvoice.Document) (einvoice.Result, error) {
	if err := einvoice.ValidateOrderNumber(doc.OrderNumber); err != nil {
		return einvoice.Result{}, err
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	if f.injectTimeout[doc.OrderNumber] {
		delete(f.injectTimeout, doc.OrderNumber)
		return einvoice.Result{}, einvoice.Ambiguous{OrderNumber: doc.OrderNumber, Reason: "fake: injected timeout"}
	}

	if existing, ok := f.byOrderNumber[doc.OrderNumber]; ok {
		return f.resultFor(existing), nil
	}

	if doc.Buyer.Carrier == einvoice.CarrierMobile {
		if err := einvoice.ValidateMobileBarcode(doc.Buyer.CarrierCode); err != nil {
			return einvoice.Result{}, err
		}
	}
	if doc.Buyer.LoveCode != "" {
		if err := einvoice.ValidateLoveCode(doc.Buyer.LoveCode); err != nil {
			return einvoice.Result{}, err
		}
	}

	rec := &invoiceRecord{
		doc:           doc,
		invoiceNumber: f.deterministicInvoiceNumber(doc.OrderNumber),
		randomNumber:  f.deterministicRandomNumber(doc.OrderNumber),
		issuedAt:      f.now(),
	}

	if f.injectAsyncPending[doc.OrderNumber] && !f.pendingResolved[doc.OrderNumber] {
		delete(f.injectAsyncPending, doc.OrderNumber)
		f.pendingResolved[doc.OrderNumber] = true
		// Reserve the order number so a retry adopts the same pending
		// invoice rather than issuing a second one, but withhold the
		// invoice number: item 1 requires Pending to be possible, with
		// InvoiceNumber empty until Query resolves it.
		f.byOrderNumber[doc.OrderNumber] = rec
		return einvoice.Result{
			Status:      einvoice.StatusPending,
			OrderNumber: doc.OrderNumber,
		}, nil
	}

	f.byOrderNumber[doc.OrderNumber] = rec
	f.byInvoiceNum[rec.invoiceNumber] = rec
	return f.resultFor(rec), nil
}

func (f *Fake) Void(ctx context.Context, invoiceNumber string, mode einvoice.Mode) (einvoice.Result, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	rec, ok := f.byInvoiceNum[invoiceNumber]
	if !ok {
		return einvoice.Result{}, einvoice.ErrInvoiceNotFound
	}
	if rec.voided {
		return einvoice.Result{}, einvoice.ErrAlreadyVoided
	}
	if f.now().After(einvoice.DefaultVoidWindow(rec.issuedAt)) {
		return einvoice.Result{}, einvoice.ErrVoidWindowClosed
	}
	rec.voided = true
	return einvoice.Result{
		Status:        einvoice.StatusVoided,
		InvoiceNumber: rec.invoiceNumber,
		RandomNumber:  rec.randomNumber,
		Date:          f.now(),
		OrderNumber:   rec.doc.OrderNumber,
	}, nil
}

func (f *Fake) Allowance(ctx context.Context, invoiceNumber string, lines []einvoice.Line, mode einvoice.Mode) (einvoice.Result, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	rec, ok := f.byInvoiceNum[invoiceNumber]
	if !ok {
		return einvoice.Result{}, einvoice.ErrInvoiceNotFound
	}
	if rec.voided {
		return einvoice.Result{}, einvoice.ErrAlreadyVoided
	}

	var allowanceGross int64
	for _, l := range lines {
		allowanceGross += l.GrossNTD
	}
	_, _, invoiceGross := rec.doc.TotalNTD()
	if rec.allowedNTD+allowanceGross > invoiceGross {
		return einvoice.Result{}, einvoice.ErrAllowanceExceedsGross
	}
	rec.allowedNTD += allowanceGross

	f.allowanceSeq[invoiceNumber]++
	allowanceNumber := fmt.Sprintf("%s-A%02d", invoiceNumber, f.allowanceSeq[invoiceNumber])
	return einvoice.Result{
		Status:        einvoice.StatusIssued,
		InvoiceNumber: allowanceNumber,
		Date:          f.now(),
		OrderNumber:   rec.doc.OrderNumber,
	}, nil
}

func (f *Fake) VoidAllowance(ctx context.Context, allowanceNumber string, mode einvoice.Mode) (einvoice.Result, error) {
	// The fake does not track allowances as separately queryable
	// records beyond their number; reversing one is not exercised by
	// the contract suite yet, so this stays a thin, honest stub rather
	// than invented bookkeeping.
	return einvoice.Result{}, einvoice.ErrUnsupported
}

func (f *Fake) QueryByOrderNumber(ctx context.Context, orderNumber einvoice.ProviderOrderNumber, mode einvoice.Mode) (einvoice.Result, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	rec, ok := f.byOrderNumber[orderNumber]
	if !ok {
		return einvoice.Result{}, einvoice.ErrInvoiceNotFound
	}
	// A pending-then-resolved scenario: the record exists but the
	// invoice number was withheld at Issue time. Resolve it now, as a
	// real vendor's async answer would on the query path (D550: an
	// async answer is read with Query, never pushed back).
	if rec.invoiceNumber == "" {
		rec.invoiceNumber = f.deterministicInvoiceNumber(orderNumber)
		rec.randomNumber = f.deterministicRandomNumber(orderNumber)
		f.byInvoiceNum[rec.invoiceNumber] = rec
	}
	return f.resultFor(rec), nil
}

func (f *Fake) QueryByInvoiceNumber(ctx context.Context, invoiceNumber string, mode einvoice.Mode) (einvoice.Result, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	rec, ok := f.byInvoiceNum[invoiceNumber]
	if !ok {
		return einvoice.Result{}, einvoice.ErrInvoiceNotFound
	}
	return f.resultFor(rec), nil
}

func (f *Fake) ValidateCarrier(carrier einvoice.CarrierKind, code string) error {
	switch carrier {
	case einvoice.CarrierMobile:
		return einvoice.ValidateMobileBarcode(code)
	case einvoice.CarrierNone, einvoice.CarrierCitizenDigital:
		return nil
	default:
		return einvoice.ErrInvalidCarrier
	}
}

func (f *Fake) ValidateLoveCode(loveCode string) error {
	return einvoice.ValidateLoveCode(loveCode)
}

func (f *Fake) LookupTrack(ctx context.Context, sellerRef, track string, mode einvoice.Mode) (einvoice.TrackStatus, error) {
	return einvoice.TrackStatus{SellerRef: sellerRef, Track: track, Remaining: 1_000_000}, nil
}

func (f *Fake) VoidWindow(issuedAt time.Time) time.Time {
	return einvoice.DefaultVoidWindow(issuedAt)
}

// RegisterCounterparty: the fake mirrors every non-ECPay adapter per
// item 1 — B2B buyer registration is ECPay-specific, and returning
// ErrUnsupported here is what makes the fake a faithful stand-in for
// e首發票 in tests rather than a more permissive provider than the real
// one will be.
func (f *Fake) RegisterCounterparty(ctx context.Context, cp einvoice.Counterparty, mode einvoice.Mode) error {
	return einvoice.ErrUnsupported
}

func (f *Fake) resultFor(rec *invoiceRecord) einvoice.Result {
	status := einvoice.StatusIssued
	if rec.voided {
		status = einvoice.StatusVoided
	}
	if rec.invoiceNumber == "" {
		status = einvoice.StatusPending
	}
	return einvoice.Result{
		Status:        status,
		InvoiceNumber: rec.invoiceNumber,
		RandomNumber:  rec.randomNumber,
		Date:          rec.issuedAt,
		OrderNumber:   rec.doc.OrderNumber,
	}
}

// deterministicInvoiceNumber and deterministicRandomNumber derive from
// a hash of the order number rather than a counter, so the same test
// input always produces the same fake invoice number across runs —
// useful for golden-file assertions in callers of this package.
func (f *Fake) deterministicInvoiceNumber(orderNumber einvoice.ProviderOrderNumber) string {
	sum := sha256.Sum256([]byte("invoice:" + orderNumber))
	letters := "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
	l1 := letters[sum[0]%26]
	l2 := letters[sum[1]%26]
	digits := binary.BigEndian.Uint32(sum[2:6]) % 100_000_000
	return fmt.Sprintf("%c%c%08d", l1, l2, digits)
}

func (f *Fake) deterministicRandomNumber(orderNumber einvoice.ProviderOrderNumber) string {
	sum := sha256.Sum256([]byte("random:" + orderNumber))
	n := binary.BigEndian.Uint16(sum[:2]) % 10000
	return fmt.Sprintf("%04d", n)
}

var _ einvoice.Provider = (*Fake)(nil)
