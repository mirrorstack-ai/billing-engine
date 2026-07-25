// Package autotopup executes durable, selected-card automatic wallet funding.
// The public account API owns policy configuration; this package owns only the
// server-side trigger, Stripe attempt, and recovery state machine.
package autotopup

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"

	"github.com/mirrorstack-ai/billing-engine/internal/account/creditledger"
	billingstripe "github.com/mirrorstack-ai/billing-engine/internal/shared/stripe"
)

const PendingGrace = 10 * time.Minute

var ErrPaymentMethodUnavailable = errors.New("configured auto-top-up payment method is unavailable")

type AcquireKind string

const (
	AcquireNone     AcquireKind = "none"
	AcquireExisting AcquireKind = "existing"
	AcquireNew      AcquireKind = "new"
)

// Attempt is the complete durable payment snapshot. StripeCustomerID and
// StripePaymentMethodID are immutable after insertion and are the only payer
// facts the executor may use.
type Attempt struct {
	ID                    uuid.UUID
	AccountID             uuid.UUID
	AmountMicros          int64
	Status                string
	BalanceAfterMicros    int64
	IdempotencyKey        string
	StripeInvoiceID       string
	ReceiptURL            string
	PaymentMethodID       uuid.UUID
	StripePaymentMethodID string
	StripeCustomerID      string
	ExpiresAt             time.Time
	FailureCode           string
	CreatedAt             time.Time
	// ObservedAt is the database transaction clock captured with a pending
	// lookup/insert. Production expiry decisions use it so application clock
	// skew cannot extend or shorten the durable ten-minute grace. Test fakes
	// may leave it zero and use the injected fallback clock.
	ObservedAt time.Time
}

func (a Attempt) Expired(fallbackNow time.Time) bool {
	observedAt := a.ObservedAt
	if observedAt.IsZero() {
		observedAt = fallbackNow
	}
	return a.ExpiresAt.IsZero() || !observedAt.Before(a.ExpiresAt)
}

// Result describes what this trigger observed after all synchronous recovery.
// Triggered is true when an attempt exists (new or recovered), not merely when
// the mutable threshold predicate was evaluated.
type Result struct {
	Triggered   bool
	NewAttempt  bool
	AttemptID   uuid.UUID
	Status      string
	FailureCode string
}

type Store interface {
	Acquire(ctx context.Context, accountID uuid.UUID, projectedChargeMicros int64, now time.Time) (Attempt, AcquireKind, error)
	Pending(ctx context.Context, accountID uuid.UUID) (Attempt, bool, error)
	Get(ctx context.Context, accountID, attemptID uuid.UUID) (Attempt, error)
	FindByStripeInvoice(ctx context.Context, stripeInvoiceID string) (Attempt, bool, error)
	AttachInvoice(ctx context.Context, attempt Attempt, invoice billingstripe.Invoice) (Attempt, error)
	Fail(ctx context.Context, attempt Attempt, failureCode, receiptURL string) (Attempt, bool, error)
}

type Settler interface {
	SettleStripeInvoice(
		ctx context.Context,
		stripeInvoiceID string,
		amountPaidCents int64,
		currency string,
		receiptURL string,
	) (creditledger.Settlement, error)
}

// SettlementObserver is structurally satisfied by the #103 runtime credit
// coordinator. It runs only after a committed first transition; errors are
// best-effort and never rewrite payment truth.
type SettlementObserver interface {
	ObserveAccount(ctx context.Context, accountID uuid.UUID) error
}

type StripeClient interface {
	billingstripe.AutoTopUpClient
}
