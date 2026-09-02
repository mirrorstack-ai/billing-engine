// Package creditledger owns the shared Stripe-invoice -> wallet settlement
// transaction. Both the foreground auto-top-up executor and the webhook router
// use this exact primitive, so browser-independent manual purchases and
// automatic top-ups share one paid-is-highest, exactly-once balance mutation.
package creditledger

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strings"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/mirrorstack-ai/billing-engine/internal/account/db"
)

const microsPerCent int64 = 10_000

type settlementObservationContextKey struct{}

// WithSettlementObservation marks a post-commit observer call. Runtime
// coordinators must still reconcile authoritative estimates and release block
// claims, but must not synchronously trigger another auto-top-up in the same
// call chain when this marker is present.
func WithSettlementObservation(ctx context.Context) context.Context {
	return context.WithValue(ctx, settlementObservationContextKey{}, true)
}

// IsSettlementObservation lets the runtime coordinator suppress recursive
// refill loops while preserving all non-payment reconciliation work.
func IsSettlementObservation(ctx context.Context) bool {
	marked, _ := ctx.Value(settlementObservationContextKey{}).(bool)
	return marked
}

// Settlement reports whether a Stripe invoice belongs to a credit operation
// and whether this call committed the first balance-advancing transition.
// Transitioned is the sole signal observers should use: webhook/RPC replays
// remain found but never reconcile/notify a second time.
type Settlement struct {
	Found        bool
	Transitioned bool
	AccountID    uuid.UUID
	LedgerID     uuid.UUID
	Type         string
}

// FailureReconciliation reports a non-paid webhook's resource-authoritative
// auto-top-up outcome. Found distinguishes ordinary invoices from durable
// auto-top-up attempts; Transitioned is true only for the first committed
// pending→failed or pending|failed→settled transition.
type FailureReconciliation struct {
	Found        bool
	Transitioned bool
	AccountID    uuid.UUID
	LedgerID     uuid.UUID
	Status       string
	FailureCode  string
}

// Store executes credit settlement against the shared pgx pool.
type Store struct {
	pool *pgxpool.Pool
	q    *db.Queries
}

func NewStore(pool *pgxpool.Pool) *Store {
	if pool == nil {
		panic("creditledger.NewStore: pool must not be nil")
	}
	return &Store{pool: pool, q: db.New(pool)}
}

// SettleStripeInvoice advances purchase/auto_topup pending|failed -> settled
// exactly once. A paid invoice is the highest authority, so a previously
// failed row is deliberately recoverable. Currency and the exact rounded
// Stripe minor-unit amount are verified before any balance mutation.
func (s *Store) SettleStripeInvoice(
	ctx context.Context,
	stripeInvoiceID string,
	amountPaidCents int64,
	currency string,
	receiptURL string,
) (Settlement, error) {
	return s.settleStripeInvoice(
		ctx,
		stripeInvoiceID,
		amountPaidCents,
		currency,
		receiptURL,
		"",
	)
}

// SettleManualStripeInvoice is the manual-purchase-only transaction invoked
// after creditpurchase.Executor has completed its resource-authoritative
// invoice/item/PaymentIntent proof. It deliberately does no Stripe read itself.
// An auto-top-up match fails closed because that operation uses its own
// frozen-card executor.
func (s *Store) SettleManualStripeInvoice(
	ctx context.Context,
	stripeInvoiceID string,
	amountPaidCents int64,
	currency string,
	receiptURL string,
) (Settlement, error) {
	return s.settleStripeInvoice(
		ctx,
		stripeInvoiceID,
		amountPaidCents,
		currency,
		receiptURL,
		"purchase",
	)
}

func (s *Store) settleStripeInvoice(
	ctx context.Context,
	stripeInvoiceID string,
	amountPaidCents int64,
	currency string,
	receiptURL string,
	requiredType string,
) (Settlement, error) {
	if stripeInvoiceID == "" {
		return Settlement{}, fmt.Errorf("stripe invoice id required")
	}

	hint, err := s.q.FindCreditAttemptByStripeInvoice(ctx, stripeInvoiceID)
	if errors.Is(err, pgx.ErrNoRows) {
		return Settlement{}, nil
	}
	if err != nil {
		return Settlement{}, err
	}
	accountID, err := uuid.Parse(hint.AccountID)
	if err != nil {
		return Settlement{}, fmt.Errorf("parse credit account id: %w", err)
	}
	ledgerID, err := uuid.Parse(hint.ID)
	if err != nil {
		return Settlement{}, fmt.Errorf("parse credit ledger id: %w", err)
	}
	result := Settlement{
		Found:     true,
		AccountID: accountID,
		LedgerID:  ledgerID,
		Type:      hint.Type,
	}
	if requiredType != "" && hint.Type != requiredType {
		return result, fmt.Errorf(
			"credit invoice %s type %q requires resource-authoritative reconciliation",
			stripeInvoiceID,
			hint.Type,
		)
	}

	err = pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		qtx := s.q.WithTx(tx)
		if _, err := qtx.LockAutoTopUpAccount(ctx, accountID.String()); err != nil {
			return err
		}
		row, err := qtx.LockCreditAttemptByStripeInvoice(ctx, stripeInvoiceID)
		if err != nil {
			return err
		}

		// Replays of an already-settled invoice are successful no-ops. Perform
		// the amount/currency checks on the transition only: historical rows
		// remain readable even if a sparse duplicate event omits presentment.
		if row.Status == "settled" {
			return nil
		}
		if row.Status != "pending" && row.Status != "failed" {
			return fmt.Errorf("credit invoice %s has unsupported status %q", stripeInvoiceID, row.Status)
		}
		if strings.ToLower(currency) != "usd" {
			return fmt.Errorf("credit invoice %s currency %q is not usd", stripeInvoiceID, currency)
		}
		// 🔴 The row must be a whole number of cents, or the two lines below
		// disagree by construction: one asserts what was CHARGED (rounded to
		// cents, because that is what a card takes) and the other credits what
		// was REQUESTED (raw micros). The gap is credit nobody paid for.
		//
		// The entry points now reject sub-cent amounts, so reaching this is
		// either a row written before that guard or a path that bypassed it.
		// Either way it must not settle quietly — refusing leaves the invoice
		// recoverable, while crediting mints money.
		if row.AmountMicros%microsPerCent != 0 {
			return fmt.Errorf(
				"credit invoice %s requests %d micros, which is not a whole number of cents; "+
					"a card can only be charged in cents, so crediting it would grant %d micros nobody paid for",
				stripeInvoiceID,
				row.AmountMicros,
				row.AmountMicros-microsToCentsRoundHalfUp(row.AmountMicros)*microsPerCent,
			)
		}
		expectedCents := microsToCentsRoundHalfUp(row.AmountMicros)
		if amountPaidCents != expectedCents {
			return fmt.Errorf(
				"credit invoice %s paid %d cents; expected %d",
				stripeInvoiceID,
				amountPaidCents,
				expectedCents,
			)
		}

		balance, err := qtx.WalletSettledBalance(ctx, accountID.String())
		if err != nil {
			return err
		}
		balanceAfter, err := checkedAdd(balance, row.AmountMicros)
		if err != nil {
			return err
		}
		if _, err := qtx.SettleCreditAttempt(ctx, db.SettleCreditAttemptParams{
			BalanceAfterMicros: balanceAfter,
			ReceiptUrl:         receiptURL,
			AttemptID:          row.ID,
			AccountID:          row.AccountID,
		}); err != nil {
			return err
		}
		result.Transitioned = true
		return nil
	})
	if err != nil {
		return Settlement{}, err
	}
	return result, nil
}

func microsToCentsRoundHalfUp(micros int64) int64 {
	return (micros + microsPerCent/2) / microsPerCent
}

func checkedAdd(a, b int64) (int64, error) {
	if (b > 0 && a > math.MaxInt64-b) || (b < 0 && a < math.MinInt64-b) {
		return 0, fmt.Errorf("credit balance overflows int64 micros")
	}
	return a + b, nil
}
