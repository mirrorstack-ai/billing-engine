package autotopup

import (
	"context"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/mirrorstack-ai/billing-engine/internal/account/db"
	billingstripe "github.com/mirrorstack-ai/billing-engine/internal/shared/stripe"
)

type pgxStore struct {
	pool *pgxpool.Pool
	q    *db.Queries
}

func NewStore(pool *pgxpool.Pool) Store {
	if pool == nil {
		panic("autotopup.NewStore: pool must not be nil")
	}
	return &pgxStore{pool: pool, q: db.New(pool)}
}

// Acquire serializes on the account, recovers an existing pending row before
// consulting mutable policy, and otherwise inserts one attempt only when:
// credits mode + zero pre-allocation + enabled policy + projected remaining
// spendable balance <= threshold. Equality intentionally triggers.
func (s *pgxStore) Acquire(
	ctx context.Context,
	accountID uuid.UUID,
	projectedChargeMicros int64,
	now time.Time,
) (Attempt, AcquireKind, error) {
	if accountID == uuid.Nil {
		return Attempt{}, AcquireNone, fmt.Errorf("account id required")
	}
	if projectedChargeMicros < 0 {
		return Attempt{}, AcquireNone, fmt.Errorf("projected charge must be non-negative")
	}
	now = now.UTC()

	var (
		attempt Attempt
		kind    = AcquireNone
	)
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		qtx := s.q.WithTx(tx)
		account, err := qtx.LockAutoTopUpAccount(ctx, accountID.String())
		if err != nil {
			return err
		}

		// Recovery wins over current policy. Once an attempt is authorized, a
		// mode/config/card edit cannot strand collected money or redirect it.
		pending, err := qtx.LatestPendingAutoTopUp(ctx, accountID.String())
		if err == nil {
			attempt, err = attemptFromPending(pending)
			if err != nil {
				return err
			}
			kind = AcquireExisting
			return nil
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			return err
		}

		if account.BillingMode != "credits" || account.CreditLimitMicros != 0 {
			return nil
		}
		policy, err := qtx.ReadAutoTopUpPolicy(ctx, accountID.String())
		if errors.Is(err, pgx.ErrNoRows) {
			return nil
		}
		if err != nil {
			return err
		}
		if !policy.Enabled {
			return nil
		}
		balance, err := qtx.ReadAutoTopUpBalance(ctx, accountID.String())
		if err != nil {
			return err
		}
		remaining, err := checkedSub(
			balance.SpendableBalanceMicros,
			projectedChargeMicros,
		)
		if err != nil {
			return err
		}
		if remaining > policy.ThresholdMicros {
			return nil
		}
		if !policy.PaymentMethodValid {
			return ErrPaymentMethodUnavailable
		}
		paymentMethodID, err := uuid.Parse(policy.PaymentMethodID)
		if err != nil {
			return fmt.Errorf("parse configured payment method id: %w", err)
		}
		balanceAfter, err := checkedAdd(balance.SettledBalanceMicros, policy.AmountMicros)
		if err != nil {
			return err
		}

		attemptID := uuid.New()
		row, err := qtx.InsertPendingAutoTopUp(ctx, db.InsertPendingAutoTopUpParams{
			AttemptID:             attemptID.String(),
			AccountID:             accountID.String(),
			AmountMicros:          policy.AmountMicros,
			BalanceAfterMicros:    balanceAfter,
			IdempotencyKey:        "credit-auto-topup:" + attemptID.String(),
			PaymentMethodID:       paymentMethodID.String(),
			StripePaymentMethodID: policy.StripePaymentMethodID,
			StripeCustomerID:      policy.StripeCustomerID,
			AttemptExpiresAt:      now.Add(PendingGrace),
			CreatedAt:             now,
		})
		if errors.Is(err, pgx.ErrNoRows) {
			// The partial unique index is a second line of defense. The account
			// lock should make this unreachable, but recover the winner if an
			// external writer inserted through another path.
			pending, lookupErr := qtx.LatestPendingAutoTopUp(ctx, accountID.String())
			if lookupErr != nil {
				return err
			}
			attempt, lookupErr = attemptFromPending(pending)
			if lookupErr != nil {
				return lookupErr
			}
			kind = AcquireExisting
			return nil
		}
		if err != nil {
			return err
		}
		attempt, err = attemptFromInsert(row)
		if err != nil {
			return err
		}
		kind = AcquireNew
		return nil
	})
	if err != nil {
		return Attempt{}, AcquireNone, err
	}
	return attempt, kind, nil
}

func (s *pgxStore) Get(ctx context.Context, accountID, attemptID uuid.UUID) (Attempt, error) {
	row, err := s.q.GetAutoTopUpAttemptByID(ctx, db.GetAutoTopUpAttemptByIDParams{
		AttemptID: attemptID.String(),
		AccountID: accountID.String(),
	})
	if err != nil {
		return Attempt{}, err
	}
	return attemptFromGet(row)
}

func (s *pgxStore) FindByStripeInvoice(
	ctx context.Context,
	stripeInvoiceID string,
) (Attempt, bool, error) {
	if stripeInvoiceID == "" {
		return Attempt{}, false, nil
	}
	row, err := s.q.GetAutoTopUpAttemptByStripeInvoice(ctx, stripeInvoiceID)
	if errors.Is(err, pgx.ErrNoRows) {
		return Attempt{}, false, nil
	}
	if err != nil {
		return Attempt{}, false, err
	}
	attempt, err := decodeAttempt(
		row.ID, row.AccountID, row.AmountMicros, row.Status,
		row.BalanceAfterMicros, row.IdempotencyKey, row.StripeInvoiceID,
		row.ReceiptUrl, row.PaymentMethodID, row.StripePaymentMethodID,
		row.StripeCustomerID, row.AttemptExpiresAt, row.FailureCode, row.CreatedAt,
	)
	if err != nil {
		return Attempt{}, false, err
	}
	return attempt, true, nil
}

func (s *pgxStore) AttachInvoice(ctx context.Context, attempt Attempt, invoice billingstripe.Invoice) (Attempt, error) {
	if invoice.ID == "" {
		return Attempt{}, fmt.Errorf("stripe invoice id required")
	}
	_, err := s.q.AttachAutoTopUpInvoice(ctx, db.AttachAutoTopUpInvoiceParams{
		StripeInvoiceID: invoice.ID,
		ReceiptUrl:      invoice.HostedInvoiceURL,
		AttemptID:       attempt.ID.String(),
		AccountID:       attempt.AccountID.String(),
	})
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return Attempt{}, err
	}
	current, getErr := s.Get(ctx, attempt.AccountID, attempt.ID)
	if getErr != nil {
		return Attempt{}, getErr
	}
	if current.StripeInvoiceID != invoice.ID {
		return Attempt{}, fmt.Errorf(
			"auto-top-up attempt %s is already attached to invoice %q",
			attempt.ID,
			current.StripeInvoiceID,
		)
	}
	return current, nil
}

func (s *pgxStore) Fail(
	ctx context.Context,
	attempt Attempt,
	failureCode string,
	receiptURL string,
) (Attempt, bool, error) {
	if failureCode == "" {
		failureCode = "payment_failed"
	}
	transitioned := false
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		qtx := s.q.WithTx(tx)
		if _, err := qtx.LockAutoTopUpAccount(ctx, attempt.AccountID.String()); err != nil {
			return err
		}
		row, err := qtx.LockAutoTopUpAttemptByID(ctx, db.LockAutoTopUpAttemptByIDParams{
			AttemptID: attempt.ID.String(),
			AccountID: attempt.AccountID.String(),
		})
		if err != nil {
			return err
		}
		if row.Status == "settled" || row.Status == "failed" {
			return nil
		}
		if row.Status != "pending" {
			return fmt.Errorf("cannot fail auto-top-up status %q", row.Status)
		}
		balance, err := qtx.WalletSettledBalance(ctx, attempt.AccountID.String())
		if err != nil {
			return err
		}
		if _, err := qtx.FailAutoTopUpAttempt(ctx, db.FailAutoTopUpAttemptParams{
			BalanceAfterMicros: balance,
			ReceiptUrl:         receiptURL,
			FailureCode:        failureCode,
			AttemptID:          attempt.ID.String(),
			AccountID:          attempt.AccountID.String(),
		}); err != nil {
			return err
		}
		transitioned = true
		return nil
	})
	if err != nil {
		return Attempt{}, false, err
	}
	current, err := s.Get(ctx, attempt.AccountID, attempt.ID)
	return current, transitioned, err
}

func attemptFromPending(row db.LatestPendingAutoTopUpRow) (Attempt, error) {
	return decodeAttempt(
		row.ID, row.AccountID, row.AmountMicros, row.Status,
		row.BalanceAfterMicros, row.IdempotencyKey, row.StripeInvoiceID,
		row.ReceiptUrl, row.PaymentMethodID, row.StripePaymentMethodID,
		row.StripeCustomerID, row.AttemptExpiresAt, row.FailureCode, row.CreatedAt,
	)
}

func attemptFromInsert(row db.InsertPendingAutoTopUpRow) (Attempt, error) {
	return decodeAttempt(
		row.ID, row.AccountID, row.AmountMicros, row.Status,
		row.BalanceAfterMicros, row.IdempotencyKey, row.StripeInvoiceID,
		row.ReceiptUrl, row.PaymentMethodID, row.StripePaymentMethodID,
		row.StripeCustomerID, row.AttemptExpiresAt, row.FailureCode, row.CreatedAt,
	)
}

func attemptFromGet(row db.GetAutoTopUpAttemptByIDRow) (Attempt, error) {
	return decodeAttempt(
		row.ID, row.AccountID, row.AmountMicros, row.Status,
		row.BalanceAfterMicros, row.IdempotencyKey, row.StripeInvoiceID,
		row.ReceiptUrl, row.PaymentMethodID, row.StripePaymentMethodID,
		row.StripeCustomerID, row.AttemptExpiresAt, row.FailureCode, row.CreatedAt,
	)
}

func decodeAttempt(
	idRaw, accountIDRaw string,
	amountMicros int64,
	status string,
	balanceAfterMicros int64,
	idempotencyKey, stripeInvoiceID, receiptURL, paymentMethodIDRaw,
	stripePaymentMethodID, stripeCustomerID string,
	expiresAt pgtype.Timestamptz,
	failureCode string,
	createdAt time.Time,
) (Attempt, error) {
	id, err := uuid.Parse(idRaw)
	if err != nil {
		return Attempt{}, fmt.Errorf("parse auto-top-up attempt id: %w", err)
	}
	accountID, err := uuid.Parse(accountIDRaw)
	if err != nil {
		return Attempt{}, fmt.Errorf("parse auto-top-up account id: %w", err)
	}
	paymentMethodID, err := uuid.Parse(paymentMethodIDRaw)
	if err != nil {
		return Attempt{}, fmt.Errorf("parse auto-top-up payment method id: %w", err)
	}
	var expiry time.Time
	if expiresAt.Valid {
		expiry = expiresAt.Time
	}
	return Attempt{
		ID:                    id,
		AccountID:             accountID,
		AmountMicros:          amountMicros,
		Status:                status,
		BalanceAfterMicros:    balanceAfterMicros,
		IdempotencyKey:        idempotencyKey,
		StripeInvoiceID:       stripeInvoiceID,
		ReceiptURL:            receiptURL,
		PaymentMethodID:       paymentMethodID,
		StripePaymentMethodID: stripePaymentMethodID,
		StripeCustomerID:      stripeCustomerID,
		ExpiresAt:             expiry,
		FailureCode:           failureCode,
		CreatedAt:             createdAt,
	}, nil
}

func checkedAdd(a, b int64) (int64, error) {
	if (b > 0 && a > math.MaxInt64-b) || (b < 0 && a < math.MinInt64-b) {
		return 0, fmt.Errorf("credit balance overflows int64 micros")
	}
	return a + b, nil
}

func checkedSub(a, b int64) (int64, error) {
	if (b > 0 && a < math.MinInt64+b) || (b < 0 && a > math.MaxInt64+b) {
		return 0, fmt.Errorf("projected credit balance overflows int64 micros")
	}
	return a - b, nil
}
