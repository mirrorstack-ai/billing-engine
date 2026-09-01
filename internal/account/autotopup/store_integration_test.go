//go:build integration

package autotopup_test

import (
	"context"
	"math"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/mirrorstack-ai/billing-engine/internal/account/autotopup"
	"github.com/mirrorstack-ai/billing-engine/internal/shared/testutil"
)

func TestStoreAcquire_ThresholdModePolicyAndSelectedCardGates(t *testing.T) {
	pool := testutil.NewTestDB(t)
	store := autotopup.NewStore(pool)
	ctx := context.Background()
	now := time.Date(2026, time.July, 25, 7, 0, 0, 0, time.UTC)

	t.Run("remaining above threshold does not trigger", func(t *testing.T) {
		accountID, pmID := seedEligibleAutoTopUp(t, pool, 7_000_000, true)
		seedSettledGrant(t, pool, accountID, 10_000_000)

		_, kind, err := store.Acquire(ctx, accountID, 2_000_000, now)

		require.NoError(t, err)
		require.Equal(t, autotopup.AcquireNone, kind) // 10 - 2 = 8 > 7
		require.Zero(t, countAutoTopUps(t, pool, accountID))
		require.NotEqual(t, uuid.Nil, pmID)
	})

	for _, tt := range []struct {
		name      string
		threshold int64
	}{
		{name: "remaining equal threshold triggers", threshold: 8_000_000},
		{name: "remaining below threshold triggers", threshold: 9_000_000},
	} {
		t.Run(tt.name, func(t *testing.T) {
			accountID, pmID := seedEligibleAutoTopUp(t, pool, tt.threshold, true)
			seedSettledGrant(t, pool, accountID, 10_000_000)

			var dbBefore time.Time
			require.NoError(t, pool.QueryRow(
				ctx,
				`SELECT clock_timestamp()`,
			).Scan(&dbBefore))
			attempt, kind, err := store.Acquire(ctx, accountID, 2_000_000, now)
			var dbAfter time.Time
			require.NoError(t, pool.QueryRow(
				ctx,
				`SELECT clock_timestamp()`,
			).Scan(&dbAfter))

			require.NoError(t, err)
			require.Equal(t, autotopup.AcquireNew, kind)
			require.Equal(t, accountID, attempt.AccountID)
			require.Equal(t, pmID, attempt.PaymentMethodID)
			require.Equal(t, "pm_"+pmID.String(), attempt.StripePaymentMethodID)
			require.Equal(t, "cus_"+accountID.String(), attempt.StripeCustomerID)
			require.False(t, attempt.CreatedAt.Before(dbBefore))
			require.False(t, attempt.CreatedAt.After(dbAfter))
			require.Equal(t, autotopup.PendingGrace, attempt.ExpiresAt.Sub(attempt.CreatedAt))
			require.Equal(t, int64(5_000_000), attempt.AmountMicros)
			require.Equal(t, int64(15_000_000), attempt.BalanceAfterMicros)
		})
	}

	t.Run("disabled policy does not trigger", func(t *testing.T) {
		accountID, _ := seedEligibleAutoTopUp(t, pool, 9_000_000, false)
		seedSettledGrant(t, pool, accountID, 10_000_000)
		_, kind, err := store.Acquire(ctx, accountID, 2_000_000, now)
		require.NoError(t, err)
		require.Equal(t, autotopup.AcquireNone, kind)
	})

	t.Run("standard mode does not trigger", func(t *testing.T) {
		accountID, _ := seedEligibleAutoTopUp(t, pool, 9_000_000, true)
		seedSettledGrant(t, pool, accountID, 10_000_000)
		_, err := pool.Exec(ctx,
			`UPDATE ms_billing.accounts SET billing_mode='standard' WHERE id=$1`,
			accountID,
		)
		require.NoError(t, err)
		_, kind, err := store.Acquire(ctx, accountID, 2_000_000, now)
		require.NoError(t, err)
		require.Equal(t, autotopup.AcquireNone, kind)
	})

	t.Run("nonzero risk allocation does not trigger", func(t *testing.T) {
		accountID, _ := seedEligibleAutoTopUp(t, pool, 9_000_000, true)
		seedSettledGrant(t, pool, accountID, 10_000_000)
		_, err := pool.Exec(ctx,
			`UPDATE ms_billing.accounts SET credit_limit_micros=1 WHERE id=$1`,
			accountID,
		)
		require.NoError(t, err)
		_, kind, err := store.Acquire(ctx, accountID, 2_000_000, now)
		require.NoError(t, err)
		require.Equal(t, autotopup.AcquireNone, kind)
	})

	t.Run("missing policy does not trigger", func(t *testing.T) {
		accountID := seedAutoAccount(t, pool)
		_, kind, err := store.Acquire(ctx, accountID, 1, now)
		require.NoError(t, err)
		require.Equal(t, autotopup.AcquireNone, kind)
	})

	t.Run("maximum projected charge cannot wrap to a positive remaining balance", func(t *testing.T) {
		accountID, _ := seedEligibleAutoTopUp(t, pool, 0, true)

		attempt, kind, err := store.Acquire(ctx, accountID, math.MaxInt64, now)

		require.NoError(t, err)
		require.Equal(t, autotopup.AcquireNew, kind)
		require.Equal(t, int64(5_000_000), attempt.AmountMicros)
		require.Equal(t, 1, countAutoTopUps(t, pool, accountID))
	})
}

func TestStoreAcquire_RejectsUnavailableOrWrongOwnerSelectedCard(t *testing.T) {
	pool := testutil.NewTestDB(t)
	store := autotopup.NewStore(pool)
	ctx := context.Background()
	now := time.Date(2026, time.July, 25, 8, 0, 0, 0, time.UTC)

	t.Run("soft deleted", func(t *testing.T) {
		accountID, _ := seedEligibleAutoTopUp(t, pool, 0, true)
		_, err := pool.Exec(ctx,
			`UPDATE ms_billing.payment_methods_mirror SET deleted_at=now() WHERE account_id=$1`,
			accountID,
		)
		require.NoError(t, err)

		_, kind, err := store.Acquire(ctx, accountID, 1, now)

		require.ErrorIs(t, err, autotopup.ErrPaymentMethodUnavailable)
		require.Equal(t, autotopup.AcquireNone, kind)
	})

	t.Run("wrong owner", func(t *testing.T) {
		accountID := seedAutoAccount(t, pool)
		foreignID := seedAutoAccount(t, pool)
		foreignPM := seedAutoPaymentMethod(t, pool, foreignID, false)
		seedAutoConfig(t, pool, accountID, foreignPM, 0, true)

		_, kind, err := store.Acquire(ctx, accountID, 1, now)

		require.ErrorIs(t, err, autotopup.ErrPaymentMethodUnavailable)
		require.Equal(t, autotopup.AcquireNone, kind)
	})

	t.Run("missing Stripe customer", func(t *testing.T) {
		accountID, _ := seedEligibleAutoTopUp(t, pool, 0, true)
		_, err := pool.Exec(ctx,
			`UPDATE ms_billing.accounts SET stripe_customer_id=NULL WHERE id=$1`,
			accountID,
		)
		require.NoError(t, err)

		_, kind, err := store.Acquire(ctx, accountID, 1, now)

		require.ErrorIs(t, err, autotopup.ErrPaymentMethodUnavailable)
		require.Equal(t, autotopup.AcquireNone, kind)
	})
}

func TestStoreAcquire_FreezesAttemptAndConcurrentTriggersConverge(t *testing.T) {
	pool := testutil.NewTestDB(t)
	store := autotopup.NewStore(pool)
	ctx := context.Background()
	now := time.Date(2026, time.July, 25, 9, 0, 0, 0, time.UTC)
	accountID, originalPM := seedEligibleAutoTopUp(t, pool, 0, true)

	const workers = 12
	start := make(chan struct{})
	results := make(chan struct {
		attempt autotopup.Attempt
		kind    autotopup.AcquireKind
		err     error
	}, workers)
	var wg sync.WaitGroup
	wg.Add(workers)
	for range workers {
		go func() {
			defer wg.Done()
			<-start
			attempt, kind, err := store.Acquire(ctx, accountID, 1, now)
			results <- struct {
				attempt autotopup.Attempt
				kind    autotopup.AcquireKind
				err     error
			}{attempt: attempt, kind: kind, err: err}
		}()
	}
	close(start)
	wg.Wait()
	close(results)

	var (
		newCount      int
		existingCount int
		attemptID     uuid.UUID
	)
	for result := range results {
		require.NoError(t, result.err)
		switch result.kind {
		case autotopup.AcquireNew:
			newCount++
		case autotopup.AcquireExisting:
			existingCount++
		default:
			t.Fatalf("unexpected acquire kind %q", result.kind)
		}
		if attemptID == uuid.Nil {
			attemptID = result.attempt.ID
		}
		require.Equal(t, attemptID, result.attempt.ID)
		require.Equal(t, originalPM, result.attempt.PaymentMethodID)
	}
	require.Equal(t, 1, newCount)
	require.Equal(t, workers-1, existingCount)
	require.Equal(t, 1, countAutoTopUps(t, pool, accountID))

	// Mutable policy and account Stripe identity may change after authorization;
	// recovery must continue to return only the frozen payer facts.
	newPM := seedAutoPaymentMethod(t, pool, accountID, false)
	_, err := pool.Exec(ctx,
		`UPDATE ms_billing.credit_auto_topup_configs SET payment_method_id=$2 WHERE account_id=$1`,
		accountID, newPM,
	)
	require.NoError(t, err)
	_, err = pool.Exec(ctx,
		`UPDATE ms_billing.accounts SET stripe_customer_id='cus_changed' WHERE id=$1`,
		accountID,
	)
	require.NoError(t, err)

	recovered, kind, err := store.Acquire(ctx, accountID, 1, now.Add(time.Minute))
	require.NoError(t, err)
	require.Equal(t, autotopup.AcquireExisting, kind)
	require.Equal(t, attemptID, recovered.ID)
	require.Equal(t, originalPM, recovered.PaymentMethodID)
	require.Equal(t, "pm_"+originalPM.String(), recovered.StripePaymentMethodID)
	require.Equal(t, "cus_"+accountID.String(), recovered.StripeCustomerID)
}

func TestStoreAcquire_FailedPolicyRevisionLatchesUntilConfigResubmission(t *testing.T) {
	pool := testutil.NewTestDB(t)
	store := autotopup.NewStore(pool)
	ctx := context.Background()
	accountID, _ := seedEligibleAutoTopUp(t, pool, 0, true)

	var configRevision time.Time
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT updated_at
		FROM ms_billing.credit_auto_topup_configs
		WHERE account_id = $1`,
		accountID,
	).Scan(&configRevision))

	attempt, kind, err := store.Acquire(ctx, accountID, 1, configRevision)
	require.NoError(t, err)
	require.Equal(t, autotopup.AcquireNew, kind)
	failed, transitioned, err := store.Fail(
		ctx,
		attempt,
		"card_declined",
		"https://stripe.test/declined",
	)
	require.NoError(t, err)
	require.True(t, transitioned)
	require.Equal(t, "failed", failed.Status)

	for i := 1; i <= 5; i++ {
		got, gotKind, acquireErr := store.Acquire(
			ctx,
			accountID,
			1,
			configRevision.Add(time.Duration(i)*time.Second),
		)
		require.NoError(t, acquireErr)
		require.Equal(t, autotopup.AcquireNone, gotKind,
			"the same failed config revision must remain latched")
		require.Equal(t, autotopup.Attempt{}, got)
	}
	require.Equal(t, 1, countAutoTopUps(t, pool, accountID),
		"repeated below-threshold probes must not append replacement attempts")

	var rearmedRevision time.Time
	require.NoError(t, pool.QueryRow(ctx, `
		UPDATE ms_billing.credit_auto_topup_configs
		SET enabled = enabled
		WHERE account_id = $1
		RETURNING updated_at`,
		accountID,
	).Scan(&rearmedRevision))
	require.True(t, rearmedRevision.After(attempt.CreatedAt),
		"even an exact config resubmission must advance the durable retry revision")

	retry, kind, err := store.Acquire(ctx, accountID, 1, rearmedRevision)
	require.NoError(t, err)
	require.Equal(t, autotopup.AcquireNew, kind)
	require.NotEqual(t, attempt.ID, retry.ID)
	require.Equal(t, 2, countAutoTopUps(t, pool, accountID))

	recovered, kind, err := store.Acquire(ctx, accountID, 1, rearmedRevision.Add(time.Second))
	require.NoError(t, err)
	require.Equal(t, autotopup.AcquireExisting, kind,
		"the rearmed revision may authorize only one pending replacement")
	require.Equal(t, retry.ID, recovered.ID)
	require.Equal(t, 2, countAutoTopUps(t, pool, accountID))
}

func TestStoreFail_ConcurrentWebhookTransitionsOnceAndAddsZeroCredit(t *testing.T) {
	pool := testutil.NewTestDB(t)
	store := autotopup.NewStore(pool)
	ctx := context.Background()
	now := time.Date(2026, time.July, 25, 10, 0, 0, 0, time.UTC)
	accountID, _ := seedEligibleAutoTopUp(t, pool, 2_000_000, true)
	seedSettledGrant(t, pool, accountID, 2_000_000)

	attempt, kind, err := store.Acquire(ctx, accountID, 0, now)
	require.NoError(t, err)
	require.Equal(t, autotopup.AcquireNew, kind)
	require.Equal(t, int64(7_000_000), attempt.BalanceAfterMicros)

	// The executor no longer has a port that writes this column — the
	// collector that did was deleted — so the pre-cutover row this test needs
	// is staged directly.
	_, err = pool.Exec(ctx, `
		UPDATE ms_billing.credit_ledger
		SET stripe_invoice_id = $2, receipt_url = $3
		WHERE id = $1`,
		attempt.ID, "in_webhook_concurrent", "https://stripe.test/in_webhook_concurrent",
	)
	require.NoError(t, err)
	attempt, err = store.Get(ctx, accountID, attempt.ID)
	require.NoError(t, err)
	found, ok, err := store.FindByStripeInvoice(ctx, "in_webhook_concurrent")
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, attempt.ID, found.ID)
	require.Equal(t, attempt.StripeCustomerID, found.StripeCustomerID)

	const workers = 12
	start := make(chan struct{})
	results := make(chan bool, workers)
	errs := make(chan error, workers)
	var wg sync.WaitGroup
	wg.Add(workers)
	for range workers {
		go func() {
			defer wg.Done()
			<-start
			_, transitioned, err := store.Fail(
				ctx,
				attempt,
				"payment_failed",
				"https://stripe.test/in_webhook_concurrent",
			)
			results <- transitioned
			errs <- err
		}()
	}
	close(start)
	wg.Wait()
	close(results)
	close(errs)

	for err := range errs {
		require.NoError(t, err)
	}
	transitioned := 0
	for result := range results {
		if result {
			transitioned++
		}
	}
	require.Equal(t, 1, transitioned)

	current, err := store.Get(ctx, accountID, attempt.ID)
	require.NoError(t, err)
	require.Equal(t, "failed", current.Status)
	require.Equal(t, "payment_failed", current.FailureCode)
	require.Equal(t, int64(2_000_000), current.BalanceAfterMicros,
		"failure snapshots the unchanged authoritative settled balance")

	var (
		settledBalance int64
		settledTopUps  int
	)
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT COALESCE(SUM(amount_micros), 0)
		   FROM ms_billing.credit_ledger
		  WHERE account_id=$1 AND status='settled'`,
		accountID,
	).Scan(&settledBalance))
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT count(*)
		   FROM ms_billing.credit_ledger
		  WHERE account_id=$1 AND type='auto_topup' AND status='settled'`,
		accountID,
	).Scan(&settledTopUps))
	require.Equal(t, int64(2_000_000), settledBalance)
	require.Zero(t, settledTopUps, "an unpaid webhook failure must add zero wallet credit")
}

func seedAutoAccount(t *testing.T, pool *pgxpool.Pool) uuid.UUID {
	t.Helper()
	accountID := uuid.New()
	_, err := pool.Exec(context.Background(),
		`INSERT INTO ms_billing.accounts
		   (id, owner_kind, owner_user_id, stripe_customer_id, billing_mode, credit_limit_micros)
		 VALUES ($1, 'user', $2, $3, 'credits', 0)`,
		accountID,
		uuid.New(),
		"cus_"+accountID.String(),
	)
	require.NoError(t, err)
	return accountID
}

func seedAutoPaymentMethod(t *testing.T, pool *pgxpool.Pool, accountID uuid.UUID, deleted bool) uuid.UUID {
	t.Helper()
	paymentMethodID := uuid.New()
	var deletedAt any
	if deleted {
		deletedAt = time.Now().UTC()
	}
	_, err := pool.Exec(context.Background(),
		`INSERT INTO ms_billing.payment_methods_mirror
		   (id, account_id, stripe_payment_method_id, brand, last4, exp_month, exp_year, deleted_at)
		 VALUES ($1, $2, $3, 'visa', '4242', 12, 2099, $4)`,
		paymentMethodID,
		accountID,
		"pm_"+paymentMethodID.String(),
		deletedAt,
	)
	require.NoError(t, err)
	return paymentMethodID
}

func seedAutoConfig(t *testing.T, pool *pgxpool.Pool, accountID, paymentMethodID uuid.UUID, threshold int64, enabled bool) {
	t.Helper()
	_, err := pool.Exec(context.Background(),
		`INSERT INTO ms_billing.credit_auto_topup_configs
		   (account_id, enabled, threshold_micros, amount_micros, payment_method_id)
		 VALUES ($1, $2, $3, 5000000, $4)`,
		accountID, enabled, threshold, paymentMethodID,
	)
	require.NoError(t, err)
}

func seedEligibleAutoTopUp(t *testing.T, pool *pgxpool.Pool, threshold int64, enabled bool) (uuid.UUID, uuid.UUID) {
	t.Helper()
	accountID := seedAutoAccount(t, pool)
	paymentMethodID := seedAutoPaymentMethod(t, pool, accountID, false)
	seedAutoConfig(t, pool, accountID, paymentMethodID, threshold, enabled)
	return accountID, paymentMethodID
}

func seedSettledGrant(t *testing.T, pool *pgxpool.Pool, accountID uuid.UUID, amountMicros int64) {
	t.Helper()
	_, err := pool.Exec(context.Background(),
		`INSERT INTO ms_billing.credit_ledger
		   (account_id, amount_micros, type, status, balance_after_micros, actor, idempotency_key)
		 VALUES ($1, $2, 'grant', 'settled', $2, 'system', $3)`,
		accountID,
		amountMicros,
		"grant:"+uuid.NewString(),
	)
	require.NoError(t, err)
}

func countAutoTopUps(t *testing.T, pool *pgxpool.Pool, accountID uuid.UUID) int {
	t.Helper()
	var count int
	require.NoError(t, pool.QueryRow(context.Background(),
		`SELECT count(*) FROM ms_billing.credit_ledger
		  WHERE account_id=$1 AND type='auto_topup'`,
		accountID,
	).Scan(&count))
	return count
}
