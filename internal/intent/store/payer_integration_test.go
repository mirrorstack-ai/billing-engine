//go:build integration

package store_test

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/mirrorstack-ai/billing-engine/internal/intent/store"
	"github.com/mirrorstack-ai/billing-engine/internal/shared/testutil"
)

// 🔴 THE TEST THAT WOULD HAVE CAUGHT IT.
//
// A sealed intent names a payer; something later turns that payer into a
// Stripe customer and an instrument. Until 2026-08-31 those two halves lived
// in different packages and disagreed: the three cut-over legs sealed
// intent.Subject{Kind:"user", ID: <ms_billing.accounts.id>} and the executor's
// resolver matched a.owner_user_id. accounts.id is a gen_random_uuid() primary
// key; owner_user_id is a soft FK to ms_account.users.id (migration 001:18-25).
// They are never equal.
//
// So EVERY intent this tree could produce was uncollectable, and nothing
// failed — because no deployment has ever executed one. Each half was
// internally consistent and they were never run against each other.
//
// This runs them against each other: seed an account with a default card,
// produce the subject a leg seals, and require the resolver to find that
// account's customer and instrument. Nothing else in the tree closes that
// loop.
func TestASealedPayerResolvesToTheAccountItCameFrom(t *testing.T) {
	pool := testutil.NewTestDB(t)
	s := store.New(pool)
	ctx := context.Background()

	for _, tc := range []struct {
		name      string
		ownerKind string
	}{
		{"user-owned account", "user"},
		{"org-owned account", "org"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			accountID, ownerID, customerID, pmID := seedPayableAccount(t, pool, tc.ownerKind)

			// 1. What a leg seals, via the proposer's resolution.
			payer, err := s.PayerForAccount(ctx, accountID)
			require.NoError(t, err)

			require.Equal(t, tc.ownerKind, payer.Kind,
				"the sealed payer kind does not match the account's owner kind")
			require.Equal(t, ownerID, payer.ID,
				"the sealed payer is not the account's owner")
			require.NotEqual(t, accountID, payer.ID,
				"the sealed payer is the ACCOUNT id. The resolver matches on owner id, so "+
					"this intent could never be collected — which is exactly the defect "+
					"this test exists for, and a fixture whose owner equalled its account "+
					"id could not see it.")

			// 2. What the executor does with it.
			gotCustomer, gotPM, err := s.ResolvePayer(ctx, payer.Kind, payer.ID)
			require.NoErrorf(t, err,
				"the payer a leg seals does not resolve to a chargeable identity. "+
					"Every intent produced against account %s is uncollectable.", accountID)
			require.Equal(t, customerID, gotCustomer)
			require.Equal(t, pmID, gotPM)
		})
	}
}

// The old shape must NOT resolve, or the test above would pass for the wrong
// reason on a database where account id and owner id happened to coincide.
func TestTheOldAccountIDPayerShapeDoesNotResolve(t *testing.T) {
	pool := testutil.NewTestDB(t)
	s := store.New(pool)

	accountID, _, _, _ := seedPayableAccount(t, pool, "user")

	_, _, err := s.ResolvePayer(context.Background(), "user", accountID)
	require.ErrorIs(t, err, store.ErrNoUsableCard,
		"an ACCOUNT id resolved as a payer. That would mean the two halves agree by "+
			"accident, and this suite could no longer tell the defect from the fix.")
}

// A payer with no default card, or whose card was removed, must not resolve.
// The executor refuses rather than charging an instrument the customer did
// not choose — a customer who removed a card removed it.
func TestAPayerWithNoUsableCardDoesNotResolve(t *testing.T) {
	pool := testutil.NewTestDB(t)
	s := store.New(pool)
	ctx := context.Background()

	accountID, ownerID, _, _ := seedPayableAccount(t, pool, "user")

	_, err := pool.Exec(ctx,
		`UPDATE ms_billing.payment_methods_mirror SET deleted_at = now() WHERE account_id = $1`,
		accountID)
	require.NoError(t, err)

	_, _, err = s.ResolvePayer(ctx, "user", ownerID)
	require.ErrorIs(t, err, store.ErrNoUsableCard)
}

// An account nothing owns must not produce a payer at all. A charge against
// nobody must not seal.
func TestAnAccountWithNoOwnerProducesNoPayer(t *testing.T) {
	pool := testutil.NewTestDB(t)
	s := store.New(pool)
	ctx := context.Background()

	var accountID string
	require.NoError(t, pool.QueryRow(ctx, `
		INSERT INTO ms_billing.accounts (owner_kind, owner_user_id)
		VALUES ('user', gen_random_uuid())
		RETURNING id::text`).Scan(&accountID))

	// The polymorphic-owner CHECK is what normally prevents this; reaching
	// the Go guard takes a row that got past it.
	_, err := pool.Exec(ctx,
		`UPDATE ms_billing.accounts SET owner_user_id = NULL WHERE id = $1`, accountID)
	if err != nil {
		t.Skipf("the schema refuses an ownerless account, which is the stronger control: %v", err)
	}

	_, err = s.PayerForAccount(ctx, accountID)
	require.ErrorIs(t, err, store.ErrNoSuchAccount)
}

func TestAPayerForAnAccountThatDoesNotExist(t *testing.T) {
	pool := testutil.NewTestDB(t)
	s := store.New(pool)
	_, err := s.PayerForAccount(context.Background(),
		"00000000-0000-0000-0000-000000000000")
	require.ErrorIs(t, err, store.ErrNoSuchAccount)
}

// seedPayableAccount creates an account with a default card and returns
// (accountID, ownerID, stripeCustomerID, stripePaymentMethodID).
//
// The owner id is generated independently of the account id, which is the
// point: they must differ, or the round trip above proves nothing.
func seedPayableAccount(t *testing.T, pool *pgxpool.Pool, ownerKind string) (string, string, string, string) {
	t.Helper()
	ctx := context.Background()

	ownerColumn := "owner_user_id"
	if ownerKind == "org" {
		ownerColumn = "owner_org_id"
	}

	var accountID, ownerID, customerID string
	require.NoError(t, pool.QueryRow(ctx, `
		INSERT INTO ms_billing.accounts (owner_kind, `+ownerColumn+`, stripe_customer_id)
		VALUES ($1, gen_random_uuid(), 'cus_' || substr(md5(random()::text), 1, 14))
		RETURNING id::text, `+ownerColumn+`::text, stripe_customer_id`,
		ownerKind).Scan(&accountID, &ownerID, &customerID))

	require.NotEqual(t, accountID, ownerID,
		"the fixture generated an owner id equal to the account id, which would make "+
			"the defect invisible")

	var pmID string
	require.NoError(t, pool.QueryRow(ctx, `
		INSERT INTO ms_billing.payment_methods_mirror
		  (account_id, stripe_payment_method_id, brand, last4, exp_month, exp_year, is_default)
		VALUES ($1, 'pm_' || substr(md5(random()::text), 1, 14), 'visa', '4242', 12, 2030, true)
		RETURNING stripe_payment_method_id`, accountID).Scan(&pmID))

	return accountID, ownerID, customerID, pmID
}
