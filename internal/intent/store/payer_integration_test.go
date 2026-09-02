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

// 🔴 THE SPONSOR HOP, WHICH THE TEST ABOVE CANNOT SEE.
//
// TestASealedPayerResolvesToTheAccountItCameFrom seeds accounts that fund
// THEMSELVES, so it passes whether or not the payer follows the funding
// designation. It is the vacuous-by-default shape: the capability it is named
// for is never exercised.
//
// An org may designate a SPONSOR — another account that pays its invoices
// (migration 041:29,42). ms_billing.account_funding_authorizations carries
// that designation per account, maintained by trigger, defaulting to the
// account itself (migration 052:92-160). The legacy rail charges the Stripe
// customer of THAT account (cycle/domain_charges.go:265, armed via
// db/queries/domains.sql:80-86).
//
// A sponsor-funded org account owns no cards — "the org account owns no cards,
// the sponsor's ..." (cycle/apps.go:300-301). So a payer resolved as the
// ACCOUNT'S OWNER rather than the FUNDER'S OWNER finds no default card and
// every intent against a sponsored org is permanently uncollectable. It fails
// closed, so no wrong party is charged; it also never charges the right one,
// and nothing at the boundary reports it.
//
// This test drives the two halves against each other for the case that
// distinguishes them.
func TestASponsorFundedOrgSealsTheSponsorAsPayer(t *testing.T) {
	pool := testutil.NewTestDB(t)
	s := store.New(pool)
	ctx := context.Background()

	sponsorAccountID, sponsorOwnerID, sponsorCustomerID, sponsorPM := seedPayableAccount(t, pool, "user")
	orgAccountID, orgOwnerID := seedCardlessOrgAccount(t, pool)
	designateSponsor(t, pool, orgAccountID, sponsorAccountID)

	// 1. What a leg seals for the sponsored org.
	payer, err := s.PayerForAccount(ctx, orgAccountID)
	require.NoError(t, err)

	require.Equal(t, "user", payer.Kind,
		"the sponsored org sealed its own owner kind, not the sponsor's")
	require.Equal(t, sponsorOwnerID, payer.ID,
		"the sealed payer is not the SPONSOR. The legacy rail charges the funding "+
			"account (domains.sql:80-86); an intent that names the sponsored org "+
			"names a party with no card.")
	require.NotEqual(t, orgOwnerID, payer.ID,
		"the sealed payer is the sponsored org's own owner — the defect this test exists for")

	// 2. What the executor does with it: the sponsor's real instrument.
	gotCustomer, gotPM, err := s.ResolvePayer(ctx, payer.Kind, payer.ID)
	require.NoErrorf(t, err,
		"the payer sealed for sponsored org account %s does not resolve to a "+
			"chargeable identity — every intent against it is uncollectable", orgAccountID)
	require.Equal(t, sponsorCustomerID, gotCustomer)
	require.Equal(t, sponsorPM, gotPM)
}

// Revoking the sponsorship must hand the account back to its own owner. The
// trigger rewrites the authorization to self-funding (052:139-160); the payer
// must follow it, or a revoked sponsor keeps paying.
func TestRevokingSponsorshipReturnsThePayerToTheOrg(t *testing.T) {
	pool := testutil.NewTestDB(t)
	s := store.New(pool)
	ctx := context.Background()

	sponsorAccountID, sponsorOwnerID, _, _ := seedPayableAccount(t, pool, "user")
	orgAccountID, orgOwnerID := seedCardlessOrgAccount(t, pool)
	designateSponsor(t, pool, orgAccountID, sponsorAccountID)

	before, err := s.PayerForAccount(ctx, orgAccountID)
	require.NoError(t, err)
	require.Equal(t, sponsorOwnerID, before.ID)

	var orgID string
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT owner_org_id::text FROM ms_billing.accounts WHERE id = $1`,
		orgAccountID).Scan(&orgID))
	_, err = pool.Exec(ctx,
		`UPDATE ms_billing.org_billing_designations
		    SET funding = 'org', sponsor_account_id = NULL, sponsor_user_id = NULL
		  WHERE org_id = $1`, orgID)
	require.NoError(t, err)

	after, err := s.PayerForAccount(ctx, orgAccountID)
	require.NoError(t, err)
	require.Equal(t, "org", after.Kind)
	require.Equal(t, orgOwnerID, after.ID,
		"a revoked sponsor is still the sealed payer — the designation was not followed")
}

// An account with no funding authorization row resolves to its own owner.
//
// The trigger makes this unreachable in practice — every account gets a row on
// insert, defaulting to itself (052:92-124) — so without this test the LEFT
// JOIN's fallback is an unexercised branch, which is how a clause comes to be
// named for a check it does not perform. Deleting the row directly is the only
// way to reach it.
//
// Self-funding is also the safe answer rather than merely the convenient one.
// A missing row cannot silently move money to a party the database never
// named: it leaves the charge with the account's own owner, and if that
// account was in fact sponsored and holds no card, ResolvePayer refuses.
func TestAnAccountWithNoFundingAuthorizationFundsItself(t *testing.T) {
	pool := testutil.NewTestDB(t)
	s := store.New(pool)
	ctx := context.Background()

	accountID, ownerID, customerID, pmID := seedPayableAccount(t, pool, "user")

	tag, err := pool.Exec(ctx,
		`DELETE FROM ms_billing.account_funding_authorizations WHERE account_id = $1`,
		accountID)
	require.NoError(t, err)
	require.EqualValues(t, 1, tag.RowsAffected(),
		"there was no authorization row to delete, so this test would exercise the "+
			"ordinary path and prove nothing about the fallback")

	payer, err := s.PayerForAccount(ctx, accountID)
	require.NoError(t, err,
		"an account whose authorization row is missing resolved to no payer at all")
	require.Equal(t, "user", payer.Kind)
	require.Equal(t, ownerID, payer.ID)

	gotCustomer, gotPM, err := s.ResolvePayer(ctx, payer.Kind, payer.ID)
	require.NoError(t, err)
	require.Equal(t, customerID, gotCustomer)
	require.Equal(t, pmID, gotPM)
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

// seedCardlessOrgAccount is an org-owned account with NO payment method — the
// shape a sponsor-funded org actually has (cycle/apps.go:300-301). Giving it a
// card would hide the defect: ResolvePayer would succeed on the org and the
// sponsor hop would never be exercised.
func seedCardlessOrgAccount(t *testing.T, pool *pgxpool.Pool) (string, string) {
	t.Helper()

	var accountID, ownerID string
	require.NoError(t, pool.QueryRow(context.Background(), `
		INSERT INTO ms_billing.accounts (owner_kind, owner_org_id, stripe_customer_id)
		VALUES ('org', gen_random_uuid(), 'cus_' || substr(md5(random()::text), 1, 14))
		RETURNING id::text, owner_org_id::text`).Scan(&accountID, &ownerID))

	var cards int
	require.NoError(t, pool.QueryRow(context.Background(),
		`SELECT count(*) FROM ms_billing.payment_methods_mirror WHERE account_id = $1`,
		accountID).Scan(&cards))
	require.Zero(t, cards, "the sponsored org fixture has a card, which hides the sponsor hop")

	return accountID, ownerID
}

// designateSponsor points an org at a sponsor account and asserts the trigger
// actually rewrote the funding authorization. Asserting it here means a test
// that fails does so because the payer is wrong, not because the fixture
// silently failed to establish the sponsorship.
func designateSponsor(t *testing.T, pool *pgxpool.Pool, orgAccountID, sponsorAccountID string) {
	t.Helper()
	ctx := context.Background()

	var orgID, sponsorOwnerID string
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT owner_org_id::text FROM ms_billing.accounts WHERE id = $1`,
		orgAccountID).Scan(&orgID))
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT owner_user_id::text FROM ms_billing.accounts WHERE id = $1`,
		sponsorAccountID).Scan(&sponsorOwnerID))

	_, err := pool.Exec(ctx, `
		INSERT INTO ms_billing.org_billing_designations
		  (org_id, funding, sponsor_account_id, sponsor_user_id, updated_by)
		VALUES ($1, 'sponsor', $2, $3, gen_random_uuid())
		ON CONFLICT (org_id) DO UPDATE SET
		  funding = 'sponsor',
		  sponsor_account_id = EXCLUDED.sponsor_account_id,
		  sponsor_user_id = EXCLUDED.sponsor_user_id`,
		orgID, sponsorAccountID, sponsorOwnerID)
	require.NoError(t, err)

	var funder string
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT funding_account_id::text FROM ms_billing.account_funding_authorizations
		  WHERE account_id = $1`, orgAccountID).Scan(&funder))
	require.Equal(t, sponsorAccountID, funder,
		"the sponsorship fixture did not take: account_funding_authorizations still "+
			"points elsewhere, so the test below would prove nothing")
}
