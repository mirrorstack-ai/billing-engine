//go:build integration

package cycle_test

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/mirrorstack-ai/billing-engine/internal/shared/testutil"
)

// Migration 093 adds ms_billing.buyer_invoice_profiles (billing-engine#285):
// the rail-neutral buyer invoice profile (統編, carrier, 愛心碼). These tests
// pin its CHECK constraints directly against a real Postgres — the claims are
// about what the DATABASE refuses, independent of any Go-side validator
// (internal/account/einvoice.Validate mirrors the same rules, but a future
// writer that skips it must still be refused here).

func insertProfile(ctx context.Context, pool *pgxpool.Pool, accountID, kind, taxID, companyName, carrierType, carrierNum, donationCode string) error {
	_, err := pool.Exec(ctx,
		`INSERT INTO ms_billing.buyer_invoice_profiles
		   (account_id, kind, tax_id, company_name, carrier_type, carrier_num, donation_code)
		 VALUES ($1, $2, NULLIF($3,''), NULLIF($4,''), NULLIF($5,''), NULLIF($6,''), NULLIF($7,''))`,
		accountID, kind, taxID, companyName, carrierType, carrierNum, donationCode)
	return err
}

func TestMigration093_CompanyRequiresAValidTaxID(t *testing.T) {
	pool := testutil.NewTestDB(t)
	ctx := context.Background()

	bad := seedAccount(t, pool)
	err := insertProfile(ctx, pool, bad.String(), "company", "12345678", "Acme Co", "", "", "")
	require.Error(t, err, "a company profile with a checksum-invalid 統編 must be refused")

	noTaxID := seedAccount(t, pool)
	err = insertProfile(ctx, pool, noTaxID.String(), "company", "", "Acme Co", "", "", "")
	require.Error(t, err, "a company profile with no 統編 at all must be refused")

	noName := seedAccount(t, pool)
	err = insertProfile(ctx, pool, noName.String(), "company", "04595257", "", "", "", "")
	require.Error(t, err, "a company profile with no company_name must be refused")

	good := seedAccount(t, pool)
	err = insertProfile(ctx, pool, good.String(), "company", "04595257", "Acme Co", "", "", "")
	require.NoError(t, err, "a company profile with a valid checksummed 統編 and a name must be accepted")
}

func TestMigration093_CarrierAndDonationCodeAreMutuallyExclusive(t *testing.T) {
	pool := testutil.NewTestDB(t)
	ctx := context.Background()

	acct := seedAccount(t, pool)
	err := insertProfile(ctx, pool, acct.String(), "personal", "", "", "mobile_barcode", "/ABC123.", "123")
	require.Error(t, err, "a profile with both a carrier and a donation code must be refused")
}

func TestMigration093_PersonalProfileRefusesATaxID(t *testing.T) {
	pool := testutil.NewTestDB(t)
	ctx := context.Background()

	acct := seedAccount(t, pool)
	err := insertProfile(ctx, pool, acct.String(), "personal", "04595257", "", "", "", "")
	require.Error(t, err, "a personal profile with a 統編 must be refused")
}

func TestMigration093_MobileBarcodeShapeEnforced(t *testing.T) {
	pool := testutil.NewTestDB(t)
	ctx := context.Background()

	bad := seedAccount(t, pool)
	err := insertProfile(ctx, pool, bad.String(), "personal", "", "", "mobile_barcode", "NOPE", "")
	require.Error(t, err, "a malformed mobile barcode carrier number must be refused")

	good := seedAccount(t, pool)
	err = insertProfile(ctx, pool, good.String(), "personal", "", "", "mobile_barcode", "/ABC123.", "")
	require.NoError(t, err, "a well-formed mobile barcode carrier number must be accepted")
}

func TestMigration093_DonationCodeShapeEnforced(t *testing.T) {
	pool := testutil.NewTestDB(t)
	ctx := context.Background()

	bad := seedAccount(t, pool)
	err := insertProfile(ctx, pool, bad.String(), "personal", "", "", "", "", "12")
	require.Error(t, err, "a 2-digit donation code must be refused")

	good := seedAccount(t, pool)
	err = insertProfile(ctx, pool, good.String(), "personal", "", "", "", "", "123")
	require.NoError(t, err, "a 3-digit donation code must be accepted")
}

func TestMigration093_ProfileCascadeDeletesWithItsAccount(t *testing.T) {
	pool := testutil.NewTestDB(t)
	ctx := context.Background()

	acct := seedAccount(t, pool)
	require.NoError(t, insertProfile(ctx, pool, acct.String(), "personal", "", "", "", "", ""))

	_, err := pool.Exec(ctx, `DELETE FROM ms_billing.accounts WHERE id = $1`, acct.String())
	require.NoError(t, err)

	var count int
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT count(*) FROM ms_billing.buyer_invoice_profiles WHERE account_id = $1`, acct.String()).Scan(&count))
	require.Equal(t, 0, count, "the profile must cascade-delete with its account")
}

func TestMigration093_DownRemovesTheTableAndChecksumFunction(t *testing.T) {
	pool := testutil.NewTestDB(t) // 093.up already applied
	ctx := context.Background()

	down := migrationSQL(t, "093_buyer_invoice_profiles.down.sql")
	_, err := pool.Exec(ctx, down)
	require.NoError(t, err)

	var tableExists bool
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'ms_billing' AND table_name = 'buyer_invoice_profiles')`).Scan(&tableExists))
	require.False(t, tableExists, "down must drop the table")

	var funcExists bool
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT EXISTS (SELECT 1 FROM pg_proc WHERE proname = 'taiwan_tax_id_checksum_valid')`).Scan(&funcExists))
	require.False(t, funcExists, "down must drop the checksum function")

	// up again — the round trip must be clean (no leftover state blocking a re-apply).
	up := migrationSQL(t, "093_buyer_invoice_profiles.up.sql")
	_, err = pool.Exec(ctx, up)
	require.NoError(t, err)
}
