//go:build integration

package legacyrestamp_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/mirrorstack-ai/billing-engine/internal/account/legacyrestamp"
	"github.com/mirrorstack-ai/billing-engine/internal/shared/testutil"
)

func TestSourceIntegration_AllOwnersKeysetSchema047AndSessionMutex(t *testing.T) {
	pool := testutil.NewTestDB(t)
	ctx := context.Background()
	accountOne, accountTwo := orderedUUID(1), orderedUUID(2)
	userID, orgID := orderedUUID(101), orderedUUID(102)
	_, err := pool.Exec(ctx, `
		INSERT INTO ms_billing.accounts (
			id, owner_kind, owner_user_id, billing_mode
		) VALUES ($1, 'user', $2, 'standard')
	`, accountOne, userID)
	require.NoError(t, err)
	_, err = pool.Exec(ctx, `
		INSERT INTO ms_billing.accounts (
			id, owner_kind, owner_org_id, billing_mode
		) VALUES ($1, 'org', $2, 'credits')
	`, accountTwo, orgID)
	require.NoError(t, err)

	source := legacyrestamp.NewSource(pool)
	full, acquired, err := source.TryBegin(ctx)
	require.NoError(t, err)
	require.True(t, acquired)
	total, err := full.CountOwners(ctx)
	require.NoError(t, err)
	require.EqualValues(t, 2, total)
	first, err := full.ListOwners(ctx, uuid.Nil, 1)
	require.NoError(t, err)
	require.Equal(t, []legacyrestamp.Owner{{
		AccountID: accountOne,
		UserID:    userID,
	}}, first)
	second, err := full.ListOwners(ctx, accountOne, 1)
	require.NoError(t, err)
	require.Equal(t, []legacyrestamp.Owner{{
		AccountID: accountTwo,
		OrgID:     orgID,
	}}, second, "credits and standard rows are both enumerated")
	require.NoError(t, full.Close())

	// Rehearse the exact schema-047 state. The restamp source must still work
	// after every 048/049 object is gone because its SQL names accounts owner
	// columns only.
	applyDownMigration(t, pool, "049_credit_auto_topup_attempts.down.sql")
	applyDownMigration(t, pool, "048_credit_wallet.down.sql")
	var (
		ledgerExists bool
		modeExists   bool
	)
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT
			to_regclass('ms_billing.credit_ledger') IS NOT NULL,
			EXISTS (
				SELECT 1
				FROM pg_catalog.pg_namespace AS namespace
				JOIN pg_catalog.pg_class AS relation
				  ON relation.relnamespace = namespace.oid
				JOIN pg_catalog.pg_attribute AS attribute
				  ON attribute.attrelid = relation.oid
				WHERE namespace.nspname = 'ms_billing'
				  AND relation.relname = 'accounts'
				  AND attribute.attname = 'billing_mode'
				  AND attribute.attnum > 0
				  AND NOT attribute.attisdropped
			)
	`).Scan(&ledgerExists, &modeExists))
	require.False(t, ledgerExists)
	require.False(t, modeExists)

	firstPass, acquired, err := source.TryBegin(ctx)
	require.NoError(t, err)
	require.True(t, acquired)
	concurrent, acquired, err := source.TryBegin(ctx)
	require.NoError(t, err)
	require.False(t, acquired, "a second database session cannot enter the pass")
	require.Nil(t, concurrent)

	total, err = firstPass.CountOwners(ctx)
	require.NoError(t, err)
	require.EqualValues(t, 2, total,
		"the workflow receipt count remains schema-047-compatible")
	owners, err := firstPass.ListOwners(ctx, uuid.Nil, 10)
	require.NoError(t, err)
	require.Equal(t, []legacyrestamp.Owner{
		{AccountID: accountOne, UserID: userID},
		{AccountID: accountTwo, OrgID: orgID},
	}, owners)
	require.NoError(t, firstPass.Close())

	afterClose, acquired, err := source.TryBegin(ctx)
	require.NoError(t, err)
	require.True(t, acquired, "Close unlocks on the same session")
	require.NoError(t, afterClose.Close())
}

func applyDownMigration(
	t *testing.T,
	pool *pgxpool.Pool,
	name string,
) {
	t.Helper()
	path := filepath.Join("..", "..", "..", "migrations", "billing", name)
	body, err := os.ReadFile(path)
	require.NoError(t, err)
	_, err = pool.Exec(context.Background(), string(body))
	require.NoError(t, err)
}
