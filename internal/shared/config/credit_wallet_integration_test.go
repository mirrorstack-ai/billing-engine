//go:build integration

package config

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/mirrorstack-ai/billing-engine/internal/shared/testutil"
)

func TestCreditRuntimeSchemaReadyRequiresMigrations048And049(t *testing.T) {
	pool := testutil.NewTestDB(t)
	ctx := context.Background()

	ready, err := CreditRuntimeSchemaReady(ctx, pool)
	require.NoError(t, err)
	require.True(t, ready)

	_, err = pool.Exec(
		ctx,
		readBillingMigration(t, "049_credit_auto_topup_attempts.down.sql"),
	)
	require.NoError(t, err)

	baseReady, err := CreditWalletSchemaReady(ctx, pool)
	require.NoError(t, err)
	require.True(t, baseReady, "migration 048 remains ready after rolling back 049")

	ready, err = CreditRuntimeSchemaReady(ctx, pool)
	require.NoError(t, err)
	require.False(t, ready, "runtime readiness requires migration 049 as well as 048")

	_, err = pool.Exec(
		ctx,
		readBillingMigration(t, "049_credit_auto_topup_attempts.up.sql"),
	)
	require.NoError(t, err)

	ready, err = CreditRuntimeSchemaReady(ctx, pool)
	require.NoError(t, err)
	require.True(t, ready)
}

func readBillingMigration(t *testing.T, name string) string {
	t.Helper()
	dir, err := os.Getwd()
	require.NoError(t, err)
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			break
		}
		parent := filepath.Dir(dir)
		require.NotEqual(t, dir, parent, "go.mod not found above %s", dir)
		dir = parent
	}
	body, err := os.ReadFile(filepath.Join(dir, "migrations", "billing", name))
	require.NoError(t, err)
	return string(body)
}
