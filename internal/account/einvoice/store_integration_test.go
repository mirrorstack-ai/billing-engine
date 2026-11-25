//go:build integration

package einvoice_test

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/mirrorstack-ai/billing-engine/internal/account/einvoice"
	"github.com/mirrorstack-ai/billing-engine/internal/shared/testutil"
)

func seedAccount(t *testing.T, pool *pgxpool.Pool) uuid.UUID {
	t.Helper()
	id := uuid.New()
	_, err := pool.Exec(context.Background(),
		`INSERT INTO ms_billing.accounts (id, owner_kind, owner_user_id) VALUES ($1, 'user', $2)`,
		id.String(), uuid.NewString())
	require.NoError(t, err)
	return id
}

func TestStore_GetProfile_NoneStored(t *testing.T) {
	pool := testutil.NewTestDB(t)
	store := einvoice.NewStore(pool)
	accountID := seedAccount(t, pool)

	_, found, err := store.GetProfile(context.Background(), accountID)
	require.NoError(t, err)
	require.False(t, found)
}

func TestStore_PutProfile_ThenGetRoundTrips(t *testing.T) {
	pool := testutil.NewTestDB(t)
	store := einvoice.NewStore(pool)
	accountID := seedAccount(t, pool)

	written, err := store.PutProfile(context.Background(), einvoice.Profile{
		AccountID:   accountID,
		Kind:        einvoice.KindCompany,
		TaxID:       "04595257",
		CompanyName: "Acme Co",
		Email:       "ap@acme.example",
	})
	require.NoError(t, err)
	require.Equal(t, einvoice.KindCompany, written.Kind)

	got, found, err := store.GetProfile(context.Background(), accountID)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, accountID, got.AccountID)
	require.Equal(t, einvoice.KindCompany, got.Kind)
	require.Equal(t, "04595257", got.TaxID)
	require.Equal(t, "Acme Co", got.CompanyName)
	require.Equal(t, "ap@acme.example", got.Email)
	require.Empty(t, got.CarrierType)
	require.Empty(t, got.DonationCode)
}

// TestStore_PutProfile_UpsertReplacesTheWholeRow pins PutBuyerInvoiceProfile's
// documented contract: every column is overwritten by a re-put, including
// clearing a field the caller omits this time (no partial update).
func TestStore_PutProfile_UpsertReplacesTheWholeRow(t *testing.T) {
	pool := testutil.NewTestDB(t)
	store := einvoice.NewStore(pool)
	accountID := seedAccount(t, pool)

	_, err := store.PutProfile(context.Background(), einvoice.Profile{
		AccountID:   accountID,
		Kind:        einvoice.KindPersonal,
		CarrierType: einvoice.CarrierMobileBarcode,
		CarrierNum:  "/ABC123.",
		Email:       "buyer@example.com",
	})
	require.NoError(t, err)

	// Re-put without a carrier: the prior carrier must be cleared, not kept.
	_, err = store.PutProfile(context.Background(), einvoice.Profile{
		AccountID: accountID,
		Kind:      einvoice.KindPersonal,
		Email:     "buyer@example.com",
	})
	require.NoError(t, err)

	got, found, err := store.GetProfile(context.Background(), accountID)
	require.NoError(t, err)
	require.True(t, found)
	require.Empty(t, got.CarrierType)
	require.Empty(t, got.CarrierNum)
	require.Equal(t, "buyer@example.com", got.Email)
}

func TestStore_PutProfile_DeletedWithTheAccount(t *testing.T) {
	pool := testutil.NewTestDB(t)
	store := einvoice.NewStore(pool)
	accountID := seedAccount(t, pool)

	_, err := store.PutProfile(context.Background(), einvoice.Profile{
		AccountID: accountID,
		Kind:      einvoice.KindPersonal,
	})
	require.NoError(t, err)

	_, err = pool.Exec(context.Background(), `DELETE FROM ms_billing.accounts WHERE id = $1`, accountID.String())
	require.NoError(t, err)

	_, found, err := store.GetProfile(context.Background(), accountID)
	require.NoError(t, err)
	require.False(t, found, "the profile must cascade-delete with its account")
}
