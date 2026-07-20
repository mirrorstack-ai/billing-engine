package cycle_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/mirrorstack-ai/billing-engine/internal/account/billing"
	"github.com/mirrorstack-ai/billing-engine/internal/account/cycle"
)

func TestRegisterDomain_UsesMirroredAppAccountAndIsIdempotent(t *testing.T) {
	store := newFakeStore()
	appID, accountID := uuid.New(), uuid.New()
	store.apps[appID] = cycle.AppMirror{AppID: appID, AccountID: accountID}
	svc := cycle.NewService(store, nil)
	activatedAt := time.Date(2026, 7, 12, 4, 30, 0, 0, time.UTC)

	first, err := svc.RegisterDomain(context.Background(), cycle.RegisterDomainRequest{
		OwnerUserID: uuid.New(),
		AppID:       appID,
		Hostname:    "api.example.com",
		ActivatedAt: activatedAt,
	})
	require.NoError(t, err)
	require.NotEqual(t, uuid.Nil, first.DomainID)
	require.Equal(t, accountID, first.AccountID)
	require.Equal(t, appID, first.AppID)
	require.Equal(t, "api.example.com", first.Hostname)
	require.Equal(t, activatedAt, first.ActivatedAt)
	require.Len(t, store.domains, 1)

	// Fire-and-forget retry: the live-hostname conflict is a no-op and the
	// readback returns the first registration's stable row, even if the retry
	// carries a different activation instant.
	retry, err := svc.RegisterDomain(context.Background(), cycle.RegisterDomainRequest{
		OwnerUserID: uuid.New(),
		AppID:       appID,
		Hostname:    "api.example.com",
		ActivatedAt: activatedAt.Add(48 * time.Hour),
	})
	require.NoError(t, err)
	require.Equal(t, first, retry)
	require.Len(t, store.domains, 1)

	// There is deliberately no domain funding gate: the mirrored app's account
	// has no activation/card fixtures, yet registration succeeds.
	_, activated := store.activation[accountID]
	require.False(t, activated)
}

func TestRegisterDomain_DefaultsActivationToUTCNow(t *testing.T) {
	store := newFakeStore()
	appID, accountID := uuid.New(), uuid.New()
	store.apps[appID] = cycle.AppMirror{AppID: appID, AccountID: accountID}
	taipei := time.FixedZone("UTC+8", 8*60*60)
	now := time.Date(2026, 7, 20, 18, 45, 0, 0, taipei)
	svc := cycle.NewService(store, nil).WithNow(func() time.Time { return now })

	resp, err := svc.RegisterDomain(context.Background(), cycle.RegisterDomainRequest{
		OwnerOrgID: uuid.New(), AppID: appID, Hostname: "www.example.com",
	})
	require.NoError(t, err)
	require.Equal(t, now.UTC(), resp.ActivatedAt)
}

func TestRemoveDomain_IsIdempotentAndKeepsFirstRemovalInstant(t *testing.T) {
	store := newFakeStore()
	appID, accountID := uuid.New(), uuid.New()
	store.apps[appID] = cycle.AppMirror{AppID: appID, AccountID: accountID}
	svc := cycle.NewService(store, nil)
	_, err := svc.RegisterDomain(context.Background(), cycle.RegisterDomainRequest{
		OwnerUserID: uuid.New(), AppID: appID, Hostname: "admin.example.com",
	})
	require.NoError(t, err)

	firstRemovedAt := time.Date(2026, 7, 18, 1, 2, 3, 0, time.UTC)
	resp, err := svc.RemoveDomain(context.Background(), cycle.RemoveDomainRequest{
		AppID: appID, Hostname: "admin.example.com", RemovedAt: firstRemovedAt,
	})
	require.NoError(t, err)
	require.Equal(t, appID, resp.AppID)
	require.Equal(t, "admin.example.com", resp.Hostname)

	_, err = svc.RemoveDomain(context.Background(), cycle.RemoveDomainRequest{
		AppID: appID, Hostname: "admin.example.com", RemovedAt: firstRemovedAt.Add(24 * time.Hour),
	})
	require.NoError(t, err)
	domain, found, err := store.DomainByHostname(context.Background(), "admin.example.com")
	require.NoError(t, err)
	require.True(t, found)
	require.True(t, domain.Removed)
	require.Equal(t, firstRemovedAt, domain.RemovedAt)
}

func TestDomainRPCs_UnknownAppIsNotFound(t *testing.T) {
	svc := cycle.NewService(newFakeStore(), nil)
	_, err := svc.RegisterDomain(context.Background(), cycle.RegisterDomainRequest{
		OwnerUserID: uuid.New(), AppID: uuid.New(), Hostname: "missing.example.com",
	})
	requireCode(t, err, billing.CodeNotFound)

	_, err = svc.RemoveDomain(context.Background(), cycle.RemoveDomainRequest{
		AppID: uuid.New(), Hostname: "missing.example.com",
	})
	requireCode(t, err, billing.CodeNotFound)
}

func TestDomainRPCs_Validation(t *testing.T) {
	svc := cycle.NewService(newFakeStore(), nil)
	for _, tc := range []struct {
		name string
		call func() error
	}{
		{
			"register missing owner",
			func() error {
				_, err := svc.RegisterDomain(context.Background(), cycle.RegisterDomainRequest{AppID: uuid.New(), Hostname: "x.example.com"})
				return err
			},
		},
		{
			"register both owners",
			func() error {
				_, err := svc.RegisterDomain(context.Background(), cycle.RegisterDomainRequest{OwnerUserID: uuid.New(), OwnerOrgID: uuid.New(), AppID: uuid.New(), Hostname: "x.example.com"})
				return err
			},
		},
		{
			"register missing app",
			func() error {
				_, err := svc.RegisterDomain(context.Background(), cycle.RegisterDomainRequest{OwnerUserID: uuid.New(), Hostname: "x.example.com"})
				return err
			},
		},
		{
			"register missing hostname",
			func() error {
				_, err := svc.RegisterDomain(context.Background(), cycle.RegisterDomainRequest{OwnerUserID: uuid.New(), AppID: uuid.New()})
				return err
			},
		},
		{
			"remove missing app",
			func() error {
				_, err := svc.RemoveDomain(context.Background(), cycle.RemoveDomainRequest{Hostname: "x.example.com"})
				return err
			},
		},
		{
			"remove missing hostname",
			func() error {
				_, err := svc.RemoveDomain(context.Background(), cycle.RemoveDomainRequest{AppID: uuid.New()})
				return err
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			requireCode(t, tc.call(), billing.CodeInvalidInput)
		})
	}
}

func TestDomainRPCs_StoreErrorsAreInternal(t *testing.T) {
	appID, accountID := uuid.New(), uuid.New()
	req := cycle.RegisterDomainRequest{
		OwnerUserID: uuid.New(), AppID: appID, Hostname: "errors.example.com",
	}

	t.Run("app lookup", func(t *testing.T) {
		store := newFakeStore()
		store.errAppMirror = errors.New("lookup down")
		_, err := cycle.NewService(store, nil).RegisterDomain(context.Background(), req)
		requireCode(t, err, billing.CodeInternal)
	})

	t.Run("insert", func(t *testing.T) {
		store := newFakeStore()
		store.apps[appID] = cycle.AppMirror{AppID: appID, AccountID: accountID}
		store.errDomainInsert = errors.New("insert down")
		_, err := cycle.NewService(store, nil).RegisterDomain(context.Background(), req)
		requireCode(t, err, billing.CodeInternal)
	})

	t.Run("readback", func(t *testing.T) {
		store := newFakeStore()
		store.apps[appID] = cycle.AppMirror{AppID: appID, AccountID: accountID}
		store.errDomainLookup = errors.New("read down")
		_, err := cycle.NewService(store, nil).RegisterDomain(context.Background(), req)
		requireCode(t, err, billing.CodeInternal)
	})

	t.Run("remove", func(t *testing.T) {
		store := newFakeStore()
		store.apps[appID] = cycle.AppMirror{AppID: appID, AccountID: accountID}
		store.errDomainRemove = errors.New("update down")
		_, err := cycle.NewService(store, nil).RemoveDomain(context.Background(), cycle.RemoveDomainRequest{
			AppID: appID, Hostname: "errors.example.com",
		})
		requireCode(t, err, billing.CodeInternal)
	})
}
