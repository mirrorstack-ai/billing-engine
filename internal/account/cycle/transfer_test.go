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

// transferNow is one pinned instant. Nothing in these tests derives a window
// from the calendar: a fixture that depends on the date passes on luck, and
// this repository has already lost a day to exactly that (migration-era credit
// fixtures pinned to 2026-09-04).
var transferNow = time.Date(2026, time.August, 15, 12, 0, 0, 0, time.UTC)

func transferService(t *testing.T) (*cycle.Service, *fakeStore) {
	t.Helper()
	store := newFakeStore()
	svc := cycle.NewService(store, nil).WithNow(func() time.Time { return transferNow })
	return svc, store
}

func validTransfer() cycle.TransferAppRequest {
	return cycle.TransferAppRequest{
		AppID:       uuid.New(),
		OwnerUserID: uuid.New(),
		Mode:        cycle.TransferModeKeep,
		RequestID:   uuid.New(),
	}
}

// The request shape is the api-platform contract. Each refusal here is a call
// that must NEVER reach the store, because every one of them would otherwise
// re-key an app on incomplete instructions.
func TestTransferAppRejectsMalformedRequests(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*cycle.TransferAppRequest)
		want   string
	}{
		{"no app", func(r *cycle.TransferAppRequest) { r.AppID = uuid.Nil }, "app_id required"},
		{"no request id", func(r *cycle.TransferAppRequest) { r.RequestID = uuid.Nil }, "request_id required"},
		{"no owner", func(r *cycle.TransferAppRequest) { r.OwnerUserID = uuid.Nil }, "owner_user_id or owner_org_id required"},
		{"both owners", func(r *cycle.TransferAppRequest) { r.OwnerOrgID = uuid.New() }, "mutually exclusive"},
		// 🔴 mode is NOT defaulted. keep and move bill different accounts, so a
		// caller that omitted it has not chosen — defaulting would silently
		// pick one of two money outcomes on their behalf.
		{"no mode", func(r *cycle.TransferAppRequest) { r.Mode = "" }, `mode must be "keep" or "move"`},
		{"unknown mode", func(r *cycle.TransferAppRequest) { r.Mode = "migrate" }, `mode must be "keep" or "move"`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			svc, store := transferService(t)
			req := validTransfer()
			tc.mutate(&req)

			_, err := svc.TransferApp(context.Background(), req)

			require.Error(t, err)
			require.Contains(t, err.Error(), tc.want)
			require.Empty(t, store.transferCalls, "a malformed request reached the store")
		})
	}
}

// The store's refusals are not errors; they are outcomes. This pins the mapping
// onto the closed billing.Code set, because api-platform matches on the CODE
// and reads the token only as a message prefix.
func TestTransferAppMapsStoreOutcomesToWireCodes(t *testing.T) {
	cases := []struct {
		name    string
		outcome cycle.TransferOutcome
		code    billing.Code
		token   string
	}{
		{"unknown app", cycle.TransferAppUnknown, billing.CodeNotFound, "app_unknown"},
		{"replayed key, different target", cycle.TransferRequestConflict, billing.CodeConflict, "app_transfer_conflict"},
		// The money refusal: a one-time charge is still owed by the old owner,
		// who is about to settle it, and re-keying now would bill it to the
		// new account. (When the old owner CANNOT settle it, the store
		// forfeits instead and this outcome never surfaces.)
		{"one-time charge pending", cycle.TransferChargesPending, billing.CodeConflict, "app_transfer_charges_pending"},
		{"period closed under the move", cycle.TransferPeriodClosed, billing.CodeConflict, "app_transfer_period_closed"},
		// The backlog refusal: NULL-account usage for this app is reachable
		// only through the roster column the transfer rewrites.
		{"unbilled backlog", cycle.TransferUnbilledBacklog, billing.CodeConflict, "app_transfer_unbilled_backlog"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			svc, store := transferService(t)
			store.transferFn = func(context.Context, cycle.TransferAppParams) (*cycle.TransferAppResponse, cycle.TransferOutcome, error) {
				return nil, tc.outcome, nil
			}

			_, err := svc.TransferApp(context.Background(), validTransfer())

			require.Error(t, err)
			var be *billing.Error
			require.True(t, errors.As(err, &be), "not a billing.Error: %v", err)
			require.Equal(t, tc.code, be.Code)
			require.Contains(t, err.Error(), tc.token)
		})
	}
}

// The transfer instant is the service clock, not the caller's, and it is the
// upper bound of the move window — so a wrong clock re-attributes the wrong
// events.
func TestTransferAppStampsTheServiceClock(t *testing.T) {
	svc, store := transferService(t)

	_, err := svc.TransferApp(context.Background(), validTransfer())
	require.NoError(t, err)

	require.Len(t, store.transferCalls, 1)
	require.Equal(t, transferNow, store.transferCalls[0].At)
}

// 🔴 The destination is resolved WITHOUT a funding gate, on purpose. Refusing
// to CREATE an app when the owner cannot pay is cheap and reversible; refusing
// to TRANSFER one strands an existing, accruing app on an owner who has asked
// to stop paying for it. The owner ruled on 2026-09-04 that an unfunded
// destination is allowed and the serving-block handles it.
func TestTransferAppAcceptsAnUnfundedDestination(t *testing.T) {
	svc, store := transferService(t)
	target := uuid.New()
	store.ensureUserAccountID = target

	resp, err := svc.TransferApp(context.Background(), validTransfer())

	require.NoError(t, err)
	require.Equal(t, target, resp.AccountID)
	require.Len(t, store.transferCalls, 1)
	require.Equal(t, target, store.transferCalls[0].ToAccount)
}

// 🔴 The service adds nothing to the store's answer. On a replay the store
// returns the STORED window and recurring_from, and the service must pass them
// through untouched — a service that "helpfully" re-derived either from its
// own clock would turn a verbatim replay back into a recomputation, which is
// the defect the ledger columns exist to prevent. The fixture's window is
// deliberately NOT the one transferNow would produce, so a recomputation is
// visible.
func TestTransferAppReturnsTheStoreResultVerbatim(t *testing.T) {
	svc, store := transferService(t)
	stored := &cycle.TransferAppResponse{
		AccountID:       uuid.New(),
		MovedEventCount: 7,
		OpenPeriod: cycle.TransferPeriod{
			Start: time.Date(2026, time.July, 10, 0, 0, 0, 0, time.UTC),
			End:   time.Date(2026, time.August, 10, 0, 0, 0, 0, time.UTC),
		},
		RecurringFrom: time.Date(2026, time.August, 10, 0, 0, 0, 0, time.UTC),
	}
	require.True(t, stored.OpenPeriod.End.Before(transferNow),
		"fixture error: the stored window must already be closed at the service clock, or a recomputation would be indistinguishable")
	store.transferFn = func(context.Context, cycle.TransferAppParams) (*cycle.TransferAppResponse, cycle.TransferOutcome, error) {
		return stored, cycle.TransferAlreadyApplied, nil
	}

	got, err := svc.TransferApp(context.Background(), validTransfer())

	require.NoError(t, err)
	require.Equal(t, stored, got)
}

// 🔴 The wallet rail reaches the store as the service's OWN predicate, never
// as a copied flag: creditWalletRailEnabled is the schema flag AND the
// per-account rollout decision, and the refuse/forfeit classification has to
// ask exactly what the proration and overage legs ask. With no rollout
// controller installed the answer is the schema flag, which is what these
// two cases read.
func TestTransferAppPassesTheWalletRailPredicate(t *testing.T) {
	for _, tc := range []struct {
		name    string
		enabled bool
	}{{"rail off", false}, {"rail on", true}} {
		t.Run(tc.name, func(t *testing.T) {
			svc, store := transferService(t)
			svc = svc.WithCreditWallet(tc.enabled)

			_, err := svc.TransferApp(context.Background(), validTransfer())
			require.NoError(t, err)

			require.Len(t, store.transferCalls, 1)
			rail := store.transferCalls[0].CreditWalletRail
			require.NotNil(t, rail, "the store was not handed the wallet rail predicate")
			require.Equal(t, tc.enabled, rail(uuid.New()))
		})
	}
}
