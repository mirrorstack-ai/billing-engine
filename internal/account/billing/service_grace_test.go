package billing_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/mirrorstack-ai/billing-engine/internal/account/billing"
)

// Unpaid-bill grace (billing-engine#329) through GetServiceStatus.

var graceNow = time.Date(2026, 11, 8, 12, 0, 0, 0, time.UTC)

// graceFixture: an account with a card and two unpaid invoices (gate 4 fails).
func graceFixture(t *testing.T) (*fakeStore, billing.GetServiceStatusRequest, uuid.UUID) {
	t.Helper()
	store := newFakeStore()
	userID, accountID := uuid.New(), uuid.New()
	store.accountsByUser[userID] = fakeAccount{id: accountID, stripeCustomerID: "cus_g"}
	store.serviceSignals[accountID] = billing.ServiceSignals{UsableCardCount: 1, FirstChargeStatus: "paid"}
	store.unpaidCount[accountID] = 2
	return store, billing.GetServiceStatusRequest{UserID: userID}, accountID
}

func graceSvc(store billing.Store, on bool) *billing.Service {
	return billing.NewService(store, &fakeStripe{}, "").
		WithUnpaidGrace(on).
		WithClock(func() time.Time { return graceNow })
}

func jsonOf(t *testing.T, v any) string {
	t.Helper()
	b, err := json.Marshal(v)
	require.NoError(t, err)
	return string(b)
}

func TestGetServiceStatus_GraceOff_UnchangedAndNoEpisodeRead(t *testing.T) {
	store, req, accountID := graceFixture(t)
	// An open episode with a fresh notice must not matter when the flag is off.
	sent := graceNow.Add(-time.Hour)
	store.unpaidEpisodes[accountID] = billing.UnpaidEpisode{AccountID: accountID, StartedAt: sent, NoticeSentAt: &sent}

	resp, err := graceSvc(store, false).GetServiceStatus(context.Background(), req)

	require.NoError(t, err)
	require.Equal(t,
		`{"blocked":true,"reason":"UNPAID_INVOICES","reasons":["UNPAID_INVOICES"],"block_reason":"streak_gate"}`,
		jsonOf(t, resp), "flag off: wire output is exactly today's")
	require.Zero(t, store.episodeReads, "flag off performs no extra query")
}

func TestGetServiceStatus_GraceOff_UnsetEnvIsOff(t *testing.T) {
	store, req, _ := graceFixture(t)
	t.Setenv("UNPAID_GRACE", "")
	resp, err := billing.NewService(store, &fakeStripe{}, "").GetServiceStatus(context.Background(), req)
	require.NoError(t, err)
	require.True(t, resp.Blocked)
	require.Zero(t, store.episodeReads)

	t.Setenv("UNPAID_GRACE", "true") // only "on" enables
	resp, err = billing.NewService(store, &fakeStripe{}, "").GetServiceStatus(context.Background(), req)
	require.NoError(t, err)
	require.True(t, resp.Blocked)
	require.Zero(t, store.episodeReads)
}

func TestGetServiceStatus_GraceEnv_On(t *testing.T) {
	store, req, accountID := graceFixture(t)
	sent := graceNow.Add(-24 * time.Hour)
	store.unpaidEpisodes[accountID] = billing.UnpaidEpisode{AccountID: accountID, StartedAt: sent, NoticeSentAt: &sent}
	t.Setenv("UNPAID_GRACE", "on")

	resp, err := billing.NewService(store, &fakeStripe{}, "").
		WithClock(func() time.Time { return graceNow }).
		GetServiceStatus(context.Background(), req)

	require.NoError(t, err)
	require.False(t, resp.Blocked)
	require.Equal(t, "GRACE", resp.Reason)
}

func TestGetServiceStatus_GraceOn_NoEpisode_NotBlocked(t *testing.T) {
	store, req, _ := graceFixture(t)

	resp, err := graceSvc(store, true).GetServiceStatus(context.Background(), req)

	require.NoError(t, err)
	require.Equal(t,
		`{"blocked":false,"reason":"GRACE","reasons":["UNPAID_INVOICES"]}`, jsonOf(t, resp),
		"no episode / no notice: fail toward service, no grace_ends_at")
}

func TestGetServiceStatus_GraceOn_EpisodeWithoutNotice_NotBlocked(t *testing.T) {
	store, req, accountID := graceFixture(t)
	store.unpaidEpisodes[accountID] = billing.UnpaidEpisode{AccountID: accountID, StartedAt: graceNow.Add(-30 * 24 * time.Hour)}

	resp, err := graceSvc(store, true).GetServiceStatus(context.Background(), req)

	require.NoError(t, err)
	require.False(t, resp.Blocked)
	require.Equal(t, "GRACE", resp.Reason)
	require.Empty(t, resp.GraceEndsAt)
}

func TestGetServiceStatus_GraceOn_NoticeWindow(t *testing.T) {
	store, req, accountID := graceFixture(t)
	sent := graceNow.Add(-(14*24*time.Hour - time.Minute)) // 1 min before day 14
	store.unpaidEpisodes[accountID] = billing.UnpaidEpisode{AccountID: accountID, StartedAt: sent, NoticeSentAt: &sent}

	resp, err := graceSvc(store, true).GetServiceStatus(context.Background(), req)

	require.NoError(t, err)
	require.Equal(t,
		`{"blocked":false,"reason":"GRACE","reasons":["UNPAID_INVOICES"],"grace_ends_at":"2026-11-08T12:01:00Z"}`,
		jsonOf(t, resp), "grace_ends_at present with the grace")

	// The clock reaches day 14: blocked with the original reason.
	resp, err = graceSvc(store, true).
		WithClock(func() time.Time { return graceNow.Add(time.Minute) }).
		GetServiceStatus(context.Background(), req)
	require.NoError(t, err)
	require.Equal(t,
		`{"blocked":true,"reason":"UNPAID_INVOICES","reasons":["UNPAID_INVOICES"],"block_reason":"streak_gate"}`,
		jsonOf(t, resp))
}

func TestGetServiceStatus_GraceOn_NoCardBlocksAtOnce(t *testing.T) {
	store, req, accountID := graceFixture(t)
	store.serviceSignals[accountID] = billing.ServiceSignals{UsableCardCount: 0, FirstChargeStatus: "paid"}
	sent := graceNow.Add(-time.Hour)
	store.unpaidEpisodes[accountID] = billing.UnpaidEpisode{AccountID: accountID, StartedAt: sent, NoticeSentAt: &sent}

	resp, err := graceSvc(store, true).GetServiceStatus(context.Background(), req)

	require.NoError(t, err)
	require.True(t, resp.Blocked)
	require.Equal(t, "NO_USABLE_CARD", resp.Reason)
	require.Empty(t, resp.GraceEndsAt)
}

func TestGetServiceStatus_GraceOn_HealedCostsNoEpisodeRead(t *testing.T) {
	store, req, accountID := graceFixture(t)
	store.unpaidCount[accountID] = 0

	resp, err := graceSvc(store, true).GetServiceStatus(context.Background(), req)

	require.NoError(t, err)
	require.False(t, resp.Blocked)
	require.Equal(t, "ELIGIBLE", resp.Reason)
	require.Zero(t, store.episodeReads)
}

func TestGetServiceStatus_GraceOn_EpisodeReadErrorSurfaces(t *testing.T) {
	store, req, _ := graceFixture(t)
	store.errEpisode = errors.New("boom")

	_, err := graceSvc(store, true).GetServiceStatus(context.Background(), req)

	var be *billing.Error
	require.ErrorAs(t, err, &be)
	require.Equal(t, billing.CodeInternal, be.Code)
}

// The wallet gate is applied after the grace and still blocks at once.
func TestGetServiceStatus_GraceOn_WalletGateStillBlocksAtOnce(t *testing.T) {
	store, req, accountID := graceFixture(t)
	sent := graceNow.Add(-time.Hour)
	store.unpaidEpisodes[accountID] = billing.UnpaidEpisode{AccountID: accountID, StartedAt: sent, NoticeSentAt: &sent}
	gate := &fixedCreditGate{blocked: true}
	svc := graceSvc(store, true).WithCreditWallet(true).WithCreditCoordinator(gate, nil)

	resp, err := svc.GetServiceStatus(context.Background(), req)

	require.NoError(t, err)
	require.True(t, resp.Blocked)
	require.Equal(t, "OUT_OF_CREDITS", resp.Reason)
	require.Equal(t, []string{"UNPAID_INVOICES", "OUT_OF_CREDITS"}, resp.Reasons)
	require.Empty(t, resp.GraceEndsAt, "a block has no grace end")
}
