package standing_test

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/mirrorstack-ai/billing-engine/internal/account/billing"
	"github.com/mirrorstack-ai/billing-engine/internal/account/standing"
)

// fakeStatus returns a fixed verdict for every owner.
type fakeStatus struct {
	blocked bool
	reason  string
	err     error
	lastReq billing.GetServiceStatusRequest
}

func (f *fakeStatus) GetServiceStatus(_ context.Context, req billing.GetServiceStatusRequest) (*billing.GetServiceStatusResponse, error) {
	f.lastReq = req
	if f.err != nil {
		return nil, f.err
	}
	return &billing.GetServiceStatusResponse{Blocked: f.blocked, Reason: f.reason}, nil
}

// fakeOwners resolves every id to the configured owner (found toggles drift).
type fakeOwners struct {
	owner standing.Owner
	found bool
}

func (f *fakeOwners) OwnerByStripeCustomer(context.Context, string) (standing.Owner, bool, error) {
	return f.owner, f.found, nil
}
func (f *fakeOwners) OwnerByStripeInvoice(context.Context, string) (standing.Owner, bool, error) {
	return f.owner, f.found, nil
}
func (f *fakeOwners) OwnerByStripePaymentMethod(context.Context, string) (standing.Owner, bool, error) {
	return f.owner, f.found, nil
}

// capture records the requests the fake serving-block endpoint received.
type capture struct {
	paths   []string
	secrets []string
	bodies  []map[string]any
}

func servingBlockServer(t *testing.T, c *capture, status int) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		var decoded map[string]any
		require.NoError(t, json.Unmarshal(body, &decoded))
		c.paths = append(c.paths, r.URL.Path)
		c.secrets = append(c.secrets, r.Header.Get("X-MS-Internal-Secret"))
		c.bodies = append(c.bodies, decoded)
		w.WriteHeader(status)
	}))
}

func TestNotifier_DisabledWhenEnvUnset_SkipsWithoutCalling(t *testing.T) {
	// Unset APPLICATIONS_INTERNAL_URL / INTERNAL_SECRET → log-and-skip: no
	// owner resolution, no verdict read, no HTTP call, no error.
	status := &fakeStatus{blocked: true}
	owners := &fakeOwners{owner: standing.Owner{UserID: uuid.New()}, found: true}

	for _, tc := range []struct{ url, secret string }{
		{"", ""},
		{"http://localhost:9", ""}, // secret missing
		{"", "shhh"},               // url missing
	} {
		n := standing.NewNotifier(tc.url, tc.secret, status, owners, slog.Default())
		n.NotifyStripeCustomer(context.Background(), "cus_x")
		n.NotifyStripeInvoice(context.Background(), "in_x")
		n.NotifyStripePaymentMethod(context.Background(), "pm_x")
	}
	require.Empty(t, status.lastReq, "a disabled notifier must not even read the verdict")
}

func TestNotifier_PostsUserVerdictWithSecret(t *testing.T) {
	userID := uuid.New()
	c := &capture{}
	srv := servingBlockServer(t, c, http.StatusOK)
	defer srv.Close()

	status := &fakeStatus{blocked: true, reason: "UNPAID_INVOICES"}
	owners := &fakeOwners{owner: standing.Owner{UserID: userID}, found: true}
	n := standing.NewNotifier(srv.URL, "shhh", status, owners, slog.Default())

	n.NotifyStripeInvoice(context.Background(), "in_123")

	require.Len(t, c.bodies, 1)
	require.Equal(t, "/internal/apps/serving-block", c.paths[0])
	require.Equal(t, "shhh", c.secrets[0])
	require.Equal(t, map[string]any{
		"owner_user_id": userID.String(),
		"blocked":       true,
	}, c.bodies[0], "user owner: owner_user_id + blocked, org field omitted")
	require.Equal(t, userID, status.lastReq.UserID, "verdict read for the resolved owner")
}

func TestNotifier_PostsOrgVerdict(t *testing.T) {
	orgID := uuid.New()
	c := &capture{}
	srv := servingBlockServer(t, c, http.StatusOK)
	defer srv.Close()

	status := &fakeStatus{blocked: false, reason: "ELIGIBLE"}
	owners := &fakeOwners{owner: standing.Owner{OrgID: orgID}, found: true}
	n := standing.NewNotifier(srv.URL, "shhh", status, owners, slog.Default())

	n.NotifyStripeCustomer(context.Background(), "cus_org")

	require.Len(t, c.bodies, 1)
	require.Equal(t, map[string]any{
		"owner_org_id": orgID.String(),
		"blocked":      false,
	}, c.bodies[0], "org owner: owner_org_id + blocked, user field omitted")
	require.Equal(t, orgID, status.lastReq.OrgID)
}

func TestNotifier_OwnerDrift_Skips(t *testing.T) {
	c := &capture{}
	srv := servingBlockServer(t, c, http.StatusOK)
	defer srv.Close()

	status := &fakeStatus{}
	owners := &fakeOwners{found: false} // object never mirrored
	n := standing.NewNotifier(srv.URL, "shhh", status, owners, slog.Default())

	n.NotifyStripePaymentMethod(context.Background(), "pm_ghost")
	require.Empty(t, c.bodies)
}

func TestNotifier_EndpointErrorIsSwallowed(t *testing.T) {
	// Best-effort contract: a 500 from the platform is logged, never raised —
	// the calling webhook handler's 200-to-Stripe must be unaffected.
	c := &capture{}
	srv := servingBlockServer(t, c, http.StatusInternalServerError)
	defer srv.Close()

	status := &fakeStatus{blocked: true}
	owners := &fakeOwners{owner: standing.Owner{UserID: uuid.New()}, found: true}
	n := standing.NewNotifier(srv.URL, "shhh", status, owners, slog.Default())

	n.NotifyStripeInvoice(context.Background(), "in_err") // must not panic
	require.Len(t, c.bodies, 1, "the attempt was made; the failure is swallowed")
}
