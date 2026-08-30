//go:build integration

package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/mirrorstack-ai/billing-engine/internal/account/billing"
	"github.com/mirrorstack-ai/billing-engine/internal/account/budget"
	"github.com/mirrorstack-ai/billing-engine/internal/account/cycle"
	"github.com/mirrorstack-ai/billing-engine/internal/account/usage"
	"github.com/mirrorstack-ai/billing-engine/internal/shared/stripe/stripetest"
	"github.com/mirrorstack-ai/billing-engine/internal/shared/testutil"
)

// docs/SECURITY.md §2 records that "a read or eligibility check is not
// capability-safe when it can reach a card charge", and lists the paths
// that could. The unit tests in internal/account/credit pin the
// coordinator's side of that. This pins the other side, and the one a
// unit test cannot: every read route, driven through the real router,
// the real dispatcher, the real services and a real database, must make
// no provider call at all.
//
// The provider client is a recorder rather than a fake returning zeros,
// because "this read did not charge" is not something a fake can show —
// the call has to be observed and classified. A read that creates a
// draft invoice has left a customer-visible object behind and holds a
// capability it should not, even though that call collects nothing, so
// the assertion here is the stricter RequireNoProviderMutation.
func TestReadRoutesMakeNoProviderCall(t *testing.T) {
	pool := testutil.NewTestDB(t)
	recorder := stripetest.New()

	ownerUserID := uuid.New()
	seedCapabilityAccount(t, pool, ownerUserID)

	// The credit wallet is enabled deliberately. docs/SECURITY.md §2's
	// gap names GetCreditStanding and the credit gate, and with the
	// wallet off those routes answer CREDIT_WALLET_DISABLED without
	// ever reaching it — the test would pass by not running the code
	// it exists to check.
	d := &dispatcher{
		svc: billing.NewService(billing.NewStore(pool), recorder, "https://return.invalid").
			WithCreditWallet(true).
			WithCreditAccess(func(uuid.UUID) bool { return true }),
		usageSvc:  usage.NewService(usage.NewStore(pool)),
		budgetSvc: budget.NewService(budget.NewStore(pool)),
		cycleSvc:  cycle.NewService(cycle.NewStore(pool), recorder),
	}

	t.Setenv("INTERNAL_SECRET", "internal-secret")
	t.Setenv("METER_SECRET", "meter-secret")
	router := buildRouter(d)

	owner := `{"user_id":"` + ownerUserID.String() + `"}`
	walletOwner := `{"owner_user_id":"` + ownerUserID.String() + `"}`

	reads := []struct {
		route string
		body  string
	}{
		{"/v1/billing.GetServiceStatus", owner},
		{"/v1/billing.GetPaymentMethods", owner},
		{"/v1/billing.GetCreditStanding", walletOwner},
		{"/v1/billing.ListCreditLedger", walletOwner},
		{"/v1/billing.GetAccountBill", walletOwner},
		{"/v1/billing.GetBillingPeriods", walletOwner},
		{"/v1/billing.ListInvoices", walletOwner},
		{"/v1/billing.ListUnpaidInvoices", walletOwner},
		{"/v1/billing.GetUsageSummary", walletOwner},
		{"/v1/billing.GetBudgetStatus", `{"scope":"app","scope_id":"` + uuid.New().String() + `"}`},
	}

	answered := 0
	for _, read := range reads {
		t.Run(strings.TrimPrefix(read.route, "/v1/billing."), func(t *testing.T) {
			recorder.Reset()

			req := httptest.NewRequest(http.MethodPost, read.route, strings.NewReader(read.body))
			req.Header.Set("X-MS-Internal-Secret", "internal-secret")
			rec := httptest.NewRecorder()
			router.ServeHTTP(rec, req)

			// The status is not the point — a route may legitimately
			// answer 404 for an account with no data. What must hold
			// either way is that answering touched no provider: a read
			// that charges on the success path and not the error path
			// is still a read that charges.
			require.NotEqual(t, http.StatusNotFound, rec.Code,
				"route is not registered; this test would pass vacuously")

			if rec.Code == http.StatusOK {
				answered++
			}
			recorder.RequireNoProviderMutation(t, read.route)
		})
	}

	// A route that refuses before it reaches its own logic proves
	// nothing about that logic, and a suite of them proves nothing at
	// all while still printing "ok". This floor is what keeps the test
	// from decaying into that: if a future change makes most routes
	// answer 503 or 400, the vacuity is a failure rather than a pass.
	// All ten answer today, so ten is the floor. A looser one would
	// let a route quietly start refusing without anyone noticing that
	// its green had stopped meaning anything.
	const minAnswered = 10
	require.GreaterOrEqualf(t, answered, minAnswered,
		"only %d of %d read routes answered 200; the rest refused before reaching the code "+
			"this test exists to check, so their green means nothing", answered, len(reads))
}

// The metering ingress is not a read, but docs/SECURITY.md §2 lists it
// among the paths that reach the auto-top-up executor, and
// docs/VERIFICATION.md §5 requires that "dispatch metering reaches
// RecordUsage and nothing else". Recording a fact must not itself move
// money.
func TestUsageIngressMakesNoProviderCall(t *testing.T) {
	pool := testutil.NewTestDB(t)
	recorder := stripetest.New()

	ownerUserID := uuid.New()
	accountID := seedCapabilityAccount(t, pool, ownerUserID)

	d := &dispatcher{
		svc:      billing.NewService(billing.NewStore(pool), recorder, "https://return.invalid"),
		usageSvc: usage.NewService(usage.NewStore(pool)),
		cycleSvc: cycle.NewService(cycle.NewStore(pool), recorder),
	}

	t.Setenv("INTERNAL_SECRET", "internal-secret")
	t.Setenv("METER_SECRET", "meter-secret")
	router := buildRouter(d)

	body, err := json.Marshal(map[string]any{
		"account_id": accountID,
		"app_id":     uuid.New(),
		"event_id":   "evt-capability-probe",
		"metric":     "quiz.render",
		"quantity":   1,
	})
	require.NoError(t, err)

	req := httptest.NewRequest(http.MethodPost, "/v1/billing.RecordUsage", strings.NewReader(string(body)))
	req.Header.Set("X-MS-Meter-Secret", "meter-secret")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	require.NotEqual(t, http.StatusNotFound, rec.Code, "RecordUsage is not registered")
	recorder.RequireNoProviderMutation(t, "RecordUsage")
}

// The public health probe answers before any credential is presented,
// so it must reach nothing at all.
func TestHealthMakesNoProviderCall(t *testing.T) {
	pool := testutil.NewTestDB(t)
	recorder := stripetest.New()

	d := &dispatcher{svc: billing.NewService(billing.NewStore(pool), recorder, "")}
	t.Setenv("INTERNAL_SECRET", "internal-secret")
	t.Setenv("METER_SECRET", "meter-secret")

	rec := httptest.NewRecorder()
	buildRouter(d).ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/__health", nil))

	require.Equal(t, http.StatusOK, rec.Code)
	recorder.RequireNoProviderMutation(t, "/__health")
}

// Capabilities is a statement about the build. It must answer without
// reaching a provider, and without a database.
func TestCapabilitiesMakesNoProviderCall(t *testing.T) {
	recorder := stripetest.New()
	d := &dispatcher{}

	_, err := d.dispatch(context.Background(), "Capabilities", nil)

	require.NoError(t, err)
	recorder.RequireNoProviderMutation(t, "Capabilities")
}

func seedCapabilityAccount(t *testing.T, pool *pgxpool.Pool, ownerUserID uuid.UUID) uuid.UUID {
	t.Helper()
	accountID := uuid.New()
	_, err := pool.Exec(context.Background(),
		`INSERT INTO ms_billing.accounts (id, owner_kind, owner_user_id, stripe_customer_id)
		 VALUES ($1, 'user', $2, $3)`,
		accountID, ownerUserID, "cus_capability_probe",
	)
	require.NoError(t, err)
	return accountID
}
