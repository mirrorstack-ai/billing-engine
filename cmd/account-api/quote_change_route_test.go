package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/mirrorstack-ai/billing-engine/internal/account/cycle"
	"github.com/mirrorstack-ai/billing-engine/internal/account/usage"
)

// QuoteChange (billing-engine#255) is dispatched by action name and served at
// /v1/billing.QuoteChange behind the internal secret. A typo in either would
// pass the cycle package's service tests and only fail at api-platform, so this
// pins both without a database: a malformed body stops at the dispatcher's
// decoder, and a well-formed body with no owner reaches Service.QuoteChange's
// first validation (it runs before the service touches its store, so a
// dispatcher with no cycle service is enough). The two answers differ, which is
// what proves the "QuoteChange" case really calls the service.
func TestQuoteChangeRoute_IsInternalSecretGatedAndDispatched(t *testing.T) {
	t.Setenv("INTERNAL_SECRET", "internal-secret")
	t.Setenv("METER_SECRET", "meter-secret")
	router := buildRouter(&dispatcher{})
	const path = "/v1/billing.QuoteChange"

	post := func(body string, secret bool) *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(body))
		if secret {
			req.Header.Set("X-MS-Internal-Secret", "internal-secret")
		}
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, req)
		return rec
	}

	malformed := post("{", true)
	require.Equal(t, http.StatusBadRequest, malformed.Code)
	require.Contains(t, malformed.Body.String(), `"code":"INVALID_INPUT"`)
	require.Contains(t, malformed.Body.String(), "malformed request payload")

	ownerless := post(`{"steps":[{"kind":"create_app","plan":"free"}]}`, true)
	require.Equal(t, http.StatusBadRequest, ownerless.Code)
	require.Contains(t, ownerless.Body.String(), `"code":"INVALID_INPUT"`)
	require.Contains(t, ownerless.Body.String(), "owner_user_id or owner_org_id required",
		"the QuoteChange case must hand the decoded request to Service.QuoteChange")
	require.NotContains(t, ownerless.Body.String(), "malformed request payload")

	require.Equal(t, http.StatusUnauthorized, post(`{}`, false).Code, "the route is internal-secret gated")
}

// quoteStore is the smallest fake cycle.Store a QuoteChange of one create_app
// step reaches: the embedded nil interface panics on any method not listed, so
// a future QuoteChange change that reads or writes more fails here loudly
// instead of silently passing. It writes nothing — QuoteChange is read-only.
type quoteStore struct {
	cycle.Store
	user, account uuid.UUID
}

func (s quoteStore) AccountIDByUser(_ context.Context, userID uuid.UUID) (uuid.UUID, bool, error) {
	return s.account, userID == s.user, nil
}

func (quoteStore) AccountActivation(context.Context, uuid.UUID) (time.Time, bool, error) {
	return time.Date(2026, 5, 4, 9, 0, 0, 0, time.UTC), true, nil
}

func (quoteStore) LiveModuleTimerCountForAccount(context.Context, uuid.UUID) (int, error) {
	return 0, nil
}

func (s quoteStore) ChargeFundingAccount(_ context.Context, accountID uuid.UUID) (uuid.UUID, error) {
	return accountID, nil
}

func (quoteStore) UsableNonFraudCardCount(context.Context, uuid.UUID) (int, error) { return 1, nil }

// The success side of the same route: a real cycle service over a fake store,
// a valid create_app step, and the whole envelope api-platform decodes —
// `ok` true and `response.steps[0].recurring_micros` equal to the Pro plan's
// base fee, taken from usage.TermsFor (the same source QuoteChange prices
// from), never a re-typed price.
func TestQuoteChangeRoute_SuccessEnvelopeCarriesTheStepQuote(t *testing.T) {
	t.Setenv("INTERNAL_SECRET", "internal-secret")
	t.Setenv("METER_SECRET", "meter-secret")
	user := uuid.New()
	svc := cycle.NewService(quoteStore{user: user, account: uuid.New()}, nil).
		WithNow(func() time.Time { return time.Date(2026, 6, 10, 12, 0, 0, 0, time.UTC) })
	router := buildRouter(&dispatcher{cycleSvc: svc})

	body := `{"owner_user_id":"` + user.String() + `","steps":[{"kind":"create_app","plan":"pro"}]}`
	req := httptest.NewRequest(http.MethodPost, "/v1/billing.QuoteChange", strings.NewReader(body))
	req.Header.Set("X-MS-Internal-Secret", "internal-secret")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	var envelope struct {
		OK       bool `json:"ok"`
		Response struct {
			Steps []struct {
				Kind            string `json:"kind"`
				Standing        string `json:"standing"`
				RecurringMicros int64  `json:"recurring_micros"`
			} `json:"steps"`
		} `json:"response"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &envelope), rec.Body.String())
	require.True(t, envelope.OK, rec.Body.String())
	require.Len(t, envelope.Response.Steps, 1)
	step := envelope.Response.Steps[0]
	require.Equal(t, "create_app", step.Kind)
	require.Equal(t, string(cycle.QuoteStandingOK), step.Standing)
	require.Positive(t, step.RecurringMicros, "a Pro app has a recurring base fee")
	require.Equal(t, usage.TermsFor(usage.PlanPro).BaseFeeMicros, step.RecurringMicros)
}
