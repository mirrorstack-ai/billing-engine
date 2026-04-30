package handler

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/mirrorstack-ai/billing-engine/internal/account/service"
	mstripe "github.com/mirrorstack-ai/billing-engine/internal/shared/stripe"
)

type fakeSvc struct {
	url       string
	err       error
	gotID     uuid.UUID
	gotReturn string
	called    int
}

func (f *fakeSvc) CreatePortalSession(_ context.Context, id uuid.UUID, returnURL string) (string, error) {
	f.called++
	f.gotID = id
	f.gotReturn = returnURL
	return f.url, f.err
}

func doRequest(t *testing.T, body string, svc BillingPortalCreator) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/billing-portal/create", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	NewBillingPortalHandler(svc).Create(rec, req)
	return rec
}

func TestBillingPortalCreate_Success(t *testing.T) {
	id := uuid.New()
	svc := &fakeSvc{url: "https://billing.stripe.com/p/session/xyz"}
	body := `{"billing_account_id":"` + id.String() + `","return_url":"https://app.example/return"}`

	rec := doRequest(t, body, svc)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", rec.Code, rec.Body.String())
	}
	var resp struct {
		PortalURL string `json:"portal_url"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if resp.PortalURL != "https://billing.stripe.com/p/session/xyz" {
		t.Fatalf("portal_url = %q", resp.PortalURL)
	}
	if svc.gotID != id {
		t.Fatalf("svc id = %s, want %s", svc.gotID, id)
	}
	if svc.gotReturn != "https://app.example/return" {
		t.Fatalf("svc return_url = %q", svc.gotReturn)
	}
}

func TestBillingPortalCreate_BillingAccountNotFound_Returns404(t *testing.T) {
	id := uuid.New()
	svc := &fakeSvc{err: service.ErrBillingAccountNotFound}
	body := `{"billing_account_id":"` + id.String() + `","return_url":"https://app.example/return"}`

	rec := doRequest(t, body, svc)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", rec.Code)
	}
}

func TestBillingPortalCreate_StripeCustomerNotFound_Returns404(t *testing.T) {
	id := uuid.New()
	svc := &fakeSvc{err: errors.Join(mstripe.ErrCustomerNotFound, errors.New("stripe says missing"))}
	body := `{"billing_account_id":"` + id.String() + `","return_url":"https://app.example/return"}`

	rec := doRequest(t, body, svc)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404; body=%s", rec.Code, rec.Body.String())
	}
}

func TestBillingPortalCreate_StripeError_Returns502(t *testing.T) {
	id := uuid.New()
	svc := &fakeSvc{err: errors.New("transient stripe failure")}
	body := `{"billing_account_id":"` + id.String() + `","return_url":"https://app.example/return"}`

	rec := doRequest(t, body, svc)

	if rec.Code != http.StatusBadGateway {
		t.Fatalf("status = %d, want 502", rec.Code)
	}
}

func TestBillingPortalCreate_MissingBillingAccountID_Returns400(t *testing.T) {
	svc := &fakeSvc{}
	rec := doRequest(t, `{"return_url":"https://app.example/return"}`, svc)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", rec.Code)
	}
	if svc.called != 0 {
		t.Fatalf("svc should not be called on validation failure")
	}
}

func TestBillingPortalCreate_MissingReturnURL_Returns400(t *testing.T) {
	svc := &fakeSvc{}
	rec := doRequest(t, `{"billing_account_id":"`+uuid.New().String()+`"}`, svc)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", rec.Code)
	}
}

func TestBillingPortalCreate_NonUUID_Returns400(t *testing.T) {
	svc := &fakeSvc{}
	rec := doRequest(t, `{"billing_account_id":"not-a-uuid","return_url":"https://app.example/return"}`, svc)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", rec.Code)
	}
}

func TestBillingPortalCreate_InvalidJSON_Returns400(t *testing.T) {
	svc := &fakeSvc{}
	rec := doRequest(t, `not-json`, svc)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", rec.Code)
	}
}
