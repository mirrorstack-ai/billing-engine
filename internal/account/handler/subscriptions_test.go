package handler

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/mirrorstack-ai/billing-engine/internal/account/service"
)

type fakeSvc struct {
	create func(ctx context.Context, in service.CreateInput) (*service.CreateOutput, error)
}

func (f *fakeSvc) Create(ctx context.Context, in service.CreateInput) (*service.CreateOutput, error) {
	return f.create(ctx, in)
}

func newRequest(t *testing.T, body any) *http.Request {
	t.Helper()
	var buf bytes.Buffer
	if body != nil {
		if err := json.NewEncoder(&buf).Encode(body); err != nil {
			t.Fatal(err)
		}
	}
	r := httptest.NewRequest(http.MethodPost, "/subscriptions/create", &buf)
	r.Header.Set("Content-Type", "application/json")
	return r
}

func validBody() map[string]any {
	return map[string]any{
		"owner_type":  "user",
		"owner_id":    uuid.New().String(),
		"plan_id":     "price_123",
		"currency":    "USD",
		"success_url": "https://example.com/success",
		"cancel_url":  "https://example.com/cancel",
	}
}

func TestCreate_Success_ReturnsCheckoutURL(t *testing.T) {
	svc := &fakeSvc{
		create: func(_ context.Context, in service.CreateInput) (*service.CreateOutput, error) {
			if in.OwnerType != "user" || in.PlanID != "price_123" || in.Currency != "USD" {
				t.Fatalf("unexpected input: %+v", in)
			}
			return &service.CreateOutput{CheckoutURL: "https://checkout.stripe.com/cs_1", SessionID: "cs_1"}, nil
		},
	}
	h := NewSubscriptions(svc)

	rr := httptest.NewRecorder()
	h.Create(rr, newRequest(t, validBody()))

	if rr.Code != http.StatusOK {
		t.Fatalf("status: want 200, got %d body=%s", rr.Code, rr.Body.String())
	}
	var resp createResponse
	if err := json.NewDecoder(rr.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.SessionID != "cs_1" || resp.CheckoutURL != "https://checkout.stripe.com/cs_1" {
		t.Fatalf("unexpected response: %+v", resp)
	}
}

func TestCreate_BadJSON_Returns400(t *testing.T) {
	h := NewSubscriptions(&fakeSvc{})
	r := httptest.NewRequest(http.MethodPost, "/subscriptions/create", strings.NewReader("not-json"))
	rr := httptest.NewRecorder()
	h.Create(rr, r)
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("expected 400; got %d", rr.Code)
	}
}

func TestCreate_BadOwnerID_Returns400(t *testing.T) {
	body := validBody()
	body["owner_id"] = "not-a-uuid"
	h := NewSubscriptions(&fakeSvc{})
	rr := httptest.NewRecorder()
	h.Create(rr, newRequest(t, body))
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("expected 400; got %d", rr.Code)
	}
	if !strings.Contains(rr.Body.String(), "invalid_owner_id") {
		t.Fatalf("expected invalid_owner_id error; got %s", rr.Body.String())
	}
}

func TestCreate_ServiceValidationErrors_MapTo400(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		wantCode string
	}{
		{"invalid owner type", service.ErrInvalidOwnerType, "invalid_owner_type"},
		{"invalid currency", service.ErrInvalidCurrency, "invalid_currency"},
		{"missing field", service.ErrMissingField, "missing_field"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			svc := &fakeSvc{
				create: func(_ context.Context, _ service.CreateInput) (*service.CreateOutput, error) {
					return nil, tc.err
				},
			}
			h := NewSubscriptions(svc)
			rr := httptest.NewRecorder()
			h.Create(rr, newRequest(t, validBody()))
			if rr.Code != http.StatusBadRequest {
				t.Fatalf("expected 400; got %d", rr.Code)
			}
			if !strings.Contains(rr.Body.String(), tc.wantCode) {
				t.Fatalf("expected error code %s; got %s", tc.wantCode, rr.Body.String())
			}
		})
	}
}

func TestCreate_ServiceUnexpectedError_Returns500(t *testing.T) {
	svc := &fakeSvc{
		create: func(_ context.Context, _ service.CreateInput) (*service.CreateOutput, error) {
			return nil, errors.New("stripe blew up")
		},
	}
	h := NewSubscriptions(svc)
	rr := httptest.NewRecorder()
	h.Create(rr, newRequest(t, validBody()))
	if rr.Code != http.StatusInternalServerError {
		t.Fatalf("expected 500; got %d", rr.Code)
	}
}
