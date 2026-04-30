package handler

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"

	"github.com/google/uuid"

	"github.com/mirrorstack-ai/billing-engine/internal/account/service"
)

// SubscriptionsService is the slice of *service.Subscriptions the handler
// touches. Defined as an interface so handler tests can inject a fake
// without spinning up the full service graph.
type SubscriptionsService interface {
	Create(ctx context.Context, in service.CreateInput) (*service.CreateOutput, error)
}

// Subscriptions is the chi handler for subscription-related endpoints.
type Subscriptions struct {
	svc SubscriptionsService
}

// NewSubscriptions wires a Subscriptions handler.
func NewSubscriptions(svc SubscriptionsService) *Subscriptions {
	return &Subscriptions{svc: svc}
}

type createRequest struct {
	OwnerType  string `json:"owner_type"`
	OwnerID    string `json:"owner_id"`
	PlanID     string `json:"plan_id"`
	Currency   string `json:"currency"`
	SuccessURL string `json:"success_url"`
	CancelURL  string `json:"cancel_url"`
}

type createResponse struct {
	CheckoutURL string `json:"checkout_url"`
	SessionID   string `json:"session_id"`
}

// Create handles POST /subscriptions/create.
func (h *Subscriptions) Create(w http.ResponseWriter, r *http.Request) {
	var req createRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_body", "invalid request body")
		return
	}

	ownerID, err := uuid.Parse(req.OwnerID)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_owner_id", "owner_id must be a UUID")
		return
	}

	out, err := h.svc.Create(r.Context(), service.CreateInput{
		OwnerType:  req.OwnerType,
		OwnerID:    ownerID,
		PlanID:     req.PlanID,
		Currency:   req.Currency,
		SuccessURL: req.SuccessURL,
		CancelURL:  req.CancelURL,
	})
	if err != nil {
		switch {
		case errors.Is(err, service.ErrInvalidOwnerType):
			writeError(w, http.StatusBadRequest, "invalid_owner_type", "owner_type must be 'user' or 'org'")
		case errors.Is(err, service.ErrInvalidCurrency):
			writeError(w, http.StatusBadRequest, "invalid_currency", "currency must be 'USD', 'TWD', or 'EUR'")
		case errors.Is(err, service.ErrMissingField):
			writeError(w, http.StatusBadRequest, "missing_field", err.Error())
		default:
			slog.ErrorContext(r.Context(), "subscriptions.create failed", "error", err)
			writeError(w, http.StatusInternalServerError, "internal_error", "internal error")
		}
		return
	}

	writeJSON(w, http.StatusOK, createResponse{
		CheckoutURL: out.CheckoutURL,
		SessionID:   out.SessionID,
	})
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

func writeError(w http.ResponseWriter, status int, code, message string) {
	writeJSON(w, status, map[string]any{
		"error": map[string]string{"code": code, "message": message},
	})
}
