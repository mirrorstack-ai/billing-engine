package handler

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"

	"github.com/google/uuid"

	"github.com/mirrorstack-ai/billing-engine/internal/account/service"
	mstripe "github.com/mirrorstack-ai/billing-engine/internal/shared/stripe"
)

// BillingPortalCreator is the service contract the handler depends on.
// Defined here (not in service) so the handler test can mock it without
// pulling in Stripe types.
type BillingPortalCreator interface {
	CreatePortalSession(ctx context.Context, billingAccountID uuid.UUID, returnURL string) (string, error)
}

// BillingPortalHandler exposes POST /billing-portal/create.
type BillingPortalHandler struct {
	svc BillingPortalCreator
}

// NewBillingPortalHandler wires the handler to its service.
func NewBillingPortalHandler(svc BillingPortalCreator) *BillingPortalHandler {
	return &BillingPortalHandler{svc: svc}
}

type billingPortalCreateRequest struct {
	BillingAccountID string `json:"billing_account_id"`
	ReturnURL        string `json:"return_url"`
}

type billingPortalCreateResponse struct {
	PortalURL string `json:"portal_url"`
}

// Create handles POST /billing-portal/create.
func (h *BillingPortalHandler) Create(w http.ResponseWriter, r *http.Request) {
	var req billingPortalCreateRequest
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", "invalid JSON body")
		return
	}

	if req.BillingAccountID == "" {
		writeError(w, http.StatusBadRequest, "invalid_request", "billing_account_id is required")
		return
	}
	if req.ReturnURL == "" {
		writeError(w, http.StatusBadRequest, "invalid_request", "return_url is required")
		return
	}
	id, err := uuid.Parse(req.BillingAccountID)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", "billing_account_id must be a UUID")
		return
	}

	url, err := h.svc.CreatePortalSession(r.Context(), id, req.ReturnURL)
	if err != nil {
		switch {
		case errors.Is(err, service.ErrBillingAccountNotFound),
			errors.Is(err, mstripe.ErrCustomerNotFound):
			writeError(w, http.StatusNotFound, "not_found", "billing account not found")
		default:
			slog.ErrorContext(r.Context(), "create portal session failed", "error", err)
			writeError(w, http.StatusBadGateway, "stripe_error", "failed to create portal session")
		}
		return
	}

	writeJSON(w, http.StatusOK, billingPortalCreateResponse{PortalURL: url})
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

func writeError(w http.ResponseWriter, status int, code, message string) {
	writeJSON(w, status, map[string]any{
		"error": map[string]string{
			"code":    code,
			"message": message,
		},
	})
}
