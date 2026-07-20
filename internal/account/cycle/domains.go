package cycle

// RegisterDomain / RemoveDomain mirror custom-domain lifecycle events from the
// platform. They never charge inline: the activation-period sweep and the
// billing-cycle boundary leg own charging. Both writes are idempotent so the
// platform can deliver either event fire-and-forget with retry.

import (
	"context"
	"time"

	"github.com/google/uuid"

	"github.com/mirrorstack-ai/billing-engine/internal/account/billing"
)

// RegisterDomainRequest is the payload of the RegisterDomain RPC. Exactly one
// owner is required for the internal wire contract, but account attribution is
// deliberately copied from the already-mirrored app rather than resolved from
// the owner again. Domains do not create apps and therefore have no funding
// gate of their own.
type RegisterDomainRequest struct {
	OwnerUserID uuid.UUID `json:"owner_user_id,omitempty"`
	OwnerOrgID  uuid.UUID `json:"owner_org_id,omitempty"`
	AppID       uuid.UUID `json:"app_id"`
	Hostname    string    `json:"hostname"`
	ActivatedAt time.Time `json:"activated_at,omitempty"`
}

// RegisterDomainResponse echoes the stable mirror row. A retry that conflicts
// with an existing live hostname returns the first registration's values.
type RegisterDomainResponse struct {
	DomainID    uuid.UUID `json:"domain_id"`
	AccountID   uuid.UUID `json:"account_id"`
	AppID       uuid.UUID `json:"app_id"`
	Hostname    string    `json:"hostname"`
	ActivatedAt time.Time `json:"activated_at"`
}

// RemoveDomainRequest is the payload of the RemoveDomain RPC. RemovedAt is the
// platform removal instant; zero defaults to the server's current time.
type RemoveDomainRequest struct {
	AppID     uuid.UUID `json:"app_id"`
	Hostname  string    `json:"hostname"`
	RemovedAt time.Time `json:"removed_at,omitempty"`
}

// RemoveDomainResponse intentionally omits removed_at: a concurrent retry may
// lose the first-write-wins UPDATE, so echoing its requested instant could
// claim a timestamp the mirror did not persist.
type RemoveDomainResponse struct {
	AppID    uuid.UUID `json:"app_id"`
	Hostname string    `json:"hostname"`
}

// RegisterDomain inserts one live custom-domain mirror row, idempotently on
// hostname, then reads it back so a duplicate or concurrent registration sees
// the row that actually won. The app must already exist in the billing mirror;
// its account_id is the domain's account attribution.
func (s *Service) RegisterDomain(ctx context.Context, req RegisterDomainRequest) (*RegisterDomainResponse, error) {
	if req.OwnerUserID == uuid.Nil && req.OwnerOrgID == uuid.Nil {
		return nil, billing.InvalidInput("owner_user_id or owner_org_id required")
	}
	if req.OwnerUserID != uuid.Nil && req.OwnerOrgID != uuid.Nil {
		return nil, billing.InvalidInput("owner_user_id and owner_org_id are mutually exclusive")
	}
	if req.AppID == uuid.Nil {
		return nil, billing.InvalidInput("app_id required")
	}
	if req.Hostname == "" {
		return nil, billing.InvalidInput("hostname required")
	}

	app, found, err := s.store.AppMirror(ctx, req.AppID)
	if err != nil {
		return nil, billing.Internal("app mirror lookup failed", err)
	}
	if !found {
		return nil, billing.NotFound("app not registered (RegisterApp must run first)")
	}

	activatedAt := req.ActivatedAt
	if activatedAt.IsZero() {
		activatedAt = s.nowFn().UTC()
	}
	if err := s.store.InsertDomain(ctx, app.AccountID, req.AppID, req.Hostname, activatedAt); err != nil {
		return nil, billing.Internal("insert domain mirror failed", err)
	}

	domain, found, err := s.store.DomainByHostname(ctx, req.Hostname)
	if err != nil {
		return nil, billing.Internal("domain mirror lookup failed", err)
	}
	if !found {
		return nil, billing.Internal("domain mirror row missing immediately after insert", nil)
	}
	return &RegisterDomainResponse{
		DomainID:    domain.ID,
		AccountID:   domain.AccountID,
		AppID:       domain.AppID,
		Hostname:    domain.Hostname,
		ActivatedAt: domain.ActivatedAt,
	}, nil
}

// RemoveDomain soft-removes one custom domain from future boundary charges.
// The store updates only a live matching (app_id, hostname), preserving the
// first removal instant; an unknown or already-removed domain is an idempotent
// no-op. The app must still be known to the billing mirror.
func (s *Service) RemoveDomain(ctx context.Context, req RemoveDomainRequest) (*RemoveDomainResponse, error) {
	if req.AppID == uuid.Nil {
		return nil, billing.InvalidInput("app_id required")
	}
	if req.Hostname == "" {
		return nil, billing.InvalidInput("hostname required")
	}

	_, found, err := s.store.AppMirror(ctx, req.AppID)
	if err != nil {
		return nil, billing.Internal("app mirror lookup failed", err)
	}
	if !found {
		return nil, billing.NotFound("app not registered (RegisterApp must run first)")
	}

	removedAt := req.RemovedAt
	if removedAt.IsZero() {
		removedAt = s.nowFn().UTC()
	}
	if err := s.store.RemoveDomain(ctx, req.AppID, req.Hostname, removedAt); err != nil {
		return nil, billing.Internal("remove domain mirror failed", err)
	}
	return &RemoveDomainResponse{AppID: req.AppID, Hostname: req.Hostname}, nil
}
