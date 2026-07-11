// Package standing implements the SERVING-BLOCK notifier — the billing side
// of the funding-gates C6 contract (docs-temp/billing-funding-gates/design.md,
// DECIDED 2026-07-11): when an account's payment standing may have
// transitioned, billing-engine POSTs the CURRENT blocked verdict to
// api-platform's POST /internal/apps/serving-block, which rewrites the
// app-stage manifests of every app that owner owns so the edge can gate
// serving without a per-request billing call.
//
// Design choice (documented per the wave spec): rather than diffing the
// verdict before/after each mutating event — which would need a second
// signals read inside every webhook handler and still race concurrent
// events — the notifier fires after EVERY standing-relevant webhook event
// (invoice lifecycle, card attach/detach, fraud flag) and sends the verdict
// as computed NOW. The receiving side is idempotent (rewriting a manifest to
// the value it already holds is a no-op), so over-notifying is safe;
// under-notifying is what the per-event hooks prevent. Charge outcomes need
// no dedicated hook: every Stripe charge emits invoice.* events, which land
// here through the webhook path.
//
// Best-effort by contract: every failure (env unset, owner drift, verdict
// read error, HTTP error) is logged and swallowed — a standing notify must
// never fail or delay webhook processing. The stage-activate-time consult on
// the api-platform side backstops any missed transition.
//
// Known narrow gap: a SPONSOR's card event changes the standing of the orgs
// it funds, but the event resolves only to the sponsor's own user principal —
// the sponsored orgs are not re-notified until one of their own invoice
// events fires (or a stage activate re-consults). Acceptable for the
// best-effort contract; a designation-fanout follow-up can close it.
package standing

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/mirrorstack-ai/billing-engine/internal/account/billing"
)

// servingBlockPath is api-platform's internal endpoint (C6), joined onto
// APPLICATIONS_INTERNAL_URL.
const servingBlockPath = "/internal/apps/serving-block"

// headerInternalSecret is the shared internal-secret header both directions
// of the api-platform ↔ billing-engine trust boundary use (see
// internal/shared/auth).
const headerInternalSecret = "X-MS-Internal-Secret"

// StatusReader is the verdict source — satisfied by *billing.Service. Reusing
// GetServiceStatus keeps this package on THE standing predicate (card /
// first-charge / streak / unpaid gates, org designation resolution) instead
// of a second derivation.
type StatusReader interface {
	GetServiceStatus(ctx context.Context, req billing.GetServiceStatusRequest) (*billing.GetServiceStatusResponse, error)
}

// Owner is the (user XOR org) principal a webhook event's account resolves
// to — the addressing api-platform's serving-block endpoint keys on.
type Owner struct {
	UserID uuid.UUID
	OrgID  uuid.UUID
}

// OwnerResolver maps the Stripe object id a webhook event carries to the
// owning account's principal. found=false is drift (object never mirrored) —
// the notifier logs and skips.
type OwnerResolver interface {
	OwnerByStripeCustomer(ctx context.Context, stripeCustomerID string) (Owner, bool, error)
	OwnerByStripeInvoice(ctx context.Context, stripeInvoiceID string) (Owner, bool, error)
	OwnerByStripePaymentMethod(ctx context.Context, stripePaymentMethodID string) (Owner, bool, error)
}

// Notifier posts serving-block verdicts to api-platform. Zero value is not
// usable; construct via NewNotifier / NewNotifierFromEnv. A Notifier with an
// empty baseURL or secret is DISABLED: every notify debug-logs and skips
// (dev-safe — local stacks without the env keep working).
type Notifier struct {
	baseURL string
	secret  string
	status  StatusReader
	owners  OwnerResolver
	httpc   *http.Client
	log     *slog.Logger
}

// NewNotifier wires a Notifier. baseURL/secret may be empty (disabled mode);
// status, owners and log must be non-nil.
func NewNotifier(baseURL, secret string, status StatusReader, owners OwnerResolver, log *slog.Logger) *Notifier {
	if status == nil || owners == nil || log == nil {
		panic("standing.NewNotifier: status, owners and log must not be nil")
	}
	return &Notifier{
		baseURL: strings.TrimRight(baseURL, "/"),
		secret:  secret,
		status:  status,
		owners:  owners,
		// Short timeout: this runs inside webhook handling; a slow platform
		// must never stall Stripe deliveries.
		httpc: &http.Client{Timeout: 5 * time.Second},
		log:   log,
	}
}

// NewNotifierFromEnv builds the production wiring: verdicts from the real
// billing.Service over the given pool (GetServiceStatus never touches Stripe,
// so no Stripe client is needed), owners from the generated standing queries,
// destination from APPLICATIONS_INTERNAL_URL + INTERNAL_SECRET (the shared
// api-platform ↔ billing-engine internal secret). Either env unset → the
// disabled notifier (log-and-skip).
func NewNotifierFromEnv(pool *pgxpool.Pool, log *slog.Logger) *Notifier {
	url := os.Getenv("APPLICATIONS_INTERNAL_URL")
	secret := os.Getenv("INTERNAL_SECRET")
	if url == "" || secret == "" {
		log.Info("serving-block notifier disabled (APPLICATIONS_INTERNAL_URL / INTERNAL_SECRET unset) — standing transitions will not push manifest rewrites")
	}
	svc := billing.NewService(billing.NewStore(pool), nil, "")
	return NewNotifier(url, secret, svc, NewStore(pool), log)
}

// NotifyStripeCustomer re-evaluates and pushes the verdict for the account
// owning the Stripe customer (card attach / fraud-flag events).
func (n *Notifier) NotifyStripeCustomer(ctx context.Context, stripeCustomerID string) {
	n.notify(ctx, "stripe_customer_id", stripeCustomerID, n.owners.OwnerByStripeCustomer)
}

// NotifyStripeInvoice re-evaluates and pushes the verdict for the account
// owning the mirrored invoice (invoice lifecycle events).
func (n *Notifier) NotifyStripeInvoice(ctx context.Context, stripeInvoiceID string) {
	n.notify(ctx, "stripe_invoice_id", stripeInvoiceID, n.owners.OwnerByStripeInvoice)
}

// NotifyStripePaymentMethod re-evaluates and pushes the verdict for the
// account owning the mirrored card (card detach events, which carry only the
// pm id).
func (n *Notifier) NotifyStripePaymentMethod(ctx context.Context, stripePaymentMethodID string) {
	n.notify(ctx, "stripe_payment_method_id", stripePaymentMethodID, n.owners.OwnerByStripePaymentMethod)
}

// servingBlockBody is the C6 wire body: exactly one owner field + the current
// blocked verdict.
type servingBlockBody struct {
	OwnerUserID string `json:"owner_user_id,omitempty"`
	OwnerOrgID  string `json:"owner_org_id,omitempty"`
	Blocked     bool   `json:"blocked"`
}

// notify is the shared resolve → evaluate → POST spine. Every failure mode
// logs and returns — never an error to the caller (best-effort contract).
func (n *Notifier) notify(ctx context.Context, refKind, refID string, resolve func(context.Context, string) (Owner, bool, error)) {
	if refID == "" {
		return
	}
	if n.baseURL == "" || n.secret == "" {
		n.log.DebugContext(ctx, "serving-block notify skipped: notifier disabled", refKind, refID)
		return
	}

	owner, found, err := resolve(ctx, refID)
	if err != nil {
		n.log.ErrorContext(ctx, "serving-block notify: owner resolution failed", refKind, refID, "error", err)
		return
	}
	if !found {
		n.log.DebugContext(ctx, "serving-block notify skipped: no owning account (drift)", refKind, refID)
		return
	}

	verdict, err := n.status.GetServiceStatus(ctx, billing.GetServiceStatusRequest{
		UserID: owner.UserID,
		OrgID:  owner.OrgID,
	})
	if err != nil {
		n.log.ErrorContext(ctx, "serving-block notify: verdict read failed", refKind, refID, "error", err)
		return
	}

	body := servingBlockBody{Blocked: verdict.Blocked}
	if owner.OrgID != uuid.Nil {
		body.OwnerOrgID = owner.OrgID.String()
	} else {
		body.OwnerUserID = owner.UserID.String()
	}
	if err := n.post(ctx, body); err != nil {
		n.log.ErrorContext(ctx, "serving-block notify: post failed (best-effort, skipped)",
			refKind, refID, "blocked", verdict.Blocked, "error", err)
		return
	}
	n.log.InfoContext(ctx, "serving-block verdict pushed", refKind, refID,
		"blocked", verdict.Blocked, "reason", verdict.Reason)
}

func (n *Notifier) post(ctx context.Context, body servingBlockBody) error {
	payload, err := json.Marshal(body)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, n.baseURL+servingBlockPath, bytes.NewReader(payload))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(headerInternalSecret, n.secret)
	resp, err := n.httpc.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	// Drain so the connection can be reused; the body itself is not consumed.
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4<<10))
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("serving-block endpoint returned %d", resp.StatusCode)
	}
	return nil
}
