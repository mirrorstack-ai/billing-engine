// Package creditrecovery guards already-authorized credit recovery paths with
// a request-time migration-048/049 capability check.
//
// Rollout can be switched off while Stripe or a durable ledger attempt still
// needs reconciliation. Those recovery paths must remain available, but they
// must not prepare or execute wallet SQL until both migrations are actually
// present. RuntimeCapability deliberately caches only a successful probe:
// false and error results are retried on a later request so an in-progress
// migration can become usable without restarting the process.
package creditrecovery

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/google/uuid"

	"github.com/mirrorstack-ai/billing-engine/internal/account/autotopup"
	"github.com/mirrorstack-ai/billing-engine/internal/account/creditledger"
	"github.com/mirrorstack-ai/billing-engine/internal/account/creditpurchase"
)

// ErrUnavailable is returned before an executor is called when the combined
// credit runtime schema is absent or its capability probe failed.
var ErrUnavailable = errors.New("credit recovery runtime schema is unavailable")

// Probe reports whether the established base-048 and migration-049 runtime
// capabilities are ready. Production supplies config.CreditRecoverySchemaReady.
type Probe func(context.Context) (bool, error)

// RuntimeCapability serializes cold probes and caches TRUE only.
type RuntimeCapability struct {
	probe Probe
	ready atomic.Bool
	mu    sync.Mutex
}

func NewRuntimeCapability(probe Probe) *RuntimeCapability {
	if probe == nil {
		panic("creditrecovery.NewRuntimeCapability: probe must not be nil")
	}
	return &RuntimeCapability{probe: probe}
}

// Require performs no work after a prior true result. A false result or probe
// error remains uncached so a later request re-checks the expanded schema.
func (c *RuntimeCapability) Require(ctx context.Context) error {
	if c == nil {
		return ErrUnavailable
	}
	if c.ready.Load() {
		return nil
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if c.ready.Load() {
		return nil
	}
	ready, err := c.probe(ctx)
	if err != nil {
		return fmt.Errorf("%w: probe failed: %v", ErrUnavailable, err)
	}
	if !ready {
		return ErrUnavailable
	}
	c.ready.Store(true)
	return nil
}

type autoTopUpRecovery interface {
	Recover(context.Context, uuid.UUID) (autotopup.Result, error)
}

type guardedAutoTopUpRecovery struct {
	capability *RuntimeCapability
	inner      autoTopUpRecovery
}

// GuardAutoTopUpRecovery prevents Pending/ledger reads until the runtime
// capability is ready.
func GuardAutoTopUpRecovery(
	capability *RuntimeCapability,
	inner autoTopUpRecovery,
) autoTopUpRecovery {
	if capability == nil || inner == nil {
		panic("creditrecovery.GuardAutoTopUpRecovery: capability and recovery must not be nil")
	}
	return &guardedAutoTopUpRecovery{capability: capability, inner: inner}
}

func (g *guardedAutoTopUpRecovery) Recover(
	ctx context.Context,
	accountID uuid.UUID,
) (autotopup.Result, error) {
	if err := g.capability.Require(ctx); err != nil {
		return autotopup.Result{}, err
	}
	return g.inner.Recover(ctx, accountID)
}

// ManualPurchaseExecutor is the narrow manual-purchase method used by the
// billing service. *creditpurchase.Executor satisfies it.
type ManualPurchaseExecutor interface {
	Resume(context.Context, creditpurchase.Attempt) (creditpurchase.Result, error)
}

type guardedManualPurchaseExecutor struct {
	capability *RuntimeCapability
	inner      ManualPurchaseExecutor
}

// GuardManualPurchaseExecutor adds a defense-in-depth check immediately before
// the resource-authoritative executor performs any durable attempt read.
func GuardManualPurchaseExecutor(
	capability *RuntimeCapability,
	inner ManualPurchaseExecutor,
) ManualPurchaseExecutor {
	if capability == nil || inner == nil {
		panic("creditrecovery.GuardManualPurchaseExecutor: capability and executor must not be nil")
	}
	return &guardedManualPurchaseExecutor{capability: capability, inner: inner}
}

func (g *guardedManualPurchaseExecutor) Resume(
	ctx context.Context,
	attempt creditpurchase.Attempt,
) (creditpurchase.Result, error) {
	if err := g.capability.Require(ctx); err != nil {
		return creditpurchase.Result{}, err
	}
	return g.inner.Resume(ctx, attempt)
}

// WebhookReconciler is the shared paid/failure surface implemented by both
// durable credit executors.
type WebhookReconciler interface {
	ReconcileWebhookPaid(context.Context, string) (creditledger.Settlement, error)
	ReconcileWebhookFailure(context.Context, string, string) (creditledger.FailureReconciliation, error)
}

type guardedWebhookReconciler struct {
	capability *RuntimeCapability
	inner      WebhookReconciler
}

// GuardWebhookReconciler gates only the canonical credit routes to which this
// wrapper is attached. Ordinary invoice events never call it and therefore
// never probe or name migration-048/049 state.
func GuardWebhookReconciler(
	capability *RuntimeCapability,
	inner WebhookReconciler,
) WebhookReconciler {
	if capability == nil || inner == nil {
		panic("creditrecovery.GuardWebhookReconciler: capability and reconciler must not be nil")
	}
	return &guardedWebhookReconciler{capability: capability, inner: inner}
}

func (g *guardedWebhookReconciler) ReconcileWebhookPaid(
	ctx context.Context,
	stripeInvoiceID string,
) (creditledger.Settlement, error) {
	if err := g.capability.Require(ctx); err != nil {
		return creditledger.Settlement{}, err
	}
	return g.inner.ReconcileWebhookPaid(ctx, stripeInvoiceID)
}

func (g *guardedWebhookReconciler) ReconcileWebhookFailure(
	ctx context.Context,
	stripeInvoiceID string,
	failureCode string,
) (creditledger.FailureReconciliation, error) {
	if err := g.capability.Require(ctx); err != nil {
		return creditledger.FailureReconciliation{}, err
	}
	return g.inner.ReconcileWebhookFailure(ctx, stripeInvoiceID, failureCode)
}
