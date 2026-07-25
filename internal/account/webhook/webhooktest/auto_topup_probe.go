package webhooktest

import (
	"context"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/mirrorstack-ai/billing-engine/internal/account/credit"
	billingstripe "github.com/mirrorstack-ai/billing-engine/internal/shared/stripe"
)

// AutoTopUpChargeProbe is a serving-block notifier whose status read reaches
// the credit coordinator's automatic-payment seam. It lets transport tests
// prove that webhook contexts suppress PayInvoiceWithMethod without requiring
// Stripe or a database.
type AutoTopUpChargeProbe struct {
	accountID   uuid.UUID
	coordinator *credit.Coordinator

	mu              sync.Mutex
	evaluationCalls int
	payCalls        int
	errors          []error
}

// NewAutoTopUpChargeProbe returns a probe whose credit snapshot is eligible
// for auto-top-up. An unsuppressed notification therefore reaches
// PayInvoiceWithMethod; a webhook-suppressed notification must not.
func NewAutoTopUpChargeProbe() *AutoTopUpChargeProbe {
	accountID := uuid.New()
	ownerUserID := uuid.New()
	probe := &AutoTopUpChargeProbe{accountID: accountID}
	probe.coordinator = credit.NewCoordinator(
		nil,
		fixedCreditSnapshot{snapshot: credit.Snapshot{
			AccountID:              accountID,
			OwnerUserID:            ownerUserID,
			BillingMode:            "credits",
			SpendableBalanceMicros: 0,
			AutoTopUpEnabled:       true,
			AutoTopUpThreshold:     1,
			ActivatedAt:            time.Date(2026, time.July, 1, 0, 0, 0, 0, time.UTC),
		}},
		fixedCreditProjection{},
		nil,
	).WithAutoTopUpTrigger(probe)
	return probe
}

type fixedCreditSnapshot struct {
	snapshot credit.Snapshot
}

func (s fixedCreditSnapshot) CreditGateSnapshot(context.Context, uuid.UUID) (credit.Snapshot, error) {
	return s.snapshot, nil
}

type fixedCreditProjection struct{}

func (fixedCreditProjection) ProjectedCreditCharge(context.Context, uuid.UUID, uuid.UUID) (credit.Projection, error) {
	return credit.Projection{}, nil
}

func (p *AutoTopUpChargeProbe) evaluate(ctx context.Context) {
	_, err := p.coordinator.OutOfCredits(ctx, p.accountID)
	p.mu.Lock()
	defer p.mu.Unlock()
	p.evaluationCalls++
	if err != nil {
		p.errors = append(p.errors, err)
	}
}

func (p *AutoTopUpChargeProbe) NotifyStripeCustomer(ctx context.Context, _ string) {
	p.evaluate(ctx)
}

func (p *AutoTopUpChargeProbe) NotifyStripeInvoice(ctx context.Context, _ string) {
	p.evaluate(ctx)
}

func (p *AutoTopUpChargeProbe) NotifyCreditInvoice(ctx context.Context, _ string) {
	p.evaluate(ctx)
}

func (p *AutoTopUpChargeProbe) NotifyStripePaymentMethod(ctx context.Context, _ string) {
	p.evaluate(ctx)
}

// TriggerAutoTopUp models the production command adapter around
// autotopup.Executor.Trigger and deliberately reaches the named Stripe seam.
func (p *AutoTopUpChargeProbe) TriggerAutoTopUp(
	ctx context.Context,
	_ uuid.UUID,
	_ int64,
) (credit.AutoTopUpTriggerResult, error) {
	_, err := p.PayInvoiceWithMethod(ctx, "in_probe", "pm_probe", "probe")
	return credit.AutoTopUpTriggerResult{Attempted: true}, err
}

// PayInvoiceWithMethod records the card-charge origin the webhook must never
// reach.
func (p *AutoTopUpChargeProbe) PayInvoiceWithMethod(
	context.Context,
	string,
	string,
	string,
) (billingstripe.Invoice, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.payCalls++
	return billingstripe.Invoice{ID: "in_probe", Status: "paid"}, nil
}

func (p *AutoTopUpChargeProbe) EvaluationCalls() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.evaluationCalls
}

func (p *AutoTopUpChargeProbe) PayInvoiceCalls() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.payCalls
}

func (p *AutoTopUpChargeProbe) Errors() []error {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]error(nil), p.errors...)
}

func (p *AutoTopUpChargeProbe) Reset() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.evaluationCalls = 0
	p.payCalls = 0
	p.errors = nil
}
