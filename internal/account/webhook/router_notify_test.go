package webhook_test

// Serving-block notifier hooks (funding-gates C6): every standing-relevant
// event pushes the owner's current verdict AFTER its store writes succeed.
// The notifier contract itself (env gating, POST shape) is tested in
// internal/account/standing; here only the router's hook wiring is.

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/mirrorstack-ai/billing-engine/internal/account/webhook"
	"github.com/mirrorstack-ai/billing-engine/internal/account/webhook/webhooktest"
)

// recordingNotifier captures which hook fired with which Stripe object id.
type recordingNotifier struct {
	customers      []string
	invoices       []string
	paymentMethods []string
}

func (n *recordingNotifier) NotifyStripeCustomer(_ context.Context, id string) {
	n.customers = append(n.customers, id)
}
func (n *recordingNotifier) NotifyStripeInvoice(_ context.Context, id string) {
	n.invoices = append(n.invoices, id)
}
func (n *recordingNotifier) NotifyStripePaymentMethod(_ context.Context, id string) {
	n.paymentMethods = append(n.paymentMethods, id)
}

func TestNotifyHooks_FireOnStandingRelevantEvents(t *testing.T) {
	t.Run("payment_method.attached notifies by customer", func(t *testing.T) {
		v := &webhooktest.FakeVerifier{Event: cardPMEvent("evt_n1", "payment_method.attached", "pm_n", "cus_n", "visa", "4242", 12, 2029)}
		n := &recordingNotifier{}
		r := newRouter(v, webhooktest.NewFakeStore()).WithServingBlockNotifier(n)

		res := r.Process(context.Background(), []byte(`{}`), "sig")
		require.Equal(t, webhook.StatusOK, res.Status)
		require.Equal(t, []string{"cus_n"}, n.customers)
	})

	t.Run("payment_method.detached notifies by pm id", func(t *testing.T) {
		v := &webhooktest.FakeVerifier{Event: cardPMEvent("evt_n2", "payment_method.detached", "pm_n", "cus_n", "visa", "4242", 12, 2029)}
		n := &recordingNotifier{}
		r := newRouter(v, webhooktest.NewFakeStore()).WithServingBlockNotifier(n)

		res := r.Process(context.Background(), []byte(`{}`), "sig")
		require.Equal(t, webhook.StatusOK, res.Status)
		require.Equal(t, []string{"pm_n"}, n.paymentMethods)
		require.Empty(t, n.customers, "detach resolves through the mirror row, not the unreliable post-detach customer")
	})

	t.Run("detached no-op (pm never mirrored) does not notify", func(t *testing.T) {
		v := &webhooktest.FakeVerifier{Event: cardPMEvent("evt_n3", "payment_method.detached", "pm_ghost", "cus_n", "visa", "4242", 12, 2029)}
		s := webhooktest.NewFakeStore()
		s.SoftDelFound = false
		n := &recordingNotifier{}
		r := newRouter(v, s).WithServingBlockNotifier(n)

		res := r.Process(context.Background(), []byte(`{}`), "sig")
		require.Equal(t, webhook.StatusOK, res.Status)
		require.Empty(t, n.paymentMethods, "nothing changed — no verdict push")
	})

	t.Run("invoice lifecycle notifies by invoice id", func(t *testing.T) {
		v := &webhooktest.FakeVerifier{Event: invoiceEvent("evt_n4", "invoice.paid", "in_n", "paid", 2000, 2000)}
		n := &recordingNotifier{}
		r := newRouter(v, webhooktest.NewFakeStore()).WithServingBlockNotifier(n)

		res := r.Process(context.Background(), []byte(`{}`), "sig")
		require.Equal(t, webhook.StatusOK, res.Status)
		require.Equal(t, []string{"in_n"}, n.invoices)
	})

	t.Run("nil notifier is tolerated (hooks no-op)", func(t *testing.T) {
		v := &webhooktest.FakeVerifier{Event: invoiceEvent("evt_n5", "invoice.paid", "in_n", "paid", 2000, 2000)}
		r := newRouter(v, webhooktest.NewFakeStore()) // no WithServingBlockNotifier

		res := r.Process(context.Background(), []byte(`{}`), "sig")
		require.Equal(t, webhook.StatusOK, res.Status)
	})
}
