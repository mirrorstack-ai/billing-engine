package stripeadapter

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/mirrorstack-ai/billing-engine/internal/intent"
	"github.com/mirrorstack-ai/billing-engine/internal/intent/executor"
	billingstripe "github.com/mirrorstack-ai/billing-engine/internal/shared/stripe"
	"github.com/mirrorstack-ai/billing-engine/internal/shared/stripe/stripetest"
)

func groupDebit(digest string, micros int64, lines ...intent.Line) executor.Debit {
	return executor.Debit{
		IntentDigest:   digest,
		Payer:          intent.Subject{Kind: "org", ID: "org-1"},
		AmountMicros:   micros,
		Currency:       "USD",
		Lines:          lines,
		IdempotencyKey: "intent-" + digest,
	}
}

// 🔴 The whole arithmetic argument for grouping: ONE rounding over the SUMMED
// remainders, not one per intent.
//
// The legacy boundary collector rounds once on its net (cycle/charge.go:595).
// Four intents rounded separately and summed give a different integer — this
// repository has already measured that divergence at one cent on a
// two-component proration — and a cutover must seal exactly what a collection
// takes.
func TestAGroupIsRoundedOnceOverTheSum(t *testing.T) {
	recorder := stripetest.New()
	recorder.Stubs["CreateDraftInvoice"] = billingstripe.Invoice{ID: "in_group"}
	recorder.Stubs["PayInvoiceWithMethod"] = billingstripe.Invoice{ID: "in_group", Status: "paid"}

	// Four remainders that each round UP on their own (…_500 micros = half a
	// cent), so per-intent rounding would total four cents more than the sum
	// rounded once.
	debits := []executor.Debit{
		groupDebit("d1", 1_005_000, line("arrears", 1_005_000)),
		groupDebit("d2", 2_005_000, line("base", 2_005_000)),
		groupDebit("d3", 3_005_000, line("overage", 3_005_000)),
		groupDebit("d4", 4_005_000, line("domains", 4_005_000)),
	}

	out, err := New(recorder, &fixedResolver{customer: "cus_1"}).
		CollectGroup(context.Background(), debits)
	if err != nil {
		t.Fatalf("CollectGroup: %v", err)
	}
	if !out.Succeeded {
		t.Fatal("a paid group did not report success")
	}

	var billed int64
	var items int
	for _, c := range recorder.Calls() {
		if c.Method == "CreateInvoiceItem" {
			items++
			billed += c.AmountCents
		}
	}

	// 10,020,000 micros / 10,000 = 1002 cents exactly.
	if want := int64(1002); billed != want {
		t.Fatalf("the group billed %d cents; one rounding over the sum is %d. "+
			"Per-intent rounding would give %d.", billed, want, 101+201+301+401)
	}
	if items != len(debits) {
		t.Fatalf("%d invoice items for %d intents", items, len(debits))
	}
}

// One invoice, one finalize, one card charge — not one per intent. That is the
// customer-facing half of the argument.
func TestAGroupIsOneInvoiceAndOneCharge(t *testing.T) {
	recorder := stripetest.New()
	recorder.Stubs["CreateDraftInvoice"] = billingstripe.Invoice{ID: "in_group"}
	recorder.Stubs["PayInvoiceWithMethod"] = billingstripe.Invoice{ID: "in_group", Status: "paid"}

	debits := []executor.Debit{
		groupDebit("d1", 5_000_000, line("a", 5_000_000)),
		groupDebit("d2", 5_000_000, line("b", 5_000_000)),
		groupDebit("d3", 5_000_000, line("c", 5_000_000)),
	}
	if _, err := New(recorder, &fixedResolver{customer: "cus_1"}).
		CollectGroup(context.Background(), debits); err != nil {
		t.Fatalf("CollectGroup: %v", err)
	}

	counts := map[string]int{}
	for _, c := range recorder.Calls() {
		counts[c.Method]++
	}
	for method, want := range map[string]int{
		"CreateDraftInvoice":                1,
		"FinalizeInvoiceWithoutAutoAdvance": 1,
		"PayInvoiceWithMethod":              1,
		"CreateInvoiceItem":                 3,
	} {
		if counts[method] != want {
			t.Errorf("%s called %d times, want %d — the group did not settle as one invoice",
				method, counts[method], want)
		}
	}
}

// Each item must carry its OWN key, derived from its own intent. A shared key
// makes the second item a retry of the first at the provider: Stripe returns
// the first again, the invoice carries fewer lines, and the customer is
// charged LESS than the group sealed — silently, because every call succeeded.
func TestEveryGroupItemIsKeyedToItsOwnIntent(t *testing.T) {
	recorder := stripetest.New()
	recorder.Stubs["CreateDraftInvoice"] = billingstripe.Invoice{ID: "in_group"}
	recorder.Stubs["PayInvoiceWithMethod"] = billingstripe.Invoice{ID: "in_group", Status: "paid"}

	debits := []executor.Debit{
		groupDebit("aaa", 3_000_000, line("a1", 1_000_000), line("a2", 2_000_000)),
		groupDebit("bbb", 4_000_000, line("b1", 4_000_000)),
	}
	if _, err := New(recorder, &fixedResolver{customer: "cus_1"}).
		CollectGroup(context.Background(), debits); err != nil {
		t.Fatalf("CollectGroup: %v", err)
	}

	keys := map[string]int{}
	for _, c := range recorder.Calls() {
		if c.Method == "CreateInvoiceItem" {
			keys[c.IdemKey]++
		}
	}
	if len(keys) != 3 {
		t.Fatalf("3 sealed lines produced %d distinct keys: %v", len(keys), keys)
	}
	for key := range keys {
		if !strings.Contains(key, "aaa") && !strings.Contains(key, "bbb") {
			t.Errorf("key %q names neither intent in the group", key)
		}
	}
}

// A group's identity is the SET of its intents, so assembling the same intents
// in a different order must not raise a second invoice for one charge.
func TestAGroupKeyIsOrderIndependent(t *testing.T) {
	a := []executor.Debit{groupDebit("d1", 1), groupDebit("d2", 1), groupDebit("d3", 1)}
	b := []executor.Debit{groupDebit("d3", 1), groupDebit("d1", 1), groupDebit("d2", 1)}
	if groupKey(a) != groupKey(b) {
		t.Fatal("the same intents in a different order produced two group keys, so one " +
			"charge could be invoiced twice")
	}
	c := []executor.Debit{groupDebit("d1", 1), groupDebit("d2", 1)}
	if groupKey(a) == groupKey(c) {
		t.Fatal("two different groups share one key")
	}
}

// A group that mixes payers or currencies is not a group. Discovering that at
// the provider, after a draft exists, is strictly worse than refusing here.
func TestAnInconsistentGroupIsRefusedBeforeAnythingIsCreated(t *testing.T) {
	for name, debits := range map[string][]executor.Debit{
		"mixed payers": {
			groupDebit("d1", 1_000_000, line("a", 1_000_000)),
			func() executor.Debit {
				d := groupDebit("d2", 1_000_000, line("b", 1_000_000))
				d.Payer = intent.Subject{Kind: "org", ID: "org-2"}
				return d
			}(),
		},
		"mixed currencies": {
			groupDebit("d1", 1_000_000, line("a", 1_000_000)),
			func() executor.Debit {
				d := groupDebit("d2", 1_000_000, line("b", 1_000_000))
				d.Currency = "TWD"
				return d
			}(),
		},
		"nothing owed": {
			groupDebit("d1", 0), groupDebit("d2", 0),
		},
		"empty": {},
	} {
		t.Run(name, func(t *testing.T) {
			recorder := stripetest.New()
			_, err := New(recorder, &fixedResolver{customer: "cus_1"}).
				CollectGroup(context.Background(), debits)
			if !errors.Is(err, ErrGroupInconsistent) {
				t.Fatalf("a %s group was accepted: %v", name, err)
			}
			recorder.RequireCollected(t, 0)
			if len(recorder.Calls()) != 0 {
				t.Errorf("%d provider calls were made before the refusal", len(recorder.Calls()))
			}
		})
	}
}

// A group of one is a plain collection. Routing it through the group path
// would give it a different key and a different invoice reference for no
// reason, and a retry across the two paths would then be two invoices.
func TestAGroupOfOneIsAnOrdinaryCollection(t *testing.T) {
	recorder := stripetest.New()
	recorder.Stubs["CreateDraftInvoice"] = billingstripe.Invoice{ID: "in_1"}
	recorder.Stubs["PayInvoiceWithMethod"] = billingstripe.Invoice{ID: "in_1", Status: "paid"}

	d := groupDebit("solo", 5_000_000, line("a", 5_000_000))
	if _, err := New(recorder, &fixedResolver{customer: "cus_1"}).
		CollectGroup(context.Background(), []executor.Debit{d}); err != nil {
		t.Fatalf("CollectGroup: %v", err)
	}
	for _, c := range recorder.Calls() {
		if c.IdemKey != "" && strings.Contains(c.IdemKey, "group") {
			t.Fatalf("a group of one used a group key: %q", c.IdemKey)
		}
	}
}

// The pay step is the one that moves money, so an error there means every
// intent in the group is ambiguous TOGETHER — the executor must retain all of
// their claims, not release the ones it happens to look at first.
func TestAFailedGroupPaymentIsAmbiguousForTheWholeGroup(t *testing.T) {
	recorder := stripetest.New()
	recorder.Stubs["CreateDraftInvoice"] = billingstripe.Invoice{ID: "in_group"}
	recorder.Errs["PayInvoiceWithMethod"] = errors.New("timeout after dispatch")

	out, err := New(recorder, &fixedResolver{customer: "cus_1"}).CollectGroup(
		context.Background(), []executor.Debit{
			groupDebit("d1", 1_000_000, line("a", 1_000_000)),
			groupDebit("d2", 1_000_000, line("b", 1_000_000)),
		})
	if err == nil {
		t.Fatal("a failed payment reported success")
	}
	if !out.Ambiguous {
		t.Fatal("a failed group payment was not reported as ambiguous; the executor would " +
			"release claims on intents the customer may already have been charged for")
	}
}
