package stripetest

import (
	"context"
	"reflect"
	"sort"
	"testing"

	billingstripe "github.com/mirrorstack-ai/billing-engine/internal/shared/stripe"
)

// The effect classification is the only thing in this package that can
// be wrong in a way that matters: an assertion built on it is exactly
// as trustworthy as the table below. A method filed as a read when it
// can charge would turn RequireNoCollection into a green that proves
// nothing.
//
// So every provider method is driven once and its recorded effect
// checked. The cases are written out rather than generated, because a
// generator would derive the expectation from the same code under test.
func TestEveryMethodIsClassified(t *testing.T) {
	ctx := context.Background()
	var period billingstripe.LinePeriod

	cases := []struct {
		method string
		want   Effect
		drive  func(r *Recorder)
	}{
		// Reads.
		{"GetCustomer", EffectRead, func(r *Recorder) { r.GetCustomer(ctx, "cus_1") }},
		{"GetInvoice", EffectRead, func(r *Recorder) { r.GetInvoice(ctx, "in_1") }},
		{"FindInvoiceByRef", EffectRead, func(r *Recorder) { r.FindInvoiceByRef(ctx, "cus_1", "run:1") }},
		{"ListInvoiceItems", EffectRead, func(r *Recorder) { r.ListInvoiceItems(ctx, "in_1") }},
		{"ListInvoicePayments", EffectRead, func(r *Recorder) { r.ListInvoicePayments(ctx, "in_1") }},
		{"RetrieveCharge", EffectRead, func(r *Recorder) { r.RetrieveCharge(ctx, "ch_1") }},

		// Mutations that cannot collect.
		{"CreateCustomer", EffectMutate, func(r *Recorder) { r.CreateCustomer(ctx, "acct_1", "a@b.invalid") }},
		{"UpdateCustomerEmail", EffectMutate, func(r *Recorder) { r.UpdateCustomerEmail(ctx, "cus_1", "a@b.invalid") }},
		{"CreateCheckoutSession", EffectMutate, func(r *Recorder) { r.CreateCheckoutSession(ctx, "cus_1", "https://x.invalid") }},
		{"DetachPaymentMethod", EffectMutate, func(r *Recorder) { r.DetachPaymentMethod(ctx, "pm_1") }},
		{"SetDefaultPaymentMethod", EffectMutate, func(r *Recorder) { r.SetDefaultPaymentMethod(ctx, "cus_1", "pm_1") }},
		{"CreateDraftInvoice", EffectMutate, func(r *Recorder) { r.CreateDraftInvoice(ctx, "cus_1", "run:1", "inv-1") }},
		{"CreateCreditPurchaseInvoice", EffectMutate, func(r *Recorder) {
			r.CreateCreditPurchaseInvoice(ctx, "cus_1", "acct_1", "led_1", "inv-1")
		}},
		{"CreateAutoTopUpInvoice", EffectMutate, func(r *Recorder) {
			r.CreateAutoTopUpInvoice(ctx, "cus_1", "pm_1", "acct_1", "led_1", "inv-1")
		}},
		{"CreateInvoiceItem", EffectMutate, func(r *Recorder) {
			r.CreateInvoiceItem(ctx, "cus_1", "in_1", 1000, "usd", "line", period, "ii-1")
		}},
		{"CreateCombinedProrationInvoiceItem", EffectMutate, func(r *Recorder) {
			r.CreateCombinedProrationInvoiceItem(ctx, "cus_1", "in_1", 1000, "usd", "line", period, "app-ii-1", billingstripe.CombinedProrationItemIdentity{})
		}},
		{"FinalizeInvoiceWithoutAutoAdvance", EffectMutate, func(r *Recorder) {
			r.FinalizeInvoiceWithoutAutoAdvance(ctx, "in_1", "fin-1")
		}},
		{"VoidInvoice", EffectMutate, func(r *Recorder) { r.VoidInvoice(ctx, "in_1", "void-1") }},
		{"DeleteDraftInvoice", EffectMutate, func(r *Recorder) { r.DeleteDraftInvoice(ctx, "in_1") }},

		// Collection: these can take money.
		{"FinalizeInvoice", EffectCollect, func(r *Recorder) { r.FinalizeInvoice(ctx, "in_1", "fin-1") }},
		{"PayInvoice", EffectCollect, func(r *Recorder) { r.PayInvoice(ctx, "in_1") }},
		{"PayInvoiceWithMethod", EffectCollect, func(r *Recorder) { r.PayInvoiceWithMethod(ctx, "in_1", "pm_1", "pay-1") }},
	}

	seen := map[string]bool{}
	for _, tc := range cases {
		t.Run(tc.method, func(t *testing.T) {
			r := New()
			tc.drive(r)

			calls := r.Calls()
			if len(calls) != 1 {
				t.Fatalf("driving %s recorded %d calls, want exactly 1", tc.method, len(calls))
			}
			if calls[0].Method != tc.method {
				t.Fatalf("recorded method %q, want %q", calls[0].Method, tc.method)
			}
			if calls[0].Effect != tc.want {
				t.Fatalf("%s recorded effect %v, want %v", tc.method, calls[0].Effect, tc.want)
			}
		})
		seen[tc.method] = true
	}

	// The compile-time interface assertions in assert.go guarantee the
	// Recorder implements every provider method. This guarantees the
	// table above drives every one of them, so a newly added method
	// cannot sit unclassified and unasserted.
	for _, m := range providerMethods() {
		if !seen[m] {
			t.Errorf("provider method %s is implemented but never classified by this test", m)
		}
	}
}

// TestReadsAreNotMutations pins the property the assertions rest on:
// the read methods, driven together, leave nothing for
// RequireNoProviderMutation to complain about.
func TestReadsAreNotMutations(t *testing.T) {
	ctx := context.Background()
	r := New()

	r.GetCustomer(ctx, "cus_1")
	r.GetInvoice(ctx, "in_1")
	r.FindInvoiceByRef(ctx, "cus_1", "run:1")
	r.ListInvoiceItems(ctx, "in_1")
	r.ListInvoicePayments(ctx, "in_1")
	r.RetrieveCharge(ctx, "ch_1")

	if got := r.CallsWithEffect(EffectMutate); len(got) != 0 {
		t.Fatalf("reads recorded %d mutations: %s", len(got), describe(got))
	}
	if got := r.CallsWithEffect(EffectCollect); len(got) != 0 {
		t.Fatalf("reads recorded %d collections: %s", len(got), describe(got))
	}
	if got := r.CallsWithEffect(EffectRead); len(got) != 6 {
		t.Fatalf("recorded %d reads, want 6", len(got))
	}
}

// TestMutationsCarryTheirIdempotencyKey pins that the recorder captures
// the key, since RequireEveryMutationKeyed is worthless if it does not.
func TestMutationsCarryTheirIdempotencyKey(t *testing.T) {
	ctx := context.Background()
	r := New()

	r.CreateDraftInvoice(ctx, "cus_1", "run:1", "inv-run-1")
	r.FinalizeInvoice(ctx, "in_1", "fin-run-1")

	calls := r.Calls()
	if calls[0].IdemKey != "inv-run-1" {
		t.Errorf("CreateDraftInvoice recorded key %q, want %q", calls[0].IdemKey, "inv-run-1")
	}
	if calls[1].IdemKey != "fin-run-1" {
		t.Errorf("FinalizeInvoice recorded key %q, want %q", calls[1].IdemKey, "fin-run-1")
	}
}

// providerMethods returns the union of the method names across every
// provider interface the Recorder implements.
//
// Derived from the interface types themselves rather than from a list,
// so that adding a method to any of them makes TestEveryMethodIsClassified
// fail until the new call has been classified.
func providerMethods() []string {
	ifaces := []reflect.Type{
		reflect.TypeOf((*billingstripe.Client)(nil)).Elem(),
		reflect.TypeOf((*billingstripe.CombinedProrationClient)(nil)).Elem(),
		reflect.TypeOf((*billingstripe.CreditPurchaseClient)(nil)).Elem(),
		reflect.TypeOf((*billingstripe.AutoTopUpClient)(nil)).Elem(),
	}
	set := map[string]struct{}{}
	for _, it := range ifaces {
		for i := 0; i < it.NumMethod(); i++ {
			set[it.Method(i).Name] = struct{}{}
		}
	}
	out := make([]string, 0, len(set))
	for name := range set {
		out = append(out, name)
	}
	sort.Strings(out)
	return out
}
