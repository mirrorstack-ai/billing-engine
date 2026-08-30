package stripeadapter

import (
	"context"
	"errors"
	"testing"

	"github.com/mirrorstack-ai/billing-engine/internal/intent"
	"github.com/mirrorstack-ai/billing-engine/internal/intent/executor"
	billingstripe "github.com/mirrorstack-ai/billing-engine/internal/shared/stripe"
	"github.com/mirrorstack-ai/billing-engine/internal/shared/stripe/stripetest"
)

type fixedResolver struct {
	customer string
	err      error
	sawKind  string
	sawID    string
}

func (r *fixedResolver) ResolvePayer(_ context.Context, kind, id string) (string, string, error) {
	r.sawKind, r.sawID = kind, id
	if r.customer == "" {
		return "", "", r.err
	}
	return r.customer, "pm_test", r.err
}

func debit() executor.Debit {
	return executor.Debit{
		IntentDigest:   "abcdef0123456789",
		Payer:          intent.Subject{Kind: "org", ID: "org-1"},
		AmountMicros:   1_234_500,
		Currency:       "USD",
		IdempotencyKey: "intent-abcdef0123456789",
	}
}

// The adapter must ask WHO to charge using the payer sealed into the
// intent. If it took a provider customer id from the request, whoever
// built that request could point a sealed intent at somebody else's
// card.
func TestTheAdapterResolvesTheSealedPayer(t *testing.T) {
	recorder := stripetest.New()
	recorder.Stubs["PayInvoiceWithMethod"] = billingstripe.Invoice{ID: "in_1", Status: "paid"}
	resolver := &fixedResolver{customer: "cus_1"}

	if _, err := New(recorder, resolver).Collect(context.Background(), debit()); err != nil {
		t.Fatalf("Collect: %v", err)
	}
	if resolver.sawKind != "org" || resolver.sawID != "org-1" {
		t.Errorf("resolved (%q, %q), want the sealed payer (org, org-1)",
			resolver.sawKind, resolver.sawID)
	}
}

// One intent, one invoice, one line, one finalize — and every step
// keyed on the digest, so a retry is the same requests rather than more
// objects.
func TestOneIntentBecomesOneKeyedInvoice(t *testing.T) {
	recorder := stripetest.New()
	recorder.Stubs["CreateDraftInvoice"] = billingstripe.Invoice{ID: "in_1"}
	recorder.Stubs["PayInvoiceWithMethod"] = billingstripe.Invoice{ID: "in_1", Status: "paid"}

	result, err := New(recorder, &fixedResolver{customer: "cus_1"}).
		Collect(context.Background(), debit())
	if err != nil {
		t.Fatalf("Collect: %v", err)
	}
	if !result.Succeeded {
		t.Fatal("a paid invoice did not report success")
	}

	calls := recorder.Calls()
	if len(calls) != 4 {
		t.Fatalf("made %d provider calls, want exactly 4 "+
			"(draft, line, finalize, pay): %v", len(calls), calls)
	}
	recorder.RequireCollected(t, 1)
	recorder.RequireEveryMutationKeyed(t)

	for _, c := range calls {
		if c.IdemKey == "" {
			continue
		}
		if want := "intent-abcdef0123456789"; len(c.IdemKey) < len(want) ||
			c.IdemKey[len(c.IdemKey)-len(want):] != want {
			t.Errorf("%s keyed %q, which is not derived from the intent digest",
				c.Method, c.IdemKey)
		}
	}
}

// 🔴 Pay is the step that moves money, so an error there means the
// customer may or may not have been charged. It must be reported as
// ambiguous — the executor retains its claim on that, and a second
// attempt cannot happen.
func TestAPayErrorIsAmbiguousNotFailed(t *testing.T) {
	recorder := stripetest.New()
	recorder.Stubs["CreateDraftInvoice"] = billingstripe.Invoice{ID: "in_1"}
	recorder.Errs["PayInvoiceWithMethod"] = errors.New("gateway timeout")

	result, err := New(recorder, &fixedResolver{customer: "cus_1"}).
		Collect(context.Background(), debit())

	if err == nil {
		t.Fatal("a pay timeout returned no error")
	}
	if !result.Ambiguous {
		t.Fatal("a pay timeout was resolved one way or the other; " +
			"the customer may already have been charged")
	}
	if result.Succeeded {
		t.Fatal("an errored finalize reported success")
	}
}

// An error BEFORE the pay is unambiguous: nothing was collected,
// because a finalized-but-unpaid invoice has taken no money.
func TestAnErrorBeforeThePayIsNotAmbiguous(t *testing.T) {
	recorder := stripetest.New()
	recorder.Errs["CreateDraftInvoice"] = errors.New("bad request")

	result, err := New(recorder, &fixedResolver{customer: "cus_1"}).
		Collect(context.Background(), debit())

	if err == nil {
		t.Fatal("a failed draft returned no error")
	}
	if result.Ambiguous {
		t.Fatal("a failure before the pay was reported as ambiguous; " +
			"an unpaid invoice cannot have charged anyone")
	}
	recorder.RequireNoCollection(t, "a failed draft creation")
}

// Stripe's own status is the evidence, and an unrecognised one is never
// read as success.
func TestProviderStatusDecidesTheOutcome(t *testing.T) {
	cases := map[string]struct {
		succeeded  bool
		ambiguous  bool
		inProgress bool
	}{
		"paid":  {succeeded: true},
		"open":  {inProgress: true},
		"draft": {inProgress: true},
		// Terminal and not collected. Not ambiguous: the rail said no.
		"void":          {},
		"uncollectible": {},
		// An unrecognised status is never read as success, and never as
		// in-progress either — claiming the rail will report on
		// something it has not acknowledged would leave the intent
		// waiting forever.
		"":              {},
		"something_new": {},
	}

	for status, want := range cases {
		t.Run(status, func(t *testing.T) {
			recorder := stripetest.New()
			recorder.Stubs["CreateDraftInvoice"] = billingstripe.Invoice{ID: "in_1"}
			recorder.Stubs["PayInvoiceWithMethod"] = billingstripe.Invoice{ID: "in_1", Status: status}

			result, err := New(recorder, &fixedResolver{customer: "cus_1"}).
				Collect(context.Background(), debit())
			if err != nil {
				t.Fatalf("Collect: %v", err)
			}
			if result.Succeeded != want.succeeded {
				t.Errorf("status %q succeeded = %v, want %v", status, result.Succeeded, want.succeeded)
			}
			if result.Ambiguous != want.ambiguous {
				t.Errorf("status %q ambiguous = %v, want %v", status, result.Ambiguous, want.ambiguous)
			}
			if result.InProgress != want.inProgress {
				t.Errorf("status %q inProgress = %v, want %v", status, result.InProgress, want.inProgress)
			}
		})
	}
}

// A payer with no provider identity is refused before anything is
// created.
func TestAPayerWithNoCustomerIsRefused(t *testing.T) {
	for name, resolver := range map[string]*fixedResolver{
		"resolver errors":   {err: errors.New("no account")},
		"resolver is empty": {customer: ""},
	} {
		t.Run(name, func(t *testing.T) {
			recorder := stripetest.New()
			_, err := New(recorder, resolver).Collect(context.Background(), debit())
			if !errors.Is(err, ErrNoCustomer) {
				t.Fatalf("err = %v, want %v", err, ErrNoCustomer)
			}
			recorder.RequireNoProviderMutation(t, "an unresolvable payer")
		})
	}
}

// Rounding is half-up rather than Go's truncation. Truncating always
// favours one party, and doing it silently on every line is how a
// systematic difference appears in reconciliation with no single cause
// to point at.
func TestCentsRoundHalfUp(t *testing.T) {
	cases := map[int64]int64{
		0:         0,
		1:         0,
		4_999:     0,
		5_000:     1,
		9_999:     1,
		10_000:    1,
		14_999:    1,
		15_000:    2,   // 1.5 cents rounds UP, where truncation gives 1
		1_234_500: 123, // $1.2345 = 123.45 cents, and .45 rounds down
		-5_000:    -1,
		-4_999:    0,
	}
	for micros, want := range cases {
		if got := centsFromMicros(micros); got != want {
			t.Errorf("centsFromMicros(%d) = %d, want %d", micros, got, want)
		}
	}
}
