package stripeadapter

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/mirrorstack-ai/billing-engine/internal/intent"
	"github.com/mirrorstack-ai/billing-engine/internal/intent/executor"
	billingstripe "github.com/mirrorstack-ai/billing-engine/internal/shared/stripe"
)

// 🔴 THIS IMPLEMENTS OPTION B OF BOUNDARY-KIND-DECISION.md, WHICH IS OPEN.
//
// The decision is the owner's and has not been made. This is built under the
// stated assumption that B is chosen, because B is the recommendation and it
// is the only option requiring engineering rather than a published rule.
//
// It is wired to NOTHING. No caller exists, no leg produces a group, and
// removing this file removes the whole of it — so choosing A, C or D costs a
// deletion and no rework anywhere else.
//
// # The problem it solves
//
// The period-boundary invoice is TWO of §6's charge kinds — module_usage for
// the closed period's arrears and platform_base for the next period's
// subscription — since §12 item 12 folded module_capacity and custom_domain
// into the base price. It was four before that fold, and the reason a group is
// needed did not change with the count: a boundary still spans more than one
// kind, because arrears and the forward subscription are different kinds no
// matter how the forward half is composed.
//
// A ChargeIntent carries ONE Kind, and migration 054's header says why
// that matters: "it selects which rule of a standing authorization applies: a
// caller that chose it could pick the permission its charge happens to fit."
// So one intent for the whole invoice weakens the authorization control.
//
// But Collect runs draft -> item -> finalize -> pay PER INTENT, rounding
// micros to cents once for that intent alone. So four intents would be four
// invoices, four card charges and four roundings where the legacy leg makes
// one — a different customer statement, and a total that disagrees with
// legacy's single round-on-the-net (cycle/charge.go:595).
//
// CollectGroup is the third answer: each charge keeps its own intent and its
// own true kind, and they settle onto ONE provider invoice, apportioned from
// ONE rounding.
//
// # What it preserves
//
//   - §6's kind-per-intent. Every intent carries the kind it actually is.
//   - INV-008. Each intent is still claimed and settled at most once; the
//     claims are per intent and this function takes none — the executor does,
//     before calling.
//   - The customer's statement: one invoice, one card charge.
//   - The arithmetic: one micros-to-cents rounding over the summed provider
//     remainders, apportioned by largest remainder, exactly as splitCents
//     already does across the lines of a single intent.

// ErrGroupInconsistent refuses a group that cannot share one invoice.
var ErrGroupInconsistent = errors.New("stripeadapter: these intents cannot settle on one invoice")

// CollectGroup settles several sealed intents onto one provider invoice.
//
// It is all-or-nothing at the provider: one draft, one item per sealed line
// across every intent, one finalize, one pay. A group that cannot be
// collected leaves no partial charge, because nothing is finalized until
// every item is on the invoice.
func (a *Adapter) CollectGroup(ctx context.Context, debits []executor.Debit) (executor.CollectResult, error) {
	if len(debits) == 0 {
		return executor.CollectResult{}, fmt.Errorf("%w: no intents", ErrGroupInconsistent)
	}
	if len(debits) == 1 {
		// A group of one is a collection. Routing it through the group path
		// would give it a different idempotency key and a different invoice
		// reference for no reason.
		return a.Collect(ctx, debits[0])
	}

	// Every intent must be for the same payer and the same currency. They are
	// sharing one invoice against one card, so a group that mixes either is
	// not a group — and discovering that at the provider, after a draft
	// exists, is strictly worse than refusing here.
	payer := debits[0].Payer
	currency := strings.ToLower(strings.TrimSpace(debits[0].Currency))
	var total int64
	for _, d := range debits {
		if d.Payer != payer {
			return executor.CollectResult{}, fmt.Errorf(
				"%w: %s and %s are different payers",
				ErrGroupInconsistent, payer.ID, d.Payer.ID)
		}
		if strings.ToLower(strings.TrimSpace(d.Currency)) != currency {
			return executor.CollectResult{}, fmt.Errorf(
				"%w: mixed currencies %q and %q", ErrGroupInconsistent, currency, d.Currency)
		}
		total += d.AmountMicros
	}
	if total <= 0 {
		return executor.CollectResult{}, fmt.Errorf("%w: the group owes %d", ErrGroupInconsistent, total)
	}

	customerID, paymentMethodID, err := a.resolver.ResolvePayer(ctx, payer.Kind, payer.ID)
	if err != nil {
		return executor.CollectResult{}, fmt.Errorf("%w: %w", ErrNoCustomer, err)
	}
	if customerID == "" || paymentMethodID == "" {
		return executor.CollectResult{}, ErrNoCustomer
	}

	// 🔴 ONE rounding, over the SUMMED provider remainders.
	//
	// This is the whole arithmetic argument for grouping. The legacy boundary
	// collector rounds once on its net (cycle/charge.go:595); rounding each
	// intent separately and summing gives a different integer, and this
	// repository has already measured that divergence at one cent on a
	// two-component proration.
	cents := centsFromMicros(total)

	// The group's identity is the SET of digests it settles, order-independent
	// and content-addressed. Two different groups can never share a key, and a
	// retry of the same group is the same requests at the provider.
	key := groupKey(debits)
	ref := "intent-group:" + key

	invoice, err := a.client.CreateDraftInvoice(ctx, customerID, ref, "inv-"+key)
	if err != nil {
		// Nothing is finalized, so nothing is collected. A leaked draft is
		// inert: it can never charge anyone.
		return executor.CollectResult{}, fmt.Errorf("create group draft: %w", err)
	}

	for _, item := range groupItems(cents, debits) {
		if _, err := a.client.CreateInvoiceItem(
			ctx, customerID, invoice.ID, item.cents, currency,
			item.description, billingstripe.LinePeriod{}, item.idemKey,
		); err != nil {
			return executor.CollectResult{}, fmt.Errorf("create group line %s: %w", item.idemKey, err)
		}
	}

	finalized, err := a.client.FinalizeInvoiceWithoutAutoAdvance(ctx, invoice.ID, "fin-"+key)
	if err != nil {
		return executor.CollectResult{}, fmt.Errorf("finalize group: %w", err)
	}

	paid, err := a.client.PayInvoiceWithMethod(ctx, finalized.ID, paymentMethodID, "pay-"+key)
	if err != nil {
		// The pay step is the one that moves money, so an error here means
		// the customer may or may not have been charged. Ambiguous, exactly
		// as it is for a single intent — and for a GROUP it means every
		// intent in it is ambiguous together, which is why the executor must
		// retain all of their claims.
		return executor.CollectResult{Ambiguous: true, Reference: finalized.ID},
			fmt.Errorf("pay group (outcome unknown): %w", err)
	}

	// Stripe's own status is the evidence, exactly as it is for a single
	// intent: a finalized invoice that is not paid has not collected,
	// whatever the call returned.
	switch paid.Status {
	case "paid":
		return executor.CollectResult{Succeeded: true, Reference: paid.ID}, nil
	case "open", "draft":
		return executor.CollectResult{InProgress: true, Reference: paid.ID}, nil
	default:
		return executor.CollectResult{Ambiguous: true, Reference: paid.ID}, nil
	}
}

// groupItem is one line of a grouped invoice.
type groupItem struct {
	cents       int64
	description string
	idemKey     string
}

// groupItems apportions the group's single rounded total across every sealed
// line of every intent.
//
// The apportionment is over LINES, not over intents, so a four-intent boundary
// charge produces the same invoice a four-line single intent would — the
// customer sees what they are being charged for, not four opaque subtotals.
//
// Each item's key carries its intent's digest and its line index, so no two
// items in a group can collide at the provider. A shared key would make the
// second item a retry of the first: Stripe returns the first again, the
// invoice carries fewer lines, and the customer is charged LESS than the group
// sealed — silently, because every call succeeded.
func groupItems(total int64, debits []executor.Debit) []groupItem {
	type located struct {
		line   intent.Line
		digest string
		index  int
	}

	var flat []located
	var lines []intent.Line
	for _, d := range debits {
		if len(d.Lines) == 0 {
			// An intent with no sealed lines still owes its remainder, so it
			// contributes one item under its own digest rather than being
			// dropped from the invoice.
			flat = append(flat, located{
				line:   intent.NewLine("MirrorStack charge", d.IntentDigest, "1", 1, d.AmountMicros),
				digest: d.IntentDigest,
			})
			lines = append(lines, flat[len(flat)-1].line)
			continue
		}
		for i, l := range d.Lines {
			flat = append(flat, located{line: l, digest: d.IntentDigest, index: i})
			lines = append(lines, l)
		}
	}

	split := splitCents(total, lines)
	out := make([]groupItem, len(split))
	for i := range split {
		out[i] = groupItem{
			cents:       split[i].cents,
			description: split[i].description,
			idemKey:     fmt.Sprintf("ii-intent-%s-%d", flat[i].digest, flat[i].index),
		}
	}
	return out
}

// groupKey is the content-addressed identity of a group.
//
// Sorted, so the same set of intents in any order is the same group — a caller
// that assembled its digests differently must not create a second invoice for
// one charge. Hashed, because a Stripe idempotency key has a length limit and
// a boundary group's digests do not fit.
func groupKey(debits []executor.Debit) string {
	digests := make([]string, 0, len(debits))
	for _, d := range debits {
		digests = append(digests, d.IntentDigest)
	}
	sort.Strings(digests)
	sum := sha256.Sum256([]byte(strings.Join(digests, "|")))
	return hex.EncodeToString(sum[:])[:16]
}
