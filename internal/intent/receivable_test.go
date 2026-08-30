package intent

import (
	"errors"
	"testing"
	"time"
)

func sourceIntent(t *testing.T, micros int64) ChargeIntent {
	t.Helper()
	d := catalogDraft(KindModuleUsage)
	d.Lines = []Line{NewLine("usage", "quiz-core", "1.4.0", 1, micros)}
	s, err := Seal(d)
	if err != nil {
		t.Fatalf("Seal source: %v", err)
	}
	return s
}

func receivableDraft(micros int64) Draft {
	d := catalogDraft(KindCollectReceivable)
	d.Lines = []Line{NewLine("remainder", "receivable", "1", 1, micros)}
	return d
}

// 🔴 A receivable LINKS, it does not SUPERSEDE.
//
// Superseding replaces a document: the original is no longer owed. A
// receivable collects what is left of one that IS still owed — both stay live,
// with a stated arithmetic relation. docs/DESIGN.md §6 lists
// collect_receivable as its own kind, funded by "a linked intent for the
// remaining amount only".
//
// Building this on supersedes would mark the original replaced while the
// customer still owed it — the reason this is a separate field rather than a
// reuse of the one that already existed.
func TestAReceivableLinksWithoutSuperseding(t *testing.T) {
	source := sourceIntent(t, 20_000_000)

	r, err := source.CollectRemainderOf(receivableDraft(5_000_000))
	if err != nil {
		t.Fatalf("CollectRemainderOf: %v", err)
	}

	if r.Collects() != source.Digest() {
		t.Fatalf("Collects() = %q, want the source digest %q", r.Collects(), source.Digest())
	}
	if r.Supersedes() != "" {
		t.Fatalf("a receivable superseded its source (%q) — the original is still owed", r.Supersedes())
	}
	if r.Digest() == source.Digest() {
		t.Fatal("the receivable carries the source's digest; it is not a distinct document")
	}
}

// The link is inside the digest, so a receivable cannot be re-pointed at a
// different source without becoming a different document.
func TestTheLinkIsPartOfTheDigest(t *testing.T) {
	a := sourceIntent(t, 20_000_000)
	b := sourceIntent(t, 30_000_000)

	ra, err := a.CollectRemainderOf(receivableDraft(5_000_000))
	if err != nil {
		t.Fatalf("CollectRemainderOf(a): %v", err)
	}
	rb, err := b.CollectRemainderOf(receivableDraft(5_000_000))
	if err != nil {
		t.Fatalf("CollectRemainderOf(b): %v", err)
	}
	if ra.Digest() == rb.Digest() {
		t.Fatal("two receivables collecting DIFFERENT sources share a digest — the link is not digested")
	}
}

// A receivable for more than the original owed is not a remainder. It is a new
// charge wearing a link, and it would collect money the customer never agreed
// to under an authority granted for something smaller.
func TestAReceivableCannotExceedWhatItCollects(t *testing.T) {
	source := sourceIntent(t, 20_000_000)

	if _, err := source.CollectRemainderOf(receivableDraft(20_000_001)); !errors.Is(err, ErrReceivableExceedsSource) {
		t.Fatalf("a receivable larger than its source was sealed: %v", err)
	}
	// Exactly the whole amount is a legitimate remainder — nothing collected yet.
	if _, err := source.CollectRemainderOf(receivableDraft(20_000_000)); err != nil {
		t.Fatalf("a receivable for the full outstanding amount was refused: %v", err)
	}
}

// The remainder must be owed by the same payer, in the same currency. A
// receivable that moves either is collecting somebody else's debt, or the same
// debt in a unit nobody agreed to.
func TestAReceivableCannotMoveThePayerOrCurrency(t *testing.T) {
	source := sourceIntent(t, 20_000_000)

	moved := receivableDraft(5_000_000)
	moved.Payer = Subject{Kind: "org", ID: "someone-else"}
	if _, err := source.CollectRemainderOf(moved); !errors.Is(err, ErrReceivablePayerMoved) {
		t.Fatalf("a receivable moved the payer: %v", err)
	}

	redenominated := receivableDraft(5_000_000)
	redenominated.Currency = "eur"
	if _, err := source.CollectRemainderOf(redenominated); !errors.Is(err, ErrReceivableCurrencyMoved) {
		t.Fatalf("a receivable moved the currency: %v", err)
	}
}

// It must be sealed as the kind §6 names for it, not as whatever the caller
// was collecting.
func TestAReceivableMustBeSealedAsAReceivable(t *testing.T) {
	source := sourceIntent(t, 20_000_000)

	wrong := catalogDraft(KindModuleUsage)
	wrong.Lines = []Line{NewLine("remainder", "receivable", "1", 1, 5_000_000)}
	if _, err := source.CollectRemainderOf(wrong); !errors.Is(err, ErrKindNotInCatalog) {
		t.Fatalf("a receivable was sealed as module_usage: %v", err)
	}
}

func TestAnUnsealedSourceCollectsNothing(t *testing.T) {
	var empty ChargeIntent
	if _, err := empty.CollectRemainderOf(receivableDraft(1_000)); !errors.Is(err, ErrNotSealed) {
		t.Fatalf("an unsealed intent produced a receivable: %v", err)
	}
	_ = time.Now
}
