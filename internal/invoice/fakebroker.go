package invoice

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"sync"
)

// FakeBroker is an in-memory Broker for sandbox and test use only. It is NOT
// the isolated sealing writer billing-engine#292 builds: everything it seals
// lives in this process's heap, which is exactly the property a real broker
// must not have. It exists so this package — and ProviderFake, the sandbox
// adapter — can be built and tested end-to-end before #292 lands, per the
// issue's "build against a fake broker first and never read a key in the
// engine process" instruction.
//
// It never returns a sealed payload once Seal has stored it: Verify
// classifies without exposing it, matching the real Broker's contract, so
// code written against FakeBroker does not accidentally depend on a leak the
// real broker would not offer.
type FakeBroker struct {
	mu     sync.Mutex
	sealed map[string]sealedEntry
	// VerifyFunc, when set, lets a test control Verify's answer per call
	// instead of the default (success if sealed and not revoked).
	VerifyFunc func(provider Provider, payload SealedPayload) (VerifyResult, error)
}

type sealedEntry struct {
	provider Provider
	payload  SealedPayload
	version  int
	revoked  bool
}

// NewFakeBroker returns an empty FakeBroker.
func NewFakeBroker() *FakeBroker {
	return &FakeBroker{sealed: make(map[string]sealedEntry)}
}

func (b *FakeBroker) Seal(_ context.Context, provider Provider, payload SealedPayload) (string, int, error) {
	ref, err := randomRef()
	if err != nil {
		return "", 0, err
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	b.sealed[ref] = sealedEntry{provider: provider, payload: payload, version: 1}
	return ref, 1, nil
}

func (b *FakeBroker) Revoke(_ context.Context, secretRef string) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	entry, ok := b.sealed[secretRef]
	if !ok {
		return fmt.Errorf("invoice: fakebroker: unknown secret ref")
	}
	entry.revoked = true
	b.sealed[secretRef] = entry
	return nil
}

func (b *FakeBroker) Verify(_ context.Context, provider Provider, secretRef string, _ int) (VerifyResult, error) {
	b.mu.Lock()
	entry, ok := b.sealed[secretRef]
	b.mu.Unlock()
	if !ok || entry.revoked {
		return VerifyResultFailure, nil
	}
	if b.VerifyFunc != nil {
		return b.VerifyFunc(provider, entry.payload)
	}
	if provider == ProviderFake {
		return VerifyResultNotVerifiable, nil
	}
	return VerifyResultSuccess, nil
}

func randomRef() (string, error) {
	var buf [16]byte
	if _, err := rand.Read(buf[:]); err != nil {
		return "", err
	}
	return "fake_" + hex.EncodeToString(buf[:]), nil
}
