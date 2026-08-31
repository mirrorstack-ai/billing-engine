package signing

import (
	"crypto/ed25519"
	"encoding/hex"
	"fmt"
	"strings"
)

// TrustRoot is the set of public keys a verifier already holds.
//
// docs/VERIFICATION.md:81-83: "Pin the root, not the response. A verifier
// that learns its trust root from the service it is checking has checked
// nothing. That root must ship in this repository, in signed releases, and
// in a separately operated channel, and no relayed response may introduce a
// new one."
//
// So a TrustRoot is CONSTRUCTED, never parsed out of a statement. Verify
// takes one and looks up the statement's key id inside it; an id the root
// does not hold fails, which is the whole control.
type TrustRoot struct {
	// keys is (domain, id) -> public key. Domain is part of the lookup
	// key rather than a field to compare afterwards, so a capabilities
	// key cannot verify a customer acceptance even by id collision.
	keys map[rootKey]ed25519.PublicKey
}

type rootKey struct{ domain, id string }

// NewTrustRoot builds a root from hex-encoded public keys.
//
// It refuses an unknown domain and a malformed key rather than skipping it:
// a root that silently dropped an entry would fail verification later with
// "unknown key id", which reads as a bad statement rather than a bad
// deployment.
func NewTrustRoot(entries []PinnedKey) (TrustRoot, error) {
	root := TrustRoot{keys: make(map[rootKey]ed25519.PublicKey, len(entries))}
	for _, e := range entries {
		if !knownDomain(e.Domain) {
			return TrustRoot{}, fmt.Errorf("%w: %q for key %q", ErrUnknownDomain, e.Domain, e.ID)
		}
		if strings.TrimSpace(e.ID) == "" {
			return TrustRoot{}, fmt.Errorf("%w: key id", ErrIncomplete)
		}
		raw, err := hex.DecodeString(e.PublicKeyHex)
		if err != nil {
			return TrustRoot{}, fmt.Errorf("%w: key %q is not hex: %v", ErrIncomplete, e.ID, err)
		}
		if len(raw) != ed25519.PublicKeySize {
			return TrustRoot{}, fmt.Errorf("%w: an ed25519 public key is %d bytes, key %q has %d",
				ErrIncomplete, ed25519.PublicKeySize, e.ID, len(raw))
		}
		// The id must be the one the material itself produces. Without
		// this a root can pin a key under an id that is not its own, and
		// every signature it should have verified fails as "bad
		// signature" rather than as the misconfiguration it is.
		// The id must be the one the material itself produces. Without
		// this a root can pin a key under an id that is not its own, and
		// every signature it should have verified fails as "bad
		// signature" rather than as the misconfiguration it is.
		if derived := KeyID(ed25519.PublicKey(raw)); derived != e.ID {
			return TrustRoot{}, fmt.Errorf("%w: key pinned as %q but its public half derives %q",
				ErrIncomplete, e.ID, derived)
		}
		k := rootKey{domain: e.Domain, id: e.ID}
		if _, dup := root.keys[k]; dup {
			return TrustRoot{}, fmt.Errorf("%w: key %q appears twice for %s", ErrIncomplete, e.ID, e.Domain)
		}
		root.keys[k] = ed25519.PublicKey(raw)
	}
	return root, nil
}

// PinnedKey is one entry of a trust root, as it appears in source.
type PinnedKey struct {
	ID           string
	Domain       string
	PublicKeyHex string
}

// PublicKey looks up a key by id WITHIN a domain.
func (r TrustRoot) PublicKey(id, domain string) (ed25519.PublicKey, bool) {
	k, ok := r.keys[rootKey{domain: domain, id: id}]
	return k, ok
}

// Len reports how many keys this root holds.
//
// It exists so a deployment can report an EMPTY root rather than behaving as
// though it had one: a root with no keys verifies nothing, and a surface
// claiming verification readiness on top of it would be asserting a capacity
// it does not have.
func (r TrustRoot) Len() int { return len(r.keys) }

// PinnedKeys is the trust root that ships in this repository.
//
// 🔴 It is EMPTY, and that is the honest state. No signing key has been
// provisioned for any MirrorStack environment, so there is no public half to
// pin. A placeholder here would be worse than nothing: Verify would find an
// id, check a signature no one can produce, and a deployment could report
// itself ready to verify evidence that cannot exist.
//
// Filling it is a deliberate act with a diff attached. Each entry names the
// environment it belongs to, and the private half lives in SSM and never in
// this repository.
//
// Until it has an entry:
//   - Repository() returns a root of length zero;
//   - every Verify fails with ErrUnknownKey;
//   - no deployment can sign, because NewSigner has no key material to load;
//   - and the Capabilities surface reports evidence signing as unsupported,
//     which is docs/VERIFICATION.md:70-72's "`unsupported` until its own
//     suite passes".
var PinnedKeys = []PinnedKey{}

// Repository returns the trust root that ships in this tree.
//
// It panics on a malformed entry, because a build carrying an unusable
// pinned key is a build that should not start: the alternative is a
// deployment that runs with a silently smaller root than its source claims.
func Repository() TrustRoot {
	root, err := NewTrustRoot(PinnedKeys)
	if err != nil {
		panic("signing: the trust root pinned in this repository is malformed: " + err.Error())
	}
	return root
}
