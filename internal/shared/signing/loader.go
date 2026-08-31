package signing

import (
	"encoding/hex"
	"fmt"
	"strings"
)

// Signer is the set of keys one deployment can sign with.
//
// A deployment holds a key for a domain or it does not. There is no third
// state, and in particular there is no key that signs "for now" while the
// real one is provisioned: SignerFor returns ErrNoKey and the caller cannot
// produce evidence. docs/SECURITY.md's recurring finding is the opposite —
// a surface that reports itself ready on top of a record nothing writes.
type Signer struct {
	keys map[string]Key
}

// Environment variables holding private key material, one per domain.
//
// 🔴 These hold SECRETS. Nothing in this package logs them, no error
// includes them, and no report echoes them. The deployment sources them from
// SSM; see the rotation work that owns their lifecycle.
//
// Each holds the hex-encoded 64-byte ed25519 private key and NOTHING else —
// no id, no domain. Both of those are derived: the id from the material (see
// KeyID) and the domain from which variable it came from. A value that has
// to agree with another value is a value that can disagree with it.
const (
	EnvCustomerAcceptanceKey = "BILLING_SIGNING_KEY_CUSTOMER_ACCEPTANCE"
	EnvBillingEvidenceKey    = "BILLING_SIGNING_KEY_BILLING_EVIDENCE"
	EnvCapabilitiesKey       = "BILLING_SIGNING_KEY_BILLING_CAPABILITIES"
)

// envForDomain maps each domain to the variable that carries its key.
var envForDomain = map[string]string{
	DomainCustomerAcceptance:  EnvCustomerAcceptanceKey,
	DomainBillingEvidence:     EnvBillingEvidenceKey,
	DomainBillingCapabilities: EnvCapabilitiesKey,
}

// Load reads whatever key material this deployment has been given.
//
// getenv is a parameter rather than os.Getenv so the loader is testable
// without an environment, and so a test cannot accidentally read a real key
// out of the developer's shell.
//
// An ABSENT variable is not an error: a deployment that signs nothing is a
// legitimate deployment, and today it is the only kind there is. A PRESENT
// but malformed one IS an error, because it means someone intended to
// provision a key and the deployment would otherwise start with a silently
// smaller set of capabilities than they configured.
func Load(getenv func(string) string) (Signer, error) {
	s := Signer{keys: map[string]Key{}}
	for domain, env := range envForDomain {
		raw := strings.TrimSpace(getenv(env))
		if raw == "" {
			continue
		}
		material, err := hex.DecodeString(raw)
		if err != nil {
			// Deliberately does not quote the value.
			return Signer{}, fmt.Errorf("%s is set but is not hex-encoded key material", env)
		}
		key, err := NewKey(domain, material)
		if err != nil {
			return Signer{}, fmt.Errorf("%s: %w", env, err)
		}
		s.keys[domain] = key
	}
	return s, nil
}

// SignerFor returns the key for a domain, or ErrNoKey.
//
// The zero Signer has no keys, so a caller that forgot to Load gets a
// refusal rather than a nil map panic — the same inversion the predicate
// uses, where every field defaults to the refusing value.
func (s Signer) SignerFor(domain string) (Key, error) {
	if !knownDomain(domain) {
		return Key{}, fmt.Errorf("%w: %q", ErrUnknownDomain, domain)
	}
	k, ok := s.keys[domain]
	if !ok {
		return Key{}, fmt.Errorf("%w: no key for %s (set %s)", ErrNoKey, domain, envForDomain[domain])
	}
	return k, nil
}

// CanSign reports whether this deployment holds a key for a domain.
//
// It is what a readiness surface asks. It is computed from the key material
// actually loaded, never configured, so no deployment can assert it.
func (s Signer) CanSign(domain string) bool {
	_, ok := s.keys[domain]
	return ok
}

// Domains lists the domains this deployment can sign for, for a report.
func (s Signer) Domains() []string {
	out := make([]string, 0, len(s.keys))
	for _, d := range []string{DomainCustomerAcceptance, DomainBillingEvidence, DomainBillingCapabilities} {
		if _, ok := s.keys[d]; ok {
			out = append(out, d)
		}
	}
	return out
}
