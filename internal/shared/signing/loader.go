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
// Each holds the hex-encoded 32-byte ed25519 SEED and NOTHING else — no id,
// no domain, and not the 64-byte private-key form. Everything else is
// derived: the whole key from the seed, the id from the key (see KeyID), and
// the domain from which variable it came from. A value that has to agree with
// another value is a value that can disagree with it, and a seed has no
// second half to disagree with.
//
// Generate one with `openssl rand -hex 32`. Sixty-four hex characters.
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

// domainOrder is the order Load reads the variables in.
//
// Explicit, because ranging a map is randomised and the first malformed
// variable is the one named in the error. A startup failure that names a
// different variable on each restart is a startup failure nobody can fix.
var domainOrder = []string{
	DomainCustomerAcceptance,
	DomainBillingEvidence,
	DomainBillingCapabilities,
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
	byID := map[string]string{}

	for _, domain := range domainOrder {
		env := envForDomain[domain]
		raw := strings.TrimSpace(getenv(env))
		if raw == "" {
			continue
		}
		seed, err := hex.DecodeString(raw)
		if err != nil {
			// Deliberately does not quote the value, and deliberately does
			// not wrap hex's own error: hex.InvalidByteError carries the
			// offending byte, which is a byte of the key.
			return Signer{}, fmt.Errorf("%w: %s is set but is not hex-encoded key material",
				ErrIncomplete, env)
		}
		key, err := NewKey(domain, seed)
		if err != nil {
			return Signer{}, fmt.Errorf("%s: %w", env, err)
		}

		// 🔴 One key must not serve two domains.
		//
		// The package's whole separation claim is that a leaked
		// capabilities key cannot mint a customer acceptance. That is true
		// of the TYPE — a Key refuses to sign outside its domain — and it
		// is not true of a DEPLOYMENT that was handed the same material
		// twice, which is the easy mistake when three variables are being
		// provisioned by hand. The ids are derived from the material, so
		// the collision is detectable here and nowhere else.
		if other, dup := byID[key.ID()]; dup {
			return Signer{}, fmt.Errorf("%w: %s and %s hold the same key",
				ErrKeyReused, other, env)
		}
		byID[key.ID()] = env
		s.keys[domain] = key
	}
	return s, nil
}

// String and GoString redact every key this Signer holds, for the reason
// Key.String does: fmt prints unexported fields, and a Signer logged with %v
// would emit the deployment's entire private key material.
func (s Signer) String() string {
	if len(s.keys) == 0 {
		return "signing.Signer{no keys}"
	}
	return "signing.Signer{domains:" + strings.Join(s.Domains(), ",") + " material:REDACTED}"
}

// GoString covers %#v, which does not route through String.
func (s Signer) GoString() string { return s.String() }

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
	for _, d := range domainOrder {
		if _, ok := s.keys[d]; ok {
			out = append(out, d)
		}
	}
	return out
}
