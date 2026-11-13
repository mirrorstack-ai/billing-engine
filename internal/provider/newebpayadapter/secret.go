package newebpayadapter

import (
	"fmt"
	"log/slog"
)

// HashKeyLen is the exact length NewebPay's HashKey must be: 32 ASCII bytes.
const HashKeyLen = 32

// HashIVLen is the exact length NewebPay's HashIV must be: 16 ASCII bytes.
const HashIVLen = 16

const redacted = "newebpayadapter(redacted)"

// HashKey is the AES-256 key half of a store's NewebPay credential.
//
// Every print, marshal and log path is redacted on purpose: a HashKey
// that leaked into a log line or an error message would let whoever
// reads it forge TradeSha for that store indefinitely, with nothing
// in this package to detect the forgery. Reveal is the one way out,
// and it is named so a reviewer reading a diff sees exactly where the
// raw bytes leave the type.
type HashKey struct {
	b [HashKeyLen]byte
}

// NewHashKey validates length and wraps raw as a HashKey.
func NewHashKey(raw []byte) (HashKey, error) {
	if len(raw) != HashKeyLen {
		return HashKey{}, fmt.Errorf("newebpayadapter: HashKey must be %d bytes, got %d", HashKeyLen, len(raw))
	}
	var k HashKey
	copy(k.b[:], raw)
	return k, nil
}

// Reveal returns the raw key bytes. Named deliberately loud: anything
// calling this is handling the secret itself, not a redacted stand-in.
func (k HashKey) Reveal() []byte {
	out := make([]byte, HashKeyLen)
	copy(out, k.b[:])
	return out
}

func (k HashKey) String() string               { return redacted }
func (k HashKey) GoString() string             { return redacted }
func (k HashKey) Format(f fmt.State, _ rune)   { _, _ = f.Write([]byte(redacted)) }
func (k HashKey) MarshalJSON() ([]byte, error) { return []byte(`"` + redacted + `"`), nil }
func (k HashKey) MarshalText() ([]byte, error) { return []byte(redacted), nil }
func (k HashKey) LogValue() slog.Value         { return slog.StringValue(redacted) }

// HashIV is the AES-256-CBC IV half of a store's NewebPay credential.
//
// Redacted on every print, marshal and log path for the same reason as
// HashKey: see its doc comment.
type HashIV struct {
	b [HashIVLen]byte
}

// NewHashIV validates length and wraps raw as a HashIV.
func NewHashIV(raw []byte) (HashIV, error) {
	if len(raw) != HashIVLen {
		return HashIV{}, fmt.Errorf("newebpayadapter: HashIV must be %d bytes, got %d", HashIVLen, len(raw))
	}
	var iv HashIV
	copy(iv.b[:], raw)
	return iv, nil
}

// Reveal returns the raw IV bytes. See HashKey.Reveal.
func (iv HashIV) Reveal() []byte {
	out := make([]byte, HashIVLen)
	copy(out, iv.b[:])
	return out
}

func (iv HashIV) String() string               { return redacted }
func (iv HashIV) GoString() string             { return redacted }
func (iv HashIV) Format(f fmt.State, _ rune)   { _, _ = f.Write([]byte(redacted)) }
func (iv HashIV) MarshalJSON() ([]byte, error) { return []byte(`"` + redacted + `"`), nil }
func (iv HashIV) MarshalText() ([]byte, error) { return []byte(redacted), nil }
func (iv HashIV) LogValue() slog.Value         { return slog.StringValue(redacted) }
