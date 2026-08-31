package intent

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"strconv"
	"time"
)

// canonicalSchema tags the encoding. A verifier that reproduces a
// digest is reproducing this exact layout, so the layout has a version
// and the version is inside the bytes it names: changing the encoding
// without changing the tag would let two different rules produce the
// same digest.
// v2 adds the tax determination's verification class. The tag moves with
// the layout by design: the doc above says changing the encoding without
// changing the tag would let two different rules produce the same digest,
// and a v1 intent that omitted the class must not collide with a v2 one
// that states it.
//
// Safe to change exactly now — production holds ZERO sealed intents
// (charge_intents = 0, measured 2026-08-31). After the first, this becomes
// a migration of live sealed documents past the INV-003 reject-sealed-update
// trigger.
const canonicalSchema = "mirrorstack.charge-intent/v2"

// canonicalEncoder builds the byte string a digest is taken over.
//
// Every value is length-prefixed. That is the whole point of the type
// and not a detail: with plain concatenation or a separator character,
// two different intents can produce identical bytes. Fields "ab" then
// "c" and fields "a" then "bc" concatenate to the same "abc", and a
// separator only moves the problem to whichever value contains the
// separator. A charge intent's digest is what a customer is told they
// approved, so two intents sharing one digest means a customer can be
// shown one document and charged under another.
//
// Length prefixes make the encoding injective: the byte string can be
// parsed back into exactly the field sequence that produced it, so
// distinct field sequences cannot collide.
type canonicalEncoder struct {
	buf []byte
}

func newCanonicalEncoder() *canonicalEncoder {
	e := &canonicalEncoder{}
	e.string(canonicalSchema)
	return e
}

// string appends a length-prefixed value.
func (e *canonicalEncoder) string(v string) {
	var n [8]byte
	binary.BigEndian.PutUint64(n[:], uint64(len(v)))
	e.buf = append(e.buf, n[:]...)
	e.buf = append(e.buf, v...)
}

// int appends an integer in its decimal form, length-prefixed like any
// other value. Money is integer micro-dollars throughout, so there is
// no float to round and no locale to depend on.
func (e *canonicalEncoder) int(v int64) { e.string(strconv.FormatInt(v, 10)) }

// count appends a collection length, so that a sequence of N items
// cannot be confused with a different sequence that happens to
// concatenate the same way.
func (e *canonicalEncoder) count(n int) { e.int(int64(n)) }

// time appends an instant as RFC 3339 in UTC with nanosecond
// precision. A zero time is encoded as an empty string rather than as
// year 1, so "unset" and "the first instant representable" stay
// distinguishable.
func (e *canonicalEncoder) time(v time.Time) {
	if v.IsZero() {
		e.string("")
		return
	}
	e.string(v.UTC().Format(time.RFC3339Nano))
}

// digest returns the hex SHA-256 of everything appended so far.
func (e *canonicalEncoder) digest() string {
	sum := sha256.Sum256(e.buf)
	return hex.EncodeToString(sum[:])
}

// bytes exposes the encoded form, for tests that need to show two
// intents encode differently rather than merely digest differently.
func (e *canonicalEncoder) bytes() []byte { return e.buf }
