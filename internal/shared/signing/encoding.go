package signing

import "strconv"

// encoder is the length-prefixed encoding a signature is taken over.
//
// It is the same construction as internal/intent's canonical encoder, and it
// is duplicated rather than shared on purpose: intent's encoding is the
// CHARGE DOCUMENT's identity and must never move for a reason belonging to
// signing. Two encodings that must not change together should not be one
// function that either can change.
//
// The property both need is injectivity. Writing a length before every value
// means no two different field sequences produce one byte string, so a
// signature over one statement can never attest to another. A separator
// would not: "a|b" and "a" + "|b" are the same bytes, and several fields
// here are free text a caller controls.
type encoder struct{ buf []byte }

func newEncoder() *encoder { return &encoder{} }

func (e *encoder) str(v string) {
	e.buf = strconv.AppendInt(e.buf, int64(len(v)), 10)
	e.buf = append(e.buf, ':')
	e.buf = append(e.buf, v...)
}

func (e *encoder) bytes() []byte { return e.buf }
