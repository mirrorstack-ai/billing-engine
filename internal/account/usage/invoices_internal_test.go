package usage

// Internal (package usage) tests for the unexported ListInvoices helpers: the
// opaque cursor codec and the mirror cents→micros decoder. The service-level
// behavior (clamping, pagination flow, draft exclusion contract) is covered
// from the outside in invoices_test.go; these pin the primitives.

import (
	"math/big"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/require"
)

func TestInvoiceCursor_RoundTrip(t *testing.T) {
	// Sub-second precision must survive: RFC3339Nano keeps the full
	// timestamptz resolution so the strict `<` keyset comparison cannot skip
	// same-second neighbors.
	createdAt := time.Date(2026, 6, 19, 12, 34, 56, 789012000, time.UTC)
	id := uuid.New()

	c, err := decodeInvoiceCursor(encodeInvoiceCursor(createdAt, id))
	require.NoError(t, err)
	require.True(t, c.CreatedAt.Equal(createdAt), "timestamp survives to the nanosecond")
	require.Equal(t, id, c.ID)
}

func TestInvoiceCursor_RoundTrip_NormalizesToUTC(t *testing.T) {
	// A zoned input encodes as its UTC instant — the cursor is a point in
	// time, not a rendering.
	loc := time.FixedZone("UTC+8", 8*3600)
	createdAt := time.Date(2026, 6, 19, 20, 0, 0, 0, loc)
	id := uuid.New()

	c, err := decodeInvoiceCursor(encodeInvoiceCursor(createdAt, id))
	require.NoError(t, err)
	require.True(t, c.CreatedAt.Equal(createdAt), "same instant regardless of zone")
}

func TestDecodeInvoiceCursor_MalformedShapes(t *testing.T) {
	for name, token := range map[string]string{
		"not base64":         "!!!",
		"no separator":       "Z2FyYmFnZQ",                              // base64("garbage")
		"bad timestamp":      "bm90LWEtdGltZXwxMjM",                     // base64("not-a-time|123")
		"bad uuid":           "MjAyNi0wNi0xOVQwMDowMDowMFp8bm90LXV1aWQ", // base64("2026-06-19T00:00:00Z|not-uuid")
		"empty":              "",
		"std base64 padding": "YWJjZA==", // padding chars are invalid in Raw encoding
	} {
		_, err := decodeInvoiceCursor(token)
		require.Error(t, err, "shape %q must be rejected", name)
	}
}

// numericFromString builds a pgtype.Numeric from its decimal text form —
// exactly how a NUMERIC arrives from Postgres.
func numericFromString(t *testing.T, s string) pgtype.Numeric {
	t.Helper()
	var n pgtype.Numeric
	require.NoError(t, n.Scan(s))
	return n
}

func TestCentsNumericToMicros(t *testing.T) {
	for name, tc := range map[string]struct {
		in   pgtype.Numeric
		want int64
	}{
		"whole cents ×10_000":  {numericFromString(t, "1234"), 12_340_000}, // $12.34
		"zero":                 {numericFromString(t, "0"), 0},
		"one cent":             {numericFromString(t, "1"), 10_000},
		"NULL decodes to zero": {pgtype.Numeric{}, 0},
		// The mirror column is NUMERIC precisely so a non-2-decimal currency
		// stays representable — fractional minor units convert exactly.
		"fractional cents": {numericFromString(t, "2.5"), 25_000},
		// Sub-micro fraction rounds half-up through the shared rounding
		// point: 0.00005 cents = 0.5 µ$ → 1 µ$.
		"sub-micro rounds half-up": {pgtype.Numeric{Int: big.NewInt(5), Exp: -5, Valid: true}, 1},
	} {
		got, err := centsNumericToMicros(tc.in)
		require.NoError(t, err, name)
		require.Equal(t, tc.want, got, name)
	}
}

func TestCentsNumericToMicros_DoesNotMutateInput(t *testing.T) {
	// The helper shifts a VALUE COPY's exponent; the caller's Numeric (and
	// its shared *big.Int) must be untouched — a mutated input would corrupt
	// a second read of the same scanned row.
	n := numericFromString(t, "1234")
	_, err := centsNumericToMicros(n)
	require.NoError(t, err)
	require.EqualValues(t, 0, n.Exp, "input exponent unchanged")
	require.EqualValues(t, 1234, n.Int.Int64(), "input mantissa unchanged")
}
