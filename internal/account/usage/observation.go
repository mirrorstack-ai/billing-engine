package usage

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"
)

const (
	observationVersionLegacy = 1
	observationVersionV2     = 2

	maxSubjectBytes        = 256
	maxMetadataBytes       = 4 << 10
	maxMetadataMembers     = 32
	maxMetadataDepth       = 4
	maxMetadataKeyBytes    = 64
	maxMetadataStringBytes = 512
	maxMetadataArrayItems  = 32
	maxMetadataNumberBytes = 128
)

// AggregationKey is an optional catalog-owned grouping identity. The only v2
// key is subject: on a peak metric it means SUM(MAX(value) per subject). A
// zero value preserves all existing aggregation semantics.
type AggregationKey string

const AggregationKeySubject AggregationKey = "subject"

// OccurrencePolicy is the persisted reason an accepted row used its billing
// timestamp. It makes v1 receipt-time behavior and accepted v2 lateness visible
// without consulting logs.
type OccurrencePolicy string

const (
	OccurrencePolicyV1IngestTime OccurrencePolicy = "v1_ingest_time"
	OccurrencePolicyOnTime       OccurrencePolicy = "on_time"
	OccurrencePolicyLateOpen     OccurrencePolicy = "late_open"
	OccurrencePolicyFirstFunded  OccurrencePolicy = "first_funded"
)

// UsageRejectionReason is the bounded audit vocabulary for rejected occurrence
// decisions. Metadata is intentionally absent from the rejection record.
type UsageRejectionReason string

const (
	UsageRejectionOccurredFuture UsageRejectionReason = "occurred_at_future"
	UsageRejectionOccurredTooOld UsageRejectionReason = "occurred_at_too_old"
	UsageRejectionPeriodClosed   UsageRejectionReason = "period_closed"
)

var (
	ErrUsageEventConflict        = errors.New("usage event id already belongs to a different canonical payload")
	ErrUsageAccountTimingChanged = errors.New("usage account activation changed during admission")
	ErrUsagePeriodClosed         = errors.New("usage observation belongs to a closed billing period")
	ErrUsageOccurredFuture       = errors.New("usage observation occurrence is too far in the future")
	ErrUsageOccurredTooOld       = errors.New("usage observation occurrence is too old")
)

func normalizedObservationVersion(version int) (int, error) {
	switch version {
	case 0, observationVersionLegacy:
		return observationVersionLegacy, nil
	case observationVersionV2:
		return observationVersionV2, nil
	default:
		return 0, fmt.Errorf("unsupported observation version %d", version)
	}
}

func validateSubject(subject string) error {
	if subject == "" {
		return nil
	}
	if len(subject) > maxSubjectBytes {
		return fmt.Errorf("subject exceeds %d bytes", maxSubjectBytes)
	}
	if !utf8.ValidString(subject) {
		return errors.New("subject must be valid UTF-8")
	}
	for _, r := range subject {
		if unicode.IsControl(r) {
			return errors.New("subject must not contain control characters")
		}
	}
	return nil
}

// canonicalMetadata validates and canonicalizes diagnostic metadata. Object
// keys are sorted and JSON numbers are normalized through finite float64, so
// whitespace, object key order, and equivalent forms such as 1 and 1.0 hash
// identically. Metadata never participates in aggregation identity.
func canonicalMetadata(raw json.RawMessage) (json.RawMessage, error) {
	if len(raw) == 0 {
		return nil, nil
	}
	if len(raw) > maxMetadataBytes {
		return nil, fmt.Errorf("metadata exceeds %d bytes", maxMetadataBytes)
	}
	if !utf8.Valid(raw) {
		return nil, errors.New("metadata must be valid UTF-8 JSON")
	}
	if err := validateMetadataMemberNames(raw); err != nil {
		return nil, err
	}

	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var value any
	if err := decoder.Decode(&value); err != nil {
		return nil, fmt.Errorf("metadata must be valid JSON: %w", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			return nil, errors.New("metadata must contain exactly one JSON value")
		}
		return nil, fmt.Errorf("metadata has trailing data: %w", err)
	}
	if _, ok := value.(map[string]any); !ok {
		return nil, errors.New("metadata must be a JSON object")
	}

	state := metadataValidationState{}
	var canonical bytes.Buffer
	if err := appendCanonicalMetadata(&canonical, value, 1, &state); err != nil {
		return nil, err
	}
	if canonical.Len() > maxMetadataBytes {
		return nil, fmt.Errorf("canonical metadata exceeds %d bytes", maxMetadataBytes)
	}
	return append(json.RawMessage(nil), canonical.Bytes()...), nil
}

// validateMetadataMemberNames walks the source token stream before decoding
// into maps. encoding/json otherwise collapses duplicate object names with
// last-write-wins semantics, which could bypass the source-member cap and make
// canonical identity depend on parser behavior.
func validateMetadataMemberNames(raw json.RawMessage) error {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	members := 0
	var walk func() error
	walk = func() error {
		token, err := decoder.Token()
		if err != nil {
			return err
		}
		delim, ok := token.(json.Delim)
		if !ok {
			return nil
		}
		switch delim {
		case '{':
			seen := make(map[string]struct{})
			for decoder.More() {
				nameToken, err := decoder.Token()
				if err != nil {
					return err
				}
				name, ok := nameToken.(string)
				if !ok {
					return errors.New("metadata object name must be a string")
				}
				members++
				if members > maxMetadataMembers {
					return fmt.Errorf("metadata exceeds %d object members", maxMetadataMembers)
				}
				if _, duplicate := seen[name]; duplicate {
					return fmt.Errorf("metadata contains duplicate key %q", name)
				}
				seen[name] = struct{}{}
				if err := walk(); err != nil {
					return err
				}
			}
			_, err = decoder.Token()
			return err
		case '[':
			for decoder.More() {
				if err := walk(); err != nil {
					return err
				}
			}
			_, err = decoder.Token()
			return err
		default:
			return errors.New("metadata contains an unexpected JSON delimiter")
		}
	}
	return walk()
}

type metadataValidationState struct {
	members int
}

func appendCanonicalMetadata(dst *bytes.Buffer, value any, depth int, state *metadataValidationState) error {
	if depth > maxMetadataDepth {
		return fmt.Errorf("metadata exceeds maximum depth %d", maxMetadataDepth)
	}
	switch value := value.(type) {
	case map[string]any:
		state.members += len(value)
		if state.members > maxMetadataMembers {
			return fmt.Errorf("metadata exceeds %d object members", maxMetadataMembers)
		}
		keys := make([]string, 0, len(value))
		for key := range value {
			if err := validateMetadataKey(key); err != nil {
				return err
			}
			keys = append(keys, key)
		}
		sort.Strings(keys)
		dst.WriteByte('{')
		for i, key := range keys {
			if i > 0 {
				dst.WriteByte(',')
			}
			encodedKey, _ := json.Marshal(key)
			dst.Write(encodedKey)
			dst.WriteByte(':')
			if err := appendCanonicalMetadata(dst, value[key], depth+1, state); err != nil {
				return err
			}
		}
		dst.WriteByte('}')
	case []any:
		if len(value) > maxMetadataArrayItems {
			return fmt.Errorf("metadata array exceeds %d items", maxMetadataArrayItems)
		}
		dst.WriteByte('[')
		for i, item := range value {
			if i > 0 {
				dst.WriteByte(',')
			}
			if err := appendCanonicalMetadata(dst, item, depth+1, state); err != nil {
				return err
			}
		}
		dst.WriteByte(']')
	case string:
		if len(value) > maxMetadataStringBytes {
			return fmt.Errorf("metadata string exceeds %d bytes", maxMetadataStringBytes)
		}
		encoded, _ := json.Marshal(value)
		dst.Write(encoded)
	case json.Number:
		if len(value.String()) > maxMetadataNumberBytes {
			return fmt.Errorf("metadata number exceeds %d bytes", maxMetadataNumberBytes)
		}
		number, err := strconv.ParseFloat(value.String(), 64)
		if err != nil || math.IsNaN(number) || math.IsInf(number, 0) {
			return errors.New("metadata number must be finite float64")
		}
		canonical, err := canonicalJSONNumber(value.String())
		if err != nil {
			return err
		}
		dst.WriteString(canonical)
	case bool:
		if value {
			dst.WriteString("true")
		} else {
			dst.WriteString("false")
		}
	case nil:
		dst.WriteString("null")
	default:
		return fmt.Errorf("metadata contains unsupported value type %T", value)
	}
	return nil
}

// canonicalJSONNumber preserves the exact decimal value instead of rounding
// through float64. It renders value = coefficient * 10^exponent after removing
// insignificant leading/trailing zeroes, so 1, 1.0, and 10e-1 share a payload
// fingerprint while adjacent integers above 2^53 remain distinct. ParseFloat
// above is retained as the explicit finite-float64 value bound.
func canonicalJSONNumber(source string) (string, error) {
	negative := false
	if strings.HasPrefix(source, "-") {
		negative = true
		source = source[1:]
	}

	mantissa, exponentSource, hasExponent := source, "", false
	if index := strings.IndexAny(source, "eE"); index >= 0 {
		mantissa, exponentSource, hasExponent = source[:index], source[index+1:], true
	}
	exponent := int64(0)
	if hasExponent {
		parsed, err := strconv.ParseInt(exponentSource, 10, 64)
		if err != nil {
			return "", errors.New("metadata number exponent is out of range")
		}
		exponent = parsed
	}

	integer, fraction := mantissa, ""
	if index := strings.IndexByte(mantissa, '.'); index >= 0 {
		integer, fraction = mantissa[:index], mantissa[index+1:]
	}
	if exponent < math.MinInt64+int64(len(fraction)) {
		return "", errors.New("metadata number exponent is out of range")
	}
	exponent -= int64(len(fraction))
	digits := strings.TrimLeft(integer+fraction, "0")
	if digits == "" {
		return "0", nil
	}
	trimmed := strings.TrimRight(digits, "0")
	trailingZeroes := len(digits) - len(trimmed)
	if exponent > math.MaxInt64-int64(trailingZeroes) {
		return "", errors.New("metadata number exponent is out of range")
	}
	exponent += int64(trailingZeroes)

	var canonical strings.Builder
	if negative {
		canonical.WriteByte('-')
	}
	canonical.WriteString(trimmed)
	if exponent != 0 {
		canonical.WriteByte('e')
		canonical.WriteString(strconv.FormatInt(exponent, 10))
	}
	return canonical.String(), nil
}

func validateMetadataKey(key string) error {
	if len(key) == 0 || len(key) > maxMetadataKeyBytes {
		return fmt.Errorf("metadata key must be between 1 and %d bytes", maxMetadataKeyBytes)
	}
	if !isASCIILetter(key[0]) {
		return fmt.Errorf("metadata key %q must start with an ASCII letter", key)
	}
	for i := 1; i < len(key); i++ {
		c := key[i]
		if !isASCIILetter(c) && (c < '0' || c > '9') && c != '_' && c != '.' && c != '-' {
			return fmt.Errorf("metadata key %q contains an invalid character", key)
		}
	}
	return nil
}

func isASCIILetter(value byte) bool {
	return value >= 'A' && value <= 'Z' || value >= 'a' && value <= 'z'
}

// observationFingerprint hashes an unambiguous length-prefixed encoding. It
// excludes receipt time and resolved account ID, while including every stable
// semantic wire/identity field approved at the trust boundary. Occurrence is
// normalized UTC. Catalog-derived kind and aggregation_key are deliberately
// excluded: a later catalog mode change must not turn an otherwise identical
// delivery retry into an event-id conflict.
func observationFingerprint(event UsageEvent) []byte {
	hash := sha256.New()
	writeFingerprintField := func(value string) {
		// 🔴 Same reasoning as meteringlock.SubjectKey: a truncated length
		// prefix breaks injectivity, and here that means two DIFFERENT usage
		// events sharing a fingerprint — one silently deduplicated away and
		// never billed. Unreachable in practice, and the width cannot change
		// without invalidating every fingerprint already stored.
		if uint64(len(value)) > math.MaxUint32 {
			panic("usage: fingerprint field exceeds the 4 GiB length prefix")
		}
		var size [4]byte
		binary.BigEndian.PutUint32(size[:], uint32(len(value))) //nolint:gosec // guarded immediately above
		_, _ = hash.Write(size[:])
		_, _ = hash.Write([]byte(value))
	}

	value := strconv.FormatFloat(event.Value, 'g', -1, 64)
	if event.Value == 0 {
		value = "0"
	}
	occurredAt := ""
	if !event.OccurredAt.IsZero() {
		occurredAt = event.OccurredAt.UTC().Format(time.RFC3339Nano)
	}
	writeFingerprintField(strconv.Itoa(event.ObservationVersion))
	writeFingerprintField(event.AppID.String())
	writeFingerprintField(event.ModuleID.String())
	writeFingerprintField(event.OwnerUserID.String())
	writeFingerprintField(event.OwnerOrgID.String())
	writeFingerprintField(event.Metric)
	writeFingerprintField(event.Model)
	writeFingerprintField(event.ModuleVersion)
	writeFingerprintField(value)
	writeFingerprintField(event.Subject)
	writeFingerprintField(string(event.Metadata))
	writeFingerprintField(occurredAt)
	return hash.Sum(nil)
}
