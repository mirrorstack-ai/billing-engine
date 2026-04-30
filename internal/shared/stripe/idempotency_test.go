package stripe

import (
	"regexp"
	"testing"
)

var idempotencyKeyRegex = regexp.MustCompile(
	`^bill\.[a-z_][a-z0-9_]*\.[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`,
)

func TestNewIdempotencyKey_Shape(t *testing.T) {
	key := NewIdempotencyKey("create_customer")
	if !idempotencyKeyRegex.MatchString(key) {
		t.Fatalf("key %q does not match expected shape", key)
	}
}

func TestNewIdempotencyKey_Unique(t *testing.T) {
	const n = 1000
	seen := make(map[string]struct{}, n)
	for i := 0; i < n; i++ {
		k := NewIdempotencyKey("op")
		if _, dup := seen[k]; dup {
			t.Fatalf("duplicate key after %d generations: %s", i, k)
		}
		seen[k] = struct{}{}
	}
}
