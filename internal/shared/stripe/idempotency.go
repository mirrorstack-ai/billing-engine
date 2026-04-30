package stripe

import "github.com/google/uuid"

// NewIdempotencyKey returns a key suitable for Stripe's Idempotency-Key
// header. Format: bill.<operation>.<uuid>. The bill. prefix namespaces our
// keys against any other Stripe integration sharing the same account.
func NewIdempotencyKey(operation string) string {
	return "bill." + operation + "." + uuid.NewString()
}
