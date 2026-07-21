// Package credit contains the disposable, real-time credit estimate cache
// contract shared by usage ingest and the account billing service. Postgres is
// always authoritative for wallet balances and ledger writes; this package is
// deliberately limited to a rebuildable counter.
package credit

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	redis "github.com/redis/go-redis/v9"
)

// ErrUnavailable identifies a deliberately unwired counter. Callers treat it
// exactly like a Redis outage: log where useful and fail open.
var ErrUnavailable = errors.New("credit estimate counter unavailable")

// Counter is the disposable per-account accrued-charge estimate. PeriodStart
// is part of the key so a stale prior-period value can never block a new
// period. Implementations must treat a missing key as (found=false, nil).
type Counter interface {
	Add(ctx context.Context, accountID uuid.UUID, periodStart time.Time, deltaMicros int64) (estimateMicros int64, err error)
	Get(ctx context.Context, accountID uuid.UUID, periodStart time.Time) (estimateMicros int64, found bool, err error)
	Set(ctx context.Context, accountID uuid.UUID, periodStart time.Time, estimateMicros int64) error
}

// UsageEvent is the narrow payload usage.Service sends to its best-effort
// credit hook after a fresh usage insert. ApproximateChargeMicros has already
// been priced at ingest from the event's snapshotted catalog definition.
type UsageEvent struct {
	AccountID               uuid.UUID
	AppID                   uuid.UUID
	EventID                 string
	ApproximateChargeMicros int64
	PeriodStart             time.Time
	PeriodEnd               time.Time
}

// UsageEvaluator is implemented by the wallet coordinator and injected into
// usage.Service. Errors never fail usage ingestion.
type UsageEvaluator interface {
	EvaluateCreditUsage(ctx context.Context, event UsageEvent) error
}

// Projection is the authoritative live account bill used to rebuild or
// reconcile the disposable estimate counter.
type Projection struct {
	AmountMicros int64
	PeriodStart  time.Time
	PeriodEnd    time.Time
}

// ProjectionProvider is implemented by usage.Service so the wallet service
// reuses the full live bill math instead of maintaining a second pricing path.
type ProjectionProvider interface {
	ProjectedCreditCharge(ctx context.Context, ownerUserID, ownerOrgID uuid.UUID) (Projection, error)
}

// RedisCounter stores estimates in Redis/Valkey. A 45-day TTL makes the cache
// self-cleaning while comfortably covering the longest anchored monthly
// period plus delayed reconciliation.
type RedisCounter struct {
	client *redis.Client
	ttl    time.Duration
}

const defaultEstimateTTL = 45 * 24 * time.Hour

// NewRedisCounter parses REDIS_URL-compatible connection strings. It does not
// ping at construction: an unreachable service is surfaced by each operation
// and callers fail open, avoiding a startup dependency on disposable state.
func NewRedisCounter(rawURL string) (*RedisCounter, error) {
	if rawURL == "" {
		return nil, ErrUnavailable
	}
	opts, err := redis.ParseURL(rawURL)
	if err != nil {
		return nil, fmt.Errorf("parse REDIS_URL: %w", err)
	}
	return &RedisCounter{client: redis.NewClient(opts), ttl: defaultEstimateTTL}, nil
}

func (c *RedisCounter) Add(ctx context.Context, accountID uuid.UUID, periodStart time.Time, deltaMicros int64) (int64, error) {
	key := estimateKey(accountID, periodStart)
	pipe := c.client.TxPipeline()
	incr := pipe.IncrBy(ctx, key, deltaMicros)
	pipe.Expire(ctx, key, c.ttl)
	if _, err := pipe.Exec(ctx); err != nil {
		return 0, err
	}
	return incr.Val(), nil
}

func (c *RedisCounter) Get(ctx context.Context, accountID uuid.UUID, periodStart time.Time) (int64, bool, error) {
	v, err := c.client.Get(ctx, estimateKey(accountID, periodStart)).Int64()
	if errors.Is(err, redis.Nil) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, err
	}
	return v, true, nil
}

func (c *RedisCounter) Set(ctx context.Context, accountID uuid.UUID, periodStart time.Time, estimateMicros int64) error {
	return c.client.Set(ctx, estimateKey(accountID, periodStart), estimateMicros, c.ttl).Err()
}

func estimateKey(accountID uuid.UUID, periodStart time.Time) string {
	return fmt.Sprintf("billing:credit-estimate:%s:%d", accountID, periodStart.UTC().Unix())
}
