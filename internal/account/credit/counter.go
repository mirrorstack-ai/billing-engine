// Package credit contains the disposable, real-time credit estimate cache
// contract shared by usage ingest and the account billing service. Postgres is
// always authoritative for wallet balances and ledger writes; this package is
// deliberately limited to a rebuildable counter.
package credit

import (
	"context"
	"errors"
	"fmt"
	"strconv"
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
	AddIfPresent(ctx context.Context, accountID uuid.UUID, periodStart time.Time, deltaMicros int64) (previousMicros, estimateMicros int64, found bool, err error)
	Get(ctx context.Context, accountID uuid.UUID, periodStart time.Time) (estimateMicros int64, found bool, err error)
	SetMax(ctx context.Context, accountID uuid.UUID, periodStart time.Time, estimateMicros int64) (MaxResult, error)
	SetIfEqual(ctx context.Context, accountID uuid.UUID, periodStart time.Time, expectedMicros, estimateMicros int64) (applied bool, err error)
	Set(ctx context.Context, accountID uuid.UUID, periodStart time.Time, estimateMicros int64) error
	ClaimBlockNotification(ctx context.Context, accountID uuid.UUID, periodStart time.Time) (claimed bool, err error)
	ClearBlockNotification(ctx context.Context, accountID uuid.UUID, periodStart time.Time) error
	ClaimBoundaryNotification(ctx context.Context, accountID uuid.UUID, periodStart time.Time) (claimed bool, err error)
	ClearBoundaryNotification(ctx context.Context, accountID uuid.UUID, periodStart time.Time) error
}

// MaxResult describes one atomic monotonic initialization attempt. Previous is
// meaningful only when FoundBefore is true. Advanced is true only for the
// transaction that actually raised (or first created) the key.
type MaxResult struct {
	PreviousMicros int64
	StoredMicros   int64
	FoundBefore    bool
	Advanced       bool
}

// UsageEvent is the narrow payload usage.Service sends to its best-effort
// credit hook after a fresh usage insert. Normal module usage carries an
// ingest-priced ApproximateChargeMicros; platform infra sets
// ForceLiveProjection because its authoritative model/catalog price is only
// available through the account bill.
type UsageEvent struct {
	AccountID               uuid.UUID
	AppID                   uuid.UUID
	EventID                 string
	ApproximateChargeMicros int64
	PeriodStart             time.Time
	PeriodEnd               time.Time
	// ForceLiveProjection is set for freshly inserted usage whose exact price
	// is not available at ingest (for example platform infra/model pricing).
	// The coordinator must bypass a warm-low fast allow and reconcile from the
	// authoritative account bill instead of inventing an approximate delta.
	ForceLiveProjection bool
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

// NewCounter is the production construction seam. Its interface return is a
// true nil on configuration failure, avoiding the typed-nil *RedisCounter that
// would otherwise make `counter != nil` true inside Coordinator.
func NewCounter(rawURL string) (Counter, error) {
	counter, err := NewRedisCounter(rawURL)
	if err != nil {
		return nil, err
	}
	return counter, nil
}

var addIfPresentScript = redis.NewScript(`
local previous = redis.call("GET", KEYS[1])
if not previous then return {} end
redis.call("INCRBY", KEYS[1], ARGV[1])
redis.call("PEXPIRE", KEYS[1], ARGV[2])
return {previous, redis.call("GET", KEYS[1])}
`)

func (c *RedisCounter) AddIfPresent(ctx context.Context, accountID uuid.UUID, periodStart time.Time, deltaMicros int64) (int64, int64, bool, error) {
	raw, err := addIfPresentScript.Run(ctx, c.client, []string{estimateKey(accountID, periodStart)}, deltaMicros, c.ttl.Milliseconds()).Slice()
	if err != nil {
		return 0, 0, false, err
	}
	if len(raw) == 0 {
		return 0, 0, false, nil
	}
	if len(raw) != 2 {
		return 0, 0, false, fmt.Errorf("credit counter add returned %d values", len(raw))
	}
	previous, err := redisInt64(raw[0])
	if err != nil {
		return 0, 0, false, fmt.Errorf("parse previous credit estimate: %w", err)
	}
	estimate, err := redisInt64(raw[1])
	if err != nil {
		return 0, 0, false, fmt.Errorf("parse updated credit estimate: %w", err)
	}
	return previous, estimate, true, nil
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

var setIfEqualScript = redis.NewScript(`
local current = redis.call("GET", KEYS[1])
if (not current) or current ~= ARGV[1] then return 0 end
redis.call("SET", KEYS[1], ARGV[2], "PX", ARGV[3])
return 1
`)

func (c *RedisCounter) SetIfEqual(ctx context.Context, accountID uuid.UUID, periodStart time.Time, expectedMicros, estimateMicros int64) (bool, error) {
	applied, err := setIfEqualScript.Run(
		ctx,
		c.client,
		[]string{estimateKey(accountID, periodStart)},
		expectedMicros,
		estimateMicros,
		c.ttl.Milliseconds(),
	).Int64()
	return applied == 1, err
}

func (c *RedisCounter) SetMax(ctx context.Context, accountID uuid.UUID, periodStart time.Time, estimateMicros int64) (MaxResult, error) {
	key := estimateKey(accountID, periodStart)
	for {
		var result MaxResult
		err := c.client.Watch(ctx, func(tx *redis.Tx) error {
			result = MaxResult{}
			current, err := tx.Get(ctx, key).Int64()
			switch {
			case errors.Is(err, redis.Nil):
				// Missing key: this transaction is the cold initializer if its
				// SET wins the WATCH below.
			case err != nil:
				return err
			default:
				result.FoundBefore = true
				result.PreviousMicros = current
			}

			if result.FoundBefore && current >= estimateMicros {
				result.StoredMicros = current
				_, err = tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
					pipe.PExpire(ctx, key, c.ttl)
					return nil
				})
				return err
			}

			result.StoredMicros = estimateMicros
			result.Advanced = true
			_, err = tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
				pipe.Set(ctx, key, estimateMicros, c.ttl)
				return nil
			})
			return err
		}, key)
		if errors.Is(err, redis.TxFailedErr) {
			continue
		}
		return result, err
	}
}

func redisInt64(value any) (int64, error) {
	switch value := value.(type) {
	case string:
		return strconv.ParseInt(value, 10, 64)
	case []byte:
		return strconv.ParseInt(string(value), 10, 64)
	default:
		return 0, fmt.Errorf("unexpected Redis value type %T", value)
	}
}

func estimateKey(accountID uuid.UUID, periodStart time.Time) string {
	return fmt.Sprintf("billing:credit-estimate:%s:%d", accountID, periodStart.UTC().Unix())
}

func (c *RedisCounter) ClaimBlockNotification(ctx context.Context, accountID uuid.UUID, periodStart time.Time) (bool, error) {
	return c.client.SetNX(ctx, blockNotificationKey(accountID, periodStart), "1", c.ttl).Result()
}

func (c *RedisCounter) ClearBlockNotification(ctx context.Context, accountID uuid.UUID, periodStart time.Time) error {
	return c.client.Del(ctx, blockNotificationKey(accountID, periodStart)).Err()
}

func (c *RedisCounter) ClaimBoundaryNotification(ctx context.Context, accountID uuid.UUID, periodStart time.Time) (bool, error) {
	return c.client.SetNX(ctx, boundaryNotificationKey(accountID, periodStart), "1", c.ttl).Result()
}

func (c *RedisCounter) ClearBoundaryNotification(ctx context.Context, accountID uuid.UUID, periodStart time.Time) error {
	return c.client.Del(ctx, boundaryNotificationKey(accountID, periodStart)).Err()
}

func blockNotificationKey(accountID uuid.UUID, periodStart time.Time) string {
	return fmt.Sprintf("billing:credit-block-notified:%s:%d", accountID, periodStart.UTC().Unix())
}

func boundaryNotificationKey(accountID uuid.UUID, periodStart time.Time) string {
	return fmt.Sprintf("billing:credit-boundary-notified:%s:%d", accountID, periodStart.UTC().Unix())
}
