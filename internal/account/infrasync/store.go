package infrasync

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// PgStore keeps the watermark in ms_billing.infra_sync_watermarks (migration
// 098). billing_svc holds SELECT, INSERT and UPDATE on it, not DELETE.
type PgStore struct {
	pool *pgxpool.Pool
}

func NewPgStore(pool *pgxpool.Pool) *PgStore {
	return &PgStore{pool: pool}
}

func (s *PgStore) Watermark(ctx context.Context, source string) (time.Time, error) {
	var through time.Time
	err := s.pool.QueryRow(ctx,
		`SELECT confirmed_through FROM ms_billing.infra_sync_watermarks WHERE source = $1`, source,
	).Scan(&through)
	if errors.Is(err, pgx.ErrNoRows) {
		return time.Time{}, nil
	}
	if err != nil {
		return time.Time{}, err
	}
	return through.UTC(), nil
}

// Advance upserts through with GREATEST, so two overlapping runs (a scheduler
// retry beside the next hour's run) can never move the watermark backwards.
func (s *PgStore) Advance(ctx context.Context, source string, through time.Time) error {
	_, err := s.pool.Exec(ctx,
		`INSERT INTO ms_billing.infra_sync_watermarks (source, confirmed_through)
		 VALUES ($1, $2)
		 ON CONFLICT (source) DO UPDATE
		 SET confirmed_through = GREATEST(ms_billing.infra_sync_watermarks.confirmed_through, EXCLUDED.confirmed_through),
		     updated_at = now()`,
		source, through.UTC(),
	)
	return err
}

// MemoryStore is an in-process Store for the pullers' unit tests.
type MemoryStore struct {
	mu   sync.Mutex
	rows map[string]time.Time
	// ReadErr / WriteErr, when set, are returned by the next calls.
	ReadErr  error
	WriteErr error
}

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{rows: map[string]time.Time{}}
}

func (m *MemoryStore) Watermark(_ context.Context, source string) (time.Time, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.ReadErr != nil {
		return time.Time{}, m.ReadErr
	}
	return m.rows[source], nil
}

func (m *MemoryStore) Advance(_ context.Context, source string, through time.Time) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.WriteErr != nil {
		return m.WriteErr
	}
	if through.After(m.rows[source]) {
		m.rows[source] = through.UTC()
	}
	return nil
}

// Set seeds a watermark directly.
func (m *MemoryStore) Set(source string, through time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.rows[source] = through.UTC()
}
