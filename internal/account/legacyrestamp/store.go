package legacyrestamp

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/mirrorstack-ai/billing-engine/internal/account/db"
)

type pgxSource struct {
	pool *pgxpool.Pool
}

func NewSource(pool *pgxpool.Pool) Source {
	if pool == nil {
		panic("legacyrestamp.NewSource: pool must not be nil")
	}
	return &pgxSource{pool: pool}
}

func (s *pgxSource) TryBegin(ctx context.Context) (Snapshot, bool, error) {
	conn, err := s.pool.Acquire(ctx)
	if err != nil {
		return nil, false, err
	}
	queries := db.New(conn)
	acquired, err := queries.TryAcquireLegacyRestampMutex(ctx)
	if err != nil {
		return nil, false, errors.Join(err, discardConn(conn))
	}
	if !acquired {
		conn.Release()
		return nil, false, nil
	}

	tx, err := conn.BeginTx(ctx, pgx.TxOptions{
		IsoLevel:   pgx.RepeatableRead,
		AccessMode: pgx.ReadOnly,
	})
	if err != nil {
		releaseErr := releaseMutex(conn)
		return nil, false, errors.Join(err, releaseErr)
	}
	return &pgxSnapshot{
		conn: conn,
		tx:   tx,
		q:    db.New(tx),
	}, true, nil
}

type pgxSnapshot struct {
	conn   *pgxpool.Conn
	tx     pgx.Tx
	q      *db.Queries
	closed bool
}

func (s *pgxSnapshot) CountOwners(ctx context.Context) (int64, error) {
	return s.q.CountAccountOwnersForLegacyRestamp(ctx)
}

func (s *pgxSnapshot) ListOwners(
	ctx context.Context,
	after uuid.UUID,
	limit int32,
) ([]Owner, error) {
	rows, err := s.q.ListAccountOwnersForLegacyRestamp(
		ctx,
		db.ListAccountOwnersForLegacyRestampParams{
			ID:    after.String(),
			Limit: limit,
		},
	)
	if err != nil {
		return nil, err
	}
	owners := make([]Owner, 0, len(rows))
	for _, row := range rows {
		accountID, err := uuid.Parse(row.ID)
		if err != nil || accountID == uuid.Nil {
			return nil, errors.New("legacy restamp source returned an invalid account id")
		}
		owner := Owner{AccountID: accountID}
		if row.OwnerUserID.Valid {
			owner.UserID = uuid.UUID(row.OwnerUserID.Bytes)
		}
		if row.OwnerOrgID.Valid {
			owner.OrgID = uuid.UUID(row.OwnerOrgID.Bytes)
		}
		owners = append(owners, owner)
	}
	return owners, nil
}

func (s *pgxSnapshot) Close() error {
	if s.closed {
		return nil
	}
	s.closed = true
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	rollbackErr := s.tx.Rollback(ctx)
	if errors.Is(rollbackErr, pgx.ErrTxClosed) {
		rollbackErr = nil
	}
	unlocked, unlockErr := db.New(s.conn).ReleaseLegacyRestampMutex(ctx)
	if unlockErr == nil && !unlocked {
		unlockErr = errors.New("legacy restamp advisory mutex was not held")
	}
	if rollbackErr != nil || unlockErr != nil {
		closeErr := discardConn(s.conn)
		return errors.Join(rollbackErr, unlockErr, closeErr)
	}
	s.conn.Release()
	return nil
}

func releaseMutex(conn *pgxpool.Conn) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	unlocked, err := db.New(conn).ReleaseLegacyRestampMutex(ctx)
	if err != nil {
		closeErr := discardConn(conn)
		return errors.Join(err, closeErr)
	}
	if !unlocked {
		unlockErr := errors.New("legacy restamp advisory mutex was not held")
		closeErr := discardConn(conn)
		return errors.Join(unlockErr, closeErr)
	}
	conn.Release()
	return nil
}

func discardConn(conn *pgxpool.Conn) error {
	raw := conn.Hijack()
	closeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return raw.Close(closeCtx)
}
