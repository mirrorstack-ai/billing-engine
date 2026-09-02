package legacyrestamp

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
)

const (
	DefaultPageSize    = 256
	DefaultConcurrency = 32
	// The billing-cycle Lambda timeout is 60 seconds. With a hard five-second
	// owner deadline and 32 workers, one 256-owner page requires at most eight
	// waves (40 seconds), leaving 20 seconds for cold start and page reads.
	ownerTimeout = 5 * time.Second
)

var ErrAlreadyRunning = errors.New("legacy standing restamp already running")

// Owner is projected only from legacy ms_billing.accounts columns. AccountID
// is the immutable keyset cursor; exactly one owner principal must be set.
type Owner struct {
	AccountID uuid.UUID
	UserID    uuid.UUID
	OrgID     uuid.UUID
}

// Snapshot is one bounded repeatable-read account page held by the same
// database session that owns its page-level advisory mutex. The protected
// workflow serializes pages into a complete pass.
type Snapshot interface {
	CountOwners(context.Context) (int64, error)
	ListOwners(context.Context, uuid.UUID, int32) ([]Owner, error)
	Close() error
}

type Source interface {
	TryBegin(context.Context) (Snapshot, bool, error)
}

type Notifier interface {
	Enabled() bool
	NotifyOwner(context.Context, uuid.UUID, uuid.UUID) (bool, error)
}

type Result struct {
	Pages       int   `json:"pages"`
	Scanned     int   `json:"scanned"`
	Delivered   int   `json:"delivered"`
	Blocked     int   `json:"blocked"`
	Failed      int   `json:"failed"`
	TotalOwners int64 `json:"total_owners"`
	// NextCursor is the exclusive account-id cursor for the next protected
	// invocation. It advances only after the entire current page succeeds.
	NextCursor string `json:"next_cursor"`
	Complete   bool   `json:"complete"`
}

type Runner struct {
	source      Source
	notifier    Notifier
	pageSize    int32
	concurrency int
}

func NewRunner(
	source Source,
	notifier Notifier,
	pageSize int,
	concurrency int,
) *Runner {
	if source == nil || notifier == nil {
		panic("legacyrestamp.NewRunner: source and notifier must not be nil")
	}
	if pageSize <= 0 || concurrency <= 0 {
		panic("legacyrestamp.NewRunner: page size and concurrency must be positive")
	}
	return &Runner{
		source:   source,
		notifier: notifier,
		// Rejected at <= 0 four lines above; the caller supplies a page size,
		// not a length.
		pageSize:    int32(pageSize), //nolint:gosec // operator-supplied page size, validated > 0
		concurrency: concurrency,
	}
}

// RunPage processes one bounded keyset page in a fresh repeatable-read
// snapshot. Every valid owner in the page is attempted even after another
// owner fails. Any invalid owner, status read, or POST failure returns the
// input cursor so a protected caller retries the same idempotent page; the
// cursor advances only after every owner succeeds.
func (r *Runner) RunPage(
	ctx context.Context,
	after uuid.UUID,
) (result Result, err error) {
	inputCursor := ""
	if after != uuid.Nil {
		inputCursor = after.String()
	}
	if !r.notifier.Enabled() {
		result.NextCursor = inputCursor
		return result, errors.New("legacy standing restamp notifier is disabled")
	}
	snapshot, acquired, err := r.source.TryBegin(ctx)
	if err != nil {
		result.NextCursor = inputCursor
		return result, fmt.Errorf("acquire legacy restamp snapshot: %w", err)
	}
	if !acquired {
		result.NextCursor = inputCursor
		return result, ErrAlreadyRunning
	}
	defer func() {
		if closeErr := snapshot.Close(); closeErr != nil {
			err = errors.Join(
				err,
				fmt.Errorf("close legacy restamp snapshot: %w", closeErr),
			)
			result.NextCursor = inputCursor
			result.Complete = false
		}
	}()

	totalOwners, countErr := snapshot.CountOwners(ctx)
	if countErr != nil {
		result.NextCursor = inputCursor
		return result, fmt.Errorf("count legacy account owners: %w", countErr)
	}
	result.TotalOwners = totalOwners

	var failures []error
	cursor := after
	owners, listErr := snapshot.ListOwners(ctx, cursor, r.pageSize)
	if listErr != nil {
		result.NextCursor = inputCursor
		return result, fmt.Errorf("list legacy account owners: %w", listErr)
	}
	if len(owners) == 0 {
		result.Complete = true
		return result, nil
	}
	result.Pages = 1
	result.Scanned = len(owners)

	previous := cursor
	validOwners := make([]Owner, 0, len(owners))
	for _, owner := range owners {
		if owner.AccountID == uuid.Nil ||
			owner.AccountID.String() <= previous.String() {
			result.NextCursor = inputCursor
			return result, errors.New(
				"legacy restamp source returned a non-advancing account id",
			)
		}
		previous = owner.AccountID
		cursor = owner.AccountID
		if (owner.UserID == uuid.Nil) == (owner.OrgID == uuid.Nil) {
			result.Failed++
			failures = append(failures, errors.New(
				"legacy account must have exactly one owner principal",
			))
			continue
		}
		validOwners = append(validOwners, owner)
	}

	var (
		pageWG       sync.WaitGroup
		pageMu       sync.Mutex
		pageFailures []error
	)
	slots := make(chan struct{}, r.concurrency)
	for _, owner := range validOwners {
		owner := owner
		pageWG.Add(1)
		slots <- struct{}{}
		go func() {
			defer pageWG.Done()
			defer func() { <-slots }()
			ownerCtx, cancel := context.WithTimeout(ctx, ownerTimeout)
			defer cancel()
			blocked, notifyErr := r.notifier.NotifyOwner(
				ownerCtx,
				owner.UserID,
				owner.OrgID,
			)
			pageMu.Lock()
			defer pageMu.Unlock()
			if notifyErr != nil {
				result.Failed++
				pageFailures = append(pageFailures, fmt.Errorf(
					"restamp owner notification failed: %w",
					notifyErr,
				))
				return
			}
			result.Delivered++
			if blocked {
				result.Blocked++
			}
		}()
	}
	pageWG.Wait()
	failures = append(failures, pageFailures...)
	if len(failures) > 0 {
		result.NextCursor = inputCursor
		return result, errors.Join(failures...)
	}
	if len(owners) < int(r.pageSize) {
		result.Complete = true
		return result, nil
	}
	result.NextCursor = cursor.String()
	return result, nil
}
