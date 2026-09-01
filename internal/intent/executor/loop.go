package executor

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
)

// Work is where an executor finds intents to consider.
//
// Narrowed to one read. This package must not be able to discover work by any
// route that could also change something.
type Work interface {
	PendingExecution(ctx context.Context, limit int) ([]string, error)
}

// RunResult is what one pass did, in the terms a reader needs to tell a
// working deployment from an idle one.
//
// 🔴 The counts are separate on purpose. "Considered 40, settled 0, refused
// 40" and "considered 0" are completely different states — the first is an
// executor working correctly against gates whose evidence does not exist yet,
// the second is one with no work. cmd/intent-executor's own header names that
// confusion as the reason it refuses to start quietly, and a summary that
// collapsed them would reintroduce it one level up.
type RunResult struct {
	Considered int
	Settled    int
	InProgress int
	Unresolved int
	Refused    int
	Errors     int
	// RefusedClauses counts, per clause, how many intents it turned down.
	// A deployment that refuses everything should be able to say WHICH gate
	// is doing it without anyone reading a log line per intent.
	RefusedClauses map[string]int
}

// ErrNoWorkSource is returned when a loop is built without one.
var ErrNoWorkSource = errors.New("executor: refusing to run with no work source")

// RunOnce considers one batch of intents and returns what happened.
//
// It is a single pass, not a daemon. This binary runs as a Lambda: an
// invocation does a bounded amount of work and returns a summary its caller
// can act on. A long-lived poller would also hold the only mutation-capable
// provider credential in the deployment open indefinitely, which
// docs/VERIFICATION.md §5's write-port isolation exists to avoid.
//
// One intent's failure does not stop the batch. An intent that errors is
// counted and the pass continues: a single poisoned document must not be able
// to starve every intent behind it, and its refusal is recorded either way.
func RunOnce(ctx context.Context, work Work, exec *Executor, limit int, log *slog.Logger) (RunResult, error) {
	if work == nil || exec == nil {
		return RunResult{}, ErrNoWorkSource
	}
	if log == nil {
		log = slog.Default()
	}

	digests, err := work.PendingExecution(ctx, limit)
	if err != nil {
		return RunResult{}, fmt.Errorf("find pending intents: %w", err)
	}

	result := RunResult{Considered: len(digests), RefusedClauses: map[string]int{}}
	for _, digest := range digests {
		// A cancelled context stops the pass rather than racing the deadline
		// on the next provider call. What has settled has settled; the rest
		// is picked up by the next invocation because it was never claimed.
		if err := ctx.Err(); err != nil {
			log.Warn("execution pass stopped early", "reason", err.Error(),
				"considered", result.Considered, "settled", result.Settled)
			return result, nil
		}

		out, err := exec.Execute(ctx, digest)
		switch {
		case err != nil:
			result.Errors++
			log.Error("intent execution failed", "digest", digest, "error", err.Error())
		case !out.Permitted:
			result.Refused++
			for _, c := range out.Refused {
				result.RefusedClauses[string(c)]++
			}
		case out.Settled:
			result.Settled++
		case out.InProgress:
			result.InProgress++
		case out.Unresolved:
			result.Unresolved++
			log.Warn("the rail's answer did not establish whether money moved",
				"digest", digest)
		}
	}
	return result, nil
}
