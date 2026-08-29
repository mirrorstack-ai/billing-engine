// Package testutil provides shared test infrastructure for
// billing-engine's integration tests. Boots an ephemeral Postgres
// container via testcontainers-go, applies all billing migrations,
// returns a pool. Cleanup is registered on t.Cleanup; no leaked
// containers when tests finish.
//
// Tests using this package are gated by the `integration` build tag
// (//go:build integration) and run via `make test-integration`. They
// require Docker to be available locally or on the CI runner.
package testutil

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

// NewTestDB boots an ephemeral Postgres 17 container, applies all
// migrations under migrations/billing/, and returns a connected pool.
//
// The container is terminated on t.Cleanup; the pool is closed on
// t.Cleanup. Tests can run in parallel — each call gets its own
// container with a unique random port.
//
// Skips with t.Skipf if Docker isn't reachable (no Docker daemon, or
// permission denied). This makes the test suite tolerant of CI
// environments that gate Docker access — but a skipped integration
// package still reports "ok", so set REQUIRE_DOCKER=1 to turn an
// unreachable daemon into a failure wherever the green is load-bearing.
func NewTestDB(t *testing.T) *pgxpool.Pool {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	container, err := tcpostgres.Run(ctx,
		"postgres:17-alpine",
		tcpostgres.WithDatabase("mirrorstack"),
		tcpostgres.WithUsername("mirrorstack"),
		tcpostgres.WithPassword("mirrorstack"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(60*time.Second),
		),
	)
	if err != nil {
		switch dockerDisposition(err, os.Getenv("REQUIRE_DOCKER")) {
		case dispositionSkip:
			t.Skipf("docker not available: %v", err)
		case dispositionRequired:
			t.Fatalf("REQUIRE_DOCKER is set but docker is not available: %v", err)
		default:
			t.Fatalf("start postgres container: %v", err)
		}
	}
	t.Cleanup(func() {
		// Terminate uses a fresh context — t.Cleanup runs after the
		// test's deadline, so the outer ctx is already canceled.
		termCtx, termCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer termCancel()
		_ = container.Terminate(termCtx)
	})

	dsn, err := container.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("get connection string: %v", err)
	}

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("pgxpool init: %v", err)
	}
	t.Cleanup(pool.Close)

	if err := applyMigrations(ctx, pool); err != nil {
		t.Fatalf("apply migrations: %v", err)
	}
	return pool
}

// applyMigrations finds every migrations/billing/*.up.sql file relative
// to the project root and applies it to the pool in lexical order.
func applyMigrations(ctx context.Context, pool *pgxpool.Pool) error {
	root, err := projectRoot()
	if err != nil {
		return err
	}
	dir := filepath.Join(root, "migrations", "billing")
	entries, err := os.ReadDir(dir)
	if err != nil {
		return err
	}
	var ups []string
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".up.sql") {
			continue
		}
		ups = append(ups, filepath.Join(dir, e.Name()))
	}
	sort.Strings(ups)
	for _, path := range ups {
		body, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		if _, err := pool.Exec(ctx, string(body)); err != nil {
			return errors.New("apply " + path + ": " + err.Error())
		}
	}
	return nil
}

// projectRoot walks up from the current working directory looking for
// a go.mod file. Integration tests run with cwd = the test package
// directory; walking up resolves the repo root regardless of which
// package's tests are running.
func projectRoot() (string, error) {
	cwd, err := os.Getwd()
	if err != nil {
		return "", err
	}
	dir := cwd
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir, nil
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return "", errors.New("project root (go.mod) not found above " + cwd)
		}
		dir = parent
	}
}

// disposition names what NewTestDB does with a container-start error.
type disposition int

const (
	// dispositionFail: the error is not about Docker being absent, so it
	// is a real failure however the run was configured.
	dispositionFail disposition = iota
	// dispositionSkip: Docker is unreachable and the run tolerates it.
	dispositionSkip
	// dispositionRequired: Docker is unreachable but the run asserted it
	// would be present.
	dispositionRequired
)

// dockerDisposition decides how an ephemeral-Postgres start failure is
// reported. It is split out from NewTestDB, and takes REQUIRE_DOCKER's
// value rather than reading the environment, so the decision itself is
// testable without an unreachable daemon.
//
// The rule it encodes: a skipped integration package still prints "ok",
// so a green from a run that asserted Docker (REQUIRE_DOCKER=1, set by
// `make test-integration`) must never be produced by a package that
// started no container.
func dockerDisposition(err error, requireDocker string) disposition {
	if !isDockerUnavailable(err) {
		return dispositionFail
	}
	if requireDocker != "" {
		return dispositionRequired
	}
	return dispositionSkip
}

// isDockerUnavailable returns true when err looks like a Docker-not-
// reachable error. Used to skip tests on CI runners without Docker
// rather than fail noisily.
func isDockerUnavailable(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	switch {
	case strings.Contains(msg, "Cannot connect to the Docker daemon"):
		return true
	case strings.Contains(msg, "docker: not found"):
		return true
	case strings.Contains(msg, "permission denied") && strings.Contains(msg, "docker.sock"):
		return true
	}
	return false
}
