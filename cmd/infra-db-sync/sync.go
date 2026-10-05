package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/mirrorstack-ai/billing-engine/internal/account/billing"
	"github.com/mirrorstack-ai/billing-engine/internal/account/usage"
)

// dbReader is the whole read surface of the sampler: two fixed read-only
// queries and nothing else (see pgreader.go). Narrow so the unit tests drive
// the sweep with a fake and never open a connection.
type dbReader interface {
	// TableSizes returns the total relation size (heap + indexes + TOAST) of
	// every ordinary table and materialized view whose schema looks like an app
	// schema, one row per table.
	TableSizes(ctx context.Context) ([]tableSize, error)
	// Installs returns the module installs of one app: which module owns which
	// table-name prefix inside that app's schema.
	Installs(ctx context.Context, app uuid.UUID) ([]install, error)
}

// recorder is the single usage.Service method the sampler calls.
type recorder interface {
	RecordInfraUsage(ctx context.Context, req usage.RecordInfraUsageRequest) (*usage.RecordInfraUsageResponse, error)
}

type tableSize struct {
	Schema string
	Table  string
	Bytes  int64
}

// install is one row of app_<id>.module_install, the two columns the sampler
// reads: the module and the "<username>_<slug>_" prefix its tables carry.
type install struct {
	Module uuid.UUID
	Prefix string
}

// syncResult tallies one sweep, for logging and the exit code.
type syncResult struct {
	Samples           int   // closed hour instants sampled
	Apps              int   // app schemas with at least one table
	AppErrors         int   // apps whose installs could not be read (skipped)
	Modules           int   // distinct (app, module) pairs sampled
	Recorded          int   // events newly inserted
	Deduped           int   // events that hit ON CONFLICT (already recorded)
	Skipped           int   // table rows whose schema is not an app schema
	RowErrors         int   // per-row RecordInfraUsage errors (logged, non-fatal)
	UnattributedBytes int64 // bytes in app schemas that no install owns; never billed
	Failed            bool
	Err               error
}

// appSchemaName composes app_<uuid with underscores>, matching the platform's
// ids.AppSchemaName and migration 004's 'app_' || replace(id::text, '-', '_').
func appSchemaName(app uuid.UUID) string {
	return "app_" + strings.ReplaceAll(app.String(), "-", "_")
}

// parseAppSchema is the inverse of appSchemaName, and strict: it accepts only
// the exact canonical form (lowercase hex, underscore-separated 8-4-4-4-12).
//
// 🔴 IT REJECTS, NOT REPAIRS. A schema that is not exactly this shape is not an
// app's database (ms_billing, org_*, mod_*, a typo), and guessing an owner for
// its bytes would bill a customer for the platform's own tables. The strictness
// also keeps the quoted identifier Installs builds injection-proof: it is
// regenerated from 16 bytes, never echoed from the catalog.
func parseAppSchema(name string) (uuid.UUID, bool) {
	rest, ok := strings.CutPrefix(name, "app_")
	if !ok || len(rest) != 36 {
		return uuid.Nil, false
	}
	for i := 0; i < len(rest); i++ {
		c := rest[i]
		switch i {
		case 8, 13, 18, 23:
			if c != '_' {
				return uuid.Nil, false
			}
		default:
			if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f') {
				return uuid.Nil, false
			}
		}
	}
	id, err := uuid.Parse(strings.ReplaceAll(rest, "_", "-"))
	if err != nil {
		return uuid.Nil, false
	}
	return id, true
}

// attribute splits one app's table sizes among its installed modules by table
// name prefix and returns bytes per module plus the bytes no module owns.
//
//   - Longest prefix wins ("acme_quiz_core_" over "acme_quiz_"), so a module
//     whose slug extends another's is not swallowed by it.
//   - Every installed module is present in the result, at 0 when it has no
//     tables. 0 is a level, not an absence: the rollup carries the last
//     observed level across a gap, so a module that emptied its tables must
//     say so or it is billed for the old size forever.
//   - A table is charged to exactly one module. Two installs sharing a prefix
//     (which composePrefix should make impossible) resolve to the smaller
//     module id and the other stays at 0, so a table is never charged twice.
//   - An empty prefix never matches: it would claim the platform's own tables.
//   - Platform tables (members, module_install, ...) and the leftovers of an
//     uninstalled module match nothing; their bytes are returned aside and are
//     the platform's cost, never a customer's.
func attribute(installs []install, tables []tableSize) (map[uuid.UUID]int64, int64) {
	owners := make([]install, 0, len(installs))
	for _, in := range installs {
		if in.Prefix != "" {
			owners = append(owners, in)
		}
	}
	sort.Slice(owners, func(i, j int) bool {
		if len(owners[i].Prefix) != len(owners[j].Prefix) {
			return len(owners[i].Prefix) > len(owners[j].Prefix)
		}
		return owners[i].Module.String() < owners[j].Module.String()
	})

	per := make(map[uuid.UUID]int64, len(installs))
	for _, in := range installs {
		per[in.Module] = 0
	}
	var unattributed int64
	for _, tb := range tables {
		matched := false
		for _, o := range owners {
			if strings.HasPrefix(tb.Table, o.Prefix) {
				per[o.Module] += tb.Bytes
				matched = true
				break
			}
		}
		if !matched {
			unattributed += tb.Bytes
		}
	}
	return per, unattributed
}

// closedHours returns the n closed hour boundaries ending at the top of the hour
// containing `at`, oldest first. The open hour is never sampled: an instant
// inside it would be re-derived differently by a later run.
func closedHours(at time.Time, n int) []time.Time {
	top := at.UTC().Truncate(time.Hour)
	out := make([]time.Time, 0, n)
	for i := n; i >= 1; i-- {
		out = append(out, top.Add(-time.Duration(i)*time.Hour))
	}
	return out
}

// dbEventID mints the deterministic id of one (app, module, instant) sample: a
// UUIDv5 over the same tuple shape the storage sampler uses, under this
// metric's own name so the two can never collide. A re-run of an
// already-sampled instant produces the same id and dedupes downstream.
func dbEventID(app, module uuid.UUID, at time.Time) string {
	name := dbMetric + "/" + app.String() + "/" + module.String() + "/" +
		at.UTC().Format(time.RFC3339Nano)
	return uuid.NewSHA1(uuid.NameSpaceURL, []byte(name)).String()
}

// syncDB measures every module's database size once and records that level at
// every closed hour instant in the lookback.
//
// 🔴 ONE READ, MANY INSTANTS, AND THAT IS THE APPROXIMATION, STATED. Postgres
// cannot answer "how big was this table three hours ago", so a catch-up run
// stamps the CURRENT level at older instants, bounded by how much a database
// can change within lookbackHours. In steady state only the newest instant is
// new and every older one dedupes on its event_id; a repeat sample never
// overwrites, because the first recording of an instant is the closest to it.
func syncDB(ctx context.Context, rec recorder, rd dbReader, at time.Time) syncResult {
	res := syncResult{}

	tables, err := rd.TableSizes(ctx)
	if err != nil {
		res.Failed, res.Err = true, fmt.Errorf("read table sizes: %w", err)
		slog.ErrorContext(ctx, "infra-db-sync: reading table sizes failed", "error", err)
		return res
	}

	byApp := map[uuid.UUID][]tableSize{}
	for _, tb := range tables {
		app, ok := parseAppSchema(tb.Schema)
		if !ok {
			res.Skipped++
			continue
		}
		byApp[app] = append(byApp[app], tb)
	}
	res.Apps = len(byApp)

	// Apps in a stable order so a partial failure is reproducible.
	apps := make([]uuid.UUID, 0, len(byApp))
	for a := range byApp {
		apps = append(apps, a)
	}
	sort.Slice(apps, func(i, j int) bool { return apps[i].String() < apps[j].String() })

	type sample struct {
		app, module uuid.UUID
		bytes       int64
	}
	var samples []sample
	for _, app := range apps {
		installs, ierr := rd.Installs(ctx, app)
		if ierr != nil {
			// Counted and logged by app id only: the error and the log carry no
			// table name or prefix (a prefix is a customer's handle).
			res.AppErrors++
			slog.ErrorContext(ctx, "infra-db-sync: reading module installs failed",
				"app_id", app, "error", ierr)
			continue
		}
		per, un := attribute(installs, byApp[app])
		res.UnattributedBytes += un
		for mod, b := range per {
			samples = append(samples, sample{app: app, module: mod, bytes: b})
		}
	}
	sort.Slice(samples, func(i, j int) bool {
		if samples[i].app != samples[j].app {
			return samples[i].app.String() < samples[j].app.String()
		}
		return samples[i].module.String() < samples[j].module.String()
	})
	res.Modules = len(samples)

	// Every app unreadable means the role has no grant at all. A green run that
	// records nothing is the failure this metric must never hide.
	if res.Apps > 0 && res.AppErrors == res.Apps {
		res.Failed = true
		res.Err = errors.New("no app schema's module installs could be read: the read-only role is missing its grant")
		return res
	}

	instants := closedHours(at, lookbackHours)
	newest := instants[len(instants)-1]
	for _, instant := range instants {
		res.Samples++
		for _, s := range samples {
			// The LEVEL in GiB. The rollup integrates it over time; emitting
			// GiB-hours here would integrate twice.
			level := float64(s.bytes) / float64(bytesPerGiB)

			// No owner, exactly as infra-storage-sync: an infra sample carries no
			// principal and records as a lazy NULL-account event.
			resp, rerr := rec.RecordInfraUsage(ctx, usage.RecordInfraUsageRequest{
				EventID:    dbEventID(s.app, s.module, instant),
				AppID:      s.app,
				ModuleID:   s.module,
				Metric:     dbMetric,
				Value:      level,
				RecordedAt: instant,
			})
			if rerr != nil {
				if isStaleReplayConflict(rerr, instant, newest) {
					res.Deduped++
					continue
				}
				res.RowErrors++
				slog.ErrorContext(ctx, "record infra db size failed",
					"app_id", s.app, "module_id", s.module, "metric", dbMetric,
					"instant", instant, "gib", level, "error", rerr)
				continue
			}
			if resp.Recorded {
				res.Recorded++
			} else {
				res.Deduped++
			}
		}
	}
	return res
}

// isStaleReplayConflict tells a CONFLICT on a re-emitted OLDER instant apart from
// every other record error: the level changed between runs, the older instant is
// re-emitted under its deterministic event_id with a new payload, and
// RecordInfraUsage answers CONFLICT instead of the DO NOTHING dedupe. The first
// sample stands; only the newest instant is ever this run's to record, so a
// conflict there is real. Same rule as infra-storage-sync (09-15, twkpa-edu).
func isStaleReplayConflict(err error, instant, newest time.Time) bool {
	var be *billing.Error
	if !errors.As(err, &be) || be.Code != billing.CodeConflict {
		return false
	}
	return instant.Before(newest)
}
