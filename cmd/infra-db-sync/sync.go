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
	// Installs returns the module installs of one app.
	Installs(ctx context.Context, app uuid.UUID) ([]install, error)
}

// levelHistory is the billing-side memory the sampler needs to say 0. The rollup
// carries the last observed level to the period end, so a module that vanished
// (uninstalled, or its app dropped) is billed its last level unless the sampler
// records an explicit 0 for it. The pairs that last stood at a positive level
// are in usage_events; nothing in the app database remembers a gone install.
type levelHistory interface {
	// LivePairs returns every (app, module) whose most recent infra.db.gib_hours
	// sample since the given instant is above 0.
	LivePairs(ctx context.Context, since time.Time) ([]pair, error)
}

// historyWindow bounds LivePairs: a billing period. A pair whose last positive
// sample is older was either zeroed by an earlier run or closed with its period.
const historyWindow = 35 * 24 * time.Hour

// unattributedWarnShare is the share of app-schema bytes no install owns above
// which the run logs an alarm. Platform tables (members, module_install, the
// install-time bookkeeping) are small next to module data; a majority is a
// naming or grant fault.
const unattributedWarnShare = 0.5

type pair struct{ App, Module uuid.UUID }

// recorder is the single usage.Service method the sampler calls.
type recorder interface {
	RecordInfraUsage(ctx context.Context, req usage.RecordInfraUsageRequest) (*usage.RecordInfraUsageResponse, error)
}

type tableSize struct {
	Schema string
	Table  string
	Bytes  int64
}

// install is one row of app_<id>.module_install, the one column the sampler
// reads. The table-name prefix is NOT read: module_install.prefix is a
// "<username>_<slug>_" display value, and a deployed module's tables are named
// from its id (see modulePrefix).
type install struct {
	Module uuid.UUID
}

// syncResult tallies one sweep, for logging and the exit code.
type syncResult struct {
	Samples           int   // closed hour instants sampled
	Apps              int   // app schemas with at least one table
	AppErrors         int   // apps whose installs could not be read (skipped)
	Installs          int   // module installs read across the readable apps
	Modules           int   // distinct (app, module) pairs sampled, zeroed ones included
	Zeroed            int   // pairs that disappeared since the last sample, recorded as 0
	Recorded          int   // events newly inserted
	Deduped           int   // events that hit ON CONFLICT (already recorded)
	Skipped           int   // table rows whose schema is not an app schema
	RowErrors         int   // per-row RecordInfraUsage errors (logged, non-fatal)
	AttributedBytes   int64 // bytes in app schemas that an install owns; billed
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

// modulePrefix is the physical table-name prefix of a deployed module's tables
// inside app_<id>: "m" + the 32 lowercase hex digits of the module uuid + "_".
// It is the platform's own derivation (api-platform modulePhysicalPrefix over
// ids.ModuleIDFromUUID; app-module-sdk ids.NormalizeModuleID and the db_guard
// moduleTableRe), fixed-length 34 characters, so two modules' prefixes can never
// be a prefix of one another.
func modulePrefix(module uuid.UUID) string {
	return "m" + strings.ReplaceAll(module.String(), "-", "") + "_"
}

// tableModule is the inverse of modulePrefix on a table name: the module whose
// physical prefix the name carries, or false. Strict like parseAppSchema:
// lowercase hex, and the underscore after it.
func tableModule(table string) (uuid.UUID, bool) {
	if len(table) < 34 || table[0] != 'm' || table[33] != '_' {
		return uuid.Nil, false
	}
	hex := table[1:33]
	for i := 0; i < len(hex); i++ {
		c := hex[i]
		if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f') {
			return uuid.Nil, false
		}
	}
	id, err := uuid.Parse(hex[:8] + "-" + hex[8:12] + "-" + hex[12:16] + "-" + hex[16:20] + "-" + hex[20:])
	if err != nil {
		return uuid.Nil, false
	}
	return id, true
}

// attribute splits one app's table sizes among its installed modules by the
// fixed physical prefix and returns bytes per module plus the bytes no installed
// module owns.
//
//   - 🔴 THE PREFIX IS DERIVED FROM THE MODULE ID, NOT READ FROM
//     module_install.prefix. That column is "<username>_<slug>_", documented in
//     api-platform as a display/API value only; the SDK names a deployed
//     module's tables m<32hex>_<table>. Matching the column attributed nothing
//     on a real schema. There is no "<username>_<slug>_" fallback: no deployed
//     schema names tables that way, and a table that did would be unattributed
//     and loud (syncDB fails when installs exist and nothing matches).
//   - Every installed module is present in the result, at 0 when it has no
//     tables. 0 is a level, not an absence: the rollup carries the last
//     observed level across a gap, so a module that emptied its tables must
//     say so or it is billed for the old size forever.
//   - A table is charged to exactly one module: the one whose id its name
//     carries.
//   - Platform tables (members, module_install, ...) and the leftovers of an
//     uninstalled module match no install; their bytes are returned aside and are
//     the platform's cost, never a customer's.
func attribute(installs []install, tables []tableSize) (map[uuid.UUID]int64, int64) {
	per := make(map[uuid.UUID]int64, len(installs))
	for _, in := range installs {
		per[in.Module] = 0
	}
	var unattributed int64
	for _, tb := range tables {
		if m, ok := tableModule(tb.Table); ok {
			if _, installed := per[m]; installed {
				per[m] += tb.Bytes
				continue
			}
		}
		unattributed += tb.Bytes
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
func syncDB(ctx context.Context, rec recorder, rd dbReader, hist levelHistory, at time.Time) syncResult {
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

	// 🔴 A run that measured nothing must not look green. An empty app-schema set
	// is indistinguishable from a wrong database, a wrong role or a regex that
	// matches nothing, and the zeroing below would then zero everybody.
	if res.Apps == 0 {
		res.Failed = true
		res.Err = errors.New("no app schema found: the read-only role sees no app_<id> tables")
		slog.ErrorContext(ctx, "infra-db-sync: no app schema found", "skipped", res.Skipped)
		return res
	}

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
	live := map[pair]bool{}
	unreadable := map[uuid.UUID]bool{}
	for _, app := range apps {
		installs, ierr := rd.Installs(ctx, app)
		if ierr != nil {
			// Counted and logged by app id only: the error and the log carry no
			// table name or prefix (a prefix is a customer's handle).
			res.AppErrors++
			unreadable[app] = true
			slog.ErrorContext(ctx, "infra-db-sync: reading module installs failed",
				"app_id", app, "error", ierr)
			continue
		}
		per, un := attribute(installs, byApp[app])
		res.Installs += len(per)
		res.UnattributedBytes += un
		for mod, b := range per {
			res.AttributedBytes += b
			samples = append(samples, sample{app: app, module: mod, bytes: b})
			live[pair{app, mod}] = true
		}
	}

	// Every app unreadable means the role has no grant at all. A green run that
	// records nothing is the failure this metric must never hide.
	if res.AppErrors == res.Apps {
		res.Failed = true
		res.Err = errors.New("no app schema's module installs could be read: the read-only role is missing its grant")
		return res
	}

	// Installs exist, tables exist, and not one byte matched: every module would
	// record an explicit 0 and the run would look green. That is a naming or grant
	// fault, never a fleet of empty databases.
	if res.Installs > 0 && res.AttributedBytes == 0 && res.UnattributedBytes > 0 {
		res.Failed = true
		res.Err = errors.New("no table matched any installed module's m<id>_ prefix while unattributed bytes exist: the table naming or the install read is wrong")
		slog.ErrorContext(ctx, "infra-db-sync: nothing attributed",
			"installs", res.Installs, "unattributed_bytes", res.UnattributedBytes)
		return res
	}
	if total := res.AttributedBytes + res.UnattributedBytes; total > 0 &&
		float64(res.UnattributedBytes)/float64(total) >= unattributedWarnShare {
		slog.WarnContext(ctx, "infra-db-sync: high unattributed share",
			"unattributed_bytes", res.UnattributedBytes, "attributed_bytes", res.AttributedBytes)
	}

	// ZERO WHAT VANISHED. A pair that last stood at a positive level and is now
	// absent (module uninstalled, app dropped) gets an explicit 0, or the rollup
	// bills its last level to the period end. An app whose installs could not be
	// read is skipped: that says nothing about its modules. A failed history read
	// still records every measured size, then fails the run: a vanished module may
	// be overcharged until it is read.
	prior, histErr := hist.LivePairs(ctx, at.Add(-historyWindow))
	if histErr != nil {
		slog.ErrorContext(ctx, "infra-db-sync: reading level history failed", "error", histErr)
	}
	for _, pr := range prior {
		if live[pr] || unreadable[pr.App] {
			continue
		}
		live[pr] = true
		res.Zeroed++
		samples = append(samples, sample{app: pr.App, module: pr.Module})
	}
	sort.Slice(samples, func(i, j int) bool {
		if samples[i].app != samples[j].app {
			return samples[i].app.String() < samples[j].app.String()
		}
		return samples[i].module.String() < samples[j].module.String()
	})
	res.Modules = len(samples)

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

	// A green run is only green if it did what it is for. Every row was attempted
	// above; the verdict comes after so one bad row never drops the others.
	switch {
	case res.Modules > 0 && res.Recorded+res.Deduped == 0:
		res.Failed, res.Err = true, errors.New("no usage event was recorded or deduped")
	case res.RowErrors > 0:
		res.Failed, res.Err = true, fmt.Errorf("%d usage events failed to record", res.RowErrors)
	case histErr != nil:
		res.Failed, res.Err = true, fmt.Errorf("read level history: %w", histErr)
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
