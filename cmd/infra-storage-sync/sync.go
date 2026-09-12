package main

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/google/uuid"

	"github.com/mirrorstack-ai/billing-engine/internal/account/usage"
)

// objectLister is the narrow S3 surface this sampler needs — one paginated
// ListObjectsV2. Narrow so the unit tests can drive the whole sweep with a fake
// and never reach AWS; *s3.Client satisfies it.
type objectLister interface {
	ListObjectsV2(ctx context.Context, in *s3.ListObjectsV2Input, opts ...func(*s3.Options)) (*s3.ListObjectsV2Output, error)
}

// syncResult tallies one sweep, for logging and the exit code.
type syncResult struct {
	Samples   int // closed hour instants sampled
	Prefixes  int // distinct (app, module) prefixes found
	Recorded  int // events newly inserted
	Deduped   int // events that hit ON CONFLICT (already recorded)
	Skipped   int // keys skipped: not a module prefix, or unparseable ids
	RowErrors int // per-row RecordInfraUsage errors (logged, non-fatal)
	Failed    bool
	Err       error
}

// prefixKey identifies one module's storage root.
type prefixKey struct {
	app    uuid.UUID
	module uuid.UUID
}

// closedHours returns the lookbackHours closed hour boundaries ending at the
// top of the hour containing `at`, oldest first. The current partial hour is
// never sampled: an instant inside it would be re-derived differently by a
// later run and the two would disagree about the same period.
func closedHours(at time.Time, n int) []time.Time {
	top := at.UTC().Truncate(time.Hour)
	out := make([]time.Time, 0, n)
	for i := n; i >= 1; i-- {
		out = append(out, top.Add(-time.Duration(i)*time.Hour))
	}
	return out
}

// parseModulePrefix pulls (app, module) out of "apps/<app_id>/<module_id>/...".
//
// 🔴 IT MUST REJECT, NOT REPAIR. A key that does not match this exact shape is
// not a module's runtime object — it is platform-owned (deploy artifacts,
// avatars) or malformed — and guessing an owner for it would bill a customer
// for the platform's own bytes. Both ids must parse as UUIDs, which is the same
// gate storagecreds.Canonicalize applies when minting the prefix.
func parseModulePrefix(key string) (prefixKey, bool) {
	rest, ok := strings.CutPrefix(key, modulePrefixRoot)
	if !ok {
		return prefixKey{}, false
	}
	parts := strings.SplitN(rest, "/", 3)
	// 3 parts means "<app>/<module>/<something>" — an object, not a bare prefix.
	if len(parts) < 3 || parts[0] == "" || parts[1] == "" || parts[2] == "" {
		return prefixKey{}, false
	}
	app, err := uuid.Parse(parts[0])
	if err != nil {
		return prefixKey{}, false
	}
	mod, err := uuid.Parse(parts[1])
	if err != nil {
		return prefixKey{}, false
	}
	return prefixKey{app: app, module: mod}, true
}

// prefixSizes sums live object bytes per (app, module) under modulePrefixRoot.
//
// This is the ONLY part that changes when the bucket outgrows a full listing:
// an S3 Inventory manifest produces the same map from a daily Parquet file, and
// nothing downstream of here can tell the difference.
func prefixSizes(ctx context.Context, lister objectLister, bucket string) (map[prefixKey]int64, int, error) {
	sizes := map[prefixKey]int64{}
	skipped := 0
	var token *string
	for {
		out, err := lister.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
			Bucket:            &bucket,
			Prefix:            ptr(modulePrefixRoot),
			ContinuationToken: token,
		})
		if err != nil {
			return nil, skipped, fmt.Errorf("list %s/%s: %w", bucket, modulePrefixRoot, err)
		}
		for _, obj := range out.Contents {
			if obj.Key == nil {
				skipped++
				continue
			}
			pk, ok := parseModulePrefix(*obj.Key)
			if !ok {
				skipped++
				continue
			}
			if obj.Size != nil {
				sizes[pk] += *obj.Size
			}
		}
		if out.IsTruncated == nil || !*out.IsTruncated {
			return sizes, skipped, nil
		}
		token = out.NextContinuationToken
	}
}

// storageEventID mints the deterministic id for one (module, instant) sample —
// a UUIDv5 over the same tuple shape infra-egress-sync uses, so a re-run of an
// already-sampled instant dedupes downstream instead of double-counting.
func storageEventID(app, module uuid.UUID, at time.Time) string {
	name := storageMetric + "/" + app.String() + "/" + module.String() + "/" +
		at.UTC().Format(time.RFC3339Nano)
	return uuid.NewSHA1(uuid.NameSpaceURL, []byte(name)).String()
}

// syncStorage samples the bucket once and records that level at every closed
// hour instant in the lookback.
//
// 🔴 ONE LISTING, MANY INSTANTS — AND THAT IS THE APPROXIMATION, STATED. S3
// cannot answer "how big was this prefix three hours ago", so a catch-up run
// stamps the CURRENT level at older instants. The error is bounded by how much a
// prefix can change within lookbackHours, and it only arises when a run was
// missed: in steady state the newest instant is the only one not already
// recorded, and every older one dedupes on its event_id. That is also why a
// repeat sample must never overwrite — the first recording of an instant is the
// closest measurement to it that will ever exist.
func syncStorage(ctx context.Context, svc *usage.Service, lister objectLister, bucket string, at time.Time) syncResult {
	res := syncResult{}

	sizes, skipped, err := prefixSizes(ctx, lister, bucket)
	res.Skipped = skipped
	if err != nil {
		res.Failed, res.Err = true, err
		slog.ErrorContext(ctx, "infra-storage-sync: listing failed", "bucket", bucket, "error", err)
		return res
	}
	res.Prefixes = len(sizes)

	for _, instant := range closedHours(at, lookbackHours) {
		res.Samples++
		for pk, bytes := range sizes {
			// The LEVEL in GiB. The rollup integrates it over time (rollup.sql
			// :323); emitting GiB-hours here would integrate twice.
			gib := float64(bytes) / float64(bytesPerGiB)

			// No owner, exactly as infra-egress-sync: an infra sample carries no
			// principal and records as a lazy NULL-account event, backfilled on
			// conversion (design §8). The omitted OwnerUserID/OwnerOrgID is
			// deliberate, not missing.
			resp, rerr := svc.RecordInfraUsage(ctx, usage.RecordInfraUsageRequest{
				EventID:    storageEventID(pk.app, pk.module, instant),
				AppID:      pk.app,
				ModuleID:   pk.module,
				Metric:     storageMetric,
				Value:      gib,
				RecordedAt: instant,
			})
			if rerr != nil {
				res.RowErrors++
				slog.ErrorContext(ctx, "record infra storage failed",
					"app_id", pk.app, "module_id", pk.module, "metric", storageMetric,
					"instant", instant, "gib", gib, "error", rerr)
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

func ptr[T any](v T) *T { return &v }
