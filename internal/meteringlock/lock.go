// Package meteringlock defines the stable PostgreSQL advisory-lock namespace
// shared by observation ingestion and period rollup.
package meteringlock

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"math"
	"time"

	"github.com/google/uuid"
)

// AdvisorySQL turns a namespaced text identity into one transaction-scoped
// bigint advisory lock. hashtextextended is deterministic within PostgreSQL;
// the namespace prefixes keep event and period identities disjoint.
const AdvisorySQL = `SELECT pg_advisory_xact_lock(hashtextextended($1, 0))`

// SharedAdvisorySQL admits concurrent observation readers while remaining
// incompatible with rollup's exclusive period barrier.
const SharedAdvisorySQL = `SELECT pg_advisory_xact_lock_shared(hashtextextended($1, 0))`

// EventKey serializes all decisions for a globally unique usage event id.
func EventKey(eventID string) string { return "usage-event:" + eventID }

// PeriodKey serializes v2 admission with the rollup intake barrier. UTC RFC3339
// normalization ensures equivalent time zones select the identical lock.
func PeriodKey(accountID uuid.UUID, periodStart time.Time) string {
	return "usage-period:" + accountID.String() + ":" + periodStart.UTC().Format(time.RFC3339Nano)
}

// SubjectKey serializes the credit-delta read+insert only for one exact keyed
// bill-line subject. Length-prefixing before hashing prevents delimiter
// ambiguity in opaque subject/model/version strings.
func SubjectKey(accountID, appID, moduleID uuid.UUID, metric, model, moduleVersion, subject string, periodStart time.Time) string {
	hash := sha256.New()
	write := func(value string) {
		// 🔴 A length prefix that truncates defeats the injectivity it exists
		// to provide: two different field sequences could hash identically and
		// share a lock key. Unreachable — every field here is a UUID or a
		// bounded usage-event string — but the encoding CANNOT be widened to
		// remove the risk, because the prefix width is part of the hash and
		// changing it moves every existing key. So this fails loudly instead.
		if uint64(len(value)) > math.MaxUint32 {
			panic("meteringlock: subject field exceeds the 4 GiB length prefix")
		}
		var length [4]byte
		binary.BigEndian.PutUint32(length[:], uint32(len(value))) //nolint:gosec // guarded immediately above
		_, _ = hash.Write(length[:])
		_, _ = hash.Write([]byte(value))
	}
	write(accountID.String())
	write(appID.String())
	write(moduleID.String())
	write(metric)
	write(model)
	write(moduleVersion)
	write(subject)
	write(periodStart.UTC().Format(time.RFC3339Nano))
	return "usage-subject:" + hex.EncodeToString(hash.Sum(nil))
}
